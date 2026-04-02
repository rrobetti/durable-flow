# Frequently Asked Questions

---

## What happens if the application crashes while a step is being processed?

If the application crashes mid-execution — after a step has been saved to the database but before it finishes — the step remains in **`RUNNING`** state. It is never moved to an error state by the crashed process because the crash prevented that transition.

The `RecoveryScheduler` handles this automatically through **lease-based recovery**:

1. When a step begins executing it is atomically set to `RUNNING` with an `owner` (node ID) and a `locked_until` timestamp (the lease expiry, controlled by `leaseTimeoutSeconds`, default 60 s).
2. After a crash the step stays in `RUNNING` state, but its lease will eventually expire.
3. On the next `RecoveryScheduler` cycle (controlled by `recoveryIntervalSeconds`, default 30 s) the scheduler runs `recoverExpiredLeases()`, which resets any `RUNNING` step whose `locked_until` is in the past back to **`FAILED_RETRYABLE`**.
4. In the same cycle `findEligibleSteps()` picks up the now-`FAILED_RETRYABLE` step and re-dispatches it for execution.

In the worst case the step is retried after at most `leaseTimeoutSeconds + recoveryIntervalSeconds` (≈ 90 s with defaults). No manual intervention is needed.

📊 See the [Node Failure Recovery flow diagram](node-failure-recovery.md) for a step-by-step visual of this process, including how context is stored and restored.

To tune the recovery window adjust these two settings:

```java
DurableFlowConfig config = DurableFlowConfig.builder()
    .leaseTimeoutSeconds(60)       // how long before a RUNNING step is considered stuck
    .recoveryIntervalSeconds(30)   // how often the recovery scheduler runs
    .build();
```

See [Configuration Reference](configuration.md) for all available options.

---

## How does a retried step reconstruct the context passed between steps?

No in-memory state is involved in recovery. Before executing any step — including a step being retried after a crash or a normal failure — the engine calls `WorkflowOrchestrator.buildContext()`, which re-loads everything it needs directly from the database:

| Data | Source |
|------|--------|
| Original message payload | `messages.payload_data` (BYTEA) |
| Output of each upstream step | `message_steps.result_data` (BYTEA) |

The retried step itself had not yet written its own result (it never completed successfully), so only the outputs of its upstream dependencies matter — and those were already persisted when those steps succeeded. The reconstructed `StepContext` therefore contains the same `payload` and `previousStepOutputs` map that the original execution would have seen.

📊 See the [Node Failure Recovery flow diagram](node-failure-recovery.md) for a detailed visual of how context flows through the database and is restored on a new node.

---

## Is the step lease renewed on each retry, or does a single lease span all attempts?

The lease is **renewed fresh for every attempt**. When the engine claims a step — whether for the first execution or any subsequent retry — it atomically sets a new `locked_until = NOW() + leaseTimeoutSeconds` and `owner = nodeId` alongside transitioning to `RUNNING`:

```sql
UPDATE message_steps
SET step_state    = 'RUNNING',
    owner         = ?,              -- current node ID
    locked_until  = ?,              -- NOW() + leaseTimeoutSeconds
    attempt_count = attempt_count + 1,
    ...
WHERE id = ?
  AND step_state IN ('PENDING', 'FAILED_RETRYABLE')
  AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP)
```

When a step fails (or crashes and its lease expires), `locked_until` and `owner` are both reset to `NULL` as part of the `FAILED_RETRYABLE` transition. The next attempt therefore starts clean with a full, fresh lease window — there is no shared or cumulative lease across attempts.

📊 See the [Node Failure Recovery flow diagram](node-failure-recovery.md) for a visual showing how the lease transitions across nodes.

---

## If a step fails in normal conditions, is it retried immediately or after a configurable delay?

After a configurable delay. When a step throws an exception the engine:

1. Consults the step's `RetryPolicy` to decide whether another attempt is allowed.
2. Computes `nextRetryAt = now + policy.nextDelay(attempt)` and persists it on the step record.
3. Marks the step **`FAILED_RETRYABLE`** (or **`FAILED`** permanently if retries are exhausted).

The `RecoveryScheduler` picks up `FAILED_RETRYABLE` steps on its next cycle, but only those whose `next_retry_at` timestamp is in the past. This means the **effective retry delay is the longer of the two**: the configured `RetryPolicy` delay and the time until the next scheduler tick (`recoveryIntervalSeconds`, default 30 s).

The default `RetryPolicy` is 3 attempts with a 1-second fixed delay. Because the scheduler runs every 30 s by default, in practice each retry fires roughly 30 s after the previous failure (the 1 s floor is invisible at that interval).

You can tune this per step when building the workflow:

```java
// Fixed delay – retry at most 5 times, waiting 10 seconds between attempts
WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
    .step("step-a", ctx -> { /* … */ },
          RetryPolicy.fixedDelay(5, Duration.ofSeconds(10)))
    .build();

// Exponential back-off – doubles the delay each attempt up to 5 minutes, with jitter
WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
    .step("step-b", ctx -> { /* … */ },
          RetryPolicy.exponentialBackoff(6, Duration.ofSeconds(5), 2.0, Duration.ofMinutes(5), true))
    .build();
```

To lower the time between retry attempts also reduce `recoveryIntervalSeconds`:

```java
DurableFlowConfig config = DurableFlowConfig.builder()
    .recoveryIntervalSeconds(5)   // check for retryable steps every 5 seconds
    .build();
```

See [Configuration Reference](configuration.md) for all available options.

📊 See the [Step Retry flow diagram](retry-flow.md) for a visual of the full retry lifecycle.

---

## When do steps run in parallel, and does SYNCHRONOUS vs ASYNCHRONOUS mode affect that?

**Steps run in parallel whenever they have no unfulfilled dependencies.** The engine queries the database for all steps whose `dependsOn` list is satisfied (all upstream steps are `SUCCEEDED`) and dispatches them together. Two steps that each declare no `dependsOn`, or two steps that each declare the same completed step as their only dependency, will be picked up and executed at the same time.

```java
WorkflowDefinition wf = WorkflowDefinition.builder("order-workflow")
    // ingest-order has no dependencies — eligible immediately
    .step("ingest-order",   ctx -> { /* … */ })

    // reserve-stock and send-confirmation both depend only on ingest-order
    // → they become eligible together and run in parallel
    .step("reserve-stock",  ctx -> { /* … */ })
        .dependsOn("ingest-order")
    .step("send-confirmation", ctx -> { /* … */ })
        .dependsOn("ingest-order")

    // finalise depends on BOTH parallel steps → waits for both to complete
    .step("finalise", ctx -> { /* … */ })
        .dependsOn("reserve-stock", "send-confirmation")
    .build();
```

**`SYNCHRONOUS` vs `ASYNCHRONOUS` controls when `engine.receive()` returns relative to step execution — not whether steps run in parallel:**

| Mode | Parallel steps within one message? | What `engine.receive()` returns |
|------|------------------------------------|---------------------------------|
| `ASYNCHRONOUS` (default) | **Yes** — all currently eligible steps are submitted to a virtual-thread pool at the same time and execute concurrently | `MessageState.RECEIVED` immediately (before steps finish) |
| `SYNCHRONOUS` | **Yes** — all currently eligible steps are dispatched to virtual threads and execute concurrently; the calling thread joins them all before advancing to the next wave | Final `MessageState` (e.g. `PROCESSED`) only after all steps finish |

In **both modes** the engine dispatches every currently eligible step for a message to a `Thread.ofVirtual()` per-task executor. "Currently eligible" means the step's `dependsOn` list is empty *or* every named dependency has already succeeded — the engine never dispatches a step that still has an unmet dependency. If `reserve-stock` and `send-confirmation` are both eligible (because `ingest-order` just succeeded), they are dispatched to two different virtual threads and genuinely overlap regardless of mode.

The key difference is **when `receive()` returns**:

- In **`ASYNCHRONOUS` mode** `receive()` returns immediately with `MessageState.RECEIVED` and the steps continue in the background.
- In **`SYNCHRONOUS` mode** `receive()` blocks, joining each wave of concurrent virtual threads, and only returns once all currently-executable steps have finished. Use this mode when you need the final `MessageState` before proceeding.

Configure the mode via `DurableFlowConfig`:

```java
DurableFlowConfig config = DurableFlowConfig.builder()
    .executionMode(ExecutionMode.SYNCHRONOUS)   // or ExecutionMode.ASYNCHRONOUS (default)
    .build();
```

---

## Can a step return any Java object? How is step output stored and passed to downstream steps?

A step must return a **`StepResult`** value. The output inside `StepResult` is always a **`byte[]`** — the engine stores and retrieves raw bytes with no automatic serialization. You are responsible for serializing your business object to bytes before creating the `StepResult`, and for deserializing it in any downstream step that reads the output.

```java
// Step A: serialize output to bytes
.step("step-a", ctx -> {
    MyResult result = /* … */;
    byte[] bytes = objectMapper.writeValueAsBytes(result);   // JSON, Protobuf, etc.
    return StepResult.of(bytes);
})

// Step B: deserialize bytes from upstream step
.step("step-b", ctx -> {
    byte[] bytes = ctx.getPreviousStepOutputs().get("step-a");
    MyResult upstream = objectMapper.readValue(bytes, MyResult.class);
    // … use upstream result
    return StepResult.empty();
})
.dependsOn("step-a")
```

**How output travels through the database:**

| Stage | What happens |
|-------|-------------|
| Step completes | `result_data = ?` (JDBC `setBytes`) written to `message_steps` |
| Dependent step starts | `buildContext()` calls `getStepOutputs()` — JDBC `getBytes` reads `result_data` for every dependency |
| Context exposed | `StepContext.getPreviousStepOutputs()` returns `Map<String, byte[]>` keyed by step name |

The engine never inspects or transforms the bytes. You can use any serialization format — Jackson JSON, Protobuf, Java's built-in serialization, or plain UTF-8 strings — as long as the producing and consuming steps agree on the format.

Step metadata (the `Map<String, String>` side-channel in `StepResult`) is stored separately as JSON and is also available via `StepContext.getMetadata()`, but it is limited to string key-value pairs.

If a step has no output to pass downstream, use `StepResult.empty()`.

See [Core Concepts](core-concepts.md) for a full description of `StepContext` and `StepResult`.
