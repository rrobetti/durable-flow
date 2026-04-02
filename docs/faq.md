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
