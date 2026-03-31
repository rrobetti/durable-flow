# Core Concepts

---

## InboundMessage

```java
public record InboundMessage(String source, byte[] rawPayload, Map<String, String> headers) {}
```

| Field | Description |
|---|---|
| `source` | Logical origin identifier (e.g. queue name, topic, system name) |
| `rawPayload` | Raw bytes to process |
| `headers` | Transport-layer metadata |

---

## WorkflowDefinition

Describes an immutable DAG of steps, built with a fluent API:

```java
WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
    .step("step-a", ctx -> {
        // ... do work
        return StepResult.of(outputBytes);
    })
    .step("step-b", ctx -> {
        byte[] fromA = ctx.getPreviousStepOutputs().get("step-a");
        // ...
        return StepResult.empty();
    }).dependsOn("step-a")
    .retryPolicy(RetryPolicy.fixedDelay(5, Duration.ofSeconds(2)))
    .build();
```

Each step receives a `StepContext` containing:

| Field | Description |
|---|---|
| `messageId` | Stable message identifier |
| `stepName` | Name of the current step |
| `attemptCount` | 1-based attempt number |
| `payload` | Stored payload bytes (empty byte array for NO_PAYLOAD mode) |
| `metadata` | Map extracted by the preprocessor |
| `previousStepOutputs` | Map of stepName → output bytes from upstream steps |
| `nodeId` | Identifier of the processing node |

---

## Lifecycle Hooks

Register `beforeProcessing` and `afterProcessing` hooks on any `WorkflowDefinition`. They run at the very start and end of every workflow run, **regardless of success or failure**, and are invoked by the same thread that executes the steps (calling thread for `SYNCHRONOUS`, background thread for `ASYNCHRONOUS`).

```java
Logger log = LoggerFactory.getLogger(MyProcessor.class);

WorkflowDefinition wf = WorkflowDefinition.builder("order-pipeline")
    .beforeProcessing(ctx -> {
        log.info("Starting workflow for message {}", ctx.getMessageId());
        // ctx.getFinalState() is null here (workflow hasn't run yet)
    })
    .step("validate", ctx -> StepResult.empty())
    .step("enrich",   ctx -> StepResult.withOutput(enrich(ctx.getPayload())))
        .dependsOn("validate")
    .step("notify",   ctx -> { notify(ctx.getPreviousStepOutputs().get("enrich")); return StepResult.empty(); })
        .dependsOn("enrich")
    .afterProcessing(ctx -> {
        log.info("Workflow {} ended: state={}", ctx.getWorkflowName(), ctx.getFinalState());
        // ctx.getFinalState() is PROCESSED / PARKED / IN_PROGRESS / ERROR
    })
    .build();
```

The `WorkflowContext` passed to both hooks exposes:

| Field | `beforeProcessing` | `afterProcessing` |
|---|---|---|
| `messageId` | ✓ | ✓ |
| `workflowName` | ✓ | ✓ |
| `payload` | ✓ | ✓ |
| `metadata` | ✓ | ✓ |
| `nodeId` | ✓ | ✓ |
| `finalState` | `null` | non-null |

Any exception thrown by a lifecycle hook is **caught and logged** — it never aborts the workflow or affects step execution.

---

## ExecutionMode

`receive()` always writes the message to the database before returning — **in both modes**. When no external connection is supplied (the default), durable-flow opens its own connection, commits the transaction, and closes the connection before returning. The message is durable the moment `receive()` returns, regardless of what happens next.

> **External connection exception:** When `ReceiveOptions.of(workflow, conn)` or
> `ReceiveOptions.withDeferredExecution(workflow, conn)` is used, the commit is **not**
> performed inside `receive()`. The caller owns the transaction and must commit (or roll
> back) the connection after `receive()` returns. See [External Connection](#external-connection)
> below for details.

What the mode controls is **what happens after that database write**:

| Mode | After the DB write … | `receive()` return state | Suitable for |
|---|---|---|---|
| `ASYNCHRONOUS` (default) | Returns immediately; steps run in the background thread pool | `RECEIVED` | high-throughput ingest, event-driven, message consumers |
| `SYNCHRONOUS` | Blocks on the calling thread until all currently-executable steps finish | actual final state | request-response, testing, scripts |

```java
// ASYNCHRONOUS (default) — returns MessageState.RECEIVED immediately
DurableFlowEngine asyncEngine = new DurableFlowEngine(dataSource,
    DurableFlowConfig.builder()
        .executionMode(ExecutionMode.ASYNCHRONOUS)  // default; may be omitted
        .build());

ReceiveResult result = asyncEngine.receive(message, ReceiveOptions.of(workflow));
// result.messageState() == MessageState.RECEIVED
// The message is already in the DB — safe to ACK the queue/topic message now.
// Steps execute in the background thread pool.

// SYNCHRONOUS — blocks until all currently-executable steps complete
DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource,
    DurableFlowConfig.builder()
        .executionMode(ExecutionMode.SYNCHRONOUS)
        .build());

ReceiveResult result = syncEngine.receive(message, ReceiveOptions.of(workflow));
// result.messageState() == PROCESSED | PARKED | IN_PROGRESS | ERROR
// Returns only after executeEligibleSteps() finishes on the calling thread
```

> **Note:** Even in `SYNCHRONOUS` mode the recovery scheduler continues to run in the background, and steps blocked on retry windows are not waited for. The returned state may be `IN_PROGRESS` if some steps still have a future `next_retry_at`.

---

## RetryPolicy

Retry policies can be applied **per step** or as a **workflow-level default**.

### Per-step policy

```java
// Fixed delay
RetryPolicy.fixedDelay(3, Duration.ofSeconds(2))

// Exponential back-off with jitter
RetryPolicy.exponentialBackoff(5,
    Duration.ofMillis(500),   // initial delay
    2.0,                       // multiplier
    Duration.ofSeconds(30),   // max delay
    true);                     // jitter

// No retry
RetryPolicy.noRetry()
```

### Workflow-level default

Use `defaultRetryPolicy(RetryPolicy.noRetry())` to turn off retries for every step in a single call:

```java
WorkflowDefinition wf = WorkflowDefinition.builder("fire-and-forget")
    .defaultRetryPolicy(RetryPolicy.noRetry())
    .step("step-a", ctx -> StepResult.empty())
    .step("step-b", ctx -> StepResult.empty())
    .build();
```

A per-step `.retryPolicy(...)` always takes precedence over the workflow default. When `defaultRetryPolicy` is not called, every step falls back to `RetryPolicy.defaultPolicy()` (3 attempts, 1-second fixed delay).

---

## MessagePreprocessor SPI

Implement this interface to control canonicalization and storage:

```java
public class MyPreprocessor implements MessagePreprocessor {
    @Override
    public PreprocessResult preprocess(InboundMessage message) {
        byte[] canonical = normalize(message.rawPayload()); // for hash
        byte[] stored    = encrypt(message.rawPayload());   // for storage

        return PreprocessResult.builder()
            .canonicalBytes(canonical)
            .storedPayload(stored)
            .payloadStorageMode(PayloadStorageMode.ENCRYPTED)
            .metadata(Map.of("origin", message.source()))
            .build();
    }
}

// Pass to receive options:
engine.receive(message, new ReceiveOptions(workflow, new MyPreprocessor(), null, false));
```

### PayloadStorageMode

| Value | Description |
|---|---|
| `INLINE` | Stored as-is in `payload_data` (BYTEA / BLOB) |
| `ENCRYPTED` | Stored encrypted inline |
| `NO_PAYLOAD` | Payload intentionally dropped; only hash and metadata retained |
| `EXTERNAL_REF` | Not stored inline; `payload_ref` contains an external URI |

---

## MetricsListener SPI

```java
public class PrometheusMetricsListener implements MetricsListener {
    @Override public void onMessageReceived() { counter("messages_received").inc(); }
    @Override public void onStepSucceeded(String step, Duration d) { histogram(step).observe(d); }
    // ...
}

DurableFlowEngine engine = new DurableFlowEngine(ds, config, new PrometheusMetricsListener());
```

---

## Deduplication

Deduplication scope: `(source, dedupe_hash, payload_length)`

The hash is computed using [zero-allocation-hashing](https://github.com/OpenHFT/Zero-Allocation-Hashing) XXH3 128-bit:

```
dedupe_hash = XXH3_128(canonicalBytes)  →  32-char lowercase hex string
```

The database enforces uniqueness via a `UNIQUE` constraint. When a duplicate is detected, `receive()` returns immediately with `ReceiveResult(duplicate=true)` without inserting new rows.

---

## External Connection

By default, durable-flow manages its own JDBC connection and transaction inside `receive()`.
You can instead pass an existing `java.sql.Connection` to `receive()` so that the message
insertion participates in **your own transaction**:

```java
// Option 1 — immediate dispatch (steps dispatched asynchronously after insert)
ReceiveOptions opts = ReceiveOptions.of(workflow, conn);

// Option 2 — deferred dispatch (caller triggers step execution from afterCommit hook)
ReceiveOptions opts = ReceiveOptions.withDeferredExecution(workflow, conn);
```

When a connection is supplied:
* durable-flow uses it for all persistence operations inside `receive()` **without committing
  or closing it**. The caller owns the full transaction lifecycle.
* If the caller's transaction rolls back, both the business writes and the durable-flow
  inserts are rolled back atomically — the workflow is never triggered.
* Step execution is always dispatched **asynchronously** so that steps never start before
  the external transaction is visible to other connections.

| Factory method | Dispatch timing | Extra caller work |
|---|---|---|
| `ReceiveOptions.of(workflow, conn)` | Immediately after insert (may race before commit; recovery scheduler handles it) | None |
| `ReceiveOptions.withDeferredExecution(workflow, conn)` | Only when caller calls `engine.dispatchSteps(messageId, workflow)` | Register `afterCommit` callback |

See the [Spring Boot Integration guide](../docs/spring-boot-integration.md#atomic-receive-with-an-external-connection-preferred)
for a full Spring `@Transactional` example using both patterns.
