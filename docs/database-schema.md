# Database Schema

Durable Flow uses three tables to persist workflow state. The schema is managed by Flyway and lives under `durable-flow-core/src/main/resources/db/migration/`.

---

## `messages`

Stores one row per inbound message received by the engine. A message is the top-level unit of work; it carries the payload and tracks the overall lifecycle of the workflow instance.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `VARCHAR(36)` | NOT NULL | — | UUID primary key assigned when the message is first received. |
| `source` | `VARCHAR(255)` | NOT NULL | — | Logical name of the integration channel or queue that produced the message (e.g. `"orders-queue"`). Together with `dedupe_hash` and `payload_length` it forms the deduplication key. |
| `dedupe_hash` | `VARCHAR(64)` | NOT NULL | — | SHA-256 (or equivalent) hash of the raw message payload. Used alongside `source` and `payload_length` to detect duplicate deliveries. |
| `payload_length` | `BIGINT` | NOT NULL | `0` | Byte length of the original payload. Part of the composite unique constraint `uq_messages_dedupe`. |
| `payload_storage_mode` | `VARCHAR(32)` | NOT NULL | `'INLINE'` | How the payload is stored. Possible values: `INLINE` (stored in `payload_data`), `ENCRYPTED` (encrypted inline), `NO_PAYLOAD` (payload dropped; only metadata kept), `EXTERNAL_REF` (payload held externally; URI in `payload_ref`). |
| `payload_data` | `BYTEA` | NULL | — | Raw binary payload bytes. Populated when `payload_storage_mode` is `INLINE` or `ENCRYPTED`; `NULL` otherwise. |
| `payload_ref` | `VARCHAR(1024)` | NULL | — | External URI pointing to the payload (e.g. an S3 object key). Populated when `payload_storage_mode` is `EXTERNAL_REF`; `NULL` otherwise. |
| `message_state` | `VARCHAR(32)` | NOT NULL | `'RECEIVED'` | Overall lifecycle state of the message. Possible values: `RECEIVED` (persisted, no steps started), `IN_PROGRESS` (at least one step running or pending), `PROCESSED` (all steps succeeded), `ERROR` (at least one step in retryable failure), `PARKED` (at least one step failed permanently; requires manual intervention). |
| `workflow_name` | `VARCHAR(255)` | NULL | — | Name of the workflow definition that handles this message. Resolved by the `WorkflowRegistry` at intake time. |
| `created_at` | `TIMESTAMP WITH TIME ZONE` | NOT NULL | `NOW()` | Wall-clock time when the row was first inserted. |
| `updated_at` | `TIMESTAMP WITH TIME ZONE` | NOT NULL | `NOW()` | Wall-clock time of the last state change. Updated on every `UPDATE` to this row. |
| `last_error` | `TEXT` | NULL | — | Human-readable description of the most recent error that moved the message into `ERROR` or `PARKED` state. |
| `metadata_json` | `TEXT` | NULL | — | JSON object (`{"key": "value"}`) carrying arbitrary caller-supplied key/value pairs forwarded alongside the payload. |

**Unique constraint:** `uq_messages_dedupe (source, dedupe_hash, payload_length)` — prevents the same logical message from being processed more than once.

**Indexes:**
- `idx_messages_state` on `(message_state)` — used by monitoring and requeue queries.
- `idx_messages_source` on `(source)` — used when looking up messages from a specific channel.

---

## `message_steps`

Stores one row per workflow step per message. Each step is an independently executable unit that the engine claims, executes, and marks as succeeded or failed. Retry state is tracked here.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `id` | `VARCHAR(36)` | NOT NULL | — | UUID primary key assigned when the step row is created. |
| `message_id` | `VARCHAR(36)` | NOT NULL | — | Foreign key to `messages.id`. Links the step back to its parent message/workflow instance. |
| `step_name` | `VARCHAR(255)` | NOT NULL | — | Logical name of the step as defined in the `WorkflowDefinition` (e.g. `"validateOrder"`). Must be unique per `message_id`. |
| `step_state` | `VARCHAR(32)` | NOT NULL | `'PENDING'` | Lifecycle state of this step. Possible values: `PENDING` (waiting to be claimed), `RUNNING` (claimed by a node), `SUCCEEDED` (completed successfully), `FAILED_RETRYABLE` (failed; will be retried), `FAILED_FINAL` (failed permanently; max attempts exhausted), `SKIPPED` (skipped due to upstream failure). |
| `attempt_count` | `INTEGER` | NOT NULL | `0` | Number of execution attempts made so far. Incremented every time the step is claimed for execution. |
| `max_attempts` | `INTEGER` | NOT NULL | `3` | Maximum number of attempts allowed before the step transitions to `FAILED_FINAL`. Sourced from the `RetryPolicy` on the step definition. |
| `next_retry_at` | `TIMESTAMP WITH TIME ZONE` | NULL | — | The earliest time at which the step is eligible to be claimed again after a retryable failure. `NULL` when the step is not waiting for a retry. |
| `locked_until` | `TIMESTAMP WITH TIME ZONE` | NULL | — | Lease expiry timestamp set when the step is claimed (`RUNNING`). If this timestamp is in the past and the step is still `RUNNING`, the lease is considered expired and can be recovered by another node. |
| `owner` | `VARCHAR(255)` | NULL | — | Identifier of the node that currently holds the execution lease (typically the hostname or pod name). Cleared when the step finishes or the lease expires. |
| `last_error` | `TEXT` | NULL | — | Error message or stack-trace snippet from the most recent failed attempt. Truncated to 4 096 characters. |
| `result_data` | `BYTEA` | NULL | — | Serialized output of a successful step execution. Read by downstream steps that declare a dependency on this step. `NULL` if the step has not yet succeeded. |
| `retry_delay_ms` | `BIGINT` | NOT NULL | `1000` | Base delay in milliseconds before the first retry. |
| `retry_multiplier` | `DOUBLE PRECISION` | NOT NULL | `2.0` | Exponential back-off multiplier applied to `retry_delay_ms` on each subsequent attempt. |
| `retry_max_delay_ms` | `BIGINT` | NOT NULL | `60000` | Maximum delay cap in milliseconds regardless of the calculated exponential back-off. |
| `retry_jitter` | `BOOLEAN` | NOT NULL | `FALSE` | When `TRUE`, a random jitter is applied to the retry delay to spread out thundering-herd retries. |
| `created_at` | `TIMESTAMP WITH TIME ZONE` | NOT NULL | `NOW()` | Wall-clock time when the step row was inserted. |
| `updated_at` | `TIMESTAMP WITH TIME ZONE` | NOT NULL | `NOW()` | Wall-clock time of the last state change. Updated after every claim, success, or failure. |

**Unique constraint:** `uq_message_steps (message_id, step_name)` — ensures each step is registered exactly once per workflow instance.

**Indexes:**
- `idx_steps_message_id` on `(message_id)` — efficient lookup of all steps belonging to a message.
- `idx_steps_eligible` on `(step_state, next_retry_at)` filtered to `step_state IN ('PENDING', 'FAILED_RETRYABLE')` — used by the polling loop to find steps ready to execute.
- `idx_steps_expired` on `(locked_until)` filtered to `step_state = 'RUNNING'` — used by the lease-recovery job to find steps whose node has gone silent.

---

## `message_step_dependencies`

A join table that encodes the directed acyclic graph (DAG) of step dependencies within a workflow instance. Each row means "step `step_name` must not start until step `depends_on_step_name` has succeeded".

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `message_id` | `VARCHAR(36)` | NOT NULL | Foreign key to `messages.id`. Scopes the dependency to a single workflow instance. |
| `step_name` | `VARCHAR(255)` | NOT NULL | The dependent step — the one that cannot start yet. |
| `depends_on_step_name` | `VARCHAR(255)` | NOT NULL | The prerequisite step — must be in `SUCCEEDED` state before `step_name` becomes eligible. |

**Primary key:** `(message_id, step_name, depends_on_step_name)`.

**Index:** `idx_deps_message` on `(message_id)` — used when loading the full dependency graph for a message to evaluate which steps are now unblocked.

---

## Entity Relationships

```
messages (1) ──────────────< message_steps (N)
                                    │
messages (1) ──< message_step_dependencies (N)
                 (message_step_dependencies also references step names
                  within the same message_steps rows)
```

The engine uses these three tables together on every step completion:

1. Load all `message_steps` for the message.
2. Load all `message_step_dependencies` for the message.
3. Determine which steps are now unblocked (all prerequisites in `SUCCEEDED`).
4. Update `message_state` in `messages` based on the aggregate step states.
