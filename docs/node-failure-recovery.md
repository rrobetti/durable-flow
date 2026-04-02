# Node Failure Recovery Flow

This diagram shows what happens when a node crashes while executing a step, and how a surviving node detects and recovers the work — including how step context is stored and restored.

```mermaid
flowchart TD
    subgraph NodeA ["Node A  (crashes)"]
        A1([Step scheduled\nstate: PENDING]) --> A2

        A2["Node A claims step\n─────────────────\nstep_state = RUNNING\nowner = nodeA\nlocked_until = NOW() + leaseTimeoutSeconds\nattempt_count++"]

        A2 --> A3["Context built from DB\n─────────────────\npayload ← messages.payload_data\npreviousStepOutputs ← completed\nupstream message_steps.result_data"]

        A3 --> A4[Step executes ...]

        A4 --> A5["💥 Node A crashes\n\nstep_state stays RUNNING\nlocked_until still set\nNo result written to DB"]
    end

    subgraph DB ["Database"]
        DB1[("messages\n─────────────\npayload_data (BYTEA)")]
        DB2[("message_steps\n─────────────\nstep_state = RUNNING\nowner = nodeA\nlocked_until = T+60s\nresult_data = NULL  ← not written")]
        DB3[("message_steps\n(upstream steps)\n─────────────\nresult_data (BYTEA)  ← already persisted")]
    end

    subgraph NodeB ["Node B  (surviving node)"]
        B1[/"⏱ RecoveryScheduler\nruns every recoveryIntervalSeconds"/]

        B1 --> B2{"locked_until\n< NOW() ?"}

        B2 -->|Lease still valid| B1
        B2 -->|Lease expired| B3

        B3["recoverExpiredLeases()\n─────────────────\nstep_state = FAILED_RETRYABLE\nowner = NULL\nlocked_until = NULL\nnext_retry_at = NOW()"]

        B3 --> B4["findEligibleSteps() finds step\n─────────────────\nstep_state = FAILED_RETRYABLE\nAND next_retry_at ≤ NOW()\nAND locked_until IS NULL"]

        B4 --> B5["Node B claims step\n─────────────────\nstep_state = RUNNING\nowner = nodeB  ← new owner\nlocked_until = NOW() + leaseTimeoutSeconds  ← fresh lease\nattempt_count++"]

        B5 --> B6["buildContext() reloads from DB\n─────────────────\npayload ← messages.payload_data\npreviousStepOutputs ← completed\nupstream message_steps.result_data\n\n⚠️ No in-memory state used —\nNode B never saw Node A's execution"]

        B6 --> B7[Step re-executes on Node B]

        B7 -->|Success| B8["Persist result\n─────────────────\nresult_data = output\nstep_state = COMPLETED"]
        B7 -->|Fail again| B9[Normal retry path\nsee retry-flow diagram]
    end

    A3 -.->|reads| DB1
    A3 -.->|reads| DB3
    A2 -.->|writes RUNNING| DB2
    A5 -.->|result_data stays NULL| DB2

    B2 -.->|checks locked_until| DB2
    B3 -.->|resets to FAILED_RETRYABLE| DB2
    B6 -.->|reads payload| DB1
    B6 -.->|reads upstream outputs| DB3
    B5 -.->|writes RUNNING nodeB| DB2
    B8 -.->|writes result_data| DB2

    style A5 fill:#ffcdd2
    style B8 fill:#c8e6c9
    style B3 fill:#fff9c4
    style B6 fill:#e3f2fd
```

## How context survives a node crash

| What | Where stored | When written | When read |
|------|-------------|--------------|-----------|
| Original message payload | `messages.payload_data` (BYTEA) | When the workflow is first received | Before every step execution |
| Output of each upstream step | `message_steps.result_data` (BYTEA) | When that step completes successfully | Before every downstream step execution |
| In-progress step result | never written | — | — |

The crashed step (on Node A) never wrote its `result_data` because it never completed. Node B's `buildContext()` simply does not include it — only the outputs of the steps that succeeded before the crash are loaded. The reconstructed `StepContext` is identical to what Node A would have seen at the start of its execution.

## Recovery timing

The worst-case recovery window is:

```
max wait ≈ leaseTimeoutSeconds + recoveryIntervalSeconds
         ≈ 60 s             + 30 s  (defaults)
         = 90 s
```

The step cannot be double-dispatched: the `UPDATE … WHERE locked_until IS NULL` claim is atomic, so only one node can claim the step even if multiple schedulers run concurrently.

See the [FAQ](faq.md) for configuration options that control the recovery window.
