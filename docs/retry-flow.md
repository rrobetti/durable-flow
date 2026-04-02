# Step Retry Flow

This diagram shows the full lifecycle of a step that fails and is retried under normal (non-crash) conditions.

```mermaid
flowchart TD
    A([Step scheduled\nstate: PENDING]) --> B

    B["Engine claims step\n─────────────────\nstep_state = RUNNING\nowner = nodeId\nlocked_until = NOW() + leaseTimeoutSeconds\nattempt_count++"]

    B --> C[Step executes]

    C -->|Success| D["Persist result\n─────────────────\nmessage_steps.result_data = output\nstep_state = COMPLETED"]
    D --> E([Done])

    C -->|Exception thrown| F{Retries\nremaining?}

    F -->|No| G["Mark permanently failed\n─────────────────\nstep_state = FAILED\nlocked_until = NULL\nowner = NULL"]
    G --> H([Workflow fails])

    F -->|Yes| I["Compute next retry time\n─────────────────\nnext_retry_at = NOW() + RetryPolicy.nextDelay(attempt)\nstep_state = FAILED_RETRYABLE\nlocked_until = NULL\nowner = NULL"]

    I --> J[/"⏱ Wait...\nRecoveryScheduler wakes up\nevery recoveryIntervalSeconds"/]

    J --> K{"next_retry_at\n≤ NOW()?"}
    K -->|Not yet| J
    K -->|Yes| L["Step eligible for retry\n─────────────────\nstep_state IN (FAILED_RETRYABLE)\nAND next_retry_at ≤ NOW()\nAND locked_until IS NULL"]

    L --> M["Build context from DB\n─────────────────\npayload ← messages.payload_data\npreviousStepOutputs ← completed\nupstream message_steps.result_data"]

    M --> B

    style D fill:#c8e6c9
    style E fill:#c8e6c9
    style G fill:#ffcdd2
    style H fill:#ffcdd2
    style I fill:#fff9c4
    style J fill:#e3f2fd
    style K fill:#e3f2fd
    style L fill:#fff9c4
```

## Key points

- The **effective retry delay** is `max(RetryPolicy.nextDelay, recoveryIntervalSeconds)`. If the policy delay is shorter than the scheduler interval, the step waits for the next scheduler tick.
- Each retry **re-claims** the step with a fresh lease (`locked_until`, `owner`), so the step cannot be double-dispatched.
- Step context (payload and upstream outputs) is **always reconstructed from the database** — no in-memory state is involved, making retries safe even after a JVM restart.
- `next_retry_at` and `locked_until` are reset to `NULL` when transitioning to `FAILED_RETRYABLE`, allowing any healthy node's scheduler to pick up the step.

See the [FAQ](faq.md) for configuration options that control retry behaviour.
