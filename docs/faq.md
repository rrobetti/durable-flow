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
