package io.github.durableflow.api;

/**
 * Lifecycle states of a single workflow step.
 *
 * <ul>
 *   <li>{@code PENDING}          – waiting to be claimed</li>
 *   <li>{@code RUNNING}          – currently claimed by a node</li>
 *   <li>{@code SUCCEEDED}        – completed successfully</li>
 *   <li>{@code FAILED_RETRYABLE} – failed but will be retried</li>
 *   <li>{@code FAILED_FINAL}     – failed permanently; max attempts exhausted</li>
 *   <li>{@code SKIPPED}          – step was skipped (e.g., due to upstream failure)</li>
 * </ul>
 */
public enum StepState {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED_RETRYABLE,
    FAILED_FINAL,
    SKIPPED
}
