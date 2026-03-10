package io.github.durableflow.api;

/**
 * Lifecycle states of a message in the durable-flow engine.
 *
 * <ul>
 *   <li>{@code RECEIVED}    – message persisted, no steps started yet</li>
 *   <li>{@code IN_PROGRESS} – at least one step is RUNNING or PENDING</li>
 *   <li>{@code PROCESSED}   – all steps SUCCEEDED</li>
 *   <li>{@code ERROR}       – at least one step is in a retryable-failure state</li>
 *   <li>{@code PARKED}      – at least one step failed permanently; requires manual intervention</li>
 * </ul>
 */
public enum MessageState {
    RECEIVED,
    IN_PROGRESS,
    PROCESSED,
    ERROR,
    PARKED
}
