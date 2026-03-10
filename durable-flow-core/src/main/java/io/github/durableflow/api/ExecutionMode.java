package io.github.durableflow.api;

/**
 * Controls whether workflow step execution runs synchronously (blocking) or asynchronously
 * after {@link io.github.durableflow.DurableFlowEngine#receive} persists the message.
 *
 * <p><strong>What is always the same in both modes:</strong> the message and its steps are
 * written to the database and committed <em>before</em> {@code receive()} returns, regardless
 * of which mode is active. This means the message is durable the moment you get a result
 * back — safe to acknowledge to a queue or topic.
 *
 * <h3>ASYNCHRONOUS (default)</h3>
 * <p>After the database commit, {@code receive()} returns immediately with
 * {@link MessageState#RECEIVED}. Steps are dispatched to a background thread pool.
 * The caller does not block waiting for step execution.
 *
 * <h3>SYNCHRONOUS</h3>
 * <p>After the database commit, {@code receive()} blocks on the calling thread until all
 * eligible steps have been executed (to completion, failure, or the point where further
 * steps depend on asynchronous retries). The returned
 * {@link io.github.durableflow.api.ReceiveResult} reflects the actual final
 * {@link MessageState} at the time of return — which may be {@link MessageState#PROCESSED},
 * {@link MessageState#ERROR}, {@link MessageState#PARKED}, or
 * {@link MessageState#IN_PROGRESS} if some steps are still waiting for a retry window.
 */
public enum ExecutionMode {

    /**
     * Default mode — steps are dispatched to the background executor and
     * {@code receive()} returns immediately with {@link MessageState#RECEIVED}.
     */
    ASYNCHRONOUS,

    /**
     * {@code receive()} blocks on the calling thread until the workflow run
     * completes and returns the actual final {@link MessageState}.
     */
    SYNCHRONOUS
}
