package io.github.durableflow.api;

import io.github.durableflow.spi.MessagePreprocessor;

import java.sql.Connection;
import java.util.Objects;

/**
 * Options controlling how an inbound message is received and processed.
 *
 * @param workflow        the workflow definition to execute for this message
 * @param preprocessor    optional preprocessor; if null a default identity preprocessor is used
 * @param connection      optional external JDBC connection; when provided, durable-flow will use it
 *                        for all persistence operations in {@code receive()} and will neither commit
 *                        nor close it — the caller is responsible for the full transaction lifecycle.
 * @param deferExecution  when {@code true} and an external connection is also provided, step
 *                        execution is <em>not</em> dispatched automatically after the insert.
 *                        The caller is responsible for triggering dispatch (e.g. from an
 *                        after-commit callback) by calling
 *                        {@code DurableFlowEngine.dispatchSteps(messageId, workflow)}.
 *                        This guarantees that steps only run after the external transaction is
 *                        visible to other connections.
 */
public record ReceiveOptions(
        WorkflowDefinition workflow,
        MessagePreprocessor preprocessor,
        Connection connection,
        boolean deferExecution) {

    public ReceiveOptions {
        Objects.requireNonNull(workflow, "workflow must not be null");
    }

    /**
     * Creates options using only a workflow definition, with no external connection.
     *
     * <p>The engine manages the database connection internally: the message and its steps are
     * written to the database and the transaction is committed before this method returns.
     */
    public static ReceiveOptions withDeferredExecution(WorkflowDefinition workflow) {
        return new ReceiveOptions(workflow, null, null, false);
    }

    /**
     * Creates options that use an external JDBC connection and defer step execution until the
     * caller explicitly triggers it.
     *
     * <p>This is the recommended pattern when integrating with a Spring Boot {@code @Transactional}
     * method: step dispatch is skipped inside {@code receive()}, and the caller registers an
     * after-commit callback that calls {@code DurableFlowEngine.dispatchSteps(messageId, workflow)}.
     * This guarantees that steps never start before the external transaction is committed and its
     * rows are visible to other database connections.
     *
     * <pre>{@code
     * @Transactional
     * public String placeOrder(Order order) {
     *     orderRepository.save(order);
     *     Connection conn = DataSourceUtils.getConnection(dataSource);
     *     ReceiveResult result = engine.receive(msg,
     *             ReceiveOptions.withDeferredExecution(workflow, conn));
     *
     *     // Register after-commit trigger — runs only when the transaction commits successfully
     *     TransactionSynchronizationManager.registerSynchronization(
     *         new TransactionSynchronization() {
     *             public void afterCommit() {
     *                 engine.dispatchSteps(result.messageId(), workflow);
     *             }
     *         });
     *     return result.messageId();
     * }
     * }</pre>
     *
     * <p>If the transaction rolls back, both the business write and the durable-flow insert are
     * undone atomically and {@code afterCommit()} is never called — the workflow is never
     * triggered.
     */
    public static ReceiveOptions withDeferredExecution(WorkflowDefinition workflow, Connection connection) {
        return new ReceiveOptions(workflow, null, connection, true);
    }
}
