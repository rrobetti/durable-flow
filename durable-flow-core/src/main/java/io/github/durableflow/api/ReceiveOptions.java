package io.github.durableflow.api;

import io.github.durableflow.spi.MessagePreprocessor;

import java.sql.Connection;
import java.util.Objects;

/**
 * Options controlling how an inbound message is received and processed.
 *
 * @param workflow      the workflow definition to execute for this message
 * @param preprocessor  optional preprocessor; if null a default identity preprocessor is used
 * @param connection    optional external JDBC connection; when provided, durable-flow will use it
 *                      for all persistence operations in {@code receive()} and will neither commit
 *                      nor close it — the caller is responsible for the full transaction lifecycle.
 *                      Step execution will always be dispatched asynchronously in this case,
 *                      because steps cannot run before the caller's transaction is committed.
 */
public record ReceiveOptions(
        WorkflowDefinition workflow,
        MessagePreprocessor preprocessor,
        Connection connection) {

    public ReceiveOptions {
        Objects.requireNonNull(workflow, "workflow must not be null");
    }

    /** Convenience constructor using only a workflow definition. */
    public static ReceiveOptions of(WorkflowDefinition workflow) {
        return new ReceiveOptions(workflow, null, null);
    }

    /**
     * Convenience constructor using a workflow definition and an external JDBC connection.
     *
     * <p>When a connection is supplied durable-flow uses it for all persistence operations
     * inside {@code receive()} without committing or closing it. This allows the message
     * insertion and the caller's own business logic to share a single database transaction.
     * The caller must commit (or roll back) the connection after {@code receive()} returns.
     *
     * <p>Step execution is always dispatched asynchronously when an external connection is
     * used, because steps must not run before the caller's transaction is committed.
     */
    public static ReceiveOptions of(WorkflowDefinition workflow, Connection connection) {
        return new ReceiveOptions(workflow, null, connection);
    }
}
