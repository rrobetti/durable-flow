package io.github.durableflow.api;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Execution context provided to a {@link StepHandler}.
 */
public final class StepContext {

    private final String messageId;
    private final String stepName;
    private final int attemptCount;
    private final byte[] payload;
    private final Map<String, String> metadata;
    private final Map<String, byte[]> previousStepOutputs;
    private final String nodeId;
    private final Connection connection;

    /**
     * Constructs a {@code StepContext} without an explicit JDBC connection.
     * This constructor is retained for backward compatibility and for use in unit tests.
     */
    public StepContext(
            String messageId,
            String stepName,
            int attemptCount,
            byte[] payload,
            Map<String, String> metadata,
            Map<String, byte[]> previousStepOutputs,
            String nodeId) {
        this(messageId, stepName, attemptCount, payload, metadata, previousStepOutputs, nodeId, null);
    }

    /**
     * Constructs a {@code StepContext} with an optional JDBC connection.
     *
     * @param connection the JDBC connection that durable-flow is using for step-state
     *                   management, or {@code null} if none is available
     */
    public StepContext(
            String messageId,
            String stepName,
            int attemptCount,
            byte[] payload,
            Map<String, String> metadata,
            Map<String, byte[]> previousStepOutputs,
            String nodeId,
            Connection connection) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.stepName = Objects.requireNonNull(stepName, "stepName must not be null");
        this.attemptCount = attemptCount;
        this.payload = payload != null ? Arrays.copyOf(payload, payload.length) : new byte[0];
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.previousStepOutputs = previousStepOutputs != null
                ? Collections.unmodifiableMap(previousStepOutputs)
                : Collections.emptyMap();
        this.nodeId = nodeId;
        this.connection = connection;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getStepName() {
        return stepName;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    /** Returns a defensive copy of the stored payload. */
    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public Map<String, byte[]> getPreviousStepOutputs() {
        return previousStepOutputs;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns the JDBC connection that durable-flow is currently using for step-state
     * management, if one is available.
     *
     * <p>When present, any database operations performed on this connection will be
     * committed together with durable-flow's own {@code markSucceeded} update on
     * success, or rolled back together on failure.  This lets a step handler's
     * business-logic writes and durable-flow's step-state update share a single
     * atomic database transaction.
     *
     * <p><strong>Important:</strong> the caller must not commit, roll back, or close
     * this connection — its lifecycle is managed entirely by durable-flow.
     */
    public Optional<Connection> getConnection() {
        return Optional.ofNullable(connection);
    }
}
