package io.github.durableflow.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

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

    public StepContext(
            String messageId,
            String stepName,
            int attemptCount,
            byte[] payload,
            Map<String, String> metadata,
            Map<String, byte[]> previousStepOutputs,
            String nodeId) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.stepName = Objects.requireNonNull(stepName, "stepName must not be null");
        this.attemptCount = attemptCount;
        this.payload = payload != null ? Arrays.copyOf(payload, payload.length) : new byte[0];
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.previousStepOutputs = previousStepOutputs != null
                ? Collections.unmodifiableMap(previousStepOutputs)
                : Collections.emptyMap();
        this.nodeId = nodeId;
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
}
