package io.github.durableflow.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Execution context provided to a {@link WorkflowLifecycleHandler}.
 *
 * <p>Passed to {@code beforeProcessing} and {@code afterProcessing} hooks on a
 * {@link WorkflowDefinition}. The {@link #getFinalState()} field is only populated
 * for {@code afterProcessing} calls; it is {@code null} when called from
 * {@code beforeProcessing}.
 */
public final class WorkflowContext {

    private final String messageId;
    private final String workflowName;
    private final byte[] payload;
    private final Map<String, String> metadata;
    private final String nodeId;
    /** Non-null only for {@code afterProcessing}; null for {@code beforeProcessing}. */
    private final MessageState finalState;

    public WorkflowContext(
            String messageId,
            String workflowName,
            byte[] payload,
            Map<String, String> metadata,
            String nodeId,
            MessageState finalState) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.workflowName = Objects.requireNonNull(workflowName, "workflowName must not be null");
        this.payload = payload != null ? Arrays.copyOf(payload, payload.length) : new byte[0];
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
        this.nodeId = nodeId;
        this.finalState = finalState;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    /** Returns a defensive copy of the stored message payload. */
    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * Returns the final {@link MessageState} of the workflow run, or {@code null} when called
     * from {@code beforeProcessing} (where the workflow has not yet executed).
     */
    public MessageState getFinalState() {
        return finalState;
    }
}
