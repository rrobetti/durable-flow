package io.github.durableflow.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A snapshot of the current status of a message and its steps.
 */
public final class MessageStatus {

    private final String messageId;
    private final MessageState messageState;
    private final String workflowName;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final List<StepStatus> steps;

    public MessageStatus(
            String messageId,
            MessageState messageState,
            String workflowName,
            Instant createdAt,
            Instant updatedAt,
            List<StepStatus> steps) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.messageState = Objects.requireNonNull(messageState, "messageState must not be null");
        this.workflowName = workflowName;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.steps = steps != null ? List.copyOf(steps) : List.of();
    }

    public String getMessageId() {
        return messageId;
    }

    public MessageState getMessageState() {
        return messageState;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public List<StepStatus> getSteps() {
        return steps;
    }

    // -------------------------------------------------------------------------
    // Inner DTO
    // -------------------------------------------------------------------------

    public static final class StepStatus {
        private final String stepName;
        private final StepState stepState;
        private final int attemptCount;
        private final String lastError;

        public StepStatus(String stepName, StepState stepState, int attemptCount, String lastError) {
            this.stepName = Objects.requireNonNull(stepName, "stepName must not be null");
            this.stepState = Objects.requireNonNull(stepState, "stepState must not be null");
            this.attemptCount = attemptCount;
            this.lastError = lastError;
        }

        public String getStepName() { return stepName; }
        public StepState getStepState() { return stepState; }
        public int getAttemptCount() { return attemptCount; }
        public String getLastError() { return lastError; }
    }
}
