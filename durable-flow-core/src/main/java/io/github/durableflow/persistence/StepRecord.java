package io.github.durableflow.persistence;

import io.github.durableflow.api.StepState;

import java.time.Instant;

/**
 * Database row for the {@code message_steps} table.
 */
public class StepRecord {

    private String id;
    private String messageId;
    private String stepName;
    private StepState stepState;
    private int attemptCount;
    private int maxAttempts;
    private Instant nextRetryAt;
    private Instant lockedUntil;
    private String owner;
    private String lastError;
    private byte[] resultData;
    private long retryDelayMs;
    private double retryMultiplier;
    private long retryMaxDelayMs;
    private boolean retryJitter;
    private Instant createdAt;
    private Instant updatedAt;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }

    public StepState getStepState() { return stepState; }
    public void setStepState(StepState stepState) { this.stepState = stepState; }

    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public Instant getNextRetryAt() { return nextRetryAt; }
    public void setNextRetryAt(Instant nextRetryAt) { this.nextRetryAt = nextRetryAt; }

    public Instant getLockedUntil() { return lockedUntil; }
    public void setLockedUntil(Instant lockedUntil) { this.lockedUntil = lockedUntil; }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public byte[] getResultData() { return resultData; }
    public void setResultData(byte[] resultData) { this.resultData = resultData; }

    public long getRetryDelayMs() { return retryDelayMs; }
    public void setRetryDelayMs(long retryDelayMs) { this.retryDelayMs = retryDelayMs; }

    public double getRetryMultiplier() { return retryMultiplier; }
    public void setRetryMultiplier(double retryMultiplier) { this.retryMultiplier = retryMultiplier; }

    public long getRetryMaxDelayMs() { return retryMaxDelayMs; }
    public void setRetryMaxDelayMs(long retryMaxDelayMs) { this.retryMaxDelayMs = retryMaxDelayMs; }

    public boolean isRetryJitter() { return retryJitter; }
    public void setRetryJitter(boolean retryJitter) { this.retryJitter = retryJitter; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
