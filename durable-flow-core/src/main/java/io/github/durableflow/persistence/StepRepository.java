package io.github.durableflow.persistence;

import io.github.durableflow.api.StepDefinition;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Repository for step records.
 */
public interface StepRepository {

    void insertSteps(Connection conn, String messageId, List<StepDefinition> steps);

    /**
     * Returns eligible steps: PENDING or FAILED_RETRYABLE, all dependencies SUCCEEDED,
     * not currently locked.
     */
    List<StepRecord> findEligibleSteps(int limit);

    /**
     * Atomically claims a step. Returns {@code true} if the claim succeeded.
     */
    boolean claimStep(Connection conn, String stepId, String nodeId, Instant lockedUntil);

    void markSucceeded(Connection conn, String stepId, byte[] output);

    void markFailed(Connection conn, String stepId, boolean retryable,
                    int nextAttempt, Instant nextRetryAt, String error);

    List<StepRecord> findStepsForMessage(String messageId);

    Map<String, byte[]> getStepOutputs(String messageId, List<String> stepNames);

    int recoverExpiredLeases();

    void resetFailedFinalSteps(String messageId);
}
