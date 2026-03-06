package io.github.durableflow.engine;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.api.*;
import io.github.durableflow.persistence.*;
import io.github.durableflow.spi.MetricsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Handles the execution lifecycle of a single workflow step: claim → execute → record result.
 *
 * <p>This class is used directly by {@link WorkflowOrchestrator} for fine-grained step control.
 */
public class StepExecutor {

    private static final Logger log = LoggerFactory.getLogger(StepExecutor.class);

    private final DataSource dataSource;
    private final StepRepository stepRepository;
    private final MessageRepository messageRepository;
    private final DurableFlowConfig config;
    private final MetricsListener metricsListener;

    public StepExecutor(
            DataSource dataSource,
            StepRepository stepRepository,
            MessageRepository messageRepository,
            DurableFlowConfig config,
            MetricsListener metricsListener) {
        this.dataSource = dataSource;
        this.stepRepository = stepRepository;
        this.messageRepository = messageRepository;
        this.config = config;
        this.metricsListener = metricsListener;
    }

    /**
     * Attempts to claim and execute the given step.
     *
     * @return {@code true} if the step was claimed and executed (success or failure recorded),
     *         {@code false} if the claim was lost to another node
     */
    public boolean execute(StepRecord step, StepDefinition def, WorkflowDefinition workflow) {
        Instant lockedUntil = Instant.now().plusSeconds(config.getLeaseTimeoutSeconds());

        // Atomic claim
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                boolean claimed = stepRepository.claimStep(conn, step.getId(), config.getNodeId(), lockedUntil);
                conn.commit();
                if (!claimed) {
                    return false;
                }
            } catch (Exception e) {
                conn.rollback();
                return false;
            }
        } catch (SQLException e) {
            log.error("Failed to open connection for step claim: {}", step.getId(), e);
            return false;
        }

        // Dispatch
        Instant start = Instant.now();
        metricsListener.onStepStarted(def.getName());
        try {
            StepContext ctx = buildContext(step, def);
            StepResult result = def.getHandler().execute(ctx);
            byte[] output = result.getOutput().orElse(null);

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    stepRepository.markSucceeded(conn, step.getId(), output);
                    conn.commit();
                } catch (Exception e) {
                    conn.rollback();
                    throw new RuntimeException("Failed to persist step success", e);
                }
            }

            metricsListener.onStepSucceeded(def.getName(), Duration.between(start, Instant.now()));
            return true;

        } catch (Exception e) {
            recordFailure(step, def, e);
            return true;
        }
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private void recordFailure(StepRecord step, StepDefinition def, Exception e) {
        RetryPolicy policy = def.getRetryPolicy();
        int attempt = step.getAttemptCount() + 1;
        boolean retryable = policy.shouldRetry(attempt, e);

        Instant nextRetryAt = null;
        if (retryable) {
            nextRetryAt = Instant.now().plus(policy.nextDelay(attempt));
        }

        metricsListener.onStepFailed(def.getName(), !retryable);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                stepRepository.markFailed(conn, step.getId(), retryable, attempt + 1, nextRetryAt,
                        e.getClass().getName() + ": " + e.getMessage());
                conn.commit();
            } catch (Exception dbEx) {
                conn.rollback();
                log.error("Failed to persist step failure for {}", step.getId(), dbEx);
            }
        } catch (SQLException dbEx) {
            log.error("DB error persisting step failure for {}", step.getId(), dbEx);
        }
        log.warn("Step failed: {} step={} attempt={} retryable={}", step.getMessageId(), step.getStepName(), attempt, retryable, e);
    }

    private StepContext buildContext(StepRecord step, StepDefinition def) {
        List<String> depNames = def.getDependsOn();
        Map<String, byte[]> prevOutputs = depNames.isEmpty()
                ? Collections.emptyMap()
                : stepRepository.getStepOutputs(step.getMessageId(), depNames);

        byte[] payload = messageRepository.findById(step.getMessageId())
                .map(r -> r.getPayloadData() != null ? r.getPayloadData() : new byte[0])
                .orElse(new byte[0]);

        return new StepContext(
                step.getMessageId(),
                step.getStepName(),
                step.getAttemptCount(),
                payload,
                Collections.emptyMap(),
                prevOutputs,
                config.getNodeId());
    }
}
