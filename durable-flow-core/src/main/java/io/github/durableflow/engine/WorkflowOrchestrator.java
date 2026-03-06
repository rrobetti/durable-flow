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
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Orchestrates the execution of eligible workflow steps for a message.
 *
 * <p>Called both immediately after a message is received (inline) and from the
 * recovery scheduler.
 */
public class WorkflowOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(WorkflowOrchestrator.class);
    private static final int ELIGIBLE_STEP_BATCH_SIZE = 50;

    private final DataSource dataSource;
    private final StepRepository stepRepository;
    private final MessageRepository messageRepository;
    private final DurableFlowConfig config;
    private final MetricsListener metricsListener;
    private final ExecutorService executorService;

    public WorkflowOrchestrator(
            DataSource dataSource,
            StepRepository stepRepository,
            MessageRepository messageRepository,
            DurableFlowConfig config,
            MetricsListener metricsListener,
            ExecutorService executorService) {
        this.dataSource = dataSource;
        this.stepRepository = stepRepository;
        this.messageRepository = messageRepository;
        this.config = config;
        this.metricsListener = metricsListener;
        this.executorService = executorService;
    }

    /**
     * Find and execute all eligible steps for the given message.
     * This method is safe to call concurrently; step claiming is atomic.
     */
    public void executeEligibleSteps(String messageId, WorkflowDefinition workflow) {
        Map<String, StepDefinition> stepsByName = indexByName(workflow.getSteps());

        boolean anyExecuted = true;
        while (anyExecuted) {
            anyExecuted = false;
            List<StepRecord> eligible = stepRepository.findEligibleSteps(ELIGIBLE_STEP_BATCH_SIZE);
            for (StepRecord step : eligible) {
                if (!step.getMessageId().equals(messageId)) continue;
                StepDefinition def = stepsByName.get(step.getStepName());
                if (def == null) {
                    log.warn("No StepDefinition found for step: {}", step.getStepName());
                    continue;
                }
                boolean executed = executeStep(step, def, workflow);
                if (executed) anyExecuted = true;
            }
        }

        // Recalculate and persist message state
        recalculateMessageState(messageId);
    }

    /**
     * Batch-scan eligible steps across all messages (used by recovery scheduler).
     */
    public void executeAllEligibleSteps(Map<String, WorkflowDefinition> workflowsByName) {
        List<StepRecord> eligible = stepRepository.findEligibleSteps(ELIGIBLE_STEP_BATCH_SIZE);
        Set<String> affectedMessages = new HashSet<>();

        for (StepRecord step : eligible) {
            WorkflowDefinition workflow = workflowsByName.get(step.getMessageId());
            if (workflow == null) {
                // Try to look up via message record
                log.debug("Skipping step {} - workflow not available in registry for message {}",
                        step.getStepName(), step.getMessageId());
                continue;
            }
            Map<String, StepDefinition> stepsByName = indexByName(workflow.getSteps());
            StepDefinition def = stepsByName.get(step.getStepName());
            if (def == null) continue;
            if (executeStep(step, def, workflow)) {
                affectedMessages.add(step.getMessageId());
            }
        }

        for (String messageId : affectedMessages) {
            recalculateMessageState(messageId);
        }
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private boolean executeStep(StepRecord step, StepDefinition def, WorkflowDefinition workflow) {
        Instant lockedUntil = Instant.now().plusSeconds(config.getLeaseTimeoutSeconds());
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                boolean claimed = stepRepository.claimStep(conn, step.getId(), config.getNodeId(), lockedUntil);
                if (!claimed) {
                    conn.rollback();
                    return false;
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("DB error claiming step {}", step.getId(), e);
            return false;
        }

        // Execute outside the claim transaction
        Instant startTime = Instant.now();
        metricsListener.onStepStarted(def.getName());
        try {
            StepContext ctx = buildContext(step, def, workflow);
            StepResult result = def.getHandler().execute(ctx);
            byte[] output = result.getOutput().orElse(null);

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    stepRepository.markSucceeded(conn, step.getId(), output);
                    conn.commit();
                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                }
            }

            Duration elapsed = Duration.between(startTime, Instant.now());
            metricsListener.onStepSucceeded(def.getName(), elapsed);
            log.debug("Step succeeded: message={} step={} attempt={}",
                    step.getMessageId(), step.getStepName(), step.getAttemptCount() + 1);
            return true;

        } catch (Exception e) {
            handleStepFailure(step, def, e);
            return true;
        }
    }

    private void handleStepFailure(StepRecord step, StepDefinition def, Exception e) {
        RetryPolicy policy = def.getRetryPolicy();
        int currentAttempt = step.getAttemptCount() + 1; // already incremented by claim
        boolean retryable = policy.shouldRetry(currentAttempt, e);

        Instant nextRetryAt = null;
        if (retryable) {
            Duration delay = policy.nextDelay(currentAttempt);
            nextRetryAt = Instant.now().plus(delay);
        }

        String errorMessage = e.getClass().getName() + ": " + e.getMessage();
        metricsListener.onStepFailed(def.getName(), !retryable);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                stepRepository.markFailed(conn, step.getId(), retryable, currentAttempt + 1, nextRetryAt, errorMessage);
                conn.commit();
            } catch (Exception dbEx) {
                conn.rollback();
                log.error("Failed to persist step failure for step {}", step.getId(), dbEx);
            }
        } catch (SQLException dbEx) {
            log.error("DB error while handling step failure for step {}", step.getId(), dbEx);
        }

        log.warn("Step failed: message={} step={} attempt={} retryable={}",
                step.getMessageId(), step.getStepName(), currentAttempt, retryable, e);
    }

    private StepContext buildContext(StepRecord step, StepDefinition def, WorkflowDefinition workflow) {
        // Gather outputs from upstream steps
        List<String> depNames = def.getDependsOn();
        Map<String, byte[]> previousOutputs = Collections.emptyMap();
        if (!depNames.isEmpty()) {
            previousOutputs = stepRepository.getStepOutputs(step.getMessageId(), depNames);
        }

        // Load the stored payload from the message record
        byte[] payload = loadPayload(step.getMessageId());

        return new StepContext(
                step.getMessageId(),
                step.getStepName(),
                step.getAttemptCount(),
                payload,
                Collections.emptyMap(),
                previousOutputs,
                config.getNodeId());
    }

    private byte[] loadPayload(String messageId) {
        // We use the messageRepository without opening a new transaction just for a read
        return messageRepository.findById(messageId)
                .map(r -> r.getPayloadData())
                .orElse(null);
    }

    private void recalculateMessageState(String messageId) {
        List<StepRecord> steps = stepRepository.findStepsForMessage(messageId);
        MessageState newState = MessageStateCalculator.calculate(steps);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                messageRepository.updateMessageState(conn, messageId, newState);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                log.error("Failed to update message state for {}", messageId, e);
            }
        } catch (SQLException e) {
            log.error("DB error while updating message state for {}", messageId, e);
        }

        if (newState == MessageState.PROCESSED) metricsListener.onMessageProcessed();
        if (newState == MessageState.PARKED) metricsListener.onMessageParked();
    }

    private static Map<String, StepDefinition> indexByName(List<StepDefinition> steps) {
        Map<String, StepDefinition> map = new HashMap<>();
        for (StepDefinition s : steps) {
            map.put(s.getName(), s);
        }
        return map;
    }
}
