package io.github.durableflow.scheduler;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.api.WorkflowDefinition;
import io.github.durableflow.engine.WorkflowOrchestrator;
import io.github.durableflow.engine.WorkflowRegistry;
import io.github.durableflow.persistence.MessageRecord;
import io.github.durableflow.persistence.MessageRepository;
import io.github.durableflow.persistence.StepRecord;
import io.github.durableflow.persistence.StepRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background scheduler that:
 * <ol>
 *   <li>Recovers expired step leases (RUNNING steps whose locked_until has passed)</li>
 *   <li>Scans for and dispatches eligible steps across all registered workflows</li>
 * </ol>
 */
public class RecoveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RecoveryScheduler.class);

    private final DataSource dataSource;
    private final StepRepository stepRepository;
    private final MessageRepository messageRepository;
    private final WorkflowRegistry workflowRegistry;
    private final WorkflowOrchestrator orchestrator;
    private final DurableFlowConfig config;

    private ScheduledExecutorService scheduler;

    public RecoveryScheduler(
            DataSource dataSource,
            StepRepository stepRepository,
            MessageRepository messageRepository,
            WorkflowRegistry workflowRegistry,
            WorkflowOrchestrator orchestrator,
            DurableFlowConfig config) {
        this.dataSource = dataSource;
        this.stepRepository = stepRepository;
        this.messageRepository = messageRepository;
        this.workflowRegistry = workflowRegistry;
        this.orchestrator = orchestrator;
        this.config = config;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "durable-flow-recovery");
            t.setDaemon(true);
            return t;
        });
        long intervalSeconds = config.getRecoveryIntervalSeconds();
        scheduler.scheduleWithFixedDelay(this::runRecovery, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        log.info("RecoveryScheduler started with interval={}s", intervalSeconds);
    }

    public void stop() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("RecoveryScheduler stopped");
        }
    }

    // -------------------------------------------------------------------------
    // Recovery logic
    // -------------------------------------------------------------------------

    private void runRecovery() {
        try {
            int recovered = stepRepository.recoverExpiredLeases();
            if (recovered > 0) {
                log.info("Recovered {} expired step leases", recovered);
            }

            // Build a mapping from messageId to WorkflowDefinition for eligible steps
            List<StepRecord> eligible = stepRepository.findEligibleSteps(100);
            Map<String, WorkflowDefinition> workflowByMessageId = buildWorkflowMap(eligible);

            if (!workflowByMessageId.isEmpty()) {
                orchestrator.executeAllEligibleSteps(workflowByMessageId);
            }
        } catch (Exception e) {
            log.error("Error during recovery run", e);
        }
    }

    private Map<String, WorkflowDefinition> buildWorkflowMap(List<StepRecord> steps) {
        Map<String, WorkflowDefinition> result = new HashMap<>();
        Set<String> messageIds = new HashSet<>();
        for (StepRecord step : steps) {
            messageIds.add(step.getMessageId());
        }
        for (String messageId : messageIds) {
            Optional<MessageRecord> msgOpt = messageRepository.findById(messageId);
            msgOpt.ifPresent(msg -> {
                String workflowName = msg.getWorkflowName();
                if (workflowName != null) {
                    workflowRegistry.find(workflowName).ifPresent(wf ->
                            result.put(messageId, wf));
                }
            });
        }
        return result;
    }
}
