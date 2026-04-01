package io.github.durableflow;

import io.github.durableflow.api.*;
import io.github.durableflow.engine.MessageStateCalculator;
import io.github.durableflow.engine.WorkflowOrchestrator;
import io.github.durableflow.engine.WorkflowRegistry;
import io.github.durableflow.persistence.*;
import io.github.durableflow.persistence.dialect.DatabaseDialect;
import io.github.durableflow.persistence.dialect.DatabaseDialectFactory;
import io.github.durableflow.scheduler.RecoveryScheduler;
import io.github.durableflow.spi.*;
import net.openhft.hashing.LongHashFunction;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry point for the durable-flow engine.
 *
 * <p>Typical usage:
 * <pre>{@code
 * DataSource ds = ...;
 * DurableFlowEngine engine = new DurableFlowEngine(ds, DurableFlowConfig.defaults());
 * engine.start();
 *
 * WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
 *     .step("step1", ctx -> StepResult.empty())
 *     .build();
 *
 * ReceiveResult result = engine.receive(
 *     new InboundMessage("my-source", payload, headers),
 *     ReceiveOptions.withDeferredExecution(wf));
 *
 * engine.close();
 * }</pre>
 */
public class DurableFlowEngine implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DurableFlowEngine.class);

    private final DataSource dataSource;
    private final DurableFlowConfig config;
    private final MessageRepository messageRepository;
    private final StepRepository stepRepository;
    private final WorkflowOrchestrator orchestrator;
    private final WorkflowRegistry workflowRegistry;
    private final RecoveryScheduler recoveryScheduler;
    private final MetricsListener metricsListener;
    private final ExecutorService executorService;

    private volatile boolean started = false;

    public DurableFlowEngine(DataSource dataSource, DurableFlowConfig config) {
        this(dataSource, config, NoOpMetricsListener.INSTANCE);
    }

    public DurableFlowEngine(DataSource dataSource, DurableFlowConfig config, MetricsListener metricsListener) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metricsListener = metricsListener != null ? metricsListener : NoOpMetricsListener.INSTANCE;

        // Resolve dialect: use explicit config override, or auto-detect from JDBC metadata
        DatabaseDialect dialect = config.getDialect() != null
                ? config.getDialect()
                : DatabaseDialectFactory.detect(dataSource);

        if (config.isSchemaAutoMigrate()) {
            runMigrations(dataSource, dialect);
        }

        this.messageRepository = new JdbcMessageRepository(dataSource, dialect);
        this.stepRepository = new JdbcStepRepository(dataSource, dialect);
        this.workflowRegistry = new WorkflowRegistry();
        this.executorService = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("durable-flow-worker-", 0).factory());
        this.orchestrator = new WorkflowOrchestrator(
                dataSource, stepRepository, messageRepository,
                config, this.metricsListener, executorService);
        this.recoveryScheduler = new RecoveryScheduler(
                dataSource, stepRepository, messageRepository,
                workflowRegistry, orchestrator, config);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Receives an inbound message, persists it (deduplicated), and executes eligible steps
     * either synchronously (blocking) or asynchronously depending on
     * {@link DurableFlowConfig#getExecutionMode()}.
     *
     * <p><strong>What always happens synchronously (before this method returns):</strong>
     * regardless of the configured execution mode, the message and its steps are written to
     * the database and the database transaction is committed <em>before</em> this method
     * returns to the caller. The database connection is also fully closed at that point.
     * This means the message is durably stored the moment you get a {@link ReceiveResult}
     * back — it is safe to acknowledge the message to a queue or topic immediately after
     * this call returns.
     *
     * <p><strong>What differs between modes (what happens after the DB write):</strong>
     * <ul>
     *   <li>In {@link ExecutionMode#ASYNCHRONOUS} mode (the default) this method returns
     *       immediately with {@link MessageState#RECEIVED} and step execution is dispatched
     *       to a background thread pool. The caller does not wait for any step to run.</li>
     *   <li>In {@link ExecutionMode#SYNCHRONOUS} mode this method blocks on the calling
     *       thread until all currently-executable steps have run, and returns the actual
     *       final {@link MessageState} ({@code PROCESSED}, {@code PARKED}, {@code ERROR},
     *       or {@code IN_PROGRESS} if some steps are still waiting for a retry window).</li>
     * </ul>
     *
     * <p><strong>Using an external JDBC connection ({@link ReceiveOptions#connection()}):</strong>
     * when a connection is supplied via {@link ReceiveOptions}, durable-flow uses it for all
     * persistence operations without committing or closing it. This lets the message insertion
     * and the caller's own business logic share a single database transaction:
     * <pre>{@code
     * // Spring example — transactional message listener
     * @Transactional
     * public void onMessage(String rawMessage) {
     *     Connection conn = DataSourceUtils.getConnection(dataSource);
     *     ReceiveResult result = engine.receive(
     *         new InboundMessage("my-source", rawMessage.getBytes(), Map.of()),
     *         ReceiveOptions.withDeferredExecution(workflow, conn));
     *     // Spring commits conn together with the rest of the transaction
     * }
     * }</pre>
     * Step execution is always dispatched asynchronously when an external connection is
     * provided, because steps must not run before the caller's transaction is committed.
     * Committed steps will be picked up by the immediate background dispatcher or, as a
     * fallback, by the recovery scheduler.
     *
     * <p>Example — ASYNCHRONOUS mode (the default):
     * <pre>{@code
     * // The message is written to the DB and the transaction committed synchronously.
     * // receive() then returns immediately — safe to ACK the queue message right here.
     * ReceiveResult result = engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
     * // result.messageState() == MessageState.RECEIVED
     * queue.acknowledge(message);  // safe: message is already durable in the DB
     * // Steps run in the background thread pool concurrently
     * }</pre>
     *
     * <p>Example — SYNCHRONOUS mode:
     * <pre>{@code
     * // The message is written to the DB and then steps execute on this thread.
     * // receive() blocks until all currently-executable steps have finished.
     * ReceiveResult result = engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
     * // result.messageState() == PROCESSED | PARKED | IN_PROGRESS | ERROR
     * }</pre>
     */
    public ReceiveResult receive(InboundMessage message, ReceiveOptions options) {
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(options, "options must not be null");

        metricsListener.onMessageReceived();

        MessagePreprocessor preprocessor = options.preprocessor() != null
                ? options.preprocessor()
                : DefaultMessagePreprocessor.INSTANCE;

        PreprocessResult preprocessed = preprocessor.preprocess(message);
        String dedupeHash = computeHash(preprocessed.getCanonicalBytes());
        long payloadLength = preprocessed.getCanonicalBytes().length;

        WorkflowDefinition workflow = options.workflow();
        workflowRegistry.register(workflow);

        final String messageId;
        Connection externalConn = options.connection();

        if (externalConn != null) {
            // Use the caller-provided connection without committing or closing it.
            // Transaction lifecycle is managed entirely by the caller.
            try {
                MessageRecord record = buildMessageRecord(message, preprocessed, dedupeHash, payloadLength, workflow);
                InsertResult insertResult = messageRepository.insertMessage(externalConn, record);

                if (insertResult.isDuplicate()) {
                    metricsListener.onDuplicateMessage();
                    log.debug("Duplicate message detected: source={} hash={}", message.source(), dedupeHash);
                    return new ReceiveResult(insertResult.getMessageId(), true, insertResult.getExistingState());
                }

                messageId = insertResult.getMessageId();
                stepRepository.insertSteps(externalConn, messageId, workflow.getSteps());
                log.debug("Message persisted (external connection): id={} workflow={}", messageId, workflow.getName());

            } catch (Exception e) {
                throw new RuntimeException("Failed to persist message: " + e.getMessage(), e);
            }

            // Steps must be dispatched asynchronously: they cannot run before the caller
            // commits the external transaction that makes the message and steps visible.
            // When deferExecution is set the caller is responsible for triggering dispatch
            // (e.g. from an after-commit callback) via dispatchSteps().
            if (!options.deferExecution()) {
                submitSteps(messageId, workflow);
            }
            return new ReceiveResult(messageId, false, MessageState.RECEIVED);

        } else {
            // Default path: acquire a connection, persist in a transaction, commit, and release.
            // Always synchronous: persist the message and its steps to the database and commit.
            // The connection is fully closed before any step execution begins, ensuring the
            // message is durable before the caller can acknowledge it to a queue/topic.
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                try {
                    MessageRecord record = buildMessageRecord(message, preprocessed, dedupeHash, payloadLength, workflow);
                    InsertResult insertResult = messageRepository.insertMessage(conn, record);

                    if (insertResult.isDuplicate()) {
                        conn.rollback();
                        metricsListener.onDuplicateMessage();
                        log.debug("Duplicate message detected: source={} hash={}", message.source(), dedupeHash);
                        return new ReceiveResult(insertResult.getMessageId(), true, insertResult.getExistingState());
                    }

                    messageId = insertResult.getMessageId();
                    stepRepository.insertSteps(conn, messageId, workflow.getSteps());
                    conn.commit();

                    log.debug("Message persisted: id={} workflow={}", messageId, workflow.getName());

                } catch (Exception e) {
                    conn.rollback();
                    throw new RuntimeException("Failed to persist message: " + e.getMessage(), e);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Database connection error", e);
            }

            // Execute eligible steps: on the calling thread (SYNCHRONOUS) or dispatched to the
            // background executor (ASYNCHRONOUS). The persistence connection has already been
            // closed, so it does not compete with execution-time connections.
            if (config.getExecutionMode() == ExecutionMode.SYNCHRONOUS) {
                // Execute steps on the calling thread and return the actual final state
                MessageState finalState = orchestrator.executeEligibleSteps(messageId, workflow);
                return new ReceiveResult(messageId, false, finalState);
            } else {
                // Best-effort immediate step execution in the background (default)
                submitSteps(messageId, workflow);
                return new ReceiveResult(messageId, false, MessageState.RECEIVED);
            }
        }
    }

    /**
     * Convenience overload of {@link #receive(InboundMessage, ReceiveOptions)} for text payloads.
     *
     * <p>The {@code textPayload} string is encoded to bytes using UTF-8 internally, so callers
     * that receive messages as plain text (e.g. a JMS {@code @JmsListener(String)}) do not need
     * to perform the conversion themselves:
     *
     * <pre>{@code
     * @JmsListener(destination = "orders")
     * @Transactional
     * public void onMessage(String rawMessage) {
     *     Connection conn = DataSourceUtils.getConnection(dataSource);
     *     engine.receive("orders", rawMessage, Map.of(),
     *         ReceiveOptions.withDeferredExecution(orderWorkflow, conn));
     * }
     * }</pre>
     *
     * @param source      logical source identifier (e.g. queue name, topic)
     * @param textPayload raw text payload; encoded to UTF-8 bytes before storage
     * @param headers     optional transport-level headers
     * @param options     receive options (workflow, optional external connection, etc.)
     * @return the receive result
     */
    public ReceiveResult receive(String source, String textPayload, Map<String, String> headers, ReceiveOptions options) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(textPayload, "textPayload must not be null");
        return receive(
                new InboundMessage(source, textPayload.getBytes(StandardCharsets.UTF_8), headers),
                options);
    }

    /**
     * Returns the current status of a message and its steps.
     */
    public Optional<MessageStatus> getMessageStatus(String messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Optional<MessageRecord> record = messageRepository.findById(messageId);
        if (record.isEmpty()) {
            return Optional.empty();
        }
        MessageRecord msg = record.get();
        List<StepRecord> steps = stepRepository.findStepsForMessage(messageId);
        List<MessageStatus.StepStatus> stepStatuses = steps.stream()
                .map(s -> new MessageStatus.StepStatus(s.getStepName(), s.getStepState(), s.getAttemptCount(), s.getLastError()))
                .toList();
        return Optional.of(new MessageStatus(
                msg.getId(), msg.getMessageState(), msg.getWorkflowName(),
                msg.getCreatedAt(), msg.getUpdatedAt(), stepStatuses));
    }

    /**
     * Re-drives a PARKED message by resetting FAILED_FINAL steps to PENDING.
     */
    public void redrive(String messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Optional<MessageRecord> record = messageRepository.findById(messageId);
        if (record.isEmpty()) {
            throw new IllegalArgumentException("Message not found: " + messageId);
        }
        if (record.get().getMessageState() != MessageState.PARKED) {
            throw new IllegalStateException("Only PARKED messages can be redriven. Current state: "
                    + record.get().getMessageState());
        }
        stepRepository.resetFailedFinalSteps(messageId);
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                messageRepository.updateMessageState(conn, messageId, MessageState.IN_PROGRESS);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw new RuntimeException("Failed to redrive message", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Database connection error", e);
        }
        String workflowName = record.get().getWorkflowName();
        WorkflowDefinition workflow = workflowRegistry.find(workflowName)
                .orElseThrow(() -> new IllegalStateException("Workflow not registered: " + workflowName));
        executorService.submit(() -> orchestrator.executeEligibleSteps(messageId, workflow));
    }

    /**
     * Dispatches eligible steps for the given message to the background thread pool.
     *
     * <p>This method is intended to be called from an after-commit callback when using an
     * external connection with {@link ReceiveOptions#withDeferredExecution(WorkflowDefinition, java.sql.Connection)}.
     * It guarantees that steps are dispatched only after the external transaction has been
     * committed and its rows are visible to other database connections.
     *
     * <pre>{@code
     * // In a Spring Boot @Transactional method:
     * TransactionSynchronizationManager.registerSynchronization(
     *     new TransactionSynchronization() {
     *         public void afterCommit() {
     *             engine.dispatchSteps(result.messageId(), workflow);
     *         }
     *     });
     * }</pre>
     *
     * @param messageId the ID returned by {@link #receive(InboundMessage, ReceiveOptions)}
     * @param workflow  the workflow definition associated with the message
     */
    public void dispatchSteps(String messageId, WorkflowDefinition workflow) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(workflow, "workflow must not be null");
        workflowRegistry.register(workflow);
        submitSteps(messageId, workflow);
    }

    /** Starts the background recovery scheduler. */
    public void start() {
        if (!started) {
            recoveryScheduler.start();
            started = true;
            log.info("DurableFlowEngine started. nodeId={}", config.getNodeId());
        }
    }

    @Override
    public void close() {
        recoveryScheduler.stop();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("DurableFlowEngine closed. nodeId={}", config.getNodeId());
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void submitSteps(String messageId, WorkflowDefinition workflow) {
        executorService.submit(() -> orchestrator.executeEligibleSteps(messageId, workflow));
    }

    private static void runMigrations(DataSource dataSource, DatabaseDialect dialect) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(dialect.flywayLocation())
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        flyway.migrate();
    }

    private static String computeHash(byte[] bytes) {
        long hi = LongHashFunction.xx3().hashBytes(bytes);
        // Use seeded call for low 64 bits
        long lo = LongHashFunction.xx3(0xDEAD_BEEF_CAFE_BABEL).hashBytes(bytes);
        return String.format("%016x%016x", hi, lo);
    }

    private static MessageRecord buildMessageRecord(
            InboundMessage message,
            PreprocessResult preprocessed,
            String dedupeHash,
            long payloadLength,
            WorkflowDefinition workflow) {
        MessageRecord r = new MessageRecord();
        r.setId(UUID.randomUUID().toString());
        r.setSource(message.source());
        r.setDedupeHash(dedupeHash);
        r.setPayloadLength(payloadLength);
        r.setPayloadStorageMode(preprocessed.getPayloadStorageMode());
        r.setPayloadData(preprocessed.getStoredPayload());
        r.setMessageState(MessageState.RECEIVED);
        r.setCreatedAt(Instant.now());
        r.setUpdatedAt(Instant.now());
        r.setMetadata(preprocessed.getMetadata());
        r.setWorkflowName(workflow.getName());
        return r;
    }
}
