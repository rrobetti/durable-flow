package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.ExecutionMode;
import io.github.durableflow.api.InboundMessage;
import io.github.durableflow.api.ReceiveOptions;
import io.github.durableflow.api.ReceiveResult;
import io.github.durableflow.api.WorkflowDefinition;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Map;

/**
 * Spring-aware facade over {@link DurableFlowEngine#receive} that automatically handles
 * transaction integration.
 *
 * <p>Use this bean instead of calling {@code engine.receive()} directly.  It inspects the
 * current thread's transaction state and the configured {@link ExecutionMode}, then chooses
 * the correct receive strategy:
 *
 * <ul>
 *   <li><b>Outside any transaction (any mode)</b> — delegates to
 *       {@link ReceiveOptions#withDeferredExecution(WorkflowDefinition)} so the engine manages
 *       its own connection, commits the insert, and dispatches steps immediately (asynchronously)
 *       or runs them on the calling thread (synchronously).</li>
 *   <li><b>Inside a {@code @Transactional} method — {@link ExecutionMode#ASYNCHRONOUS} mode
 *       (default)</b> — borrows the connection already bound to the active transaction so the
 *       durable-flow insert is atomically part of the surrounding transaction, and registers an
 *       {@code afterCommit} synchronization that calls {@link DurableFlowEngine#dispatchSteps}
 *       only after the transaction commits successfully.  If the transaction rolls back, neither
 *       the insert nor the dispatch ever happens.</li>
 *   <li><b>Inside a {@code @Transactional} method — {@link ExecutionMode#SYNCHRONOUS} mode</b>
 *       — the outer transaction is intentionally bypassed.  The engine opens its own JDBC
 *       connection, persists and commits the message independently, and then runs all eligible
 *       steps on the calling thread before returning.  This gives the caller a fully-resolved
 *       {@link io.github.durableflow.api.MessageState} immediately after {@code receive()}
 *       returns.
 *       <br><em>Note:</em> because the message is committed in a separate transaction, a
 *       subsequent rollback of the outer transaction will <strong>not</strong> undo the message
 *       insert.  If transactional atomicity between your business write and the workflow trigger
 *       matters, use {@link ExecutionMode#ASYNCHRONOUS} mode instead.</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * @Autowired
 * private DurableFlowTemplate durableFlow;
 *
 * // ASYNCHRONOUS mode (default) — atomic insert + guaranteed post-commit dispatch
 * @JmsListener(destination = "${orders.topic}")
 * @Transactional
 * public void onMessage(String rawJson) {
 *     durableFlow.receive("orders", rawJson, Map.of(), orderWorkflowDefinition);
 *     // afterCommit is registered automatically; workflow never starts on rollback
 * }
 *
 * // SYNCHRONOUS mode — steps run on the calling thread; outer transaction is bypassed
 * @PostMapping("/orders")
 * @Transactional
 * public ResponseEntity<String> placeOrder(@RequestBody String rawJson) {
 *     ReceiveResult result = durableFlow.receive("orders", rawJson, Map.of(), orderWorkflowDefinition);
 *     // receive() blocks until all steps have finished; result.messageState() is the final state
 *     return result.messageState() == MessageState.SUCCEEDED
 *         ? ResponseEntity.ok(result.messageId())
 *         : ResponseEntity.internalServerError().body("Workflow ended in state: " + result.messageState());
 * }
 * }</pre>
 */
public class DurableFlowTemplate {

    private final DurableFlowEngine engine;
    private final DataSource dataSource;
    private final DurableFlowConfig config;

    public DurableFlowTemplate(DurableFlowEngine engine, DataSource dataSource, DurableFlowConfig config) {
        this.engine = engine;
        this.dataSource = dataSource;
        this.config = config;
    }

    /**
     * Receives {@code message} into the given {@code workflow}, automatically integrating
     * with any active Spring transaction.
     *
     * @param message  the inbound message to persist and process
     * @param workflow the workflow definition that describes how to process the message
     * @return the result of the receive operation, including the stable message ID
     */
    public ReceiveResult receive(InboundMessage message, WorkflowDefinition workflow) {
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && config.getExecutionMode() != ExecutionMode.SYNCHRONOUS) {
            return receiveWithinTransaction(message, workflow);
        }
        return engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
    }

    /**
     * Convenience overload for text payloads — the {@code textPayload} string is encoded
     * to UTF-8 bytes internally, so callers that receive messages as plain text
     * (e.g. a JMS {@code @JmsListener(String)}) do not need to perform the conversion
     * themselves.
     *
     * @param source      logical source identifier (e.g. queue name, topic)
     * @param textPayload raw text payload; encoded to UTF-8 bytes before storage
     * @param headers     optional transport-level headers
     * @param workflow    the workflow definition that describes how to process the message
     * @return the result of the receive operation, including the stable message ID
     */
    public ReceiveResult receive(String source, String textPayload,
                                 Map<String, String> headers, WorkflowDefinition workflow) {
        return receive(
                new InboundMessage(source, textPayload.getBytes(StandardCharsets.UTF_8), headers),
                workflow);
    }

    private ReceiveResult receiveWithinTransaction(InboundMessage message, WorkflowDefinition workflow) {
        Connection conn = DataSourceUtils.getConnection(dataSource);
        ReceiveResult result = engine.receive(message, ReceiveOptions.withDeferredExecution(workflow, conn));

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                engine.dispatchSteps(result.messageId(), workflow);
            }
        });

        return result;
    }
}
