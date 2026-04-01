package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.InboundMessage;
import io.github.durableflow.api.ReceiveOptions;
import io.github.durableflow.api.ReceiveResult;
import io.github.durableflow.api.WorkflowDefinition;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * Spring-aware facade over {@link DurableFlowEngine#receive} that automatically handles
 * transaction integration.
 *
 * <p>Use this bean instead of calling {@code engine.receive()} directly.  It inspects the
 * current thread's transaction state and chooses the correct receive strategy:
 *
 * <ul>
 *   <li><b>Inside a {@code @Transactional} method</b> — borrows the connection already bound
 *       to the active transaction, calls
 *       {@link ReceiveOptions#withDeferredExecution(WorkflowDefinition, Connection)} so the
 *       durable-flow insert is atomically part of the surrounding transaction, and registers
 *       an {@code afterCommit} synchronization that calls
 *       {@link DurableFlowEngine#dispatchSteps} only after the transaction commits
 *       successfully.  If the transaction rolls back, neither the insert nor the dispatch
 *       ever happens.</li>
 *   <li><b>Outside any transaction</b> — delegates to
 *       {@link ReceiveOptions#withDeferredExecution(WorkflowDefinition)} so the engine manages
 *       its own connection, commits the insert, and dispatches steps immediately.</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * @Autowired
 * private DurableFlowTemplate durableFlow;
 *
 * // Pattern C — atomic insert + guaranteed post-commit dispatch, zero boilerplate
 * @Transactional
 * public void placeOrder(Order order) {
 *     orderRepository.save(order);
 *     durableFlow.receive(
 *         new InboundMessage("orders", serialize(order), Map.of()),
 *         orderWorkflowDefinition);
 *     // afterCommit is registered automatically; workflow never starts on rollback
 * }
 *
 * // Pattern A — no active transaction, engine self-manages
 * @JmsListener(destination = "${orders.topic}")
 * public void onMessage(String rawJson) {
 *     durableFlow.receive(
 *         new InboundMessage("orders", rawJson.getBytes(), Map.of()),
 *         orderWorkflowDefinition);
 * }
 * }</pre>
 *
 * <p>For the rare Pattern B case (immediate step dispatch inside a transaction, before
 * commit), call {@link DurableFlowEngine#receive} directly with
 * {@link ReceiveOptions#withDeferredExecution(WorkflowDefinition, Connection)}.
 */
public class DurableFlowTemplate {

    private final DurableFlowEngine engine;
    private final DataSource dataSource;

    public DurableFlowTemplate(DurableFlowEngine engine, DataSource dataSource) {
        this.engine = engine;
        this.dataSource = dataSource;
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
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return receiveWithinTransaction(message, workflow);
        }
        return engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
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
