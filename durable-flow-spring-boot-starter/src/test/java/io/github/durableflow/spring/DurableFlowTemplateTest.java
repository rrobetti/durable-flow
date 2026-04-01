package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.ExecutionMode;
import io.github.durableflow.api.InboundMessage;
import io.github.durableflow.api.MessageState;
import io.github.durableflow.api.ReceiveOptions;
import io.github.durableflow.api.ReceiveResult;
import io.github.durableflow.api.WorkflowDefinition;
import io.github.durableflow.api.StepResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link DurableFlowTemplate} routing logic.
 *
 * <p>These tests verify that the template selects the correct
 * {@link ReceiveOptions} strategy depending on the active transaction state
 * and the configured {@link ExecutionMode}, without hitting a real database.
 */
@ExtendWith(MockitoExtension.class)
class DurableFlowTemplateTest {

    @Mock
    private DurableFlowEngine engine;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection mockConnection;

    private static final WorkflowDefinition WORKFLOW =
            WorkflowDefinition.builder("test-wf")
                    .step("step1", ctx -> StepResult.empty())
                    .build();

    private static final InboundMessage MESSAGE =
            new InboundMessage("src", "payload".getBytes(), Map.of());

    private static final ReceiveResult DUMMY_RESULT =
            new ReceiveResult("msg-1", false, MessageState.RECEIVED);

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
        if (TransactionSynchronizationManager.hasResource(dataSource)) {
            TransactionSynchronizationManager.unbindResource(dataSource);
        }
    }

    // -------------------------------------------------------------------------
    // No active transaction — engine self-manages regardless of mode
    // -------------------------------------------------------------------------

    @Test
    void noTransaction_asyncMode_usesNoConnectionPath() {
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = asyncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        ReceiveOptions captured = captureOptions();
        assertNull(captured.connection(), "No external connection expected outside a transaction");
        assertFalse(captured.deferExecution());
    }

    @Test
    void noTransaction_syncMode_usesNoConnectionPath() {
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = syncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        ReceiveOptions captured = captureOptions();
        assertNull(captured.connection(), "No external connection expected outside a transaction");
        assertFalse(captured.deferExecution());
    }

    // -------------------------------------------------------------------------
    // Active transaction — ASYNCHRONOUS mode: borrow connection + afterCommit
    // -------------------------------------------------------------------------

    @Test
    void withinTransaction_asyncMode_borrowsConnectionAndRegistersAfterCommit() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = asyncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        ReceiveOptions captured = captureOptions();
        assertNotNull(captured.connection(), "External connection must be borrowed in ASYNC mode");
        assertTrue(captured.deferExecution(), "Execution must be deferred until afterCommit in ASYNC mode");

        List<TransactionSynchronization> syncs =
                TransactionSynchronizationManager.getSynchronizations();
        assertEquals(1, syncs.size(), "Exactly one afterCommit synchronization must be registered");
    }

    @Test
    void withinTransaction_asyncMode_afterCommitCallsDispatchSteps() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = asyncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        // Simulate the transaction committing
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(TransactionSynchronization::afterCommit);

        verify(engine).dispatchSteps(DUMMY_RESULT.messageId(), WORKFLOW);
    }

    @Test
    void withinTransaction_asyncMode_noDispatchOnRollback() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = asyncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        // afterCommit is NOT called (simulate rollback path)
        verify(engine, never()).dispatchSteps(any(), any());
    }

    // -------------------------------------------------------------------------
    // Active transaction — SYNCHRONOUS mode: bypass outer transaction
    // -------------------------------------------------------------------------

    @Test
    void withinTransaction_syncMode_bypassesOuterTransaction() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = syncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        ReceiveOptions captured = captureOptions();
        assertNull(captured.connection(),
                "SYNCHRONOUS mode must bypass the outer transaction and use no external connection");
        assertFalse(captured.deferExecution());
    }

    @Test
    void withinTransaction_syncMode_doesNotRegisterAfterCommit() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = syncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        List<TransactionSynchronization> syncs =
                TransactionSynchronizationManager.getSynchronizations();
        assertTrue(syncs.isEmpty(),
                "SYNCHRONOUS mode must not register any afterCommit synchronization");
    }

    @Test
    void withinTransaction_syncMode_doesNotCallDispatchSteps() {
        activateTransaction();
        when(engine.receive(eq(MESSAGE), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = syncTemplate();
        template.receive(MESSAGE, WORKFLOW);

        verify(engine, never()).dispatchSteps(any(), any());
    }

    // -------------------------------------------------------------------------
    // Text-payload convenience overload delegates to the message overload
    // -------------------------------------------------------------------------

    @Test
    void textPayloadOverload_noTransaction_usesNoConnectionPath() {
        when(engine.receive(any(InboundMessage.class), any(ReceiveOptions.class))).thenReturn(DUMMY_RESULT);

        DurableFlowTemplate template = asyncTemplate();
        template.receive("src", "payload", Map.of(), WORKFLOW);

        ReceiveOptions captured = captureOptions();
        assertNull(captured.connection());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private DurableFlowTemplate asyncTemplate() {
        DurableFlowConfig config = DurableFlowConfig.builder()
                .executionMode(ExecutionMode.ASYNCHRONOUS)
                .schemaAutoMigrate(false)
                .build();
        return new DurableFlowTemplate(engine, dataSource, config);
    }

    private DurableFlowTemplate syncTemplate() {
        DurableFlowConfig config = DurableFlowConfig.builder()
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .schemaAutoMigrate(false)
                .build();
        return new DurableFlowTemplate(engine, dataSource, config);
    }

    private void activateTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.bindResource(dataSource, new ConnectionHolder(mockConnection));
    }

    private ReceiveOptions captureOptions() {
        ArgumentCaptor<ReceiveOptions> captor = ArgumentCaptor.forClass(ReceiveOptions.class);
        verify(engine).receive(any(InboundMessage.class), captor.capture());
        return captor.getValue();
    }
}
