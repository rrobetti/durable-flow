package io.github.durableflow.integration;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for synchronous/asynchronous execution mode and
 * beforeProcessing / afterProcessing lifecycle hooks.
 */
class ExecutionModeAndLifecycleIntegrationTest extends BaseIntegrationTest {

    // -------------------------------------------------------------------------
    // Execution mode tests
    // -------------------------------------------------------------------------

    @Test
    void synchronousMode_parallelSteps_runConcurrently() throws Exception {
        // Both steps count down the latch and then await it.
        // If steps were sequential, the first step would block forever waiting for the
        // second step to count down — proving that only concurrent execution can pass.
        CountDownLatch bothStarted = new CountDownLatch(2);

        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());

        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("sync-parallel-wf")
                    // Both steps have no dependencies → eligible together in the first wave
                    .step("parallel-a", ctx -> {
                        bothStarted.countDown();
                        assertTrue(bothStarted.await(10, TimeUnit.SECONDS),
                                "parallel-a timed out waiting for parallel-b to start");
                        return StepResult.empty();
                    })
                    .step("parallel-b", ctx -> {
                        bothStarted.countDown();
                        assertTrue(bothStarted.await(10, TimeUnit.SECONDS),
                                "parallel-b timed out waiting for parallel-a to start");
                        return StepResult.empty();
                    })
                    .build();

            ReceiveResult result = syncEngine.receive(
                    message("sync-parallel-src", "data"), ReceiveOptions.withDeferredExecution(wf));

            assertEquals(MessageState.PROCESSED, result.messageState(),
                    "SYNCHRONOUS receive must return PROCESSED after all parallel steps complete");
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void synchronousMode_receiveBlocksUntilCompletion() throws Exception {
        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());

        try {
            syncEngine.start();
            WorkflowDefinition wf = singleStepWorkflow("sync-wf", ctx -> StepResult.empty());
            ReceiveResult result = syncEngine.receive(message("sync-src", "sync-payload"), ReceiveOptions.withDeferredExecution(wf));

            // In synchronous mode the result state must be PROCESSED, not just RECEIVED
            assertEquals(MessageState.PROCESSED, result.messageState(),
                    "Synchronous receive must return PROCESSED state after all steps complete");
            assertFalse(result.duplicate());
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void asynchronousMode_receiveReturnsReceivedImmediately() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = singleStepWorkflow("async-wf", ctx -> {
            latch.countDown();
            return StepResult.empty();
        });

        // Default mode is ASYNCHRONOUS
        ReceiveResult result = engine.receive(message("async-src", "async-payload"), ReceiveOptions.withDeferredExecution(wf));

        // Return value should be RECEIVED immediately (before async execution finishes)
        assertEquals(MessageState.RECEIVED, result.messageState());
        assertFalse(result.duplicate());

        // Verify that async execution does eventually happen
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should execute asynchronously");
    }

    @Test
    void asynchronousMode_messageIsPersistedSynchronously() throws Exception {
        // Use a latch to freeze the background step execution so it cannot complete
        // before we inspect the database. This proves that receive() persisted the
        // message synchronously — before any background work began.
        CountDownLatch stepStartLatch = new CountDownLatch(1);
        CountDownLatch stepReleaseLatch = new CountDownLatch(1);

        WorkflowDefinition wf = singleStepWorkflow("async-persist-wf", ctx -> {
            stepStartLatch.countDown();   // signal: background thread has started
            try {
                stepReleaseLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return StepResult.empty();
        });

        ReceiveResult result = engine.receive(message("async-persist-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

        // receive() must have returned with RECEIVED, meaning the message was persisted
        assertEquals(MessageState.RECEIVED, result.messageState());
        assertFalse(result.duplicate());

        // The message must already be in the database with state RECEIVED,
        // even though the background step has not yet completed.
        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent(), "Message must be persisted in the DB before receive() returns");
        assertEquals(MessageState.RECEIVED, status.get().getMessageState(),
                "Message state must be RECEIVED immediately after receive() returns in ASYNC mode");

        // Allow the background step to complete
        stepReleaseLatch.countDown();
        assertTrue(stepStartLatch.await(10, TimeUnit.SECONDS), "Background step should have started");
    }

    @Test
    void synchronousMode_multiStepWorkflow_returnsProcessed() throws Exception {
        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());

        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("sync-multi-wf")
                    .step("validate", ctx -> StepResult.empty())
                    .step("enrich", ctx -> StepResult.empty()).dependsOn("validate")
                    .step("notify", ctx -> StepResult.empty()).dependsOn("enrich")
                    .build();

            ReceiveResult result = syncEngine.receive(
                    message("sync-multi-src", "data"), ReceiveOptions.withDeferredExecution(wf));

            assertEquals(MessageState.PROCESSED, result.messageState(),
                    "Multi-step synchronous workflow should complete and return PROCESSED");
        } finally {
            syncEngine.close();
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle hook tests
    // -------------------------------------------------------------------------

    @Test
    void beforeProcessing_isCalledBeforeSteps() throws Exception {
        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean stepCalledWhileBeforeAlreadyRan = new AtomicBoolean(false);

        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());
        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("lifecycle-before-wf")
                    .beforeProcessing(ctx -> {
                        assertNotNull(ctx.getMessageId());
                        assertEquals("lifecycle-before-wf", ctx.getWorkflowName());
                        assertNull(ctx.getFinalState()); // no final state yet in beforeProcessing
                        beforeCalled.set(true);
                    })
                    .step("step1", ctx -> {
                        stepCalledWhileBeforeAlreadyRan.set(beforeCalled.get());
                        return StepResult.empty();
                    })
                    .build();

            syncEngine.receive(message("lifecycle-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

            assertTrue(beforeCalled.get(), "beforeProcessing hook must be called");
            assertTrue(stepCalledWhileBeforeAlreadyRan.get(),
                    "beforeProcessing must run before steps execute");
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void afterProcessing_isCalledAfterStepsWithFinalState() throws Exception {
        AtomicReference<MessageState> capturedFinalState = new AtomicReference<>();
        AtomicBoolean afterCalled = new AtomicBoolean(false);

        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());
        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("lifecycle-after-wf")
                    .step("step1", ctx -> StepResult.empty())
                    .afterProcessing(ctx -> {
                        assertNotNull(ctx.getMessageId());
                        assertEquals("lifecycle-after-wf", ctx.getWorkflowName());
                        capturedFinalState.set(ctx.getFinalState());
                        afterCalled.set(true);
                    })
                    .build();

            syncEngine.receive(message("lifecycle-after-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

            assertTrue(afterCalled.get(), "afterProcessing hook must be called");
            assertEquals(MessageState.PROCESSED, capturedFinalState.get(),
                    "afterProcessing must receive the final PROCESSED state");
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void afterProcessing_isCalledEvenWhenStepFails() throws Exception {
        AtomicReference<MessageState> capturedFinalState = new AtomicReference<>();
        AtomicBoolean afterCalled = new AtomicBoolean(false);

        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());
        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("lifecycle-fail-wf")
                    .step("failing-step", ctx -> { throw new RuntimeException("intentional failure"); })
                    .retryPolicy(RetryPolicy.noRetry())
                    .afterProcessing(ctx -> {
                        capturedFinalState.set(ctx.getFinalState());
                        afterCalled.set(true);
                    })
                    .build();

            syncEngine.receive(message("lifecycle-fail-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

            assertTrue(afterCalled.get(), "afterProcessing must be called even when a step fails");
            // PARKED because noRetry means step goes FAILED_FINAL → message PARKED
            assertEquals(MessageState.PARKED, capturedFinalState.get(),
                    "afterProcessing must receive PARKED when a step permanently fails");
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void lifecycleHookException_doesNotAbortWorkflow() throws Exception {
        AtomicBoolean stepExecuted = new AtomicBoolean(false);

        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());
        try {
            syncEngine.start();
            WorkflowDefinition wf = WorkflowDefinition.builder("lifecycle-exc-wf")
                    .beforeProcessing(ctx -> { throw new RuntimeException("hook exception must be swallowed"); })
                    .step("step1", ctx -> {
                        stepExecuted.set(true);
                        return StepResult.empty();
                    })
                    .afterProcessing(ctx -> { throw new RuntimeException("after hook exception also swallowed"); })
                    .build();

            ReceiveResult result = syncEngine.receive(
                    message("lifecycle-exc-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

            assertTrue(stepExecuted.get(), "Steps must run even if beforeProcessing hook throws");
            assertEquals(MessageState.PROCESSED, result.messageState(),
                    "Workflow must complete even if lifecycle hooks throw");
        } finally {
            syncEngine.close();
        }
    }

    @Test
    void asyncMode_lifecycleHooks_areCalledDuringBackgroundExecution() throws Exception {
        CountDownLatch afterLatch = new CountDownLatch(1);
        AtomicReference<MessageState> capturedFinalState = new AtomicReference<>();

        WorkflowDefinition wf = WorkflowDefinition.builder("async-lifecycle-wf")
                .beforeProcessing(ctx -> assertNotNull(ctx.getMessageId()))
                .step("step1", ctx -> StepResult.empty())
                .afterProcessing(ctx -> {
                    capturedFinalState.set(ctx.getFinalState());
                    afterLatch.countDown();
                })
                .build();

        engine.receive(message("async-hook-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

        assertTrue(afterLatch.await(10, TimeUnit.SECONDS),
                "afterProcessing hook must be called during async execution");
        assertEquals(MessageState.PROCESSED, capturedFinalState.get());
    }

    @Test
    void synchronousMode_receiveResult_messageStateIsAccurate() throws Exception {
        DurableFlowEngine syncEngine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(false)
                .executionMode(ExecutionMode.SYNCHRONOUS)
                .build());
        try {
            syncEngine.start();
            WorkflowDefinition wf = singleStepWorkflow("sync-state-wf", ctx -> StepResult.empty());

            ReceiveResult result = syncEngine.receive(
                    message("sync-state-src", "payload"), ReceiveOptions.withDeferredExecution(wf));

            // Verify the DB also agrees
            Optional<MessageStatus> status = syncEngine.getMessageStatus(result.messageId());
            assertTrue(status.isPresent());
            assertEquals(result.messageState(), status.get().getMessageState(),
                    "ReceiveResult state must match the persisted message state");
        } finally {
            syncEngine.close();
        }
    }
}
