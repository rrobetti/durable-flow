package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DurableFlowEngineIntegrationTest extends BaseIntegrationTest {

    @Test
    void receive_persistsMessageAndReturnsId() {
        WorkflowDefinition wf = singleStepWorkflow("basic", ctx -> StepResult.empty());
        ReceiveResult result = engine.receive(message("source1", "payload"), ReceiveOptions.of(wf));

        assertNotNull(result.messageId());
        assertFalse(result.duplicate());
        assertEquals(MessageState.RECEIVED, result.messageState());
    }

    @Test
    void receive_executesStepSuccessfully() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = singleStepWorkflow("exec-test", ctx -> {
            latch.countDown();
            return StepResult.empty();
        });

        engine.receive(message("exec-src", "data"), ReceiveOptions.of(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should have executed");
    }

    @Test
    void getMessageStatus_returnsCurrentState() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = singleStepWorkflow("status-test", ctx -> {
            latch.countDown();
            return StepResult.empty();
        });

        ReceiveResult result = engine.receive(message("status-src", "test"), ReceiveOptions.of(wf));
        latch.await(10, TimeUnit.SECONDS);
        waitFor(200); // allow state update to propagate

        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PROCESSED, status.get().getMessageState());
    }

    @Test
    void getMessageStatus_notFound_returnsEmpty() {
        Optional<MessageStatus> status = engine.getMessageStatus("non-existent-id");
        assertTrue(status.isEmpty());
    }

    @Test
    void receive_multipleMessages_isolatedExecution() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);
        WorkflowDefinition wf = singleStepWorkflow("multi", ctx -> {
            counter.incrementAndGet();
            latch.countDown();
            return StepResult.empty();
        });

        engine.receive(message("multi-src", "msg1"), ReceiveOptions.of(wf));
        engine.receive(message("multi-src", "msg2"), ReceiveOptions.of(wf));
        engine.receive(message("multi-src", "msg3"), ReceiveOptions.of(wf));

        assertTrue(latch.await(15, TimeUnit.SECONDS), "All steps should execute");
        assertEquals(3, counter.get());
    }
}
