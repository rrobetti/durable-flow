package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DurableFlowEngineIntegrationTest extends BaseIntegrationTest {

    @Test
    void receive_persistsMessageAndReturnsId() {
        WorkflowDefinition wf = singleStepWorkflow("basic", ctx -> StepResult.empty());
        ReceiveResult result = engine.receive(message("source1", "payload"), ReceiveOptions.withDeferredExecution(wf));

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

        engine.receive(message("exec-src", "data"), ReceiveOptions.withDeferredExecution(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should have executed");
    }

    @Test
    void getMessageStatus_returnsCurrentState() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = singleStepWorkflow("status-test", ctx -> {
            latch.countDown();
            return StepResult.empty();
        });

        ReceiveResult result = engine.receive(message("status-src", "test"), ReceiveOptions.withDeferredExecution(wf));
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

        engine.receive(message("multi-src", "msg1"), ReceiveOptions.withDeferredExecution(wf));
        engine.receive(message("multi-src", "msg2"), ReceiveOptions.withDeferredExecution(wf));
        engine.receive(message("multi-src", "msg3"), ReceiveOptions.withDeferredExecution(wf));

        assertTrue(latch.await(15, TimeUnit.SECONDS), "All steps should execute");
        assertEquals(3, counter.get());
    }

    // ------------------------------------------------------------------
    // String payload convenience overload
    // ------------------------------------------------------------------

    @Test
    void receive_stringPayload_persistsAndExecutesStep() throws Exception {
        AtomicReference<byte[]> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = singleStepWorkflow("str-payload", ctx -> {
            captured.set(ctx.getPayload());
            latch.countDown();
            return StepResult.empty();
        });

        String text = "hello from queue";
        ReceiveResult result = engine.receive("orders", text, Map.of("x-src", "test"), ReceiveOptions.withDeferredExecution(wf));

        assertNotNull(result.messageId());
        assertFalse(result.duplicate());
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should execute");
        assertEquals(text, new String(captured.get(), java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void receive_stringPayload_deduplication_returnsDuplicate() {
        WorkflowDefinition wf = singleStepWorkflow("str-dup", ctx -> StepResult.empty());

        String text = "duplicate-text";
        ReceiveResult first  = engine.receive("src", text, Map.of(), ReceiveOptions.withDeferredExecution(wf));
        ReceiveResult second = engine.receive("src", text, Map.of(), ReceiveOptions.withDeferredExecution(wf));

        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertEquals(first.messageId(), second.messageId());
    }
}
