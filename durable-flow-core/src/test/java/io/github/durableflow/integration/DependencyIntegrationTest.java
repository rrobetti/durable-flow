package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class DependencyIntegrationTest extends BaseIntegrationTest {

    @Test
    void dependentStep_executesAfterUpstream() throws Exception {
        CountDownLatch validateLatch = new CountDownLatch(1);
        CountDownLatch enrichLatch = new CountDownLatch(1);
        CountDownLatch notifyLatch = new CountDownLatch(1);

        AtomicReference<byte[]> enrichInput = new AtomicReference<>();

        WorkflowDefinition wf = WorkflowDefinition.builder("dag-wf")
                .step("validate", ctx -> {
                    validateLatch.countDown();
                    return StepResult.of("validated".getBytes(StandardCharsets.UTF_8));
                })
                .step("enrich", ctx -> {
                    enrichInput.set(ctx.getPreviousStepOutputs().get("validate"));
                    enrichLatch.countDown();
                    return StepResult.of("enriched".getBytes(StandardCharsets.UTF_8));
                }).dependsOn("validate")
                .step("notify", ctx -> {
                    notifyLatch.countDown();
                    return StepResult.empty();
                }).dependsOn("enrich")
                .build();

        ReceiveResult result = engine.receive(message("dag-src", "dag-data"), ReceiveOptions.withDeferredExecution(wf));

        assertTrue(validateLatch.await(10, TimeUnit.SECONDS), "validate should run");
        assertTrue(enrichLatch.await(10, TimeUnit.SECONDS), "enrich should run after validate");
        assertTrue(notifyLatch.await(10, TimeUnit.SECONDS), "notify should run after enrich");

        assertNotNull(enrichInput.get(), "enrich should receive validate output");
        assertEquals("validated", new String(enrichInput.get(), StandardCharsets.UTF_8));

        waitFor(500);
        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PROCESSED, status.get().getMessageState());
    }

    @Test
    void parallelSteps_bothExecute() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);

        WorkflowDefinition wf = WorkflowDefinition.builder("parallel-wf")
                .step("parallel-a", ctx -> {
                    latch.countDown();
                    return StepResult.empty();
                })
                .step("parallel-b", ctx -> {
                    latch.countDown();
                    return StepResult.empty();
                })
                .build();

        engine.receive(message("parallel-src", "parallel-data"), ReceiveOptions.withDeferredExecution(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Both parallel steps should execute");
    }

    @Test
    void dependentStep_notExecutedWhenUpstreamFails() throws Exception {
        CountDownLatch downstreamLatch = new CountDownLatch(1);
        RetryPolicy noRetry = RetryPolicy.noRetry();

        WorkflowDefinition wf = WorkflowDefinition.builder("dep-fail-wf")
                .step("upstream", ctx -> {
                    throw new RuntimeException("Upstream fails");
                }).retryPolicy(noRetry)
                .step("downstream", ctx -> {
                    downstreamLatch.countDown();
                    return StepResult.empty();
                }).dependsOn("upstream")
                .build();

        engine.receive(message("dep-fail-src", "data"), ReceiveOptions.withDeferredExecution(wf));

        // downstream should NOT execute since upstream failed
        assertFalse(downstreamLatch.await(3, TimeUnit.SECONDS),
                "Downstream step should not execute when upstream fails permanently");
    }
}
