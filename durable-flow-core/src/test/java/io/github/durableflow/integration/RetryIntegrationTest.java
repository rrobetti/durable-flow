package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryIntegrationTest extends BaseIntegrationTest {

    @Test
    void failingStep_isRetriedUpToMaxAttempts() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        RetryPolicy policy = RetryPolicy.fixedDelay(3, java.time.Duration.ofMillis(200));
        WorkflowDefinition wf = WorkflowDefinition.builder("retry-wf")
                .step("failing", ctx -> {
                    int attempt = attempts.incrementAndGet();
                    if (attempt < 3) {
                        throw new RuntimeException("Simulated failure attempt " + attempt);
                    }
                    latch.countDown();
                    return StepResult.empty();
                }).retryPolicy(policy)
                .build();

        ReceiveResult result = engine.receive(message("retry-src", "retry-data"), ReceiveOptions.of(wf));

        assertTrue(latch.await(20, TimeUnit.SECONDS), "Step should eventually succeed after retries");
        waitFor(500);

        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PROCESSED, status.get().getMessageState());
    }

    @Test
    void stepExceedingMaxAttempts_parksMessage() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch failureLatch = new CountDownLatch(3);

        RetryPolicy policy = RetryPolicy.fixedDelay(3, java.time.Duration.ofMillis(100));
        WorkflowDefinition wf = WorkflowDefinition.builder("park-wf")
                .step("always-fail", ctx -> {
                    attempts.incrementAndGet();
                    failureLatch.countDown();
                    throw new RuntimeException("Always fails");
                }).retryPolicy(policy)
                .build();

        ReceiveResult result = engine.receive(message("park-src", "park-data"), ReceiveOptions.of(wf));
        assertTrue(failureLatch.await(30, TimeUnit.SECONDS), "Should exhaust all attempts");
        waitFor(1000);

        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PARKED, status.get().getMessageState());
    }

    @Test
    void noRetryPolicy_failsImmediately() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        WorkflowDefinition wf = WorkflowDefinition.builder("no-retry-wf")
                .step("no-retry", ctx -> {
                    latch.countDown();
                    throw new RuntimeException("Fail once");
                }).retryPolicy(RetryPolicy.noRetry())
                .build();

        ReceiveResult result = engine.receive(message("no-retry-src", "data"), ReceiveOptions.of(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        waitFor(500);

        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PARKED, status.get().getMessageState());
    }
}
