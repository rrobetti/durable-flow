package io.github.durableflow.integration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the MySQL/MariaDB dialect, running against a real MariaDB container.
 */
@Tag("integration")
@Testcontainers
class MariaDbIntegrationTest {

    @Container
    static final MariaDBContainer<?> MARIADB = new MariaDBContainer<>("mariadb:10.11")
            .withDatabaseName("durable_flow_test")
            .withUsername("test")
            .withPassword("test");

    private HikariDataSource dataSource;
    private DurableFlowEngine engine;

    @BeforeEach
    void setUp() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(MARIADB.getJdbcUrl());
        cfg.setUsername(MARIADB.getUsername());
        cfg.setPassword(MARIADB.getPassword());
        cfg.setMaximumPoolSize(10);
        cfg.setAutoCommit(false);
        dataSource = new HikariDataSource(cfg);

        engine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .leaseTimeoutSeconds(5)
                .recoveryIntervalSeconds(2)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(true)
                .build());
        engine.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) engine.close();
        if (dataSource != null) dataSource.close();
    }

    private static InboundMessage message(String source, String body) {
        return new InboundMessage(source, body.getBytes(), java.util.Map.of());
    }

    private static WorkflowDefinition singleStepWorkflow(String name, StepHandler handler) {
        return WorkflowDefinition.builder(name)
                .step("step1", handler)
                .build();
    }

    private void waitFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

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
        waitFor(200);

        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
        assertEquals(MessageState.PROCESSED, status.get().getMessageState());
    }

    @Test
    void samePayload_sameSource_isDeduplicated() {
        AtomicInteger execCount = new AtomicInteger(0);
        WorkflowDefinition wf = singleStepWorkflow("dedupe-wf", ctx -> {
            execCount.incrementAndGet();
            return StepResult.empty();
        });

        ReceiveResult r1 = engine.receive(message("src", "same-payload"), ReceiveOptions.of(wf));
        ReceiveResult r2 = engine.receive(message("src", "same-payload"), ReceiveOptions.of(wf));

        assertFalse(r1.duplicate(), "First message should not be a duplicate");
        assertTrue(r2.duplicate(), "Second message with same payload should be a duplicate");
        assertEquals(r1.messageId(), r2.messageId(), "Duplicate should return same messageId");
    }

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
    void stepWithDependency_executesInOrder() throws Exception {
        AtomicInteger order = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        WorkflowDefinition wf = WorkflowDefinition.builder("dep-wf")
                .step("first", ctx -> {
                    order.set(1);
                    latch.countDown();
                    return StepResult.empty();
                })
                .step("second", ctx -> {
                    order.compareAndSet(1, 2);
                    latch.countDown();
                    return StepResult.empty();
                }).dependsOn("first")
                .build();

        engine.receive(message("dep-src", "dep-data"), ReceiveOptions.of(wf));
        assertTrue(latch.await(15, TimeUnit.SECONDS), "Both steps should execute");
        assertEquals(2, order.get(), "Steps should execute in dependency order");
    }
}
