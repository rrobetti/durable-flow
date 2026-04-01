package io.github.durableflow.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating a workflow with parallel independent steps that later converge:
 *
 * <pre>
 *        ┌── enrich-a ──┐
 * ingest ┤              ├── aggregate
 *        └── enrich-b ──┘
 * </pre>
 */
public class ParallelWorkflowExample {

    public static void main(String[] args) throws Exception {

        HikariDataSource dataSource = buildDataSource();

        DurableFlowEngine engine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .nodeId("example-node-parallel")
                .build());
        engine.start();

        CountDownLatch latch = new CountDownLatch(1);

        WorkflowDefinition workflow = WorkflowDefinition.builder("parallel-enrichment")
                .step("ingest", ctx -> {
                    System.out.println("[ingest]    Ingesting message");
                    return StepResult.of("raw-data".getBytes(StandardCharsets.UTF_8));
                })
                .step("enrich-a", ctx -> {
                    System.out.println("[enrich-a]  Running enrichment A");
                    Thread.sleep(200); // simulate I/O
                    return StepResult.of("enriched-by-A".getBytes(StandardCharsets.UTF_8));
                }).dependsOn("ingest")
                .step("enrich-b", ctx -> {
                    System.out.println("[enrich-b]  Running enrichment B");
                    Thread.sleep(150); // simulate I/O
                    return StepResult.of("enriched-by-B".getBytes(StandardCharsets.UTF_8));
                }).dependsOn("ingest")
                .step("aggregate", ctx -> {
                    byte[] a = ctx.getPreviousStepOutputs().get("enrich-a");
                    byte[] b = ctx.getPreviousStepOutputs().get("enrich-b");
                    System.out.println("[aggregate] A=" + new String(a, StandardCharsets.UTF_8)
                            + " B=" + new String(b, StandardCharsets.UTF_8));
                    latch.countDown();
                    return StepResult.empty();
                }).dependsOn("enrich-a", "enrich-b")
                .build();

        InboundMessage message = new InboundMessage(
                "event-bus",
                "{\"eventId\":\"EVT-42\"}".getBytes(StandardCharsets.UTF_8),
                Map.of());

        ReceiveResult result = engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
        System.out.println("Received: id=" + result.messageId());

        boolean completed = latch.await(15, TimeUnit.SECONDS);
        System.out.println("Workflow completed: " + completed);

        Thread.sleep(500);
        engine.getMessageStatus(result.messageId()).ifPresent(s ->
                System.out.println("Final state: " + s.getMessageState()));

        engine.close();
        dataSource.close();
    }

    private static HikariDataSource buildDataSource() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/durable_flow"));
        cfg.setUsername(System.getenv().getOrDefault("DB_USER", "postgres"));
        cfg.setPassword(System.getenv().getOrDefault("DB_PASS", "postgres"));
        cfg.setMaximumPoolSize(5);
        cfg.setAutoCommit(false);
        return new HikariDataSource(cfg);
    }
}
