package io.github.durableflow.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Example demonstrating a sequential three-step workflow:
 * validate → enrich → notify
 *
 * <p><b>Prerequisites:</b> a running PostgreSQL instance with the
 * {@code JDBC_URL}, {@code DB_USER}, and {@code DB_PASS} environment variables set.
 */
public class SequentialWorkflowExample {

    public static void main(String[] args) throws Exception {

        HikariDataSource dataSource = buildDataSource();

        DurableFlowEngine engine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .nodeId("example-node-1")
                .leaseTimeoutSeconds(30)
                .build());
        engine.start();

        WorkflowDefinition workflow = WorkflowDefinition.builder("order-processing")
                .step("validate", ctx -> {
                    System.out.println("[validate] Validating payload: " +
                            new String(ctx.getPayload(), StandardCharsets.UTF_8));
                    return StepResult.of("validated-ok".getBytes(StandardCharsets.UTF_8));
                })
                .step("enrich", ctx -> {
                    byte[] upstream = ctx.getPreviousStepOutputs().get("validate");
                    System.out.println("[enrich]   Received from validate: " +
                            (upstream != null ? new String(upstream, StandardCharsets.UTF_8) : "null"));
                    return StepResult.of("enriched-data".getBytes(StandardCharsets.UTF_8));
                }).dependsOn("validate")
                .retryPolicy(RetryPolicy.exponentialBackoff(3,
                        java.time.Duration.ofMillis(500), 2.0, java.time.Duration.ofSeconds(5), false))
                .step("notify", ctx -> {
                    byte[] enriched = ctx.getPreviousStepOutputs().get("enrich");
                    System.out.println("[notify]   Sending notification with: " +
                            (enriched != null ? new String(enriched, StandardCharsets.UTF_8) : "null"));
                    return StepResult.empty();
                }).dependsOn("enrich")
                .build();

        InboundMessage message = new InboundMessage(
                "order-service",
                "{\"orderId\":\"ORD-001\",\"amount\":99.99}".getBytes(StandardCharsets.UTF_8),
                Map.of("content-type", "application/json"));

        ReceiveResult result = engine.receive(message, ReceiveOptions.withDeferredExecution(workflow));
        System.out.println("Message received: id=" + result.messageId() +
                " duplicate=" + result.duplicate());

        // Allow steps to complete
        Thread.sleep(5_000);

        engine.getMessageStatus(result.messageId()).ifPresent(status ->
                System.out.println("Final state: " + status.getMessageState()));

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
