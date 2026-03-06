package io.github.durableflow.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import io.github.durableflow.spi.MessagePreprocessor;
import io.github.durableflow.spi.PreprocessResult;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Example showing a preprocessor that intentionally drops the message payload
 * (storing only a hash for deduplication), suitable for privacy-sensitive messages.
 */
public class NoPayloadExample {

    public static void main(String[] args) throws Exception {

        HikariDataSource dataSource = buildDataSource();

        DurableFlowEngine engine = new DurableFlowEngine(dataSource, DurableFlowConfig.builder()
                .nodeId("example-node-nopayload")
                .build());
        engine.start();

        // Preprocessor that keeps canonical bytes for hashing but stores nothing
        MessagePreprocessor noPayloadPreprocessor = message -> {
            byte[] raw = message.rawPayload();
            return PreprocessResult.builder()
                    .canonicalBytes(raw)        // used for deduplication hash
                    .storedPayload(null)         // don't store the actual bytes
                    .payloadStorageMode(PayloadStorageMode.NO_PAYLOAD)
                    .metadata(Map.of(
                            "source", message.source(),
                            "payload-length", String.valueOf(raw.length)))
                    .build();
        };

        WorkflowDefinition workflow = WorkflowDefinition.builder("privacy-aware-wf")
                .step("audit-log", ctx -> {
                    System.out.println("[audit-log] Processing message: " + ctx.getMessageId());
                    System.out.println("            Payload is null (dropped): " + (ctx.getPayload().length == 0));
                    System.out.println("            Metadata: " + ctx.getMetadata());
                    return StepResult.empty();
                })
                .build();

        InboundMessage sensitiveMessage = new InboundMessage(
                "pii-service",
                "{\"ssn\":\"123-45-6789\",\"name\":\"John Doe\"}".getBytes(StandardCharsets.UTF_8),
                Map.of("sensitivity", "HIGH"));

        ReceiveResult result = engine.receive(sensitiveMessage,
                new ReceiveOptions(workflow, noPayloadPreprocessor));

        System.out.println("Message received (no payload stored): id=" + result.messageId());

        Thread.sleep(3_000);

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
