package io.github.durableflow.integration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

@Tag("integration")
@Testcontainers
public abstract class BaseIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("durable_flow_test")
            .withUsername("test")
            .withPassword("test");

    protected HikariDataSource dataSource;
    protected DurableFlowEngine engine;

    @BeforeEach
    void setUp() {
        dataSource = createDataSource(POSTGRES);
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

    protected static HikariDataSource createDataSource(PostgreSQLContainer<?> pg) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.getJdbcUrl());
        cfg.setUsername(pg.getUsername());
        cfg.setPassword(pg.getPassword());
        cfg.setMaximumPoolSize(10);
        cfg.setAutoCommit(false);
        return new HikariDataSource(cfg);
    }

    protected static InboundMessage message(String source, String body) {
        return new InboundMessage(source, body.getBytes(), java.util.Map.of());
    }

    protected static WorkflowDefinition singleStepWorkflow(String name, StepHandler handler) {
        return WorkflowDefinition.builder(name)
                .step("step1", handler)
                .build();
    }

    protected void waitFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
