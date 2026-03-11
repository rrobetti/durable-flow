package io.github.durableflow.sample.config;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Spring configuration that creates and starts the {@link DurableFlowEngine} bean.
 *
 * <p>The engine uses the application's {@link DataSource} (auto-configured by Spring Boot)
 * and runs its own Flyway schema migration on startup. Dialect auto-detection falls back to
 * PostgreSQL, which is compatible with H2 running in {@code MODE=PostgreSQL}.
 *
 * <p>The {@code destroyMethod = "close"} attribute ensures the engine shuts down gracefully
 * (draining the worker thread pool and stopping the recovery scheduler) when the Spring
 * context closes.
 */
@Configuration
public class DurableFlowEngineConfig {

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(DataSource dataSource) {
        DurableFlowConfig config = DurableFlowConfig.builder()
                .nodeId("spring-sample-node")
                .leaseTimeoutSeconds(30)
                .recoveryIntervalSeconds(15)
                .schemaAutoMigrate(true)
                .build();

        DurableFlowEngine engine = new DurableFlowEngine(dataSource, config);
        engine.start();
        return engine;
    }
}
