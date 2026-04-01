package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.spi.MetricsListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Spring Boot auto-configuration for the durable-flow engine.
 *
 * <p>Activated automatically when:
 * <ul>
 *   <li>A {@link DataSource} bean is present (provided by
 *       {@link DataSourceAutoConfiguration}).</li>
 *   <li>{@link DurableFlowEngine} is on the classpath.</li>
 * </ul>
 *
 * <p>Every bean is annotated with {@link ConditionalOnMissingBean} so any individual
 * piece can be overridden by declaring a bean of the same type in an
 * {@code @Configuration} class.
 *
 * <p>Typical minimal setup — add the starter dependency and provide
 * {@code spring.datasource.*} properties; no {@code @Configuration} class needed:
 *
 * <pre>{@code
 * # application.yml
 * spring:
 *   datasource:
 *     url: jdbc:postgresql://localhost:5432/mydb
 *     username: myuser
 *     password: secret
 *
 * durable-flow:
 *   node-id: ${HOSTNAME:node-1}
 *   lease-timeout-seconds: 60
 *   execution-mode: ASYNCHRONOUS
 * }</pre>
 */
@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@ConditionalOnClass(DurableFlowEngine.class)
@ConditionalOnBean(DataSource.class)
@EnableConfigurationProperties(DurableFlowProperties.class)
public class DurableFlowAutoConfiguration {

    /**
     * Builds a {@link DurableFlowConfig} from the bound
     * {@link DurableFlowProperties}.
     */
    @Bean
    @ConditionalOnMissingBean
    public DurableFlowConfig durableFlowConfig(DurableFlowProperties props) {
        DurableFlowConfig.Builder builder = DurableFlowConfig.builder()
                .leaseTimeoutSeconds(props.getLeaseTimeoutSeconds())
                .recoveryIntervalSeconds(props.getRecoveryIntervalSeconds())
                .schemaAutoMigrate(props.isSchemaAutoMigrate())
                .executionMode(props.getExecutionMode());

        if (props.getNodeId() != null && !props.getNodeId().isBlank()) {
            builder.nodeId(props.getNodeId());
        }

        return builder.build();
    }

    /**
     * Creates the {@link DurableFlowEngine} bean.
     *
     * <p>The engine is <em>not</em> started here — {@link DurableFlowLifecycle} calls
     * {@code start()} at the appropriate point in the application lifecycle.
     * The {@code destroyMethod = "close"} attribute is retained as a safety net in case
     * the lifecycle bean is bypassed.
     *
     * <p>If a {@link MetricsListener} bean is present in the context it is wired in
     * automatically.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public DurableFlowEngine durableFlowEngine(DataSource dataSource,
                                               DurableFlowConfig durableFlowConfig,
                                               ObjectProvider<MetricsListener> metricsListener) {
        return metricsListener.getIfAvailable(() -> null) != null
                ? new DurableFlowEngine(dataSource, durableFlowConfig,
                        metricsListener.getIfAvailable())
                : new DurableFlowEngine(dataSource, durableFlowConfig);
    }

    /**
     * Registers the {@link SmartLifecycle} bean that starts and stops the engine
     * in the correct phase relative to message consumers.
     */
    @Bean
    @ConditionalOnMissingBean
    public DurableFlowLifecycle durableFlowLifecycle(DurableFlowEngine durableFlowEngine) {
        return new DurableFlowLifecycle(durableFlowEngine);
    }

    /**
     * Registers the {@link DurableFlowTemplate} bean that transparently handles
     * Spring transaction integration, including automatic {@code afterCommit} dispatch
     * when called from within a {@code @Transactional} method.
     */
    @Bean
    @ConditionalOnMissingBean
    public DurableFlowTemplate durableFlowTemplate(DurableFlowEngine durableFlowEngine,
                                                   DataSource dataSource) {
        return new DurableFlowTemplate(durableFlowEngine, dataSource);
    }
}
