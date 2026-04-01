package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.api.ExecutionMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration-properties binding for the durable-flow engine.
 *
 * <p>All properties are optional; defaults match {@link DurableFlowConfig} constants.
 *
 * <pre>{@code
 * # application.yml
 * durable-flow:
 *   node-id: ${HOSTNAME:node-1}
 *   lease-timeout-seconds: 60
 *   recovery-interval-seconds: 30
 *   schema-auto-migrate: true
 *   execution-mode: ASYNCHRONOUS
 * }</pre>
 */
@ConfigurationProperties(prefix = "durable-flow")
public class DurableFlowProperties {

    /**
     * Unique identifier for this node.
     * Defaults to a random UUID when not set, which is appropriate for ephemeral
     * containers; set an explicit value for persistent nodes so lease ownership
     * survives restarts.
     */
    private String nodeId;

    /**
     * Number of seconds before an in-progress step lease is considered abandoned and
     * eligible for recovery.
     */
    private int leaseTimeoutSeconds = DurableFlowConfig.DEFAULT_LEASE_TIMEOUT_SECONDS;

    /**
     * Interval in seconds between recovery scheduler sweeps.
     */
    private int recoveryIntervalSeconds = DurableFlowConfig.DEFAULT_RECOVERY_INTERVAL_SECONDS;

    /**
     * Whether the engine should run Flyway migrations to create or update the
     * durable-flow schema on startup.
     */
    private boolean schemaAutoMigrate = DurableFlowConfig.DEFAULT_SCHEMA_AUTO_MIGRATE;

    /**
     * Controls whether {@code receive()} returns immediately ({@code ASYNCHRONOUS}) or
     * blocks until all currently-executable steps have run ({@code SYNCHRONOUS}).
     */
    private ExecutionMode executionMode = DurableFlowConfig.DEFAULT_EXECUTION_MODE;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getLeaseTimeoutSeconds() {
        return leaseTimeoutSeconds;
    }

    public void setLeaseTimeoutSeconds(int leaseTimeoutSeconds) {
        this.leaseTimeoutSeconds = leaseTimeoutSeconds;
    }

    public int getRecoveryIntervalSeconds() {
        return recoveryIntervalSeconds;
    }

    public void setRecoveryIntervalSeconds(int recoveryIntervalSeconds) {
        this.recoveryIntervalSeconds = recoveryIntervalSeconds;
    }

    public boolean isSchemaAutoMigrate() {
        return schemaAutoMigrate;
    }

    public void setSchemaAutoMigrate(boolean schemaAutoMigrate) {
        this.schemaAutoMigrate = schemaAutoMigrate;
    }

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(ExecutionMode executionMode) {
        this.executionMode = executionMode;
    }
}
