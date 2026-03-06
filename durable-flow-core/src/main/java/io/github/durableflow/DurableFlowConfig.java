package io.github.durableflow;

import java.util.Objects;
import java.util.UUID;

/**
 * Configuration for a {@link DurableFlowEngine} instance.
 */
public final class DurableFlowConfig {

    public static final int DEFAULT_LEASE_TIMEOUT_SECONDS = 60;
    public static final int DEFAULT_RECOVERY_INTERVAL_SECONDS = 30;
    public static final int DEFAULT_IMMEDIATE_EXECUTION_THREADS = 4;
    public static final boolean DEFAULT_SCHEMA_AUTO_MIGRATE = true;

    private final String nodeId;
    private final int leaseTimeoutSeconds;
    private final int recoveryIntervalSeconds;
    private final int immediateExecutionThreads;
    private final boolean schemaAutoMigrate;

    private DurableFlowConfig(Builder builder) {
        this.nodeId = builder.nodeId;
        this.leaseTimeoutSeconds = builder.leaseTimeoutSeconds;
        this.recoveryIntervalSeconds = builder.recoveryIntervalSeconds;
        this.immediateExecutionThreads = builder.immediateExecutionThreads;
        this.schemaAutoMigrate = builder.schemaAutoMigrate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DurableFlowConfig defaults() {
        return builder().build();
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getLeaseTimeoutSeconds() {
        return leaseTimeoutSeconds;
    }

    public int getRecoveryIntervalSeconds() {
        return recoveryIntervalSeconds;
    }

    public int getImmediateExecutionThreads() {
        return immediateExecutionThreads;
    }

    public boolean isSchemaAutoMigrate() {
        return schemaAutoMigrate;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static final class Builder {
        private String nodeId = UUID.randomUUID().toString();
        private int leaseTimeoutSeconds = DEFAULT_LEASE_TIMEOUT_SECONDS;
        private int recoveryIntervalSeconds = DEFAULT_RECOVERY_INTERVAL_SECONDS;
        private int immediateExecutionThreads = DEFAULT_IMMEDIATE_EXECUTION_THREADS;
        private boolean schemaAutoMigrate = DEFAULT_SCHEMA_AUTO_MIGRATE;

        private Builder() {}

        public Builder nodeId(String nodeId) {
            this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
            return this;
        }

        public Builder leaseTimeoutSeconds(int seconds) {
            if (seconds <= 0) throw new IllegalArgumentException("leaseTimeoutSeconds must be positive");
            this.leaseTimeoutSeconds = seconds;
            return this;
        }

        public Builder recoveryIntervalSeconds(int seconds) {
            if (seconds <= 0) throw new IllegalArgumentException("recoveryIntervalSeconds must be positive");
            this.recoveryIntervalSeconds = seconds;
            return this;
        }

        public Builder immediateExecutionThreads(int threads) {
            if (threads <= 0) throw new IllegalArgumentException("immediateExecutionThreads must be positive");
            this.immediateExecutionThreads = threads;
            return this;
        }

        public Builder schemaAutoMigrate(boolean migrate) {
            this.schemaAutoMigrate = migrate;
            return this;
        }

        public DurableFlowConfig build() {
            return new DurableFlowConfig(this);
        }
    }
}
