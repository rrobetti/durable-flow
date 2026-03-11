package io.github.durableflow;

import io.github.durableflow.api.ExecutionMode;
import io.github.durableflow.persistence.dialect.DatabaseDialect;

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
    public static final ExecutionMode DEFAULT_EXECUTION_MODE = ExecutionMode.ASYNCHRONOUS;

    private final String nodeId;
    private final int leaseTimeoutSeconds;
    private final int recoveryIntervalSeconds;
    private final int immediateExecutionThreads;
    private final boolean schemaAutoMigrate;
    /** Optional override; {@code null} means auto-detect from JDBC metadata. */
    private final DatabaseDialect dialect;
    private final ExecutionMode executionMode;

    private DurableFlowConfig(Builder builder) {
        this.nodeId = builder.nodeId;
        this.leaseTimeoutSeconds = builder.leaseTimeoutSeconds;
        this.recoveryIntervalSeconds = builder.recoveryIntervalSeconds;
        this.immediateExecutionThreads = builder.immediateExecutionThreads;
        this.schemaAutoMigrate = builder.schemaAutoMigrate;
        this.dialect = builder.dialect;
        this.executionMode = builder.executionMode;
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

    /**
     * Returns the number of immediate execution threads.
     *
     * @deprecated Since Java 21 virtual threads are used, this setting is no longer applied.
     *             The engine now uses one virtual thread per task, so a fixed pool size is not needed.
     */
    @Deprecated
    public int getImmediateExecutionThreads() {
        return immediateExecutionThreads;
    }

    public boolean isSchemaAutoMigrate() {
        return schemaAutoMigrate;
    }

    /**
     * Returns the explicitly configured {@link DatabaseDialect}, or {@code null} if the engine
     * should auto-detect the dialect from JDBC metadata.
     */
    public DatabaseDialect getDialect() {
        return dialect;
    }

    /**
     * Returns the execution mode controlling whether {@code receive()} blocks until the workflow
     * completes ({@link ExecutionMode#SYNCHRONOUS}) or returns immediately
     * ({@link ExecutionMode#ASYNCHRONOUS}).
     */
    public ExecutionMode getExecutionMode() {
        return executionMode;
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
        private DatabaseDialect dialect = null;
        private ExecutionMode executionMode = DEFAULT_EXECUTION_MODE;

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

        /**
         * Sets the number of threads to use for immediate step execution.
         *
         * @deprecated Since Java 21 virtual threads are used, this setting is no longer applied.
         *             The engine now uses one virtual thread per task, so a fixed pool size is not needed.
         */
        @Deprecated
        public Builder immediateExecutionThreads(int threads) {
            if (threads <= 0) throw new IllegalArgumentException("immediateExecutionThreads must be positive");
            this.immediateExecutionThreads = threads;
            return this;
        }

        public Builder schemaAutoMigrate(boolean migrate) {
            this.schemaAutoMigrate = migrate;
            return this;
        }

        /**
         * Explicitly sets the database dialect, bypassing auto-detection.
         * Useful when the JDBC URL does not unambiguously identify the database,
         * or when using a custom dialect implementation.
         */
        public Builder dialect(DatabaseDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        /**
         * Sets the execution mode for post-receive step dispatch.
         * Defaults to {@link ExecutionMode#ASYNCHRONOUS}.
         */
        public Builder executionMode(ExecutionMode executionMode) {
            this.executionMode = Objects.requireNonNull(executionMode, "executionMode must not be null");
            return this;
        }

        public DurableFlowConfig build() {
            return new DurableFlowConfig(this);
        }
    }
}
