package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * Abstraction for database-specific SQL syntax and behavior differences.
 *
 * <p>Implement this interface to support additional relational databases beyond
 * the bundled PostgreSQL, Oracle, MySQL/MariaDB, DB2, and SQL Server dialects.
 * Use {@link DatabaseDialectFactory} to obtain an instance automatically from
 * a {@link javax.sql.DataSource}, or supply one explicitly via
 * {@link io.github.durableflow.DurableFlowConfig.Builder#dialect(DatabaseDialect)}.
 */
public interface DatabaseDialect {

    /** Human-readable name used in logs. */
    String name();

    /**
     * SQL expression that returns the current UTC instant.
     * E.g. {@code NOW()} for PostgreSQL/MySQL, {@code CURRENT_TIMESTAMP} for Oracle/DB2/SQL Server.
     */
    String nowExpression();

    /**
     * Full INSERT statement for the {@code messages} table with built-in idempotency on
     * {@code (source, dedupe_hash, payload_length)}.
     *
     * <p>Parameters (12): id, source, dedupe_hash, payload_length, payload_storage_mode,
     * payload_data, payload_ref, message_state, created_at, updated_at, metadata_json, workflow_name.
     *
     * <p>PostgreSQL uses {@code ON CONFLICT … DO NOTHING}; MySQL uses {@code INSERT IGNORE};
     * other dialects use a plain INSERT and let the repository catch the unique-constraint exception.
     */
    String insertMessageSql();

    /**
     * INSERT statement for {@code message_steps} with idempotency on
     * {@code (message_id, step_name)}.
     *
     * <p>Parameters (8): id, message_id, step_name, max_attempts, retry_delay_ms,
     * retry_multiplier, retry_max_delay_ms, retry_jitter.
     * {@code step_state = 'PENDING'}, {@code attempt_count = 0}, and timestamp columns
     * are embedded as SQL literals.
     */
    String insertStepSql();

    /**
     * INSERT statement for {@code message_step_dependencies}.
     *
     * <p>Parameters (3): message_id, step_name, depends_on_step_name.
     */
    String insertDependencySql();

    /**
     * Full SELECT that returns eligible step rows (PENDING or FAILED_RETRYABLE, with all
     * dependencies SUCCEEDED, respecting retry-at and lock-until windows) up to {@code limit}
     * rows, with appropriate SKIP-LOCKED semantics for multi-node safety.
     *
     * @param limit maximum rows to return (embedded as a literal; not a bind parameter)
     */
    String findEligibleStepsSql(int limit);

    /**
     * UPDATE that atomically claims a step: transitions it to RUNNING, sets owner/locked_until,
     * and increments attempt_count.
     *
     * <p>Parameters: (owner VARCHAR, locked_until TIMESTAMP, id VARCHAR).
     * Execute with {@link java.sql.PreparedStatement#executeUpdate()};
     * a return value &gt; 0 means the claim succeeded.
     */
    String claimStepSql();

    /**
     * UPDATE that resets expired RUNNING leases back to FAILED_RETRYABLE for recovery.
     */
    String recoverExpiredLeasesSql();

    /**
     * Returns {@code true} when the given {@link SQLException} represents a unique-constraint
     * violation (i.e. duplicate row on the dedupe index).
     */
    boolean isUniqueConstraintViolation(SQLException e);

    /**
     * Flyway classpath location that contains the schema migration scripts for this dialect.
     * E.g. {@code "classpath:db/migration"} for PostgreSQL.
     */
    String flywayLocation();
}
