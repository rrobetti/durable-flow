package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * IBM DB2 LUW dialect (version 11.1+).
 *
 * <p>Uses plain INSERT (unique-constraint violations are caught by the repository),
 * {@code FETCH FIRST n ROWS ONLY} for pagination, and
 * {@code FOR UPDATE WITH RR SKIP LOCKED DATA} for multi-node step eligibility scanning.
 * Regular (non-partial) indexes are used since DB2 does not support partial/filtered indexes.
 *
 * <p>The {@code retry_jitter} column is {@code SMALLINT} in the DB2 schema.
 * JDBC {@code setBoolean}/{@code getBoolean} map transparently to 0/1.
 *
 * <p>DB2 DDL uses {@code NOT NULL WITH DEFAULT} syntax, which is reflected in the
 * Flyway migration script at {@code classpath:db/migration/db2}.
 */
public final class Db2Dialect implements DatabaseDialect {

    public static final Db2Dialect INSTANCE = new Db2Dialect();

    private Db2Dialect() {}

    @Override
    public String name() { return "DB2"; }

    @Override
    public String nowExpression() { return "CURRENT_TIMESTAMP"; }

    @Override
    public String insertMessageSql() {
        return """
                INSERT INTO messages (
                    id, source, dedupe_hash, payload_length, payload_storage_mode,
                    payload_data, payload_ref, message_state, created_at, updated_at,
                    metadata_json, workflow_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
    }

    @Override
    public String insertStepSql() {
        return """
                INSERT INTO message_steps (
                    id, message_id, step_name, step_state, attempt_count,
                    max_attempts, retry_delay_ms, retry_multiplier, retry_max_delay_ms, retry_jitter,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'PENDING', 0, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """;
    }

    @Override
    public String insertDependencySql() {
        return "INSERT INTO message_step_dependencies (message_id, step_name, depends_on_step_name) VALUES (?, ?, ?)";
    }

    @Override
    public String findEligibleStepsSql(int limit) {
        return """
                SELECT ms.* FROM message_steps ms
                WHERE ms.step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (ms.next_retry_at IS NULL OR ms.next_retry_at <= CURRENT_TIMESTAMP)
                  AND (ms.locked_until IS NULL OR ms.locked_until < CURRENT_TIMESTAMP)
                  AND NOT EXISTS (
                    SELECT 1 FROM message_step_dependencies d
                    JOIN message_steps dep ON dep.message_id = d.message_id
                        AND dep.step_name = d.depends_on_step_name
                    WHERE d.message_id = ms.message_id AND d.step_name = ms.step_name
                      AND dep.step_state <> 'SUCCEEDED'
                  )
                """ + "FETCH FIRST " + limit + " ROWS ONLY\n"
                + "FOR UPDATE WITH RR SKIP LOCKED DATA";
    }

    @Override
    public String claimStepSql() {
        return """
                UPDATE message_steps
                SET step_state    = 'RUNNING',
                    owner         = ?,
                    locked_until  = ?,
                    attempt_count = attempt_count + 1,
                    updated_at    = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
                  AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP)
                """;
    }

    @Override
    public String recoverExpiredLeasesSql() {
        return """
                UPDATE message_steps
                SET step_state    = 'FAILED_RETRYABLE',
                    locked_until  = NULL,
                    owner         = NULL,
                    next_retry_at = CURRENT_TIMESTAMP,
                    updated_at    = CURRENT_TIMESTAMP
                WHERE step_state = 'RUNNING'
                  AND locked_until < CURRENT_TIMESTAMP
                """;
    }

    @Override
    public boolean isUniqueConstraintViolation(SQLException e) {
        // DB2: SQLSTATE 23505 — unique constraint or index violation
        return "23505".equals(e.getSQLState());
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/db2";
    }
}
