package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * MySQL / MariaDB dialect.
 *
 * <p>Minimum supported versions: MySQL 8.0+, MariaDB 10.6+.
 * Both support {@code FOR UPDATE SKIP LOCKED}.
 *
 * <p>Uses {@code INSERT IGNORE} for idempotent inserts (silently skips duplicates).
 * Datetime columns are {@code DATETIME(6)} (microsecond precision, UTC convention).
 * Ensure the JDBC connection includes {@code serverTimezone=UTC} to store consistent UTC values.
 *
 * <p>The {@code retry_jitter} column is {@code TINYINT(1)}.
 * JDBC {@code setBoolean}/{@code getBoolean} map transparently to 0/1.
 */
public final class MySqlDialect implements DatabaseDialect {

    public static final MySqlDialect INSTANCE = new MySqlDialect();

    private MySqlDialect() {}

    @Override
    public String name() { return "MySQL/MariaDB"; }

    @Override
    public String nowExpression() { return "CURRENT_TIMESTAMP(6)"; }

    @Override
    public String insertMessageSql() {
        return """
                INSERT IGNORE INTO messages (
                    id, source, dedupe_hash, payload_length, payload_storage_mode,
                    payload_data, payload_ref, message_state, created_at, updated_at,
                    metadata_json, workflow_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
    }

    @Override
    public String insertStepSql() {
        return """
                INSERT IGNORE INTO message_steps (
                    id, message_id, step_name, step_state, attempt_count,
                    max_attempts, retry_delay_ms, retry_multiplier, retry_max_delay_ms, retry_jitter,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'PENDING', 0, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6))
                """;
    }

    @Override
    public String insertDependencySql() {
        return "INSERT IGNORE INTO message_step_dependencies (message_id, step_name, depends_on_step_name) VALUES (?, ?, ?)";
    }

    @Override
    public String findEligibleStepsSql(int limit) {
        return """
                SELECT ms.* FROM message_steps ms
                WHERE ms.step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (ms.next_retry_at IS NULL OR ms.next_retry_at <= CURRENT_TIMESTAMP(6))
                  AND (ms.locked_until IS NULL OR ms.locked_until < CURRENT_TIMESTAMP(6))
                  AND NOT EXISTS (
                    SELECT 1 FROM message_step_dependencies d
                    JOIN message_steps dep ON dep.message_id = d.message_id
                        AND dep.step_name = d.depends_on_step_name
                    WHERE d.message_id = ms.message_id AND d.step_name = ms.step_name
                      AND dep.step_state != 'SUCCEEDED'
                  )
                """ + "LIMIT " + limit + "\n"
                + "FOR UPDATE SKIP LOCKED";
    }

    @Override
    public String claimStepSql() {
        return """
                UPDATE message_steps
                SET step_state    = 'RUNNING',
                    owner         = ?,
                    locked_until  = ?,
                    attempt_count = attempt_count + 1,
                    updated_at    = CURRENT_TIMESTAMP(6)
                WHERE id = ?
                  AND step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP(6))
                  AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP(6))
                """;
    }

    @Override
    public String recoverExpiredLeasesSql() {
        return """
                UPDATE message_steps
                SET step_state    = 'FAILED_RETRYABLE',
                    locked_until  = NULL,
                    owner         = NULL,
                    next_retry_at = CURRENT_TIMESTAMP(6),
                    updated_at    = CURRENT_TIMESTAMP(6)
                WHERE step_state = 'RUNNING'
                  AND locked_until < CURRENT_TIMESTAMP(6)
                """;
    }

    @Override
    public boolean isUniqueConstraintViolation(SQLException e) {
        // MySQL/MariaDB: SQLState 23000, error code 1062 (ER_DUP_ENTRY)
        // With INSERT IGNORE, the exception is suppressed — this is still needed as fallback.
        return "23000".equals(e.getSQLState()) || e.getErrorCode() == 1062;
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/mysql";
    }
}
