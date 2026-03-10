package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * PostgreSQL dialect (version 9.5+).
 *
 * <p>Uses {@code ON CONFLICT … DO NOTHING} for idempotent inserts,
 * {@code FOR UPDATE SKIP LOCKED} for safe multi-node step claiming, and
 * partial indexes for efficient eligibility scans.
 */
public final class PostgreSqlDialect implements DatabaseDialect {

    public static final PostgreSqlDialect INSTANCE = new PostgreSqlDialect();

    private PostgreSqlDialect() {}

    @Override
    public String name() { return "PostgreSQL"; }

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
                ON CONFLICT (source, dedupe_hash, payload_length) DO NOTHING
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
                ON CONFLICT (message_id, step_name) DO NOTHING
                """;
    }

    @Override
    public String insertDependencySql() {
        return """
                INSERT INTO message_step_dependencies (message_id, step_name, depends_on_step_name)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """;
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
        return "23505".equals(e.getSQLState());
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/postgresql";
    }
}
