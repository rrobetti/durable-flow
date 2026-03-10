package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * Oracle Database dialect (12c Release 2+).
 *
 * <p>Uses plain INSERT (unique-constraint violations are caught by the repository),
 * {@code FETCH FIRST n ROWS ONLY} for pagination, and {@code FOR UPDATE SKIP LOCKED}
 * (supported since Oracle 11g R2). Regular indexes replace PostgreSQL partial indexes.
 *
 * <p><strong>Note:</strong> The {@code retry_jitter} column is {@code NUMBER(1,0)} in the
 * Oracle schema. Oracle JDBC transparently maps JDBC {@code setBoolean}/{@code getBoolean}
 * to {@code 0}/{@code 1} for numeric columns.
 */
public final class OracleDialect implements DatabaseDialect {

    public static final OracleDialect INSTANCE = new OracleDialect();

    private OracleDialect() {}

    @Override
    public String name() { return "Oracle"; }

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
        // ORA-00001: unique constraint violated — SQLState 23000, error code 1
        return "23000".equals(e.getSQLState()) || e.getErrorCode() == 1;
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/oracle";
    }
}
