package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * Microsoft SQL Server dialect (SQL Server 2016+).
 *
 * <p>Uses plain INSERT (unique-constraint violations are caught by the repository),
 * {@code TOP(n)} for pagination (no {@code ORDER BY} required), and
 * {@code WITH (UPDLOCK, READPAST)} table hints as the SQL Server equivalent of
 * {@code FOR UPDATE SKIP LOCKED}.
 *
 * <p>Datetime columns are {@code DATETIME2(6)} storing UTC values.
 * {@code CURRENT_TIMESTAMP} is implicitly promoted to {@code DATETIME2} by SQL Server.
 *
 * <p>The {@code retry_jitter} column is {@code BIT} in the SQL Server schema.
 * JDBC {@code setBoolean}/{@code getBoolean} map transparently to 0/1.
 *
 * <p>Filtered indexes (SQL Server 2008+) are used for the eligibility and expiry scans,
 * matching the PostgreSQL partial-index behavior.
 */
public final class SqlServerDialect implements DatabaseDialect {

    public static final SqlServerDialect INSTANCE = new SqlServerDialect();

    private SqlServerDialect() {}

    @Override
    public String name() { return "SQL Server"; }

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
        return "SELECT TOP(" + limit + ") ms.*\n"
                + "FROM message_steps ms WITH (UPDLOCK, READPAST)\n"
                + "WHERE ms.step_state IN ('PENDING', 'FAILED_RETRYABLE')\n"
                + "  AND (ms.next_retry_at IS NULL OR ms.next_retry_at <= CURRENT_TIMESTAMP)\n"
                + "  AND (ms.locked_until IS NULL OR ms.locked_until < CURRENT_TIMESTAMP)\n"
                + "  AND NOT EXISTS (\n"
                + "    SELECT 1 FROM message_step_dependencies d\n"
                + "    JOIN message_steps dep ON dep.message_id = d.message_id\n"
                + "        AND dep.step_name = d.depends_on_step_name\n"
                + "    WHERE d.message_id = ms.message_id AND d.step_name = ms.step_name\n"
                + "      AND dep.step_state <> 'SUCCEEDED'\n"
                + "  )";
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
        // SQL Server: SQLState 23000; error 2627 (PK/UQ violation) or 2601 (unique index violation)
        return "23000".equals(e.getSQLState())
                || e.getErrorCode() == 2627
                || e.getErrorCode() == 2601;
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/sqlserver";
    }
}
