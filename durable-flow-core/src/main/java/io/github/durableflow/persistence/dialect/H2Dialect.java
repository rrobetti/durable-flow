package io.github.durableflow.persistence.dialect;

import java.sql.SQLException;

/**
 * H2 in-memory database dialect (PostgreSQL compatibility mode).
 *
 * <p>H2 is not a production database for durable-flow; it is used exclusively for
 * integration tests via {@code MODE=PostgreSQL}. Most SQL statements are identical to
 * {@link PostgreSqlDialect} because H2 supports {@code FOR UPDATE SKIP LOCKED} in
 * PostgreSQL compatibility mode.
 *
 * <p>Differences from the PostgreSQL dialect:
 * <ul>
 *   <li>Insert statements use plain {@code INSERT INTO} without any idempotency clause.
 *       {@link io.github.durableflow.persistence.JdbcMessageRepository} catches the
 *       resulting unique-constraint violation (SQLState {@code 23505} / {@code 23001}) and
 *       returns the existing duplicate record, which is the same behaviour as
 *       {@code ON CONFLICT … DO NOTHING} in PostgreSQL.</li>
 *   <li>The Flyway migration location points to {@code db/migration/h2} which uses plain
 *       indexes instead of partial (filtered) indexes not supported by H2.</li>
 * </ul>
 */
public final class H2Dialect implements DatabaseDialect {

    public static final H2Dialect INSTANCE = new H2Dialect();

    private H2Dialect() {}

    @Override
    public String name() { return "H2"; }

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
        return PostgreSqlDialect.INSTANCE.findEligibleStepsSql(limit);
    }

    @Override
    public String claimStepSql() { return PostgreSqlDialect.INSTANCE.claimStepSql(); }

    @Override
    public String recoverExpiredLeasesSql() {
        return PostgreSqlDialect.INSTANCE.recoverExpiredLeasesSql();
    }

    @Override
    public boolean isUniqueConstraintViolation(SQLException e) {
        return PostgreSqlDialect.INSTANCE.isUniqueConstraintViolation(e);
    }

    @Override
    public String flywayLocation() {
        return "classpath:db/migration/h2";
    }
}
