package io.github.durableflow.persistence;

import io.github.durableflow.api.RetryPolicy;
import io.github.durableflow.api.StepDefinition;
import io.github.durableflow.api.StepState;
import io.github.durableflow.persistence.dialect.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * JDBC implementation of {@link StepRepository}.
 * Database-specific SQL is provided by the injected {@link DatabaseDialect}.
 */
public class JdbcStepRepository implements StepRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcStepRepository.class);

    private final DataSource dataSource;
    private final DatabaseDialect dialect;

    public JdbcStepRepository(DataSource dataSource, DatabaseDialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    @Override
    public void insertSteps(Connection conn, String messageId, List<StepDefinition> steps) {
        try (PreparedStatement stepPs = conn.prepareStatement(dialect.insertStepSql());
             PreparedStatement depPs = conn.prepareStatement(dialect.insertDependencySql())) {

            for (StepDefinition step : steps) {
                RetryPolicy policy = step.getRetryPolicy();
                long delayMs = policy.nextDelay(1).toMillis();

                stepPs.setString(1, UUID.randomUUID().toString());
                stepPs.setString(2, messageId);
                stepPs.setString(3, step.getName());
                stepPs.setInt(4, policy.getMaxAttempts());
                stepPs.setLong(5, delayMs);
                stepPs.setDouble(6, 2.0);
                stepPs.setLong(7, 60_000L);
                stepPs.setBoolean(8, false);
                stepPs.addBatch();

                for (String dep : step.getDependsOn()) {
                    depPs.setString(1, messageId);
                    depPs.setString(2, step.getName());
                    depPs.setString(3, dep);
                    depPs.addBatch();
                }
            }
            stepPs.executeBatch();
            depPs.executeBatch();

        } catch (SQLException e) {
            if (dialect.isUniqueConstraintViolation(e)) {
                log.debug("Step insert skipped due to duplicate key for message: {}", messageId);
                return;
            }
            throw new RuntimeException("Failed to insert steps for message: " + messageId, e);
        }
    }

    @Override
    public List<StepRecord> findEligibleSteps(int limit) {
        String sql = dialect.findEligibleStepsSql(limit);
        List<StepRecord> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find eligible steps", e);
        }
        return results;
    }

    @Override
    public boolean claimStep(Connection conn, String stepId, String nodeId, Instant lockedUntil) {
        try (PreparedStatement ps = conn.prepareStatement(dialect.claimStepSql())) {
            ps.setString(1, nodeId);
            ps.setTimestamp(2, Timestamp.from(lockedUntil));
            ps.setString(3, stepId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim step: " + stepId, e);
        }
    }

    @Override
    public void markSucceeded(Connection conn, String stepId, byte[] output) {
        String sql = """
                UPDATE message_steps
                SET step_state   = 'SUCCEEDED',
                    result_data  = ?,
                    locked_until = NULL,
                    owner        = NULL,
                    updated_at   = CURRENT_TIMESTAMP
                WHERE id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setBytes(1, output);
            ps.setString(2, stepId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark step succeeded: " + stepId, e);
        }
    }

    @Override
    public void markFailed(Connection conn, String stepId, boolean retryable,
                           int nextAttempt, Instant nextRetryAt, String error) {
        String state = retryable ? "FAILED_RETRYABLE" : "FAILED_FINAL";
        String sql = """
                UPDATE message_steps
                SET step_state    = ?,
                    next_retry_at = ?,
                    locked_until  = NULL,
                    owner         = NULL,
                    last_error    = ?,
                    updated_at    = CURRENT_TIMESTAMP
                WHERE id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state);
            ps.setTimestamp(2, nextRetryAt != null ? Timestamp.from(nextRetryAt) : null);
            ps.setString(3, truncate(error, 4096));
            ps.setString(4, stepId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark step failed: " + stepId, e);
        }
    }

    @Override
    public List<StepRecord> findStepsForMessage(String messageId) {
        String sql = "SELECT * FROM message_steps WHERE message_id = ? ORDER BY created_at";
        List<StepRecord> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, messageId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find steps for message: " + messageId, e);
        }
        return results;
    }

    @Override
    public Map<String, byte[]> getStepOutputs(String messageId, List<String> stepNames) {
        if (stepNames == null || stepNames.isEmpty()) {
            return Collections.emptyMap();
        }
        String placeholders = String.join(",", Collections.nCopies(stepNames.size(), "?"));
        String sql = "SELECT step_name, result_data FROM message_steps WHERE message_id = ? AND step_name IN (" + placeholders + ")";
        Map<String, byte[]> results = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, messageId);
            for (int i = 0; i < stepNames.size(); i++) {
                ps.setString(i + 2, stepNames.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.put(rs.getString("step_name"), rs.getBytes("result_data"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get step outputs for message: " + messageId, e);
        }
        return results;
    }

    @Override
    public int recoverExpiredLeases() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(dialect.recoverExpiredLeasesSql())) {
            int updated = ps.executeUpdate();
            conn.commit();
            return updated;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to recover expired leases", e);
        }
    }

    @Override
    public void resetFailedFinalSteps(String messageId) {
        String sql = """
                UPDATE message_steps
                SET step_state    = 'PENDING',
                    attempt_count = 0,
                    last_error    = NULL,
                    next_retry_at = NULL,
                    updated_at    = CURRENT_TIMESTAMP
                WHERE message_id = ? AND step_state = 'FAILED_FINAL'
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(true);
            ps.setString(1, messageId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to reset failed final steps for message: " + messageId, e);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private StepRecord mapRow(ResultSet rs) throws SQLException {
        StepRecord r = new StepRecord();
        r.setId(rs.getString("id"));
        r.setMessageId(rs.getString("message_id"));
        r.setStepName(rs.getString("step_name"));
        r.setStepState(StepState.valueOf(rs.getString("step_state")));
        r.setAttemptCount(rs.getInt("attempt_count"));
        r.setMaxAttempts(rs.getInt("max_attempts"));
        Timestamp nextRetry = rs.getTimestamp("next_retry_at");
        r.setNextRetryAt(nextRetry != null ? nextRetry.toInstant() : null);
        Timestamp lockedUntil = rs.getTimestamp("locked_until");
        r.setLockedUntil(lockedUntil != null ? lockedUntil.toInstant() : null);
        r.setOwner(rs.getString("owner"));
        r.setLastError(rs.getString("last_error"));
        r.setResultData(rs.getBytes("result_data"));
        r.setRetryDelayMs(rs.getLong("retry_delay_ms"));
        r.setRetryMultiplier(rs.getDouble("retry_multiplier"));
        r.setRetryMaxDelayMs(rs.getLong("retry_max_delay_ms"));
        r.setRetryJitter(rs.getBoolean("retry_jitter"));
        Timestamp createdAt = rs.getTimestamp("created_at");
        r.setCreatedAt(createdAt != null ? createdAt.toInstant() : null);
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        r.setUpdatedAt(updatedAt != null ? updatedAt.toInstant() : null);
        return r;
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() > maxLen ? s.substring(0, maxLen) : s;
    }
}
