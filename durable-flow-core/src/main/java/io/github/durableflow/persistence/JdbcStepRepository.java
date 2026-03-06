package io.github.durableflow.persistence;

import io.github.durableflow.api.RetryPolicy;
import io.github.durableflow.api.StepDefinition;
import io.github.durableflow.api.StepState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * JDBC implementation of {@link StepRepository} targeting PostgreSQL.
 */
public class JdbcStepRepository implements StepRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcStepRepository.class);

    private final DataSource dataSource;

    public JdbcStepRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void insertSteps(Connection conn, String messageId, List<StepDefinition> steps) {
        String stepSql = """
                INSERT INTO message_steps (
                    id, message_id, step_name, step_state, attempt_count,
                    max_attempts, retry_delay_ms, retry_multiplier, retry_max_delay_ms, retry_jitter,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'PENDING', 0, ?, ?, ?, ?, ?, NOW(), NOW())
                ON CONFLICT (message_id, step_name) DO NOTHING
                """;
        String depSql = """
                INSERT INTO message_step_dependencies (message_id, step_name, depends_on_step_name)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """;

        try (PreparedStatement stepPs = conn.prepareStatement(stepSql);
             PreparedStatement depPs = conn.prepareStatement(depSql)) {

            for (StepDefinition step : steps) {
                RetryPolicy policy = step.getRetryPolicy();
                // Compute representative delay params from policy
                long delayMs = policy.nextDelay(1).toMillis();

                stepPs.setString(1, UUID.randomUUID().toString());
                stepPs.setString(2, messageId);
                stepPs.setString(3, step.getName());
                stepPs.setInt(4, policy.getMaxAttempts());
                stepPs.setLong(5, delayMs);
                stepPs.setDouble(6, 2.0); // default multiplier; stored for recovery
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
            throw new RuntimeException("Failed to insert steps for message: " + messageId, e);
        }
    }

    @Override
    public List<StepRecord> findEligibleSteps(int limit) {
        String sql = """
                SELECT ms.* FROM message_steps ms
                WHERE ms.step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (ms.next_retry_at IS NULL OR ms.next_retry_at <= NOW())
                  AND (ms.locked_until IS NULL OR ms.locked_until < NOW())
                  AND NOT EXISTS (
                    SELECT 1 FROM message_step_dependencies d
                    JOIN message_steps dep ON dep.message_id = d.message_id
                        AND dep.step_name = d.depends_on_step_name
                    WHERE d.message_id = ms.message_id AND d.step_name = ms.step_name
                      AND dep.step_state != 'SUCCEEDED'
                  )
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        List<StepRecord> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(mapRow(rs));
                    }
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
        String sql = """
                UPDATE message_steps
                SET step_state = 'RUNNING',
                    owner = ?,
                    locked_until = ?,
                    attempt_count = attempt_count + 1,
                    updated_at = NOW()
                WHERE id = ?
                  AND step_state IN ('PENDING', 'FAILED_RETRYABLE')
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                  AND (locked_until IS NULL OR locked_until < NOW())
                RETURNING id
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.setTimestamp(2, Timestamp.from(lockedUntil));
            ps.setString(3, stepId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim step: " + stepId, e);
        }
    }

    @Override
    public void markSucceeded(Connection conn, String stepId, byte[] output) {
        String sql = """
                UPDATE message_steps
                SET step_state = 'SUCCEEDED',
                    result_data = ?,
                    locked_until = NULL,
                    owner = NULL,
                    updated_at = NOW()
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
                SET step_state = ?,
                    next_retry_at = ?,
                    locked_until = NULL,
                    owner = NULL,
                    last_error = ?,
                    updated_at = NOW()
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
        String sql = """
                UPDATE message_steps
                SET step_state = 'FAILED_RETRYABLE',
                    locked_until = NULL,
                    owner = NULL,
                    next_retry_at = NOW(),
                    updated_at = NOW()
                WHERE step_state = 'RUNNING'
                  AND locked_until < NOW()
                """;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
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
                SET step_state = 'PENDING',
                    attempt_count = 0,
                    last_error = NULL,
                    next_retry_at = NULL,
                    updated_at = NOW()
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
