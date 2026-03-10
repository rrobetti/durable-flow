package io.github.durableflow.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.durableflow.api.MessageState;
import io.github.durableflow.api.PayloadStorageMode;
import io.github.durableflow.persistence.dialect.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * JDBC implementation of {@link MessageRepository}.
 * Database-specific SQL is provided by the injected {@link DatabaseDialect}.
 */
public class JdbcMessageRepository implements MessageRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcMessageRepository.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final DataSource dataSource;
    private final DatabaseDialect dialect;

    public JdbcMessageRepository(DataSource dataSource, DatabaseDialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    @Override
    public InsertResult insertMessage(Connection conn, MessageRecord message) {
        try (PreparedStatement ps = conn.prepareStatement(dialect.insertMessageSql())) {
            ps.setString(1, message.getId());
            ps.setString(2, message.getSource());
            ps.setString(3, message.getDedupeHash());
            ps.setLong(4, message.getPayloadLength());
            ps.setString(5, message.getPayloadStorageMode().name());
            ps.setBytes(6, message.getPayloadData());
            ps.setString(7, message.getPayloadRef());
            ps.setString(8, message.getMessageState().name());
            ps.setTimestamp(9, Timestamp.from(message.getCreatedAt()));
            ps.setTimestamp(10, Timestamp.from(message.getUpdatedAt()));
            ps.setString(11, toJson(message.getMetadata()));
            ps.setString(12, message.getWorkflowName());

            int rows = ps.executeUpdate();
            if (rows > 0) {
                return InsertResult.newlyInserted(message.getId());
            }
            // rows == 0: dialect silently swallowed the duplicate (e.g. ON CONFLICT DO NOTHING / INSERT IGNORE)
            return findExisting(conn, message.getSource(), message.getDedupeHash(), message.getPayloadLength());
        } catch (SQLException e) {
            if (dialect.isUniqueConstraintViolation(e)) {
                // Other dialects raise an exception on duplicate rather than returning 0 rows
                return findExisting(conn, message.getSource(), message.getDedupeHash(), message.getPayloadLength());
            }
            throw new RuntimeException("Failed to insert message", e);
        }
    }

    @Override
    public Optional<MessageRecord> findById(String id) {
        String sql = "SELECT * FROM messages WHERE id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find message by id: " + id, e);
        }
        return Optional.empty();
    }

    @Override
    public void updateMessageState(Connection conn, String id, MessageState state) {
        String sql = "UPDATE messages SET message_state = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state.name());
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update message state for id: " + id, e);
        }
    }

    @Override
    public void updateMessageStateWithError(Connection conn, String id, MessageState state, String error) {
        String sql = "UPDATE messages SET message_state = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state.name());
            ps.setString(2, error);
            ps.setString(3, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update message state with error for id: " + id, e);
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private InsertResult findExisting(Connection conn, String source, String dedupeHash, long payloadLength) {
        String sql = "SELECT id, message_state FROM messages WHERE source = ? AND dedupe_hash = ? AND payload_length = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, source);
            ps.setString(2, dedupeHash);
            ps.setLong(3, payloadLength);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String existingId = rs.getString("id");
                    MessageState existingState = MessageState.valueOf(rs.getString("message_state"));
                    return InsertResult.duplicate(existingId, existingState);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find existing message", e);
        }
        throw new IllegalStateException("Expected duplicate record but none found");
    }

    private MessageRecord mapRow(ResultSet rs) throws SQLException {
        MessageRecord r = new MessageRecord();
        r.setId(rs.getString("id"));
        r.setSource(rs.getString("source"));
        r.setDedupeHash(rs.getString("dedupe_hash"));
        r.setPayloadLength(rs.getLong("payload_length"));
        r.setPayloadStorageMode(PayloadStorageMode.valueOf(rs.getString("payload_storage_mode")));
        r.setPayloadData(rs.getBytes("payload_data"));
        r.setPayloadRef(rs.getString("payload_ref"));
        r.setMessageState(MessageState.valueOf(rs.getString("message_state")));
        Timestamp createdAt = rs.getTimestamp("created_at");
        r.setCreatedAt(createdAt != null ? createdAt.toInstant() : null);
        Timestamp updatedAt = rs.getTimestamp("updated_at");
        r.setUpdatedAt(updatedAt != null ? updatedAt.toInstant() : null);
        r.setLastError(rs.getString("last_error"));
        r.setMetadata(fromJson(rs.getString("metadata_json")));
        r.setWorkflowName(rs.getString("workflow_name"));
        return r;
    }

    private String toJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) return null;
        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize metadata to JSON", e);
            return null;
        }
    }

    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) return new HashMap<>();
        try {
            return MAPPER.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize metadata JSON", e);
            return new HashMap<>();
        }
    }
}
