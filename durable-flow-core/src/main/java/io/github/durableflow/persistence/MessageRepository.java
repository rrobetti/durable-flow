package io.github.durableflow.persistence;

import io.github.durableflow.api.MessageState;

import java.sql.Connection;
import java.util.Optional;

/**
 * Repository for message records.
 */
public interface MessageRepository {

    /**
     * Persists a new message.
     *
     * @return {@link InsertResult} indicating whether the insert was new or a duplicate
     */
    InsertResult insertMessage(Connection conn, MessageRecord message);

    Optional<MessageRecord> findById(String id);

    void updateMessageState(Connection conn, String id, MessageState state);

    void updateMessageStateWithError(Connection conn, String id, MessageState state, String error);
}
