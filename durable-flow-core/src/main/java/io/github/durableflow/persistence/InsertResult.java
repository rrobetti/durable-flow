package io.github.durableflow.persistence;

import io.github.durableflow.api.MessageState;

/**
 * Result of an {@link MessageRepository#insertMessage} call.
 */
public final class InsertResult {

    private final String messageId;
    private final boolean duplicate;
    private final MessageState existingState;

    private InsertResult(String messageId, boolean duplicate, MessageState existingState) {
        this.messageId = messageId;
        this.duplicate = duplicate;
        this.existingState = existingState;
    }

    public static InsertResult newlyInserted(String messageId) {
        return new InsertResult(messageId, false, null);
    }

    public static InsertResult duplicate(String messageId, MessageState existingState) {
        return new InsertResult(messageId, true, existingState);
    }

    public String getMessageId() { return messageId; }

    public boolean isDuplicate() { return duplicate; }

    /** Returns the existing message state when {@link #isDuplicate()} is true. */
    public MessageState getExistingState() { return existingState; }
}
