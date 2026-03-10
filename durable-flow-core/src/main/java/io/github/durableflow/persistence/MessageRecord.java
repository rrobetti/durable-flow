package io.github.durableflow.persistence;

import io.github.durableflow.api.MessageState;
import io.github.durableflow.api.PayloadStorageMode;

import java.time.Instant;
import java.util.Map;

/**
 * Database row for the {@code messages} table.
 */
public class MessageRecord {

    private String id;
    private String source;
    private String dedupeHash;
    private long payloadLength;
    private PayloadStorageMode payloadStorageMode;
    private byte[] payloadData;
    private String payloadRef;
    private MessageState messageState;
    private Instant createdAt;
    private Instant updatedAt;
    private String lastError;
    private Map<String, String> metadata;
    private String workflowName;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getDedupeHash() { return dedupeHash; }
    public void setDedupeHash(String dedupeHash) { this.dedupeHash = dedupeHash; }

    public long getPayloadLength() { return payloadLength; }
    public void setPayloadLength(long payloadLength) { this.payloadLength = payloadLength; }

    public PayloadStorageMode getPayloadStorageMode() { return payloadStorageMode; }
    public void setPayloadStorageMode(PayloadStorageMode payloadStorageMode) { this.payloadStorageMode = payloadStorageMode; }

    public byte[] getPayloadData() { return payloadData; }
    public void setPayloadData(byte[] payloadData) { this.payloadData = payloadData; }

    public String getPayloadRef() { return payloadRef; }
    public void setPayloadRef(String payloadRef) { this.payloadRef = payloadRef; }

    public MessageState getMessageState() { return messageState; }
    public void setMessageState(MessageState messageState) { this.messageState = messageState; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }

    public String getWorkflowName() { return workflowName; }
    public void setWorkflowName(String workflowName) { this.workflowName = workflowName; }
}
