package io.github.durableflow.spi;

import io.github.durableflow.api.PayloadStorageMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Result of running a {@link MessagePreprocessor}.
 */
public final class PreprocessResult {

    private final byte[] canonicalBytes;
    private final byte[] storedPayload;
    private final PayloadStorageMode payloadStorageMode;
    private final Map<String, String> metadata;

    private PreprocessResult(
            byte[] canonicalBytes,
            byte[] storedPayload,
            PayloadStorageMode payloadStorageMode,
            Map<String, String> metadata) {
        this.canonicalBytes = Objects.requireNonNull(canonicalBytes, "canonicalBytes must not be null");
        this.storedPayload = storedPayload;
        this.payloadStorageMode = Objects.requireNonNull(payloadStorageMode, "payloadStorageMode must not be null");
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Returns a defensive copy of the canonical bytes used for hashing. */
    public byte[] getCanonicalBytes() {
        return Arrays.copyOf(canonicalBytes, canonicalBytes.length);
    }

    /** Returns a defensive copy of the payload that will be stored, or {@code null} if not stored. */
    public byte[] getStoredPayload() {
        return storedPayload != null ? Arrays.copyOf(storedPayload, storedPayload.length) : null;
    }

    public PayloadStorageMode getPayloadStorageMode() {
        return payloadStorageMode;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static final class Builder {
        private byte[] canonicalBytes;
        private byte[] storedPayload;
        private PayloadStorageMode payloadStorageMode = PayloadStorageMode.INLINE;
        private Map<String, String> metadata;

        private Builder() {}

        public Builder canonicalBytes(byte[] canonicalBytes) {
            this.canonicalBytes = canonicalBytes;
            return this;
        }

        public Builder storedPayload(byte[] storedPayload) {
            this.storedPayload = storedPayload;
            return this;
        }

        public Builder payloadStorageMode(PayloadStorageMode mode) {
            this.payloadStorageMode = mode;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public PreprocessResult build() {
            return new PreprocessResult(canonicalBytes, storedPayload, payloadStorageMode, metadata);
        }
    }
}
