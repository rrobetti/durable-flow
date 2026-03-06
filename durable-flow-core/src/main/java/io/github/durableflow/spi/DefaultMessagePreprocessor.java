package io.github.durableflow.spi;

import io.github.durableflow.api.InboundMessage;
import io.github.durableflow.api.PayloadStorageMode;

/**
 * Default {@link MessagePreprocessor} that passes through the raw payload unchanged.
 *
 * <p>The raw payload is used as canonical bytes (for hashing) and also stored inline.
 */
public final class DefaultMessagePreprocessor implements MessagePreprocessor {

    public static final DefaultMessagePreprocessor INSTANCE = new DefaultMessagePreprocessor();

    private DefaultMessagePreprocessor() {}

    @Override
    public PreprocessResult preprocess(InboundMessage message) {
        byte[] raw = message.rawPayload();
        return PreprocessResult.builder()
                .canonicalBytes(raw)
                .storedPayload(raw)
                .payloadStorageMode(PayloadStorageMode.INLINE)
                .metadata(message.headers())
                .build();
    }
}
