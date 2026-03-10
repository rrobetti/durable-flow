package io.github.durableflow.spi;

import io.github.durableflow.api.InboundMessage;

/**
 * SPI for pre-processing an inbound message before it is persisted.
 *
 * <p>Implementations may:
 * <ul>
 *   <li>normalise / canonicalise the payload bytes used for deduplication hashing</li>
 *   <li>strip or encrypt portions of the payload before storage</li>
 *   <li>extract metadata from the payload</li>
 * </ul>
 */
public interface MessagePreprocessor {

    /**
     * Pre-process the message.
     *
     * @param message the raw inbound message
     * @return pre-process result carrying canonical bytes, optional stored payload, and metadata
     */
    PreprocessResult preprocess(InboundMessage message);
}
