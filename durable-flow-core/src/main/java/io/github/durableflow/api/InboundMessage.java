package io.github.durableflow.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An inbound message to be processed by the durable flow engine.
 *
 * @param source      logical source identifier (e.g., queue name, topic, system name)
 * @param rawPayload  raw bytes of the message payload
 * @param headers     optional headers / metadata from the transport layer
 */
public record InboundMessage(
        String source,
        byte[] rawPayload,
        Map<String, String> headers) {

    public InboundMessage {
        Objects.requireNonNull(source, "source must not be null");
        rawPayload = rawPayload != null ? Arrays.copyOf(rawPayload, rawPayload.length) : new byte[0];
        headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    /** Returns a defensive copy of the raw payload. */
    @Override
    public byte[] rawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }
}
