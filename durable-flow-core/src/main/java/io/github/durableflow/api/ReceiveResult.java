package io.github.durableflow.api;

import java.util.Objects;

/**
 * Result returned from {@link io.github.durableflow.DurableFlowEngine#receive}.
 *
 * @param messageId    stable identifier for this message
 * @param duplicate    {@code true} when the message was already seen (deduplicated)
 * @param messageState current state of the message after processing the receive call
 */
public record ReceiveResult(
        String messageId,
        boolean duplicate,
        MessageState messageState) {

    public ReceiveResult {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(messageState, "messageState must not be null");
    }
}
