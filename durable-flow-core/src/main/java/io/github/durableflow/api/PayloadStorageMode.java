package io.github.durableflow.api;

/**
 * Describes how the message payload is stored.
 *
 * <ul>
 *   <li>{@code INLINE}       – payload stored directly in the {@code messages} table</li>
 *   <li>{@code ENCRYPTED}    – payload stored encrypted inline</li>
 *   <li>{@code NO_PAYLOAD}   – payload intentionally dropped; only metadata retained</li>
 *   <li>{@code EXTERNAL_REF} – payload stored externally; {@code payload_ref} contains the URI</li>
 * </ul>
 */
public enum PayloadStorageMode {
    INLINE,
    ENCRYPTED,
    NO_PAYLOAD,
    EXTERNAL_REF
}
