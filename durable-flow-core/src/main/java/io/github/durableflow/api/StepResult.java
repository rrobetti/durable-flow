package io.github.durableflow.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Result produced by a successfully-completed step.
 */
public final class StepResult {

    private final byte[] output;
    private final Map<String, String> metadata;

    private StepResult(byte[] output, Map<String, String> metadata) {
        this.output = output;
        this.metadata = metadata != null ? Collections.unmodifiableMap(metadata) : Collections.emptyMap();
    }

    /** A result with no output bytes and no metadata. */
    public static StepResult empty() {
        return new StepResult(null, null);
    }

    /** A result carrying output bytes. */
    public static StepResult of(byte[] output) {
        return new StepResult(output != null ? Arrays.copyOf(output, output.length) : null, null);
    }

    /** A result carrying output bytes and metadata. */
    public static StepResult of(byte[] output, Map<String, String> metadata) {
        return new StepResult(output != null ? Arrays.copyOf(output, output.length) : null, metadata);
    }

    /** Returns a defensive copy of the output bytes, or empty if there are none. */
    public Optional<byte[]> getOutput() {
        return output == null ? Optional.empty() : Optional.of(Arrays.copyOf(output, output.length));
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
