package io.github.durableflow.spi;

import java.time.Duration;

/**
 * SPI for receiving engine-level metrics events.
 *
 * <p>Implementations should be low-latency; they are invoked on critical processing paths.
 * All methods have empty default implementations so that partial implementations compile.
 */
public interface MetricsListener {

    default void onMessageReceived() {}

    default void onDuplicateMessage() {}

    default void onStepStarted(String stepName) {}

    default void onStepSucceeded(String stepName, Duration duration) {}

    default void onStepFailed(String stepName, boolean finalFailure) {}

    default void onMessageProcessed() {}

    default void onMessageParked() {}
}
