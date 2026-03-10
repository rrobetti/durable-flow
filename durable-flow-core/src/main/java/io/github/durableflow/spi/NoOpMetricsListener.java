package io.github.durableflow.spi;

import java.time.Duration;

/** No-op implementation of {@link MetricsListener}. */
public final class NoOpMetricsListener implements MetricsListener {

    public static final NoOpMetricsListener INSTANCE = new NoOpMetricsListener();

    private NoOpMetricsListener() {}

    @Override public void onMessageReceived() {}
    @Override public void onDuplicateMessage() {}
    @Override public void onStepStarted(String stepName) {}
    @Override public void onStepSucceeded(String stepName, Duration duration) {}
    @Override public void onStepFailed(String stepName, boolean finalFailure) {}
    @Override public void onMessageProcessed() {}
    @Override public void onMessageParked() {}
}
