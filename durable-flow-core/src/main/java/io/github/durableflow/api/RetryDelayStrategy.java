package io.github.durableflow.api;

import java.time.Duration;

/**
 * Strategy that computes the delay before the next retry attempt.
 */
@FunctionalInterface
public interface RetryDelayStrategy {

    /**
     * Returns the delay to apply before attempt number {@code attemptNumber}.
     *
     * @param attemptNumber 1-based attempt number (1 = first retry after initial failure)
     * @return non-negative delay
     */
    Duration nextDelay(int attemptNumber);
}
