package io.github.durableflow.api;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Encapsulates how a step should be retried after failure.
 */
public final class RetryPolicy {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    private final int maxAttempts;
    private final RetryDelayStrategy delayStrategy;
    private final ExceptionClassifier exceptionClassifier;

    private RetryPolicy(int maxAttempts, RetryDelayStrategy delayStrategy, ExceptionClassifier exceptionClassifier) {
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must be >= 0");
        }
        this.maxAttempts = maxAttempts;
        this.delayStrategy = Objects.requireNonNull(delayStrategy, "delayStrategy must not be null");
        this.exceptionClassifier = exceptionClassifier != null ? exceptionClassifier : t -> true;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /** Default policy: 3 attempts, 1-second fixed delay, all exceptions retryable. */
    public static RetryPolicy defaultPolicy() {
        return fixedDelay(DEFAULT_MAX_ATTEMPTS, Duration.ofSeconds(1));
    }

    /** No retries – the step fails immediately. */
    public static RetryPolicy noRetry() {
        return new RetryPolicy(0, attempt -> Duration.ZERO, t -> false);
    }

    /** Fixed delay between every attempt. */
    public static RetryPolicy fixedDelay(int maxAttempts, Duration delay) {
        Objects.requireNonNull(delay, "delay must not be null");
        return new RetryPolicy(maxAttempts, attempt -> delay, t -> true);
    }

    /**
     * Exponential back-off with optional jitter.
     *
     * @param maxAttempts maximum number of attempts (including the first)
     * @param initial     initial delay
     * @param multiplier  back-off multiplier (e.g. 2.0 for doubling)
     * @param max         upper cap for the computed delay
     * @param jitter      when {@code true} applies uniform jitter in [0, delay]
     */
    public static RetryPolicy exponentialBackoff(
            int maxAttempts, Duration initial, double multiplier, Duration max, boolean jitter) {
        Objects.requireNonNull(initial, "initial must not be null");
        Objects.requireNonNull(max, "max must not be null");
        if (multiplier <= 0) throw new IllegalArgumentException("multiplier must be > 0");

        RetryDelayStrategy strategy = attempt -> {
            double ms = initial.toMillis() * Math.pow(multiplier, attempt - 1);
            long cappedMs = Math.min((long) ms, max.toMillis());
            if (jitter) {
                cappedMs = ThreadLocalRandom.current().nextLong(0, cappedMs + 1);
            }
            return Duration.ofMillis(cappedMs);
        };
        return new RetryPolicy(maxAttempts, strategy, t -> true);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public RetryDelayStrategy getDelayStrategy() {
        return delayStrategy;
    }

    public ExceptionClassifier getExceptionClassifier() {
        return exceptionClassifier;
    }

    /** Returns {@code true} when more attempts are allowed and the exception is retryable. */
    public boolean shouldRetry(int attemptCount, Throwable t) {
        return attemptCount < maxAttempts && exceptionClassifier.isRetryable(t);
    }

    /** Computes the delay before the next attempt (1-based attempt number). */
    public Duration nextDelay(int attemptNumber) {
        return delayStrategy.nextDelay(attemptNumber);
    }
}
