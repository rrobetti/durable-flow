package io.github.durableflow.api;

/**
 * Determines whether a particular exception should trigger a retry.
 */
@FunctionalInterface
public interface ExceptionClassifier {

    /**
     * @param t the throwable that was thrown
     * @return {@code true} if the exception is transient and the step should be retried
     */
    boolean isRetryable(Throwable t);

    /** Marks all exceptions as retryable. */
    static ExceptionClassifier alwaysRetryable() {
        return t -> true;
    }

    /** Marks all exceptions as non-retryable. */
    static ExceptionClassifier neverRetryable() {
        return t -> false;
    }
}
