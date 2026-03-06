package io.github.durableflow.api;

/**
 * Executes a single workflow step.
 *
 * <p>Implementations must be thread-safe; a single instance may be invoked
 * concurrently by multiple threads for different messages.
 */
@FunctionalInterface
public interface StepHandler {

    /**
     * Execute this step.
     *
     * @param context step execution context
     * @return result of the step
     * @throws Exception any exception – retryable vs. final is determined by the {@link RetryPolicy}
     */
    StepResult execute(StepContext context) throws Exception;
}
