package io.github.durableflow.api;

/**
 * Lifecycle hook invoked at the very beginning ({@code beforeProcessing}) or very end
 * ({@code afterProcessing}) of a workflow run, regardless of whether the workflow
 * succeeded, failed, or was parked.
 *
 * <p>Registered on a {@link WorkflowDefinition} via the builder:
 * <pre>{@code
 * WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
 *     .beforeProcessing(ctx -> log.info("Starting workflow for message {}", ctx.getMessageId()))
 *     .step("step1", ctx -> StepResult.empty())
 *     .afterProcessing(ctx -> log.info("Workflow ended with state {}", ctx.getFinalState()))
 *     .build();
 * }</pre>
 *
 * <p>Any exception thrown by a lifecycle hook is caught and logged; it does not
 * affect the workflow's own state or step execution.
 *
 * <p>Implementations must be thread-safe; a single instance may be invoked
 * concurrently for different messages.
 */
@FunctionalInterface
public interface WorkflowLifecycleHandler {

    /**
     * Invoked as a lifecycle hook.
     *
     * @param context workflow context; {@link WorkflowContext#getFinalState()} is {@code null}
     *                for {@code beforeProcessing} and non-null for {@code afterProcessing}
     * @throws Exception any exception — it will be caught and logged, not propagated
     */
    void handle(WorkflowContext context) throws Exception;
}
