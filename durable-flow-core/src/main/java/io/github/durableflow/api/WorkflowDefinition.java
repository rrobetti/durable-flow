package io.github.durableflow.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable description of a workflow – a named, ordered set of steps with optional
 * lifecycle hooks.
 *
 * <p>Use {@link #builder(String)} to construct instances:
 * <pre>{@code
 * WorkflowDefinition wf = WorkflowDefinition.builder("order-processing")
 *     .beforeProcessing(ctx -> log.info("Starting {}", ctx.getMessageId()))
 *     .step("validate",  ctx -> StepResult.empty())
 *     .step("enrich",    ctx -> StepResult.empty()).dependsOn("validate").retryPolicy(policy)
 *     .step("notify",    ctx -> StepResult.empty()).dependsOn("enrich")
 *     .afterProcessing(ctx -> log.info("Done: {}", ctx.getFinalState()))
 *     .build();
 * }</pre>
 *
 * <p>To disable retries for every step in a workflow, set a workflow-level default:
 * <pre>{@code
 * WorkflowDefinition wf = WorkflowDefinition.builder("fire-and-forget")
 *     .defaultRetryPolicy(RetryPolicy.noRetry())
 *     .step("step-a", ctx -> StepResult.empty())
 *     .step("step-b", ctx -> StepResult.empty())
 *     .build();
 * }</pre>
 * A per-step {@link StepBuilder#retryPolicy(RetryPolicy)} call always takes precedence over the
 * workflow-level default.
 */
public final class WorkflowDefinition {

    private final String name;
    private final List<StepDefinition> steps;
    private final WorkflowLifecycleHandler beforeProcessing;
    private final WorkflowLifecycleHandler afterProcessing;

    private WorkflowDefinition(String name, List<StepDefinition> steps,
                                WorkflowLifecycleHandler beforeProcessing,
                                WorkflowLifecycleHandler afterProcessing) {
        this.name = name;
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
        this.beforeProcessing = beforeProcessing;
        this.afterProcessing = afterProcessing;
    }

    public String getName() {
        return name;
    }

    public List<StepDefinition> getSteps() {
        return steps;
    }

    /**
     * Returns the lifecycle hook invoked before any steps execute, or {@code null} if none
     * was registered.
     */
    public WorkflowLifecycleHandler getBeforeProcessing() {
        return beforeProcessing;
    }

    /**
     * Returns the lifecycle hook invoked after all steps finish (regardless of outcome),
     * or {@code null} if none was registered.
     */
    public WorkflowLifecycleHandler getAfterProcessing() {
        return afterProcessing;
    }

    public static StepBuilder builder(String workflowName) {
        return new StepBuilder(workflowName);
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static final class StepBuilder {

        private final String workflowName;
        private final List<StepDefinition> steps = new ArrayList<>();
        private PendingStep pending;
        private WorkflowLifecycleHandler beforeProcessing;
        private WorkflowLifecycleHandler afterProcessing;
        private RetryPolicy defaultRetryPolicy;

        private StepBuilder(String workflowName) {
            this.workflowName = Objects.requireNonNull(workflowName, "workflowName must not be null");
        }

        /**
         * Registers a lifecycle hook invoked before any steps execute for a given message.
         * May be called at any point during building; order relative to {@code step()} calls
         * does not matter.
         */
        public StepBuilder beforeProcessing(WorkflowLifecycleHandler handler) {
            this.beforeProcessing = Objects.requireNonNull(handler, "beforeProcessing handler must not be null");
            return this;
        }

        /**
         * Registers a lifecycle hook invoked after all steps finish (regardless of outcome).
         * May be called at any point during building.
         */
        public StepBuilder afterProcessing(WorkflowLifecycleHandler handler) {
            this.afterProcessing = Objects.requireNonNull(handler, "afterProcessing handler must not be null");
            return this;
        }

        /**
         * Sets the default {@link RetryPolicy} applied to every step that does not declare its
         * own policy.  When not set, steps fall back to {@link RetryPolicy#defaultPolicy()}.
         *
         * <p>Use {@link RetryPolicy#noRetry()} to disable retries for the entire workflow:
         * <pre>{@code
         * WorkflowDefinition.builder("fire-and-forget")
         *     .defaultRetryPolicy(RetryPolicy.noRetry())
         *     .step("step-a", ctx -> StepResult.empty())
         *     .step("step-b", ctx -> StepResult.empty())
         *     .build();
         * }</pre>
         *
         * <p>A per-step {@link #retryPolicy(RetryPolicy)} call always overrides this default.
         */
        public StepBuilder defaultRetryPolicy(RetryPolicy policy) {
            this.defaultRetryPolicy = Objects.requireNonNull(policy, "defaultRetryPolicy must not be null");
            return this;
        }

        /** Add a step with an explicit name and handler. */
        public StepBuilder step(String name, StepHandler handler) {
            commitPending();
            pending = new PendingStep(name, handler);
            return this;
        }

        /** Declare dependency of the most recently added step on one or more preceding steps. */
        public StepBuilder dependsOn(String... stepNames) {
            requirePending();
            for (String dep : stepNames) {
                pending.dependsOn.add(dep);
            }
            return this;
        }

        /** Set a retry policy for the most recently added step. */
        public StepBuilder retryPolicy(RetryPolicy policy) {
            requirePending();
            pending.retryPolicy = policy;
            return this;
        }

        public WorkflowDefinition build() {
            commitPending();
            if (steps.isEmpty()) {
                throw new IllegalStateException("WorkflowDefinition must have at least one step");
            }
            return new WorkflowDefinition(workflowName, steps, beforeProcessing, afterProcessing);
        }

        private void commitPending() {
            if (pending != null) {
                RetryPolicy effective = pending.retryPolicy != null
                        ? pending.retryPolicy
                        : (defaultRetryPolicy != null ? defaultRetryPolicy : RetryPolicy.defaultPolicy());
                steps.add(new StepDefinition(
                        pending.name,
                        pending.handler,
                        Collections.unmodifiableList(new ArrayList<>(pending.dependsOn)),
                        effective));
                pending = null;
            }
        }

        private void requirePending() {
            if (pending == null) {
                throw new IllegalStateException("No step currently being configured. Call step() first.");
            }
        }

        private static final class PendingStep {
            String name;
            StepHandler handler;
            List<String> dependsOn = new ArrayList<>();
            RetryPolicy retryPolicy;

            PendingStep(String name, StepHandler handler) {
                this.name = Objects.requireNonNull(name, "step name must not be null");
                this.handler = Objects.requireNonNull(handler, "step handler must not be null");
            }
        }
    }
}
