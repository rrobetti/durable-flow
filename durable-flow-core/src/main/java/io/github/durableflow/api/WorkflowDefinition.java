package io.github.durableflow.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable description of a workflow – a named, ordered set of steps.
 *
 * <p>Use {@link #builder(String)} to construct instances:
 * <pre>{@code
 * WorkflowDefinition wf = WorkflowDefinition.builder("order-processing")
 *     .step("validate",  ctx -> StepResult.empty())
 *     .step("enrich",    ctx -> StepResult.empty()).dependsOn("validate").retryPolicy(policy)
 *     .step("notify",    ctx -> StepResult.empty()).dependsOn("enrich")
 *     .build();
 * }</pre>
 */
public final class WorkflowDefinition {

    private final String name;
    private final List<StepDefinition> steps;

    private WorkflowDefinition(String name, List<StepDefinition> steps) {
        this.name = name;
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
    }

    public String getName() {
        return name;
    }

    public List<StepDefinition> getSteps() {
        return steps;
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

        private StepBuilder(String workflowName) {
            this.workflowName = Objects.requireNonNull(workflowName, "workflowName must not be null");
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
            return new WorkflowDefinition(workflowName, steps);
        }

        private void commitPending() {
            if (pending != null) {
                steps.add(new StepDefinition(
                        pending.name,
                        pending.handler,
                        Collections.unmodifiableList(new ArrayList<>(pending.dependsOn)),
                        pending.retryPolicy != null ? pending.retryPolicy : RetryPolicy.defaultPolicy()));
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
