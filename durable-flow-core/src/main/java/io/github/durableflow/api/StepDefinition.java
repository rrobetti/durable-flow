package io.github.durableflow.api;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable description of a single step within a {@link WorkflowDefinition}.
 */
public final class StepDefinition {

    private final String name;
    private final StepHandler handler;
    private final List<String> dependsOn;
    private final RetryPolicy retryPolicy;

    public StepDefinition(String name, StepHandler handler, List<String> dependsOn, RetryPolicy retryPolicy) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
        this.dependsOn = dependsOn != null ? Collections.unmodifiableList(dependsOn) : Collections.emptyList();
        this.retryPolicy = retryPolicy != null ? retryPolicy : RetryPolicy.defaultPolicy();
    }

    public String getName() {
        return name;
    }

    public StepHandler getHandler() {
        return handler;
    }

    public List<String> getDependsOn() {
        return dependsOn;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
