package io.github.durableflow;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowDefinitionTest {

    @Test
    void builder_createsStepsInOrder() {
        WorkflowDefinition wf = WorkflowDefinition.builder("test-workflow")
                .step("s1", ctx -> StepResult.empty())
                .step("s2", ctx -> StepResult.empty())
                .step("s3", ctx -> StepResult.empty())
                .build();
        List<StepDefinition> steps = wf.getSteps();
        assertEquals(3, steps.size());
        assertEquals("s1", steps.get(0).getName());
        assertEquals("s2", steps.get(1).getName());
        assertEquals("s3", steps.get(2).getName());
    }

    @Test
    void builder_setsDependencies() {
        WorkflowDefinition wf = WorkflowDefinition.builder("dep-test")
                .step("validate", ctx -> StepResult.empty())
                .step("enrich", ctx -> StepResult.empty()).dependsOn("validate")
                .step("notify", ctx -> StepResult.empty()).dependsOn("enrich", "validate")
                .build();
        StepDefinition enrich = wf.getSteps().get(1);
        assertEquals(List.of("validate"), enrich.getDependsOn());

        StepDefinition notify = wf.getSteps().get(2);
        assertEquals(List.of("enrich", "validate"), notify.getDependsOn());
    }

    @Test
    void builder_setsRetryPolicy() {
        RetryPolicy policy = RetryPolicy.fixedDelay(5, java.time.Duration.ofSeconds(3));
        WorkflowDefinition wf = WorkflowDefinition.builder("retry-test")
                .step("step1", ctx -> StepResult.empty()).retryPolicy(policy)
                .build();
        assertEquals(5, wf.getSteps().get(0).getRetryPolicy().getMaxAttempts());
    }

    @Test
    void builder_defaultRetryPolicy_whenNotSet() {
        WorkflowDefinition wf = WorkflowDefinition.builder("default-policy")
                .step("step1", ctx -> StepResult.empty())
                .build();
        RetryPolicy policy = wf.getSteps().get(0).getRetryPolicy();
        assertEquals(RetryPolicy.DEFAULT_MAX_ATTEMPTS, policy.getMaxAttempts());
    }

    @Test
    void builder_emptyWorkflow_throws() {
        var builder = WorkflowDefinition.builder("empty");
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void workflowName_preserved() {
        WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
                .step("s1", ctx -> StepResult.empty())
                .build();
        assertEquals("my-workflow", wf.getName());
    }

    @Test
    void dependsOn_beforeStep_throws() {
        var builder = WorkflowDefinition.builder("test");
        assertThrows(IllegalStateException.class, () -> builder.dependsOn("nonexistent"));
    }

    @Test
    void nullWorkflowName_throws() {
        assertThrows(NullPointerException.class, () -> WorkflowDefinition.builder(null));
    }
}
