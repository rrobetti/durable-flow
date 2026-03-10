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

    @Test
    void beforeProcessing_isStoredOnWorkflow() {
        WorkflowLifecycleHandler hook = ctx -> {};
        WorkflowDefinition wf = WorkflowDefinition.builder("hook-test")
                .beforeProcessing(hook)
                .step("s1", ctx -> StepResult.empty())
                .build();
        assertSame(hook, wf.getBeforeProcessing());
        assertNull(wf.getAfterProcessing());
    }

    @Test
    void afterProcessing_isStoredOnWorkflow() {
        WorkflowLifecycleHandler hook = ctx -> {};
        WorkflowDefinition wf = WorkflowDefinition.builder("hook-test2")
                .step("s1", ctx -> StepResult.empty())
                .afterProcessing(hook)
                .build();
        assertSame(hook, wf.getAfterProcessing());
        assertNull(wf.getBeforeProcessing());
    }

    @Test
    void bothLifecycleHooks_canBeSetIndependently() {
        WorkflowLifecycleHandler before = ctx -> {};
        WorkflowLifecycleHandler after = ctx -> {};
        WorkflowDefinition wf = WorkflowDefinition.builder("hook-both")
                .beforeProcessing(before)
                .step("s1", ctx -> StepResult.empty())
                .afterProcessing(after)
                .build();
        assertSame(before, wf.getBeforeProcessing());
        assertSame(after, wf.getAfterProcessing());
    }

    @Test
    void noLifecycleHooks_returnsNull() {
        WorkflowDefinition wf = WorkflowDefinition.builder("no-hooks")
                .step("s1", ctx -> StepResult.empty())
                .build();
        assertNull(wf.getBeforeProcessing());
        assertNull(wf.getAfterProcessing());
    }

    @Test
    void nullBeforeProcessingHook_throws() {
        assertThrows(NullPointerException.class,
                () -> WorkflowDefinition.builder("test").beforeProcessing(null));
    }

    @Test
    void nullAfterProcessingHook_throws() {
        assertThrows(NullPointerException.class,
                () -> WorkflowDefinition.builder("test").afterProcessing(null));
    }
}
