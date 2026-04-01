package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationIntegrationTest extends BaseIntegrationTest {

    @Test
    void samePayload_sameSource_isDeduplicated() {
        AtomicInteger execCount = new AtomicInteger(0);
        WorkflowDefinition wf = singleStepWorkflow("dedupe-wf", ctx -> {
            execCount.incrementAndGet();
            return StepResult.empty();
        });

        ReceiveResult r1 = engine.receive(message("src", "same-payload"), ReceiveOptions.withDeferredExecution(wf));
        ReceiveResult r2 = engine.receive(message("src", "same-payload"), ReceiveOptions.withDeferredExecution(wf));

        assertFalse(r1.duplicate(), "First message should not be a duplicate");
        assertTrue(r2.duplicate(), "Second message with same payload should be a duplicate");
        assertEquals(r1.messageId(), r2.messageId(), "Duplicate should return same messageId");
    }

    @Test
    void differentPayloads_sameSource_notDeduplicated() {
        WorkflowDefinition wf = singleStepWorkflow("no-dedupe-wf", ctx -> StepResult.empty());

        ReceiveResult r1 = engine.receive(message("src", "payload-a"), ReceiveOptions.withDeferredExecution(wf));
        ReceiveResult r2 = engine.receive(message("src", "payload-b"), ReceiveOptions.withDeferredExecution(wf));

        assertFalse(r1.duplicate());
        assertFalse(r2.duplicate());
        assertNotEquals(r1.messageId(), r2.messageId());
    }

    @Test
    void samePayload_differentSources_notDeduplicated() {
        WorkflowDefinition wf = singleStepWorkflow("src-dedupe-wf", ctx -> StepResult.empty());

        ReceiveResult r1 = engine.receive(message("source-A", "shared-payload"), ReceiveOptions.withDeferredExecution(wf));
        ReceiveResult r2 = engine.receive(message("source-B", "shared-payload"), ReceiveOptions.withDeferredExecution(wf));

        assertFalse(r1.duplicate());
        assertFalse(r2.duplicate());
        assertNotEquals(r1.messageId(), r2.messageId());
    }

    @Test
    void duplicate_returnsOriginalMessageState() throws Exception {
        WorkflowDefinition wf = singleStepWorkflow("state-dedupe-wf", ctx -> StepResult.empty());
        ReceiveResult first = engine.receive(message("src", "dup-payload"), ReceiveOptions.withDeferredExecution(wf));

        waitFor(3000); // let step execute

        ReceiveResult second = engine.receive(message("src", "dup-payload"), ReceiveOptions.withDeferredExecution(wf));
        assertTrue(second.duplicate());
        // The original message should now be PROCESSED
        assertEquals(first.messageId(), second.messageId());
    }
}
