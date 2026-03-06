package io.github.durableflow.api;

import io.github.durableflow.spi.MessagePreprocessor;

import java.util.Objects;

/**
 * Options controlling how an inbound message is received and processed.
 *
 * @param workflow      the workflow definition to execute for this message
 * @param preprocessor  optional preprocessor; if null a default identity preprocessor is used
 */
public record ReceiveOptions(
        WorkflowDefinition workflow,
        MessagePreprocessor preprocessor) {

    public ReceiveOptions {
        Objects.requireNonNull(workflow, "workflow must not be null");
    }

    /** Convenience constructor using only a workflow definition. */
    public static ReceiveOptions of(WorkflowDefinition workflow) {
        return new ReceiveOptions(workflow, null);
    }
}
