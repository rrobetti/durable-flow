package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the external JDBC connection feature.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>{@code receive()} with an external connection does not commit — the message
 *       remains invisible to other connections until the caller commits;</li>
 *   <li>a rollback on the external connection leaves no trace in the database;</li>
 *   <li>duplicate detection works correctly when an external connection is used.</li>
 * </ul>
 */
class ExternalConnectionIntegrationTest extends BaseIntegrationTest {

    // ------------------------------------------------------------------
    // receive() with external connection — transaction not committed yet
    // ------------------------------------------------------------------

    @Test
    void receive_withExternalConnection_doesNotCommitAutomatically() throws Exception {
        WorkflowDefinition wf = singleStepWorkflow("ext-conn-test", ctx -> StepResult.empty());

        try (Connection externalConn = dataSource.getConnection()) {
            externalConn.setAutoCommit(false);

            ReceiveResult result = engine.receive(
                    message("ext-src", "payload"),
                    ReceiveOptions.of(wf, externalConn));

            assertNotNull(result.messageId());
            assertFalse(result.duplicate());
            // State is RECEIVED because step execution is always async with an external connection
            assertEquals(MessageState.RECEIVED, result.messageState());

            // The message must NOT be visible from another connection before the caller commits
            Optional<MessageStatus> beforeCommit = engine.getMessageStatus(result.messageId());
            assertFalse(beforeCommit.isPresent(),
                    "Message should not be visible to other connections before the external commit");

            // After the caller commits the message becomes durable
            externalConn.commit();

            Optional<MessageStatus> afterCommit = engine.getMessageStatus(result.messageId());
            assertTrue(afterCommit.isPresent(),
                    "Message should be visible after the external commit");
        }
    }

    @Test
    void receive_withExternalConnection_rollback_leavesNoTrace() throws Exception {
        WorkflowDefinition wf = singleStepWorkflow("ext-rollback-test", ctx -> StepResult.empty());

        String messageId;
        try (Connection externalConn = dataSource.getConnection()) {
            externalConn.setAutoCommit(false);

            ReceiveResult result = engine.receive(
                    message("rollback-src", "payload"),
                    ReceiveOptions.of(wf, externalConn));

            messageId = result.messageId();

            // Caller decides to roll back
            externalConn.rollback();
        }

        // The message must not exist in the database after rollback
        Optional<MessageStatus> status = engine.getMessageStatus(messageId);
        assertFalse(status.isPresent(),
                "Message should not exist after the external connection is rolled back");
    }

    // ------------------------------------------------------------------
    // Duplicate detection with external connection
    // ------------------------------------------------------------------

    @Test
    void receive_withExternalConnection_duplicate_returnsDuplicateResult() throws Exception {
        WorkflowDefinition wf = singleStepWorkflow("ext-dup-test", ctx -> StepResult.empty());

        // First receive without an external connection so the record is committed
        ReceiveResult first = engine.receive(message("dup-src", "dup-payload"), ReceiveOptions.of(wf));
        assertFalse(first.duplicate());

        // Second receive of the same message using an external connection
        try (Connection externalConn = dataSource.getConnection()) {
            externalConn.setAutoCommit(false);

            ReceiveResult dup = engine.receive(
                    message("dup-src", "dup-payload"),
                    ReceiveOptions.of(wf, externalConn));

            assertTrue(dup.duplicate(), "Should be detected as a duplicate");
            assertEquals(first.messageId(), dup.messageId(),
                    "Duplicate result should carry the original message ID");

            externalConn.rollback();
        }
    }
}
