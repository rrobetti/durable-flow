package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the external JDBC connection feature.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>{@code receive()} with an external connection does not commit — the message
 *       remains invisible to other connections until the caller commits;</li>
 *   <li>duplicate detection works correctly when an external connection is used;</li>
 *   <li>{@link StepContext#getConnection()} is present during step execution and the
 *       step handler can participate in durable-flow's step-execution transaction.</li>
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

    // ------------------------------------------------------------------
    // StepContext exposes a live connection to step handlers
    // ------------------------------------------------------------------

    @Test
    void stepContext_connectionIsPresent() throws Exception {
        AtomicBoolean connectionPresent = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        WorkflowDefinition wf = WorkflowDefinition.builder("conn-step-test")
                .step("step1", ctx -> {
                    connectionPresent.set(ctx.getConnection().isPresent());
                    latch.countDown();
                    return StepResult.empty();
                })
                .build();

        engine.receive(message("conn-step-src", "payload"), ReceiveOptions.of(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should execute within timeout");
        assertTrue(connectionPresent.get(), "StepContext should provide a JDBC connection");
    }

    @Test
    void stepContext_connectionIsNotClosed() throws Exception {
        AtomicBoolean connectionValid = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        WorkflowDefinition wf = WorkflowDefinition.builder("conn-open-test")
                .step("step1", ctx -> {
                    ctx.getConnection().ifPresent(conn -> {
                        try {
                            connectionValid.set(!conn.isClosed());
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to check connection state", e);
                        }
                    });
                    latch.countDown();
                    return StepResult.empty();
                })
                .build();

        engine.receive(message("conn-open-src", "payload"), ReceiveOptions.of(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should execute within timeout");
        assertTrue(connectionValid.get(), "Connection in StepContext should be open during step execution");
    }

    @Test
    void stepContext_connectionCanQueryDatabase() throws Exception {
        AtomicBoolean querySucceeded = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        WorkflowDefinition wf = WorkflowDefinition.builder("conn-query-test")
                .step("step1", ctx -> {
                    ctx.getConnection().ifPresent(conn -> {
                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT id FROM messages WHERE id = ?")) {
                            ps.setString(1, ctx.getMessageId());
                            try (ResultSet rs = ps.executeQuery()) {
                                querySucceeded.set(rs.next());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Query inside step handler failed", e);
                        }
                    });
                    latch.countDown();
                    return StepResult.empty();
                })
                .build();

        engine.receive(message("conn-query-src", "payload"), ReceiveOptions.of(wf));
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Step should execute within timeout");
        assertTrue(querySucceeded.get(),
                "Step handler should be able to query the database using the connection from StepContext");
    }

    @Test
    void stepContext_failingStep_rollsBackHandlerWrites() throws Exception {
        // The step inserts a row into the messages table under a unique marker, then fails.
        // durable-flow must roll back that insert together with its own step-state update.
        AtomicReference<String> markerMessageId = new AtomicReference<>();
        CountDownLatch failLatch = new CountDownLatch(1);

        WorkflowDefinition wf = WorkflowDefinition.builder("rollback-step-test")
                .step("step1", ctx -> {
                    ctx.getConnection().ifPresent(conn -> {
                        try (PreparedStatement ps = conn.prepareStatement(
                                "INSERT INTO messages " +
                                "(id, source, dedupe_hash, payload_length, payload_storage_mode, " +
                                " message_state, created_at, updated_at, workflow_name) " +
                                "VALUES (?, ?, ?, 0, 'INLINE', 'RECEIVED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)")) {
                            // Use a fresh UUID so it fits in the VARCHAR(36) id column
                            String markerId = java.util.UUID.randomUUID().toString();
                            markerMessageId.set(markerId);
                            ps.setString(1, markerId);
                            ps.setString(2, "marker-src");
                            ps.setString(3, "marker-hash-" + ctx.getMessageId().substring(0, 8));
                            ps.setString(4, "rollback-step-test");
                            ps.executeUpdate();
                        } catch (Exception e) {
                            throw new RuntimeException("Marker insert in step handler failed", e);
                        }
                    });
                    failLatch.countDown();
                    throw new RuntimeException("Intentional step failure to trigger rollback");
                })
                .build();

        engine.receive(message("rollback-step-src", "payload"), ReceiveOptions.of(wf));
        assertTrue(failLatch.await(10, TimeUnit.SECONDS), "Step should attempt execution within timeout");

        // Allow time for the failure handling to complete
        waitFor(500);

        String markerId = markerMessageId.get();
        assertNotNull(markerId, "Marker ID should have been set");

        // The marker row inserted by the step handler must have been rolled back
        Optional<MessageStatus> marker = engine.getMessageStatus(markerId);
        assertFalse(marker.isPresent(),
                "Step handler's DB write should have been rolled back together with the failed step");
    }
}
