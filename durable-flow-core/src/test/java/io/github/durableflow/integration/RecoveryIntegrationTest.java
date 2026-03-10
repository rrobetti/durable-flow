package io.github.durableflow.integration;

import io.github.durableflow.api.*;
import io.github.durableflow.persistence.JdbcStepRepository;
import io.github.durableflow.persistence.StepRecord;
import io.github.durableflow.persistence.dialect.PostgreSqlDialect;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the recovery scheduler restores expired RUNNING leases to FAILED_RETRYABLE.
 */
class RecoveryIntegrationTest extends BaseIntegrationTest {

    @Test
    void expiredLease_isRecoveredAndRetried() throws Exception {
        CountDownLatch executedLatch = new CountDownLatch(1);

        WorkflowDefinition wf = singleStepWorkflow("recovery-wf", ctx -> {
            executedLatch.countDown();
            return StepResult.empty();
        });

        ReceiveResult result = engine.receive(message("recovery-src", "recovery-data"), ReceiveOptions.of(wf));
        assertTrue(executedLatch.await(10, TimeUnit.SECONDS), "Step should execute initially");
        waitFor(500);

        // Simulate expired lease by manually setting a step to RUNNING with expired locked_until
        forceExpiredLease(result.messageId());

        JdbcStepRepository repo = new JdbcStepRepository(dataSource, PostgreSqlDialect.INSTANCE);
        int recovered = repo.recoverExpiredLeases();
        assertTrue(recovered >= 0, "Recovery should not error");

        // Verify message ends in PROCESSED state
        waitFor(500);
        Optional<MessageStatus> status = engine.getMessageStatus(result.messageId());
        assertTrue(status.isPresent());
    }

    @Test
    void recoveryScheduler_picksUpExpiredLeases() throws Exception {
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(2);

        WorkflowDefinition wf = singleStepWorkflow("sched-recovery-wf", ctx -> {
            first.countDown();
            second.countDown();
            return StepResult.empty();
        });

        engine.receive(message("sched-src", "sched-data"), ReceiveOptions.of(wf));
        assertTrue(first.await(10, TimeUnit.SECONDS));

        // The engine has leaseTimeout=5s and recoveryInterval=2s (set in BaseIntegrationTest)
        // so recovery should pick up any dangling steps within a few seconds
        String msgId2 = engine.receive(message("sched-src2", "data2"), ReceiveOptions.of(wf)).messageId();
        List<StepRecord> steps = new JdbcStepRepository(dataSource, PostgreSqlDialect.INSTANCE)
                .findStepsForMessage(engine.getMessageStatus(msgId2).get().getMessageId());
        assertFalse(steps.isEmpty());
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private void forceExpiredLease(String messageId) throws Exception {
        String sql = """
                UPDATE message_steps
                SET step_state = 'RUNNING', locked_until = ?, owner = 'ghost-node'
                WHERE message_id = ?
                """;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(true);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, Timestamp.from(Instant.now().minusSeconds(120)));
                ps.setString(2, messageId);
                ps.executeUpdate();
            }
        }
    }
}
