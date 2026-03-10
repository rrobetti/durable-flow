package io.github.durableflow.integration;

import com.zaxxer.hikari.HikariDataSource;
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests concurrent step claiming across multiple engine instances (simulated nodes).
 */
class MultiNodeIntegrationTest extends BaseIntegrationTest {

    @Test
    void twoNodes_doNotProcessSameStepTwice() throws Exception {
        AtomicInteger execCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        WorkflowDefinition wf = singleStepWorkflow("multi-node-wf", ctx -> {
            execCount.incrementAndGet();
            latch.countDown();
            return StepResult.empty();
        });

        // Create a second engine node pointing at the same DB
        HikariDataSource ds2 = createDataSource(POSTGRES);
        DurableFlowEngine engine2 = new DurableFlowEngine(ds2, DurableFlowConfig.builder()
                .nodeId("node-2")
                .leaseTimeoutSeconds(5)
                .schemaAutoMigrate(false)
                .build());
        engine2.start();

        try {
            engine.receive(message("multi-src", "multi-data"), ReceiveOptions.of(wf));
            assertTrue(latch.await(15, TimeUnit.SECONDS), "Step should execute exactly once");
            waitFor(500);
            assertEquals(1, execCount.get(), "Step must execute exactly once across both nodes");
        } finally {
            engine2.close();
            ds2.close();
        }
    }

    @Test
    void concurrentReceives_allProcessed() throws Exception {
        int msgCount = 10;
        CountDownLatch latch = new CountDownLatch(msgCount);
        AtomicInteger counter = new AtomicInteger(0);

        WorkflowDefinition wf = singleStepWorkflow("concurrent-wf", ctx -> {
            counter.incrementAndGet();
            latch.countDown();
            return StepResult.empty();
        });

        ExecutorService senders = Executors.newFixedThreadPool(4);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < msgCount; i++) {
            final int idx = i;
            futures.add(senders.submit(() ->
                    engine.receive(message("conc-src", "payload-" + idx), ReceiveOptions.of(wf))));
        }
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        senders.shutdown();

        assertTrue(latch.await(30, TimeUnit.SECONDS), "All steps should complete");
        assertEquals(msgCount, counter.get());
    }
}
