package io.github.durableflow.spring;

import io.github.durableflow.DurableFlowEngine;
import org.springframework.context.SmartLifecycle;

/**
 * {@link SmartLifecycle} bean that manages the {@link DurableFlowEngine} startup and
 * shutdown within the Spring application context.
 *
 * <p>The engine is intentionally <em>not</em> started in its factory method so that Spring's
 * lifecycle ordering can be respected: the engine starts before message consumers and stops
 * after they have all shut down.
 *
 * <p>Phase {@code Integer.MIN_VALUE} ensures:
 * <ul>
 *   <li><b>Startup</b>: the engine starts before all other {@link SmartLifecycle} beans
 *       (e.g. JMS/Kafka message listeners at phase 0), so it is ready to accept work before
 *       any messages can arrive.</li>
 *   <li><b>Shutdown</b>: the engine stops last (after all message consumers have stopped),
 *       giving it time to drain in-flight steps cleanly.</li>
 * </ul>
 */
public class DurableFlowLifecycle implements SmartLifecycle {

    private final DurableFlowEngine engine;
    private volatile boolean running = false;

    public DurableFlowLifecycle(DurableFlowEngine engine) {
        this.engine = engine;
    }

    @Override
    public void start() {
        engine.start();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        engine.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
