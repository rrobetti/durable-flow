package io.github.durableflow.engine;

import io.github.durableflow.api.WorkflowDefinition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory registry of {@link WorkflowDefinition}s, keyed by workflow name.
 *
 * <p>The registry is populated at message receive time and used during background
 * recovery to look up the appropriate workflow.
 */
public final class WorkflowRegistry {

    private final Map<String, WorkflowDefinition> registry = new ConcurrentHashMap<>();

    public void register(WorkflowDefinition definition) {
        registry.put(definition.getName(), definition);
    }

    public Optional<WorkflowDefinition> find(String workflowName) {
        return Optional.ofNullable(registry.get(workflowName));
    }

    public boolean isRegistered(String workflowName) {
        return registry.containsKey(workflowName);
    }
}
