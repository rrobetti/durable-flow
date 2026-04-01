package io.github.durableflow.sample.listener;

import io.github.durableflow.api.ReceiveResult;
import io.github.durableflow.api.WorkflowDefinition;
import io.github.durableflow.spring.DurableFlowTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

/**
 * JMS listener that consumes JSON order messages from an ActiveMQ Artemis topic and
 * hands each message off to the {@link DurableFlowTemplate} for durable, fault-tolerant
 * processing.
 *
 * <p>Because the listener is annotated with {@code @Transactional},
 * {@link DurableFlowTemplate#receive} borrows the transaction connection, inserts the
 * message atomically, and registers an {@code afterCommit} hook that dispatches steps
 * only after the transaction commits successfully.
 *
 * <p>Step execution (publish-notification &amp; save-order) then proceeds in the
 * engine's background thread pool.
 */
@Component
public class OrderMessageListener {

    private static final Logger log = LoggerFactory.getLogger(OrderMessageListener.class);

    private final DurableFlowTemplate durableFlowTemplate;
    private final WorkflowDefinition orderWorkflow;
    private final String inputTopic;

    public OrderMessageListener(DurableFlowTemplate durableFlowTemplate,
                                WorkflowDefinition orderWorkflow,
                                @Value("${app.order.input-topic}") String inputTopic) {
        this.durableFlowTemplate = durableFlowTemplate;
        this.orderWorkflow = orderWorkflow;
        this.inputTopic = inputTopic;
    }

    /**
     * Receives a raw JSON string from the order topic and submits it to the durable-flow
     * engine via {@link DurableFlowTemplate}.
     *
     * <p>The {@code topicListenerFactory} bean (defined in
     * {@link io.github.durableflow.sample.config.JmsConfig}) enables pub/sub (topic) mode.
     *
     * @param message the raw JSON payload from the topic
     */
    @JmsListener(
            destination = "${app.order.input-topic}",
            containerFactory = "topicListenerFactory")
    @Transactional
    public void onMessage(String message) {
        log.debug("Received raw order message from topic");

        ReceiveResult result = durableFlowTemplate.receive(
                inputTopic,
                message,
                Map.of("content-type", "application/json"),
                orderWorkflow);

        log.info("Order message durably stored – messageId={} duplicate={}",
                result.messageId(), result.duplicate());
    }
}
