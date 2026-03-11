package io.github.durableflow.sample.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.durableflow.api.*;
import io.github.durableflow.sample.entity.OrderRecord;
import io.github.durableflow.sample.model.OrderMessage;
import io.github.durableflow.sample.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;

import java.time.Instant;

/**
 * Defines the {@code order-processing} workflow with two independent steps:
 *
 * <ol>
 *   <li><b>publish-notification</b> – serialises an {@code OrderNotification} event and
 *       sends it to the configured notifications queue via {@link JmsTemplate}.</li>
 *   <li><b>save-order</b> – persists the incoming {@link OrderMessage} as an
 *       {@link OrderRecord} in the relational database via Spring Data JPA.</li>
 * </ol>
 *
 * <p>The two steps have no declared dependency on each other, so the durable-flow engine
 * executes them in parallel on its worker thread pool. Both steps are retried up to three
 * times with a 1-second fixed delay if they throw an exception.
 *
 * <p>Step handlers capture Spring beans through constructor injection and are therefore
 * fully wired by the Spring context while being executed on durable-flow's threads.
 */
@Configuration
public class OrderWorkflow {

    private static final Logger log = LoggerFactory.getLogger(OrderWorkflow.class);

    private final JmsTemplate jmsTemplate;
    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;

    @Value("${app.order.notifications-queue:order.notifications}")
    private String notificationsQueue;

    public OrderWorkflow(JmsTemplate jmsTemplate,
                         OrderRepository orderRepository,
                         ObjectMapper objectMapper) {
        this.jmsTemplate = jmsTemplate;
        this.orderRepository = orderRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Builds and returns the {@link WorkflowDefinition} used by
     * {@link io.github.durableflow.sample.listener.OrderMessageListener}.
     */
    @Bean
    public WorkflowDefinition orderWorkflowDefinition() {
        return WorkflowDefinition.builder("order-processing")
                .beforeProcessing(ctx ->
                        log.info("Starting order workflow – messageId={}", ctx.getMessageId()))
                .step("publish-notification", this::publishNotification)
                .step("save-order", this::saveOrderToDatabase)
                .afterProcessing(ctx ->
                        log.info("Order workflow finished – messageId={} finalState={}",
                                ctx.getMessageId(), ctx.getFinalState()))
                .build();
    }

    // -------------------------------------------------------------------------
    // Step handlers
    // -------------------------------------------------------------------------

    /**
     * Step 1: publishes an order-received notification to the notifications queue.
     */
    private StepResult publishNotification(StepContext ctx) throws Exception {
        OrderMessage order = objectMapper.readValue(ctx.getPayload(), OrderMessage.class);

        OrderNotification notification = new OrderNotification(
                order.getOrderId(), "ORDER_RECEIVED", Instant.now().toString());
        String payload = objectMapper.writeValueAsString(notification);

        jmsTemplate.convertAndSend(notificationsQueue, payload);
        log.info("Published notification for orderId={}", order.getOrderId());
        return StepResult.empty();
    }

    /**
     * Step 2: persists the order as an {@link OrderRecord} in the database.
     */
    private StepResult saveOrderToDatabase(StepContext ctx) throws Exception {
        OrderMessage order = objectMapper.readValue(ctx.getPayload(), OrderMessage.class);

        OrderRecord record = new OrderRecord();
        record.setOrderId(order.getOrderId());
        record.setCustomerId(order.getCustomerId());
        record.setAmount(order.getAmount());
        record.setDescription(order.getDescription());
        record.setStatus("RECEIVED");
        record.setCreatedAt(Instant.now());

        orderRepository.save(record);
        log.info("Saved order to database – orderId={}", order.getOrderId());
        return StepResult.empty();
    }

    // -------------------------------------------------------------------------
    // Internal transfer object
    // -------------------------------------------------------------------------

    /** Payload sent to the notifications queue. */
    private record OrderNotification(String orderId, String event, String timestamp) {}
}
