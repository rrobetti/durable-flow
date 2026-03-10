package io.github.durableflow.sample;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.durableflow.sample.entity.OrderRecord;
import io.github.durableflow.sample.model.OrderMessage;
import io.github.durableflow.sample.repository.OrderRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for the order processing workflow.
 *
 * <p>Infrastructure used:
 * <ul>
 *   <li><b>ActiveMQ Artemis</b> – started by Testcontainers
 *       ({@code apache/activemq-artemis:latest}). The mapped TCP port is injected
 *       into the Spring context via {@link DynamicPropertySource}.</li>
 *   <li><b>H2 in-memory database</b> (PostgreSQL compatibility mode) – configured in
 *       {@code application-test.yml}. No container is required for an embedded database.</li>
 * </ul>
 *
 * <p>The test verifies that, after a JSON order message is published to the Artemis
 * topic, the durable-flow engine:
 * <ol>
 *   <li>Persists the order in the {@code order_records} table (save-order step).</li>
 *   <li>Publishes an order-received notification to the notifications queue
 *       (publish-notification step).</li>
 * </ol>
 */
@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
@Testcontainers
class OrderWorkflowIntegrationTest {

    private static final int ARTEMIS_JMS_PORT = 61616;

    /**
     * Artemis broker container.
     *
     * <p>{@code ANONYMOUS_LOGIN=true} disables credential requirements so the
     * default Spring Boot Artemis auto-configuration (no explicit user/password)
     * can connect without additional setup.
     */
    @Container
    static final GenericContainer<?> artemis =
            new GenericContainer<>("apache/activemq-artemis:latest")
                    .withExposedPorts(ARTEMIS_JMS_PORT)
                    .withEnv("ANONYMOUS_LOGIN", "true")
                    .waitingFor(Wait.forListeningPort())
                    .withStartupTimeout(Duration.ofMinutes(2));

    /**
     * Overrides {@code spring.artemis.broker-url} with the container's dynamically
     * assigned port before the Spring application context is started.
     * Credentials are cleared because {@code ANONYMOUS_LOGIN=true} is set on the container.
     */
    @DynamicPropertySource
    static void artemisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.artemis.broker-url",
                () -> "tcp://" + artemis.getHost() + ":" + artemis.getMappedPort(ARTEMIS_JMS_PORT));
        // Override the application-test.yml defaults: anonymous login needs no credentials
        registry.add("spring.artemis.user", () -> "");
        registry.add("spring.artemis.password", () -> "");
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepository orderRepository;

    /** Default JmsTemplate (queue mode) – used to publish to the input topic.
     *  Because we publish to a topic, we need a topic-aware template; we create
     *  it inline via the connection factory. */
    @Autowired
    private JmsTemplate jmsTemplate;

    @Value("${app.order.input-topic}")
    private String inputTopic;

    @Value("${app.order.notifications-queue}")
    private String notificationsQueue;

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void shouldSaveOrderToDatabaseWhenMessageIsPublishedToTopic() throws Exception {
        // Arrange
        OrderMessage order = new OrderMessage(
                "ORD-001", "CUST-42", new BigDecimal("149.99"), "Integration test order");
        publishToTopic(inputTopic, objectMapper.writeValueAsString(order));

        // Assert – order saved to database by the save-order step
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Optional<OrderRecord> saved = orderRepository.findByOrderId("ORD-001");
                    assertThat(saved).isPresent();
                    assertThat(saved.get().getCustomerId()).isEqualTo("CUST-42");
                    assertThat(saved.get().getAmount()).isEqualByComparingTo("149.99");
                    assertThat(saved.get().getDescription()).isEqualTo("Integration test order");
                    assertThat(saved.get().getStatus()).isEqualTo("RECEIVED");
                    assertThat(saved.get().getCreatedAt()).isNotNull();
                });
    }

    @Test
    void shouldPublishNotificationWhenMessageIsPublishedToTopic() throws Exception {
        // Arrange
        OrderMessage order = new OrderMessage(
                "ORD-002", "CUST-99", new BigDecimal("49.00"), "Notification test order");
        publishToTopic(inputTopic, objectMapper.writeValueAsString(order));

        // Assert – notification published to the notifications queue by the
        // publish-notification step.
        // spring.jms.template.receive-timeout=1000 (set in application-test.yml) means
        // each receiveAndConvert() waits at most 1 second before returning null;
        // Awaitility retries until the notification arrives or the overall timeout elapses.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    String notification = (String) jmsTemplate.receiveAndConvert(notificationsQueue);
                    if (notification != null) {
                        assertThat(notification).contains("ORD-002");
                        assertThat(notification).contains("ORDER_RECEIVED");
                        return true;
                    }
                    return false;
                });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Publishes a text message to a JMS topic using a temporary
     * {@link JmsTemplate} configured for pub/sub mode.
     */
    private void publishToTopic(String topic, String payload) {
        JmsTemplate topicTemplate = new JmsTemplate(jmsTemplate.getConnectionFactory());
        topicTemplate.setPubSubDomain(true);
        topicTemplate.convertAndSend(topic, payload);
    }
}
