package io.github.durableflow.sample.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

/**
 * JMS configuration for the sample application.
 *
 * <p>Provides a dedicated {@link DefaultJmsListenerContainerFactory} for topic subscriptions.
 * Spring Boot auto-configures a default factory for queues; this bean supplements it with
 * topic (pub/sub) support so the {@code @JmsListener} on the order topic works correctly.
 */
@Configuration
public class JmsConfig {

    /**
     * Listener container factory configured for JMS topics (pub/sub mode).
     *
     * <p>Use {@code containerFactory = "topicListenerFactory"} in
     * {@link org.springframework.jms.annotation.JmsListener} to activate topic subscription.
     */
    @Bean
    public DefaultJmsListenerContainerFactory topicListenerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setPubSubDomain(true);
        factory.setConcurrency("1-4");
        factory.setSessionTransacted(false);
        return factory;
    }
}
