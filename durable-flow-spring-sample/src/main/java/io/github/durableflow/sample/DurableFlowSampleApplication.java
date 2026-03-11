package io.github.durableflow.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the durable-flow sample application.
 *
 * <p>This application demonstrates how to integrate the durable-flow library
 * with Spring Boot, ActiveMQ Artemis, and Spring Data JPA. It listens for
 * JSON order messages on an Artemis topic and processes each one through a
 * two-step durable workflow:
 * <ol>
 *   <li><b>publish-notification</b> – forwards an event to a notification queue.</li>
 *   <li><b>save-order</b> – persists the order record to a relational database.</li>
 * </ol>
 */
@SpringBootApplication
public class DurableFlowSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(DurableFlowSampleApplication.class, args);
    }
}
