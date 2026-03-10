# durable-flow-spring-sample

A runnable Spring Boot 3.3 application that shows how to wire **durable-flow** into a real service:
consume a JSON order message from an **ActiveMQ Artemis** topic, then process it with a two-step
durable workflow — persisting the order to a database **and** publishing a notification event.

---

## What it demonstrates

| Concern | How it is handled |
|---|---|
| Receiving messages | `@JmsListener` on an Artemis topic (pub/sub) |
| Durable processing | `DurableFlowEngine.receive()` persists the message before ACK |
| Business workflow | Two parallel steps: `save-order` + `publish-notification` |
| Database access | Spring Data JPA (`OrderRepository`) → `order_records` table |
| Schema management | durable-flow's own Flyway migration + JPA `ddl-auto` |
| Integration testing | Testcontainers Artemis + H2 in-memory (PostgreSQL-compat mode) |

---

## Module structure

```
durable-flow-spring-sample/
├── src/main/java/io/github/durableflow/sample/
│   ├── DurableFlowSampleApplication.java   ← Spring Boot entry point
│   ├── config/
│   │   ├── DurableFlowEngineConfig.java    ← creates the DurableFlowEngine bean
│   │   └── JmsConfig.java                  ← topic listener factory (pub/sub mode)
│   ├── listener/
│   │   └── OrderMessageListener.java       ← @JmsListener → engine.receive()
│   ├── model/
│   │   └── OrderMessage.java               ← JSON payload POJO
│   ├── entity/
│   │   └── OrderRecord.java                ← JPA entity (order_records table)
│   ├── repository/
│   │   └── OrderRepository.java            ← Spring Data JPA repository
│   └── workflow/
│       └── OrderWorkflow.java              ← WorkflowDefinition with two steps
├── src/main/resources/application.yml      ← Artemis + PostgreSQL config
└── src/test/
    ├── java/…/OrderWorkflowIntegrationTest.java  ← end-to-end integration test
    └── resources/application-test.yml            ← H2 + test overrides
```

---

## The workflow

```
Artemis topic (order.incoming)
        │  raw JSON
        ▼
OrderMessageListener
        │  engine.receive()  ←── message durably written to DB here
        ▼
DurableFlowEngine
   ┌────┴────┐
   ▼         ▼           (both steps run in parallel)
save-order   publish-notification
   │              │
   ▼              ▼
order_records   order.notifications queue
  (H2 / PG)      (Artemis)
```

`OrderWorkflow` builds the definition:

```java
WorkflowDefinition.builder("order-processing")
    .step("publish-notification", this::publishNotification)   // sends JSON to queue
    .step("save-order",           this::saveOrderToDatabase)   // JPA save
    .build();
```

Neither step declares `dependsOn`, so the engine runs them concurrently on its worker pool.
Both are retried up to three times (durable-flow default) if they throw.

---

## Running the application

### Prerequisites

- Java 17+
- Maven 3.9+
- A running **ActiveMQ Artemis** broker (`tcp://localhost:61616`, default credentials `artemis / artemis`)
- A running **PostgreSQL** instance with a database named `orders_db`

### Start

```bash
mvn spring-boot:run -pl durable-flow-spring-sample -am
```

Override connection details as needed:

```bash
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="\
    -Dspring.artemis.broker-url=tcp://my-broker:61616 \
    -Dspring.datasource.url=jdbc:postgresql://my-db:5432/orders_db \
    -Dspring.datasource.username=orders \
    -Dspring.datasource.password=secret"
```

### Send a test message

Once the app is running, publish a JSON message to the `order.incoming` topic from any Artemis
client or the broker web console:

```json
{
  "orderId":     "ORD-001",
  "customerId":  "CUST-42",
  "amount":      149.99,
  "description": "Two widgets and a sprocket"
}
```

You should see the order saved in `order_records` and a notification on `order.notifications`.

---

## Running the integration tests

Tests require **Docker** (for the Artemis Testcontainer). H2 runs in-memory — no extra setup needed.

```bash
# From the repo root:
mvn verify -pl durable-flow-spring-sample -am
```

`OrderWorkflowIntegrationTest` spins up `apache/activemq-artemis:latest`, overrides the broker URL
via `@DynamicPropertySource`, publishes a message, and asserts (with Awaitility) that:

1. The `OrderRecord` is saved to the H2 database.
2. An `ORDER_RECEIVED` notification appears on the notifications queue.

---

## Key integration points

### `DurableFlowEngineConfig`

Wires `DurableFlowEngine` over Spring Boot's auto-configured `DataSource`. The engine runs its own
Flyway migration (`schemaAutoMigrate = true`) for the `messages` / `message_steps` tables, so
`spring.flyway.enabled` is set to `false` in `application.yml` to prevent Spring Boot from
interfering.

### `JmsConfig`

Registers a `topicListenerFactory` bean (`pubSubDomain = true`) that `OrderMessageListener` uses via
`containerFactory = "topicListenerFactory"`. Spring Boot's default factory is queue-only; without
this the listener would silently consume from a queue instead of a topic.

### Durability guarantee

`DurableFlowEngine.receive()` commits the message record to the database **before** returning.
The JMS listener can therefore acknowledge the broker message immediately — even if the worker
threads that run `save-order` and `publish-notification` are still in flight or haven't started yet.
Failures are retried automatically; the background recovery scheduler picks up any steps whose
lease expired before completion.
