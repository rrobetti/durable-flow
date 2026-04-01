# Integrating durable-flow with Spring Boot

This guide explains how to embed `durable-flow` inside a Spring Boot application using the
**Spring Boot Starter** — the recommended and simplest path. The starter provides
`DurableFlowTemplate`, which handles all transaction integration automatically so application
code never has to manage JDBC connections, `afterCommit` hooks, or engine lifecycle.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Defining a Workflow](#defining-a-workflow)
3. [Submitting Messages with DurableFlowTemplate](#submitting-messages-with-durableflowtemplate)
   - [Inside a @Transactional listener or controller](#inside-a-transactional-listener-or-controller)
   - [Transaction safety guarantees](#transaction-safety-guarantees)
   - [Duplicate messages](#duplicate-messages)
4. [Overriding individual auto-configured beans](#overriding-individual-auto-configured-beans)
5. [Manual wiring (without the starter)](#manual-wiring-without-the-starter)
6. [Database Connectivity](#database-connectivity)
7. [Flyway Co-existence](#flyway-co-existence)
8. [Execution Mode Selection](#execution-mode-selection)
9. [Using Spring Beans inside Step Handlers](#using-spring-beans-inside-step-handlers)
10. [Observability: Wiring the MetricsListener SPI](#observability-wiring-the-metricslistener-spi)
11. [Full Worked Example](#full-worked-example)
12. [Configuration Properties Reference](#configuration-properties-reference)

---

## Quick Start

Add a single dependency:

```xml
<dependency>
    <groupId>io.github.rrobetti</groupId>
    <artifactId>durable-flow-spring-boot-starter</artifactId>
    <version>1.0.0-ALPHA</version>
</dependency>
```

Configure the engine via `application.yml` (all properties are optional):

```yaml
# application.yml
durable-flow:
  node-id: ${HOSTNAME:node-1}        # unique per JVM instance; defaults to a random UUID
  lease-timeout-seconds: 60
  recovery-interval-seconds: 30
  schema-auto-migrate: true
  execution-mode: ASYNCHRONOUS
```

The starter auto-configures:

| Bean name | Type | Purpose |
|-----------|------|---------|
| `durableFlowConfig` | `DurableFlowConfig` | Engine configuration from `durable-flow.*` properties |
| `durableFlowEngine` | `DurableFlowEngine` | Core engine (not started until lifecycle fires) |
| `durableFlowLifecycle` | `DurableFlowLifecycle` | Starts engine before message consumers; stops it after they shut down |
| `durableFlowTemplate` | `DurableFlowTemplate` | Transparent transaction integration for `receive()` calls |

> **MetricsListener auto-wiring:** if a `MetricsListener` bean is present in the context
> (e.g. a Micrometer adapter) it is automatically passed to the engine constructor.

---

## Defining a Workflow

A `WorkflowDefinition` describes the steps that process a message. Define it as a Spring `@Bean`
inside a `@Configuration` class. Inject any Spring-managed dependencies (repositories, clients,
services) into the class and close over them in the step lambdas:

```java
// OrderWorkflow.java
@Configuration
public class OrderWorkflow {

    private final InventoryClient inventoryClient;
    private final NotificationService notificationService;

    public OrderWorkflow(InventoryClient inventoryClient,
                         NotificationService notificationService) {
        this.inventoryClient     = inventoryClient;
        this.notificationService = notificationService;
    }

    @Bean
    public WorkflowDefinition orderWorkflowDefinition() {
        return WorkflowDefinition.builder("order-processing")
            .beforeProcessing(ctx ->
                log.info("Starting order workflow for message {}", ctx.getMessageId()))
            .step("validate", ctx -> {
                Order order = deserialize(ctx.getPayload());
                if (order.getAmount().signum() <= 0)
                    throw new IllegalArgumentException("Order amount must be positive");
                return StepResult.of(ctx.getPayload());
            })
            .step("reserve-inventory", ctx -> {
                Order order = deserialize(ctx.getPreviousStepOutputs().get("validate"));
                inventoryClient.reserve(order);
                return StepResult.of(ctx.getPayload());
            }).dependsOn("validate")
              .retryPolicy(RetryPolicy.exponentialBackoff(
                  5, Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5), true))
            .step("notify-customer", ctx -> {
                notificationService.sendConfirmation(deserialize(ctx.getPayload()));
                return StepResult.empty();
            }).dependsOn("reserve-inventory")
              .retryPolicy(RetryPolicy.fixedDelay(3, Duration.ofSeconds(5)))
            .afterProcessing(ctx ->
                log.info("Order workflow {} ended: state={}", ctx.getMessageId(), ctx.getFinalState()))
            .build();
    }
}
```

Multiple workflow definitions can coexist — declare one `@Bean` per workflow. The
workflow definition bean is then injected into whichever listener or controller triggers it.

---

## Submitting Messages with DurableFlowTemplate

`DurableFlowTemplate` is the primary API for submitting messages to the engine. Inject it
directly into a `@JmsListener` component or `@RestController` and call `receive()`:

```java
@Autowired
private DurableFlowTemplate durableFlow;
```

### Inside a @Transactional listener or controller

Annotate the handler method with `@Transactional` and call `durableFlow.receive()`. The
template automatically:

1. Detects the active transaction on the current thread.
2. Borrows the connection already bound to that transaction so the durable-flow insert is
   part of the same database transaction as any other writes in the method.
3. Registers an `afterCommit` callback that calls `engine.dispatchSteps()` only after the
   transaction commits successfully.

```java
// JMS listener
@Component
public class OrderMessageListener {

    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflow;

    // ... constructor injection ...

    @JmsListener(destination = "${orders.topic}")
    @Transactional
    public void onMessage(String rawJson) {
        durableFlow.receive("orders", rawJson, Map.of(), orderWorkflow);
    }
}
```

```java
// REST controller
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderRepository orderRepository;
    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflowDefinition;

    // ... constructor injection ...

    @PostMapping
    @Transactional
    public ResponseEntity<String> placeOrder(@RequestBody Order order) {
        orderRepository.save(order);
        ReceiveResult result = durableFlow.receive(
            "order-service", serialize(order),
            Map.of("orderId", order.getId()),
            orderWorkflowDefinition);
        return ResponseEntity.accepted().body(result.messageId());
    }
}
```

When called outside any `@Transactional` context the template falls back to having the engine
manage its own connection — the message is persisted and steps are dispatched immediately after
the engine's internal transaction commits.

### Transaction safety guarantees

| Calling context | What the template does |
|-----------------|------------------------|
| Inside `@Transactional` | Borrows the transaction connection; inserts atomically with any other writes; registers `afterCommit` to dispatch steps only after the transaction commits. If the transaction rolls back, neither the insert nor the dispatch ever happens. |
| Outside any transaction | Engine manages its own connection; commits the insert and dispatches steps immediately. |

The `receive(String source, String textPayload, Map<String, String> headers, WorkflowDefinition workflow)`
overload accepts a plain `String` payload and encodes it to UTF-8 internally — JMS listeners
receiving text messages do not need to call `.getBytes()`.

### Duplicate messages

When a duplicate is detected, `receive()` returns a `ReceiveResult` with `duplicate() == true`
**without rolling back the connection**. The connection lifecycle remains under Spring's control —
the transaction commits normally (with no new insert, which is harmless). Use `result.duplicate()`
to branch your own logic if needed.

---

## Overriding individual auto-configured beans

Every bean produced by the starter is annotated `@ConditionalOnMissingBean`. Declare a bean
of the same type in any `@Configuration` class to replace it:

```java
// Override only the engine configuration — everything else stays auto-configured
@Configuration
public class MyDurableFlowConfig {

    @Bean
    public DurableFlowConfig durableFlowConfig() {
        return DurableFlowConfig.builder()
                .nodeId(System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString()))
                .leaseTimeoutSeconds(30)
                .schemaAutoMigrate(false)    // you manage Flyway separately
                .build();
    }
}
```

**Override the engine to use a separate DataSource:**

```java
@Configuration
public class MyDurableFlowConfig {

    @Bean
    @ConfigurationProperties("app.durable-flow.datasource")
    public DataSource durableFlowDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(
            @Qualifier("durableFlowDataSource") DataSource durableFlowDataSource,
            DurableFlowConfig durableFlowConfig) {
        // engine is NOT started here — DurableFlowLifecycle handles that
        return new DurableFlowEngine(durableFlowDataSource, durableFlowConfig);
    }
}
```

> **Exclude the auto-configuration entirely:**
> ```java
> @SpringBootApplication(exclude = DurableFlowAutoConfiguration.class)
> ```

---

## Manual wiring (without the starter)

If you only want `durable-flow-core` without the starter, create a `@Configuration` class:

```xml
<dependency>
    <groupId>io.github.rrobetti</groupId>
    <artifactId>durable-flow-core</artifactId>
    <version>1.0.0-ALPHA</version>
</dependency>
```

```java
@Configuration
public class DurableFlowEngineConfig {

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(DataSource dataSource) {
        DurableFlowConfig config = DurableFlowConfig.builder()
                .nodeId("my-service-node-1")
                .leaseTimeoutSeconds(60)
                .recoveryIntervalSeconds(30)
                .schemaAutoMigrate(true)
                .executionMode(ExecutionMode.ASYNCHRONOUS)
                .build();

        DurableFlowEngine engine = new DurableFlowEngine(dataSource, config);
        engine.start();
        return engine;
    }
}
```

> **Kubernetes / Cloud tip:** Set `nodeId` to the pod name via an environment variable:
> ```java
> .nodeId(System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString()))
> ```

If you need tighter control over startup ordering, implement `SmartLifecycle`:

```java
@Component
public class DurableFlowLifecycle implements SmartLifecycle {

    private final DurableFlowEngine engine;
    private volatile boolean running = false;

    public DurableFlowLifecycle(DurableFlowEngine engine) {
        this.engine = engine;
    }

    @Override public void start()            { engine.start(); running = true; }
    @Override public void stop()             { engine.close(); running = false; }
    @Override public boolean isRunning()     { return running; }
    @Override public int    getPhase()       { return Integer.MIN_VALUE; } // start first, stop last
    @Override public boolean isAutoStartup() { return true; }
}
```

When calling the engine directly (without `DurableFlowTemplate`), you are responsible for
transaction integration. The recommended pattern is to pass Spring's current transaction
connection to `receive()` and register an `afterCommit` hook:

```java
@JmsListener(destination = "orders")
@Transactional
public void onMessage(String rawMessage) {
    Connection conn = DataSourceUtils.getConnection(dataSource);
    ReceiveResult result = engine.receive("orders", rawMessage, Map.of(),
        ReceiveOptions.withDeferredExecution(orderWorkflow, conn));

    String mid = result.messageId();
    TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                engine.dispatchSteps(mid, orderWorkflow);
            }
        });
}
```

`DurableFlowTemplate` automates exactly this pattern — prefer it when using the starter.

---

## Database Connectivity

By default, durable-flow shares the application's primary `DataSource`. Both your application
code (via Spring's transaction manager) and the engine draw from the same pool, but they use
**independent connections and transactions**. This is sufficient for most use cases — the engine's
deduplication guarantees prevent duplicate workflow instances even if the application transaction
rolls back after the engine has already committed.

When you need strict atomicity between a business write and the workflow insertion (e.g. an order
must only be processed once and only if it was persisted), use `DurableFlowTemplate` inside a
`@Transactional` method. The template borrows the transaction connection so both writes commit
together.

**Separate DataSource:** if you want full isolation between the application schema and the
workflow schema, override the `durableFlowEngine` bean to supply a dedicated `DataSource`
(see [Overriding individual auto-configured beans](#overriding-individual-auto-configured-beans)).
This eliminates connection pool contention and allows independent schema evolution, at the cost
of maintaining a second connection pool. True cross-database atomicity is rarely needed — the
engine's deduplication handles the edge cases.

---

## Flyway Co-existence

Spring Boot auto-configures Flyway to scan `classpath:db/migration` using the primary
`DataSource`. durable-flow also uses Flyway internally (when `schemaAutoMigrate = true`)
but scans a **vendor-specific sub-path** such as `classpath:db/migration/postgresql`.

The two Flyway instances use **separate migration locations and separate version
histories** — they do not interfere with each other's version tracking by default.

However, if you share the same schema, ensure your migrations do not create tables
named `messages`, `message_steps`, or `message_step_dependencies`.

### Approach 1 — Let durable-flow manage its own schema (default)

Keep `schema-auto-migrate: true` (the default). durable-flow's Flyway instance runs when
the engine bean is created and manages `messages`, `message_steps`, and
`message_step_dependencies` automatically.

```yaml
durable-flow:
  schema-auto-migrate: true  # default — no change needed
```

### Approach 2 — Disable durable-flow's auto-migration and merge scripts

If you want a single Flyway run for your whole schema:

1. Disable durable-flow's built-in migration:
   ```yaml
   durable-flow:
     schema-auto-migrate: false
   ```
2. Copy the SQL files from
   `durable-flow-core/src/main/resources/db/migration/<dialect>/`
   into your own `src/main/resources/db/migration/` folder, renaming them so they fit
   your version sequence (e.g. `V10__durable_flow_schema.sql`).
3. Configure Spring Boot's Flyway as usual — it will apply all scripts including the
   durable-flow tables.

### Approach 3 — Separate schema

Use a dedicated schema (PostgreSQL) or a separate database (any vendor) for durable-flow
tables and point a dedicated `DataSource` at it. This is the cleanest approach when you
want strict separation.

---

## Execution Mode Selection

`receive()` always writes the message to the database and commits the transaction before
returning — regardless of mode. The message is durable the moment `receive()` returns.
What the mode controls is **what happens after that database write**:

| Mode | After the DB write … | `receive()` returns | Steps execute on | Best for |
|------|----------------------|---------------------|-----------------|---------|
| `ASYNCHRONOUS` (default) | Returns immediately | `MessageState.RECEIVED` | Background thread pool | High-throughput ingest, event-driven, message consumers |
| `SYNCHRONOUS` | Blocks until all currently-executable steps finish | Actual `MessageState` | Calling thread | Request/response REST endpoints, scripts, testing |

```yaml
durable-flow:
  execution-mode: ASYNCHRONOUS   # or SYNCHRONOUS
```

> **Important for `SYNCHRONOUS` mode inside `@Transactional`:** The calling thread
> blocks until all steps complete. Those steps open their own connections from the pool.
> If the pool is sized too small, and the calling thread holds a connection while waiting
> for steps that also need connections, you risk a **deadlock**. Size the pool to at
> least `immediateExecutionThreads + 1` connections, or use `ASYNCHRONOUS` mode
> inside `@Transactional` methods.

---

## Using Spring Beans inside Step Handlers

Step handlers are plain Java lambdas or method references. To use Spring-managed beans
inside a step handler, inject them into the surrounding `@Configuration` class and
close over the references:

```java
@Configuration
public class OrderWorkflow {

    private final NotificationService notificationService;  // Spring-managed
    private final InventoryClient inventoryClient;          // Spring-managed

    public OrderWorkflow(NotificationService notificationService,
                         InventoryClient inventoryClient) {
        this.notificationService = notificationService;
        this.inventoryClient     = inventoryClient;
    }

    @Bean
    public WorkflowDefinition orderWorkflowDefinition() {
        return WorkflowDefinition.builder("order-processing")
            .step("validate",  ctx -> validate(ctx))
            .step("reserve",   ctx -> reserve(ctx)).dependsOn("validate")
                .retryPolicy(RetryPolicy.exponentialBackoff(
                    5, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), true))
            .step("notify",    ctx -> notify(ctx)).dependsOn("reserve")
            .build();
    }

    private StepResult validate(StepContext ctx) {
        Order order = deserialize(ctx.getPayload());
        inventoryClient.checkAvailability(order);
        return StepResult.of(ctx.getPayload());
    }

    private StepResult reserve(StepContext ctx) {
        Order order = deserialize(ctx.getPreviousStepOutputs().get("validate"));
        inventoryClient.reserve(order);
        return StepResult.of(ctx.getPayload());
    }

    private StepResult notify(StepContext ctx) {
        notificationService.sendConfirmation(deserialize(ctx.getPayload()));
        return StepResult.empty();
    }
}
```

### Transaction propagation in step handlers

Step handlers run on either the calling thread (SYNCHRONOUS) or a background thread
from the engine's own `ExecutorService` (ASYNCHRONOUS). In both cases there is **no
active Spring transaction** on that thread when the handler is invoked.

If your step handler calls a `@Transactional` Spring bean, Spring's AOP interceptor
will start a **new transaction** (using `PROPAGATION_REQUIRED` by default) for the
duration of that call. This is the expected and correct behaviour:

```
Step handler thread
│
├─ inventoryClient.reserve(order)   ← @Transactional(propagation=REQUIRED)
│     BEGIN (Spring starts a new transaction — no existing one on this thread)
│     UPDATE inventory ...
│     COMMIT
│
└─ StepResult returned to engine
   engine commits step-success in its own separate transaction
```

---

## Observability: Wiring the MetricsListener SPI

Implement `MetricsListener` and expose it as a Spring bean — the starter wires it in
automatically:

```java
import io.github.durableflow.spi.MetricsListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
public class MicrometerMetricsListener implements MetricsListener {

    private final MeterRegistry registry;

    public MicrometerMetricsListener(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onMessageReceived() {
        registry.counter("durable_flow.messages.received").increment();
    }

    @Override
    public void onDuplicateMessage() {
        registry.counter("durable_flow.messages.duplicates").increment();
    }

    @Override
    public void onStepStarted(String stepName) {
        registry.counter("durable_flow.steps.started", "step", stepName).increment();
    }

    @Override
    public void onStepSucceeded(String stepName, Duration elapsed) {
        registry.timer("durable_flow.steps.duration", "step", stepName).record(elapsed);
    }

    @Override
    public void onStepFailed(String stepName, boolean finalFailure) {
        registry.counter("durable_flow.steps.failed",
                "step", stepName, "final", String.valueOf(finalFailure)).increment();
    }

    @Override
    public void onMessageProcessed() {
        registry.counter("durable_flow.messages.processed").increment();
    }

    @Override
    public void onMessageParked() {
        registry.counter("durable_flow.messages.parked").increment();
    }
}
```

> Without the starter, wire the listener manually:
> ```java
> @Bean(destroyMethod = "close")
> public DurableFlowEngine durableFlowEngine(DataSource dataSource,
>                                            DurableFlowConfig config,
>                                            MetricsListener metricsListener) {
>     DurableFlowEngine engine = new DurableFlowEngine(dataSource, config, metricsListener);
>     engine.start();
>     return engine;
> }
> ```

---

## Full Worked Example

Below is a complete Spring Boot application that accepts orders via a REST endpoint and
processes them through a three-step durable workflow using the starter.

### 1. pom.xml dependency

```xml
<dependency>
    <groupId>io.github.rrobetti</groupId>
    <artifactId>durable-flow-spring-boot-starter</artifactId>
    <version>1.0.0-ALPHA</version>
</dependency>
```

### 2. Workflow definition

```java
// OrderWorkflow.java
import io.github.durableflow.api.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderWorkflow {

    private final InventoryClient inventoryClient;
    private final NotificationService notificationService;

    public OrderWorkflow(InventoryClient inventoryClient,
                         NotificationService notificationService) {
        this.inventoryClient     = inventoryClient;
        this.notificationService = notificationService;
    }

    @Bean
    public WorkflowDefinition orderWorkflowDefinition() {
        return WorkflowDefinition.builder("order-processing")
            .beforeProcessing(ctx ->
                log.info("Starting order workflow for message {}", ctx.getMessageId()))
            .step("validate", ctx -> {
                Order order = deserialize(ctx.getPayload());
                if (order.getAmount().signum() <= 0)
                    throw new IllegalArgumentException("Order amount must be positive");
                return StepResult.of(ctx.getPayload());
            })
            .step("reserve-inventory", ctx -> {
                Order order = deserialize(ctx.getPreviousStepOutputs().get("validate"));
                inventoryClient.reserve(order);
                return StepResult.of(ctx.getPayload());
            }).dependsOn("validate")
              .retryPolicy(RetryPolicy.exponentialBackoff(
                  5, Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5), true))
            .step("notify-customer", ctx -> {
                notificationService.sendConfirmation(deserialize(ctx.getPayload()));
                return StepResult.empty();
            }).dependsOn("reserve-inventory")
              .retryPolicy(RetryPolicy.fixedDelay(3, Duration.ofSeconds(5)))
            .afterProcessing(ctx ->
                log.info("Order workflow {} ended: state={}", ctx.getMessageId(), ctx.getFinalState()))
            .build();
    }
}
```

### 3. REST Controller

```java
// OrderController.java
import io.github.durableflow.api.*;
import io.github.durableflow.spring.DurableFlowTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderRepository orderRepository;
    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflowDefinition;

    public OrderController(OrderRepository orderRepository,
                           DurableFlowTemplate durableFlow,
                           WorkflowDefinition orderWorkflowDefinition) {
        this.orderRepository        = orderRepository;
        this.durableFlow            = durableFlow;
        this.orderWorkflowDefinition = orderWorkflowDefinition;
    }

    /**
     * Saves the order and submits it to the durable-flow engine in the same transaction.
     * DurableFlowTemplate registers the afterCommit hook automatically — steps are
     * dispatched only after the transaction commits successfully.
     */
    @PostMapping
    @Transactional
    public ResponseEntity<String> placeOrder(@RequestBody Order order) {
        orderRepository.save(order);

        ReceiveResult result = durableFlow.receive(
            "order-service", serialize(order),
            Map.of("orderId", order.getId()),
            orderWorkflowDefinition);

        return ResponseEntity.accepted().body(result.messageId());
    }
}
```

### 4. application.yml

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/myapp
    username: myapp_user
    password: myapp_pass
    hikari:
      maximum-pool-size: 10
      auto-commit: false
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true
    locations: classpath:db/migration/app

durable-flow:
  node-id: ${HOSTNAME:node-1}
  lease-timeout-seconds: 60
  recovery-interval-seconds: 30
  schema-auto-migrate: true
  execution-mode: ASYNCHRONOUS
```

---

## Configuration Properties Reference

All `durable-flow.*` properties are optional. Defaults match `DurableFlowConfig` constants.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `durable-flow.node-id` | `String` | random UUID | Unique identifier for this JVM instance. Set to `${HOSTNAME:node-1}` in Kubernetes so lease ownership survives pod restarts. |
| `durable-flow.lease-timeout-seconds` | `int` | `60` | Seconds before an in-progress step lease is considered abandoned and eligible for recovery. |
| `durable-flow.recovery-interval-seconds` | `int` | `30` | Interval between recovery scheduler sweeps. |
| `durable-flow.schema-auto-migrate` | `boolean` | `true` | Whether the engine runs Flyway to create/update its schema on startup. |
| `durable-flow.execution-mode` | `ExecutionMode` | `ASYNCHRONOUS` | `ASYNCHRONOUS` returns immediately; `SYNCHRONOUS` blocks until steps complete. |
