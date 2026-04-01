# Integrating durable-flow with Spring Boot

This guide explains how to embed `durable-flow` inside a Spring Boot application.
The recommended path is the **Spring Boot Starter** which eliminates all infrastructure
boilerplate and provides `DurableFlowTemplate` — a single method call that automatically
handles transaction integration, including the `afterCommit` dispatch hook.

---

## Table of Contents

1. [Using the Spring Boot Starter (recommended)](#using-the-spring-boot-starter-recommended)
   - [Quick Start](#quick-start)
   - [DurableFlowTemplate — automatic transaction integration](#durableflowtemplate--automatic-transaction-integration)
   - [Overriding individual auto-configured beans](#overriding-individual-auto-configured-beans)
2. [Manual wiring (without the starter)](#manual-wiring-without-the-starter)
3. [Transaction Isolation](#transaction-isolation)
   - [How durable-flow manages transactions](#how-durable-flow-manages-transactions)
   - [Option A – Shared DataSource (simple, no atomicity guarantee)](#option-a--shared-datasource-simple-no-atomicity-guarantee)
   - [Option B – Separate DataSource](#option-b--separate-datasource)
   - [Atomic receive() with an external connection (preferred)](#atomic-receive-with-an-external-connection-preferred)
   - [Guaranteed post-commit dispatch with withDeferredExecution](#guaranteed-post-commit-dispatch-with-withdeferredexecution)
4. [Flyway Co-existence](#flyway-co-existence)
5. [Execution Mode Selection](#execution-mode-selection)
6. [Using Spring Beans inside Step Handlers](#using-spring-beans-inside-step-handlers)
7. [Observability: Wiring the MetricsListener SPI](#observability-wiring-the-metricslistener-spi)
8. [Full Worked Example](#full-worked-example)
9. [Configuration Properties Reference](#configuration-properties-reference)

---

## Using the Spring Boot Starter (recommended)

### Quick Start

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

### DurableFlowTemplate — automatic transaction integration

`DurableFlowTemplate` is the primary API for submitting messages to the engine from
application code. It inspects the current thread's transaction state and selects the
correct strategy automatically:

| Calling context | Strategy | What happens |
|-----------------|----------|-------------|
| Inside `@Transactional` | Pattern C (safest) | Borrows the transaction connection; registers an `afterCommit` hook that calls `engine.dispatchSteps()` once the transaction commits. Both the business write and the workflow insert are atomic — if the transaction rolls back, neither completes and the workflow is never triggered. |
| No active transaction | Pattern A (simple) | Engine manages its own connection; commits and dispatches immediately. |

**Example — inside a `@Transactional` method (Pattern C):**

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflow;

    // ... constructor injection ...

    @Transactional
    public String placeOrder(Order order) {
        orderRepository.save(order);                        // business write

        // afterCommit is registered automatically;
        // workflow never starts if the transaction rolls back
        durableFlow.receive(
            new InboundMessage("order-service", serialize(order), Map.of("orderId", order.getId())),
            orderWorkflow);

        return order.getId();
    }
}
```

**Example — JMS / Kafka listener (Pattern A, no active transaction):**

```java
@Component
public class OrderMessageListener {

    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflow;

    // ... constructor injection ...

    @JmsListener(destination = "${orders.topic}")
    public void onMessage(String rawJson) {
        // No active transaction — engine self-manages connection, commits, and dispatches
        durableFlow.receive(
            new InboundMessage("orders", rawJson.getBytes(), Map.of()),
            orderWorkflow);
    }
}
```

For the rare Pattern B case (immediate step dispatch inside a transaction, before commit),
call `DurableFlowEngine#receive` directly with
`ReceiveOptions.withDeferredExecution(workflow, conn)`.

---

### Overriding individual auto-configured beans

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
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.ExecutionMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

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

> **Kubernetes / Cloud tip:** Set `nodeId` to the pod name via an environment variable
> so each replica has a unique identifier in the `owner` column:
>
> ```java
> .nodeId(System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString()))
> ```

If you need tighter control over startup ordering, implement `SmartLifecycle`:

```java
import io.github.durableflow.DurableFlowEngine;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

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

---

## Transaction Isolation

This is the most important topic when combining durable-flow with Spring Boot.

### How durable-flow manages transactions

By default, durable-flow **manages all database access through its own JDBC connections**. Every
operation that requires atomicity follows this pattern internally:

```java
try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);
    try {
        // ... perform DB operations ...
        conn.commit();
    } catch (Exception e) {
        conn.rollback();
        throw e;
    }
}
```

Key implications:

* By default, durable-flow calls `dataSource.getConnection()` **directly** — it does **not** use
  Spring's `DataSourceUtils.getConnection()` and does **not** participate in any
  `PlatformTransactionManager`-managed transaction.
* Every durable-flow operation except `receive()` always uses its **own dedicated connection** with
  its own transaction scope, regardless of any active Spring transaction on the calling thread.
* **Exception — `receive()` with an external connection:** when `ReceiveOptions.withDeferredExecution(workflow, conn)`
  is used, `receive()` uses the caller-supplied connection for the message and step inserts
  **without committing or closing it**. The caller owns the transaction lifecycle. See
  [Atomic receive() with an external connection (preferred)](#atomic-receive-with-an-external-connection-preferred)
  for details.
* The following discrete transactions exist inside the engine:

| Engine operation | Connection used | Atomic unit |
|-----------------|-----------------|-------------|
| `receive()` (default) | Engine-internal | Insert `messages` + insert `message_steps` + insert `message_step_dependencies` |
| `receive()` (external connection) | Caller-provided | Same inserts — caller commits/rolls back |
| `claimStep()` | Engine-internal | Atomic `UPDATE message_steps WHERE step_state IN ('PENDING','FAILED_RETRYABLE')` |
| Step success | Engine-internal | `UPDATE message_steps SET step_state = 'SUCCEEDED'` + store output |
| Step failure | Engine-internal | `UPDATE message_steps SET step_state = 'FAILED_*'` + record error |
| State recalculation | Engine-internal | `UPDATE messages SET message_state = ...` |
| `redrive()` | Engine-internal | Reset `FAILED_FINAL` steps + set message to `IN_PROGRESS` |

---

### Option A – Shared DataSource (simple, no atomicity guarantee)

Use the same `DataSource` for both your application code (via Spring's
`PlatformTransactionManager`) and the `DurableFlowEngine`. Because durable-flow
opens its own connections by default, the two subsystems operate on **independent
connection-level transactions** even when sharing the pool.

```
Spring @Transactional method                durable-flow engine
------------------------------------        ------------------------------------
pool.getConnection() -> conn-A              pool.getConnection() -> conn-B
  BEGIN (Spring JDBC / JPA)                   BEGIN (engine-internal)
  INSERT INTO orders ...                       INSERT INTO messages ...
  INSERT INTO order_items ...                  INSERT INTO message_steps ...
  COMMIT (Spring)                              COMMIT (engine)
conn-A -> returned to pool                  conn-B -> returned to pool
```

**Pros:**
* No extra DataSource configuration.
* Spring Boot auto-configuration works out of the box.
* The pool is shared, so total connections are kept bounded.

**Cons:**
* Business data and workflow state commit in **separate transactions** — see
  [Atomic receive() with an external connection (preferred)](#atomic-receive-with-an-external-connection-preferred)
  for the recommended way to eliminate this race.

---

### Option B – Separate DataSource

Create a dedicated `DataSource` (pointing to the same or a different schema/database)
exclusively for durable-flow. Using the starter, override the engine bean:

```java
@Configuration
public class DurableFlowConfig {

    @Bean
    @ConfigurationProperties("app.durable-flow.datasource")
    public DataSource durableFlowDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(
            @Qualifier("durableFlowDataSource") DataSource durableFlowDataSource,
            DurableFlowConfig durableFlowConfig) {
        // lifecycle bean still handles start/close
        return new DurableFlowEngine(durableFlowDataSource, durableFlowConfig);
    }
}
```

```yaml
# application.yml
app:
  durable-flow:
    datasource:
      url: jdbc:postgresql://localhost:5432/durable_flow_db
      username: df_user
      password: df_pass
      hikari:
        maximum-pool-size: 5
        auto-commit: false
```

**Pros:**
* Complete isolation — no connection pool contention.
* Allows independent scaling or tuning of the workflow database.
* Business schema and workflow schema evolve independently.

**Cons:**
* Requires maintaining a second DataSource / connection pool.
* True cross-database atomicity still requires an XA transaction manager (usually
  unnecessary given durable-flow's deduplication guarantees).

---

### Atomic receive() with an external connection (preferred)

The recommended way to guarantee atomicity between your business logic and durable-flow's
message insertion is to use `DurableFlowTemplate` (starter) or manually pass Spring's
current transaction connection to `receive()` via
`ReceiveOptions.withDeferredExecution(workflow, connection)`.

When a connection is supplied, durable-flow uses it for the message and step insertions
performed inside `receive()` **without committing or closing it**. Spring commits the
connection when the `@Transactional` method returns, making both writes atomic in a single
database transaction.

**With `DurableFlowTemplate` (starter — zero boilerplate):**

```java
@Transactional
public String placeOrder(Order order) {
    orderRepository.save(order);
    durableFlow.receive(
        new InboundMessage("order-service", serialize(order), Map.of()),
        orderWorkflow);
    // afterCommit registered automatically; steps dispatched after TX commits
    return order.getId();
}
```

**Without the starter (manual Pattern B — immediate dispatch):**

```java
@JmsListener(destination = "orders")
@Transactional
public void onMessage(String rawMessage) {
    Connection conn = DataSourceUtils.getConnection(dataSource);
    engine.receive("orders", rawMessage, Map.of(),
        ReceiveOptions.withDeferredExecution(orderWorkflow, conn));
    // Spring commits conn when this method returns
}
```

**Duplicate messages with an external connection:**
When a duplicate is detected, durable-flow returns a `ReceiveResult` with `duplicate() == true`
**without rolling back the connection**. The connection lifecycle remains entirely under
Spring's control — Spring will commit the transaction as normal (with no new insert, which
is harmless). Use `result.duplicate()` to branch your own logic if needed.

---

### Guaranteed post-commit dispatch with withDeferredExecution

`ReceiveOptions.withDeferredExecution(workflow, conn)` defers step dispatch entirely — no
background worker is submitted inside `receive()`. The caller triggers it from an
`afterCommit()` callback, guaranteeing that steps only start after the external transaction
is committed and its rows are visible to other connections.

**With `DurableFlowTemplate` this is done automatically.** Below is the equivalent manual
pattern for reference:

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

With this pattern:
* `receive()` persists the message rows using the caller's connection (no commit).
* If the transaction commits, `afterCommit()` is called and `dispatchSteps()` submits
  step execution to the background thread pool — steps always start after rows are visible.
* If the transaction rolls back, `afterCommit()` is **never called** — no steps ever run.

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
tables and point a dedicated `DataSource` (Option B above) at it. This is the cleanest
approach when you want strict separation.

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
inside a step handler, inject them into the surrounding `@Service` or `@Component` and
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

### 3. Service

```java
// OrderWorkflowService.java
@Service
public class OrderWorkflowService {

    private final OrderRepository orderRepository;
    private final DurableFlowTemplate durableFlow;
    private final WorkflowDefinition orderWorkflowDefinition;

    public OrderWorkflowService(OrderRepository orderRepository,
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
    @Transactional
    public String placeOrder(Order order) {
        orderRepository.save(order);

        durableFlow.receive(
            new InboundMessage("order-service", serialize(order),
                Map.of("orderId", order.getId())),
            orderWorkflowDefinition);

        return order.getId();
    }
}
```

### 4. REST Controller

```java
// OrderController.java
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderWorkflowService orderService;

    public OrderController(OrderWorkflowService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> placeOrder(@RequestBody Order order) {
        String orderId = orderService.placeOrder(order);
        return ResponseEntity.accepted().body(orderId);
    }
}
```

### 5. application.yml

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

---

## Summary: Transaction Isolation Quick-Reference

| Scenario | Behaviour | Recommendation |
|----------|-----------|---------------|
| `durableFlow.receive()` called inside `@Transactional` | Joins the transaction; `afterCommit` hook registered automatically | ✅ **Recommended** — use `DurableFlowTemplate` |
| `durableFlow.receive()` called outside any transaction | Engine commits its own independent transaction | ✅ Safe — simplest approach |
| Business transaction rolls back | Both business writes and workflow insertion are rolled back atomically | ✅ Clean — workflow is never triggered |
| `receive()` with external connection (manual) | durable-flow uses caller's connection without committing it | ✅ Safe — same as starter pattern C |
| `SYNCHRONOUS` mode inside `@Transactional` | Calling thread blocks; steps open connections from pool | ⚠️ Size pool appropriately; prefer `ASYNCHRONOUS` |
| Separate DataSource for durable-flow | Complete transaction isolation; no cross-DataSource atomicity | ✅ Cleanest isolation; XA rarely needed |
