# Integrating durable-flow with Spring Boot

This guide explains how to embed `durable-flow` inside a Spring Boot application,
covering dependency setup, bean wiring, lifecycle management, Flyway co-existence, and
— most importantly — how durable-flow's internal transactions interact with Spring's
transaction management so you can choose the right isolation strategy for your use-case.

---

## Table of Contents

1. [Adding the Dependency](#adding-the-dependency)
2. [Declaring the Engine as a Spring Bean](#declaring-the-engine-as-a-spring-bean)
3. [Lifecycle Management](#lifecycle-management)
4. [Transaction Isolation](#transaction-isolation)
   - [How durable-flow manages transactions](#how-durable-flow-manages-transactions)
   - [Option A – Shared DataSource (simple, no atomicity guarantee)](#option-a--shared-datasource-simple-no-atomicity-guarantee)
   - [Option B – Separate DataSource](#option-b--separate-datasource)
   - [Atomic receive() with an external connection (preferred)](#atomic-receive-with-an-external-connection-preferred)
   - [Guaranteed post-commit dispatch with withDeferredExecution](#guaranteed-post-commit-dispatch-with-withdeferredexecution)
5. [Flyway Co-existence](#flyway-co-existence)
6. [Execution Mode Selection](#execution-mode-selection)
7. [Using Spring Beans inside Step Handlers](#using-spring-beans-inside-step-handlers)
8. [Observability: Wiring the MetricsListener SPI](#observability-wiring-the-metricslistener-spi)
9. [Full Worked Example](#full-worked-example)
10. [Configuration Properties Reference](#configuration-properties-reference)

---

## Adding the Dependency

Add `durable-flow-core` to your Spring Boot project:

```xml
<dependency>
    <groupId>io.github.rrobetti</groupId>
    <artifactId>durable-flow-core</artifactId>
    <version>1.0.0-ALPHA</version>
</dependency>
```

> durable-flow carries **no Spring Framework dependency** of its own. It works with any
> `javax.sql.DataSource` — including the one Spring Boot auto-configures from
> `spring.datasource.*` properties.

---

## Declaring the Engine as a Spring Bean

Create a `@Configuration` class that wires Spring's auto-configured `DataSource` into a
`DurableFlowEngine` bean:

```java
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.ExecutionMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DurableFlowAutoConfiguration {

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(DataSource dataSource,
                                               DurableFlowConfig durableFlowConfig) {
        DurableFlowEngine engine = new DurableFlowEngine(dataSource, durableFlowConfig);
        engine.start();
        return engine;
    }

    @Bean
    public DurableFlowConfig durableFlowConfig() {
        return DurableFlowConfig.builder()
                .nodeId("my-service-node-1")           // unique per JVM instance
                .leaseTimeoutSeconds(60)
                .recoveryIntervalSeconds(30)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(true)               // see Flyway Co-existence section
                .executionMode(ExecutionMode.ASYNCHRONOUS)
                .build();
    }
}
```

The `destroyMethod = "close"` attribute on `@Bean` ensures that Spring calls
`DurableFlowEngine.close()` when the application context shuts down, which stops the
recovery scheduler and gracefully drains the internal thread pool.

> **Kubernetes / Cloud tip:** Set `nodeId` to the pod name via an environment variable
> so each replica has a unique identifier in the `owner` column:
>
> ```java
> .nodeId(System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString()))
> ```

---

## Lifecycle Management

`DurableFlowEngine` implements `Closeable` and exposes two explicit lifecycle methods:

| Method | Purpose |
|--------|---------|
| `engine.start()` | Starts the background `RecoveryScheduler` |
| `engine.close()` | Stops the scheduler; shuts down the internal `ExecutorService` (30 s drain) |

Both `start()` and `close()` are idempotent — calling them more than once is safe.

If you need tighter control over startup ordering (e.g. the engine must start after your
messaging consumer is ready), implement `SmartLifecycle`:

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

    @Override public void start()   { engine.start(); running = true; }
    @Override public void stop()    { engine.close(); running = false; }
    @Override public boolean isRunning()   { return running; }
    @Override public int    getPhase()     { return Integer.MAX_VALUE; } // start last, stop first
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
  is used, `receive()` uses the
  caller-supplied connection for the message and step inserts **without committing or closing it**.
  The caller owns the transaction lifecycle. See
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
exclusively for durable-flow:

```java
@Bean
@ConfigurationProperties("app.durable-flow.datasource")
public DataSource durableFlowDataSource() {
    return DataSourceBuilder.create().build();
}

@Bean(destroyMethod = "close")
public DurableFlowEngine durableFlowEngine(
        @Qualifier("durableFlowDataSource") DataSource durableFlowDataSource,
        DurableFlowConfig durableFlowConfig) {
    DurableFlowEngine engine = new DurableFlowEngine(durableFlowDataSource, durableFlowConfig);
    engine.start();
    return engine;
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
message insertion is to pass Spring's current transaction connection directly to `receive()`
via `ReceiveOptions.withDeferredExecution(workflow, connection)`.

When a connection is supplied, durable-flow uses it for the message and step insertions
performed inside `receive()` **without committing or closing it**. Spring commits the
connection when the `@Transactional` method returns, making both writes atomic in a single
database transaction.

```java
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final DurableFlowEngine engine;
    private final WorkflowDefinition orderWorkflow;
    private final DataSource dataSource;

    // ... constructor injection ...

    @Transactional
    public String placeOrder(Order order) {
        // Save business data using the Spring-managed transaction
        orderRepository.save(order);

        // Obtain the connection already bound to the current Spring transaction
        Connection conn = DataSourceUtils.getConnection(dataSource);

        // durable-flow inserts the message using this connection without committing it
        ReceiveResult result = engine.receive(
            new InboundMessage("order-service", serialize(order), Map.of()),
            ReceiveOptions.withDeferredExecution(orderWorkflow, conn));

        // Spring commits conn when this method returns — both the business save and
        // the durable-flow insert are committed atomically.
        // If an exception occurs, Spring rolls back both writes together.
        return result.messageId();
    }
}
```

**Duplicate messages with an external connection:**
When a duplicate is detected, durable-flow returns a `ReceiveResult` with `duplicate() == true`
**without rolling back the connection**. The connection lifecycle remains entirely under
Spring's control — Spring will commit the transaction as normal (with no new insert, which
is harmless). Use `result.duplicate()` to branch your own logic if needed.

**How commit ordering works:**
durable-flow inserts the message rows using the caller's connection but does **not** commit
or close it — the caller owns the transaction lifecycle. Step execution is always dispatched
asynchronously when an external connection is provided, so steps cannot start until after the
connection is committed and the rows become visible to other connections. If the background
executor races ahead and finds no visible rows yet (READ_COMMITTED isolation), it simply
exits cleanly; the recovery scheduler then picks up the committed steps on its next run.

**Behaviour on rollback:**
If the Spring transaction rolls back, both the business writes and the durable-flow inserts
are rolled back atomically — the workflow is never triggered.

---

### Guaranteed post-commit dispatch with withDeferredExecution

`ReceiveOptions.withDeferredExecution(workflow, conn)` defers step dispatch entirely — no
background worker is submitted inside `receive()`. You trigger it yourself from an `afterCommit()`
callback using `engine.dispatchSteps(messageId, workflow)`, guaranteeing that steps only start
after the external transaction is committed and its rows are visible to other connections.

```java
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Map;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final DurableFlowEngine engine;
    private final WorkflowDefinition orderWorkflow;
    private final DataSource dataSource;

    // ... constructor injection ...

    @Transactional
    public String placeOrder(Order order) {
        orderRepository.save(order);

        Connection conn = DataSourceUtils.getConnection(dataSource);

        // deferExecution=true: no background dispatch is submitted inside receive()
        ReceiveResult result = engine.receive(
            new InboundMessage("order-service", serialize(order), Map.of()),
            ReceiveOptions.withDeferredExecution(orderWorkflow, conn));

        // Register an after-commit hook that triggers step dispatch once the transaction
        // is committed and its rows are visible to other database connections.
        // If the transaction rolls back, afterCommit() is never called — the workflow
        // is never triggered.
        String mid = result.messageId();
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    engine.dispatchSteps(mid, orderWorkflow);
                }
            });

        return mid;
    }
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

Keep `schemaAutoMigrate = true` (the default). durable-flow's Flyway instance runs when
the engine bean is created and manages `messages`, `message_steps`, and
`message_step_dependencies` automatically.

```java
DurableFlowConfig.builder()
    .schemaAutoMigrate(true)   // default — no change needed
    .build();
```

### Approach 2 — Disable durable-flow's auto-migration and merge scripts

If you want a single Flyway run for your whole schema:

1. Disable durable-flow's built-in migration:
   ```java
   DurableFlowConfig.builder()
       .schemaAutoMigrate(false)
       .build();
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

```java
// ASYNCHRONOUS (default) — ideal for Kafka/RabbitMQ consumers
// receive() returns as soon as the DB write is committed.
// The message is already durable — safe to ACK the queue message immediately.
// Steps then run in the background thread pool.
DurableFlowConfig.builder()
    .executionMode(ExecutionMode.ASYNCHRONOUS)
    .build();

// SYNCHRONOUS — ideal for REST endpoints where you want the workflow result in the HTTP response
// receive() blocks until all currently-executable steps complete on the calling thread.
DurableFlowConfig.builder()
    .executionMode(ExecutionMode.SYNCHRONOUS)
    .build();
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
@Service
public class OrderWorkflowService {

    private final NotificationService notificationService;  // Spring-managed
    private final InventoryClient inventoryClient;          // Spring-managed
    private final DurableFlowEngine engine;

    private final WorkflowDefinition workflow;

    public OrderWorkflowService(NotificationService notificationService,
                                InventoryClient inventoryClient,
                                DurableFlowEngine engine) {
        this.notificationService = notificationService;
        this.inventoryClient     = inventoryClient;
        this.engine              = engine;

        this.workflow = WorkflowDefinition.builder("order-processing")
            .step("validate",  ctx -> validate(ctx))
            .step("reserve",   ctx -> reserve(ctx)).dependsOn("validate")
                .retryPolicy(RetryPolicy.exponentialBackoff(
                    5, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), true))
            .step("notify",    ctx -> notify(ctx)).dependsOn("reserve")
            .build();
    }

    private StepResult validate(StepContext ctx) {
        // ctx.getPayload() contains the serialized order
        Order order = deserialize(ctx.getPayload());
        inventoryClient.checkAvailability(order);   // Spring bean, @Transactional internally
        return StepResult.of(ctx.getPayload());
    }

    private StepResult reserve(StepContext ctx) {
        Order order = deserialize(ctx.getPreviousStepOutputs().get("validate"));
        inventoryClient.reserve(order);
        return StepResult.of(ctx.getPayload());
    }

    private StepResult notify(StepContext ctx) {
        Order order = deserialize(ctx.getPayload());
        notificationService.sendConfirmation(order);
        return StepResult.empty();
    }

    public ReceiveResult submitOrder(Order order) {
        InboundMessage msg = new InboundMessage(
            "order-service", serialize(order), Map.of("correlationId", order.getId()));
        return engine.receive(msg, ReceiveOptions.withDeferredExecution(workflow));
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

Register custom metrics by implementing `MetricsListener` and exposing it as a bean:

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
        registry.timer("durable_flow.steps.duration", "step", stepName)
                .record(elapsed);
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

Wire it into the engine:

```java
@Bean(destroyMethod = "close")
public DurableFlowEngine durableFlowEngine(DataSource dataSource,
                                           DurableFlowConfig durableFlowConfig,
                                           MetricsListener metricsListener) {
    DurableFlowEngine engine = new DurableFlowEngine(dataSource, durableFlowConfig, metricsListener);
    engine.start();
    return engine;
}
```

---

## Full Worked Example

Below is a complete Spring Boot application that accepts orders via a REST endpoint and
processes them through a three-step durable workflow.

### 1. Configuration

```java
// DurableFlowAutoConfiguration.java
import io.github.durableflow.DurableFlowConfig;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.ExecutionMode;
import io.github.durableflow.spi.MetricsListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.UUID;

@Configuration
public class DurableFlowAutoConfiguration {

    @Bean
    public DurableFlowConfig durableFlowConfig() {
        return DurableFlowConfig.builder()
                .nodeId(System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString()))
                .leaseTimeoutSeconds(60)
                .recoveryIntervalSeconds(30)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(true)
                .executionMode(ExecutionMode.ASYNCHRONOUS)
                .build();
    }

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(DataSource dataSource,
                                               DurableFlowConfig config,
                                               MetricsListener metricsListener) {
        DurableFlowEngine engine = new DurableFlowEngine(dataSource, config, metricsListener);
        engine.start();
        return engine;
    }
}
```

### 2. Service

```java
// OrderWorkflowService.java
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.durableflow.DurableFlowEngine;
import io.github.durableflow.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.Map;

@Service
public class OrderWorkflowService {

    private static final Logger log = LoggerFactory.getLogger(OrderWorkflowService.class);

    private final OrderRepository   orderRepository;
    private final InventoryClient   inventoryClient;
    private final NotificationService notificationService;
    private final DurableFlowEngine engine;
    private final DataSource        dataSource;
    private final ObjectMapper      mapper;

    private final WorkflowDefinition workflow;

    public OrderWorkflowService(OrderRepository orderRepository,
                                InventoryClient inventoryClient,
                                NotificationService notificationService,
                                DurableFlowEngine engine,
                                DataSource dataSource,
                                ObjectMapper mapper) {
        this.orderRepository    = orderRepository;
        this.inventoryClient    = inventoryClient;
        this.notificationService = notificationService;
        this.engine             = engine;
        this.dataSource         = dataSource;
        this.mapper             = mapper;

        // Build once; the definition is immutable and thread-safe
        this.workflow = WorkflowDefinition.builder("order-processing")
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
                inventoryClient.reserve(order);             // @Transactional internally
                return StepResult.of(ctx.getPayload());
            }).dependsOn("validate")
              .retryPolicy(RetryPolicy.exponentialBackoff(
                  5, Duration.ofSeconds(2), 2.0, Duration.ofMinutes(5), true))
            .step("notify-customer", ctx -> {
                Order order = deserialize(ctx.getPayload());
                notificationService.sendConfirmation(order); // @Transactional internally
                return StepResult.empty();
            }).dependsOn("reserve-inventory")
              .retryPolicy(RetryPolicy.fixedDelay(3, Duration.ofSeconds(5)))
            .afterProcessing(ctx ->
                log.info("Order workflow {} ended: state={}", ctx.getMessageId(), ctx.getFinalState()))
            .build();
    }

    /**
     * Saves the order and submits it to the durable-flow engine in the same transaction.
     * If an exception occurs, Spring rolls back both the business save and the workflow
     * message insertion atomically.
     */
    @Transactional
    public String placeOrder(Order order) {
        orderRepository.save(order);

        // Pass Spring's transaction connection to receive() so that the message insertion
        // and the business save share a single atomic transaction.
        Connection conn = DataSourceUtils.getConnection(dataSource);
        engine.receive(
            new InboundMessage(
                "order-service",
                serialize(order),
                Map.of("orderId", order.getId())),
            ReceiveOptions.withDeferredExecution(workflow, conn));

        // Spring commits conn when this method returns successfully.
        // Steps run asynchronously after the transaction is committed.
        return order.getId();
    }

    private byte[] serialize(Order order) {
        try { return mapper.writeValueAsBytes(order); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    private Order deserialize(byte[] bytes) {
        try { return mapper.readValue(bytes, Order.class); }
        catch (Exception e) { throw new RuntimeException(e); }
    }
}
```

### 3. REST Controller

```java
// OrderController.java
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

### 4. application.yml

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/myapp
    username: myapp_user
    password: myapp_pass
    hikari:
      maximum-pool-size: 10   # must be > immediateExecutionThreads if using SYNCHRONOUS mode
      auto-commit: false       # let each consumer manage its own commit
  jpa:
    hibernate:
      ddl-auto: validate
  flyway:
    enabled: true              # Spring Boot Flyway manages your app schema
    locations: classpath:db/migration/app
```

---

## Configuration Properties Reference

While durable-flow does not auto-bind `application.yml` properties, a common pattern is
to introduce a `@ConfigurationProperties` class:

```java
import io.github.durableflow.api.ExecutionMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "durable-flow")
public class DurableFlowProperties {
    private String nodeId;
    private int leaseTimeoutSeconds  = 60;
    private int recoveryIntervalSeconds = 30;
    private int immediateExecutionThreads = 4;
    private boolean schemaAutoMigrate = true;
    private ExecutionMode executionMode = ExecutionMode.ASYNCHRONOUS;

    // standard getters and setters
}
```

```yaml
durable-flow:
  node-id: ${HOSTNAME:node-1}
  lease-timeout-seconds: 60
  recovery-interval-seconds: 30
  immediate-execution-threads: 4
  schema-auto-migrate: true
  execution-mode: ASYNCHRONOUS
```

Wire the properties into the `DurableFlowConfig` bean:

```java
@Bean
public DurableFlowConfig durableFlowConfig(DurableFlowProperties props) {
    return DurableFlowConfig.builder()
            .nodeId(props.getNodeId())
            .leaseTimeoutSeconds(props.getLeaseTimeoutSeconds())
            .recoveryIntervalSeconds(props.getRecoveryIntervalSeconds())
            .immediateExecutionThreads(props.getImmediateExecutionThreads())
            .schemaAutoMigrate(props.isSchemaAutoMigrate())
            .executionMode(props.getExecutionMode())
            .build();
}
```

---

## Summary: Transaction Isolation Quick-Reference

| Scenario | Behaviour | Recommendation |
|----------|-----------|---------------|
| `receive()` called outside any Spring transaction | durable-flow commits its own independent transaction | ✅ Safe — simplest approach |
| `receive()` called with an external connection (`ReceiveOptions.withDeferredExecution(wf, conn)`) | durable-flow uses the caller's connection without committing it; both writes are atomic | ✅ **Preferred** — single atomic transaction with your business logic |
| Business transaction rolls back | Both business writes and workflow insertion are rolled back atomically | ✅ Clean — workflow is never triggered |
| `SYNCHRONOUS` mode inside `@Transactional` | Calling thread blocks; steps open connections from pool | ⚠️ Size pool to at least `immediateExecutionThreads + 1`; prefer `ASYNCHRONOUS` |
| Separate DataSource for durable-flow | Complete transaction isolation; no cross-DataSource atomicity | ✅ Cleanest isolation; cross-database atomicity requires XA (rarely needed) |
