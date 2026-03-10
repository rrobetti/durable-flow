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
   - [Option A – Shared DataSource (recommended)](#option-a--shared-datasource-recommended)
   - [Option B – Separate DataSource](#option-b--separate-datasource)
   - [Calling receive() inside a @Transactional method](#calling-receive-inside-a-transactional-method)
   - [Ensuring atomicity with TransactionSynchronizationManager](#ensuring-atomicity-with-transactionsynchronizationmanager)
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
    <groupId>io.github.durableflow</groupId>
    <artifactId>durable-flow-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
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

durable-flow **manages all database access through its own JDBC connections**. Every
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

* durable-flow calls `dataSource.getConnection()` **directly** — it does **not** use
  Spring's `DataSourceUtils.getConnection()` and does **not** participate in any
  `PlatformTransactionManager`-managed transaction.
* Every durable-flow operation uses its **own dedicated connection** with its own
  transaction scope, regardless of any active Spring transaction on the calling thread.
* The following discrete transactions exist inside the engine:

| Engine operation | Atomic unit |
|-----------------|-------------|
| `receive()` | Insert `messages` + insert `message_steps` + insert `message_step_dependencies` |
| `claimStep()` | Atomic `UPDATE message_steps WHERE step_state IN ('PENDING','FAILED_RETRYABLE')` |
| Step success | `UPDATE message_steps SET step_state = 'SUCCEEDED'` + store output |
| Step failure | `UPDATE message_steps SET step_state = 'FAILED_*'` + record error |
| State recalculation | `UPDATE messages SET message_state = ...` |
| `redrive()` | Reset `FAILED_FINAL` steps + set message to `IN_PROGRESS` |

---

### Option A – Shared DataSource (recommended)

Use the same `DataSource` for both your application code (via Spring's
`PlatformTransactionManager`) and the `DurableFlowEngine`. Because durable-flow
opens its own connections, the two subsystems operate on **independent connection-level
transactions** even when sharing the pool.

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
* Business data and workflow state commit in **separate transactions** — see the
  [atomicity section below](#calling-receive-inside-a-transactional-method) for
  strategies to handle this.

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

### Calling receive() inside a @Transactional method

Because durable-flow's `receive()` always commits its own connection **before** any
step execution, there are two important scenarios to consider:

#### Scenario 1 — Spring transaction commits after receive() (normal path)

```java
@Transactional
public void handleOrder(Order order) {
    orderRepository.save(order);                  // conn-A (Spring-managed), not yet committed
    engine.receive(toMessage(order), opts);        // conn-B, COMMITS immediately
    // ... more business logic ...
    // Spring commits conn-A here
}
```

Timeline:
1. `engine.receive()` persists the workflow message and **commits** (conn-B).
2. Steps may already start executing in the background (ASYNCHRONOUS mode).
3. If the Spring transaction later **rolls back**, the workflow message already exists.

**Effect:** The workflow will execute with a message whose corresponding business record
was rolled back. Step handlers querying the business database will find nothing for
this `orderId`.

**Mitigation:** Use `TransactionSynchronizationManager` (see next section) to only
call `receive()` after the business transaction successfully commits.

#### Scenario 2 — Spring transaction rolls back after receive()

If the business transaction rolls back and durable-flow has already committed the
message, the deduplication guarantee means a subsequent retry of the same event will be
detected as a duplicate and skipped. This is safe only if step handlers are written
to tolerate a missing or inconsistent business record.

---

### Ensuring atomicity with TransactionSynchronizationManager

To guarantee that durable-flow only receives a message after the surrounding Spring
transaction commits successfully, register an `afterCommit` synchronization:

```java
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final DurableFlowEngine engine;
    private final WorkflowDefinition orderWorkflow;

    // ... constructor injection ...

    @Transactional
    public void placeOrder(Order order) {
        orderRepository.save(order);

        // Schedule receive() to run AFTER this transaction commits
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    engine.receive(
                        new InboundMessage("order-service", serialize(order), Map.of()),
                        ReceiveOptions.of(orderWorkflow));
                }
            });
    }
}
```

With this pattern:

* If the Spring transaction rolls back, `afterCommit()` is **never called** — the
  workflow is not triggered.
* If the Spring transaction commits, `afterCommit()` runs and durable-flow persists the
  workflow message in its own transaction.
* The small window between the business commit and the durable-flow commit is
  acceptable for most applications because durable-flow's deduplication ensures
  idempotency if the event is replayed.

> **Note:** `afterCommit()` is called on the same thread that committed the transaction.
> In `ASYNCHRONOUS` mode, `receive()` returns quickly; steps run in the background.
> In `SYNCHRONOUS` mode, `afterCommit()` blocks until all eligible steps complete.

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

Choose the execution mode based on your ingest pattern:

| Mode | `receive()` returns | Steps execute on | Best for |
|------|---------------------|-----------------|---------|
| `ASYNCHRONOUS` (default) | Immediately — `MessageState.RECEIVED` | Background thread pool | High-throughput ingest, event-driven, message consumers |
| `SYNCHRONOUS` | After all currently-executable steps finish — actual `MessageState` | Calling thread | Request/response REST endpoints, scripts, testing |

```java
// ASYNCHRONOUS (default) — ideal for Kafka/RabbitMQ consumers
DurableFlowConfig.builder()
    .executionMode(ExecutionMode.ASYNCHRONOUS)
    .build();

// SYNCHRONOUS — ideal for REST endpoints where you want the workflow result in the HTTP response
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
        return engine.receive(msg, ReceiveOptions.of(workflow));
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.Map;

@Service
public class OrderWorkflowService {

    private static final Logger log = LoggerFactory.getLogger(OrderWorkflowService.class);

    private final OrderRepository   orderRepository;
    private final InventoryClient   inventoryClient;
    private final NotificationService notificationService;
    private final DurableFlowEngine engine;
    private final ObjectMapper      mapper;

    private final WorkflowDefinition workflow;

    public OrderWorkflowService(OrderRepository orderRepository,
                                InventoryClient inventoryClient,
                                NotificationService notificationService,
                                DurableFlowEngine engine,
                                ObjectMapper mapper) {
        this.orderRepository    = orderRepository;
        this.inventoryClient    = inventoryClient;
        this.notificationService = notificationService;
        this.engine             = engine;
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
     * Saves the order and — only after the business transaction commits —
     * submits it to the durable-flow engine.
     */
    @Transactional
    public String placeOrder(Order order) {
        orderRepository.save(order);

        // Register a post-commit hook so that receive() is called only if
        // the business transaction commits successfully.
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    engine.receive(
                        new InboundMessage(
                            "order-service",
                            serialize(order),
                            Map.of("orderId", order.getId())),
                        ReceiveOptions.of(workflow));
                }
            });

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
| `receive()` called inside `@Transactional` | durable-flow commits immediately; business transaction commits later | ⚠️ Use `TransactionSynchronizationManager.afterCommit()` if you need atomicity |
| Business transaction rolls back after `receive()` | Workflow message is already persisted; steps may execute against missing data | ⚠️ Design step handlers to tolerate missing records, or use `afterCommit()` |
| `SYNCHRONOUS` mode inside `@Transactional` | Calling thread blocks; steps open connections from pool | ⚠️ Size pool to at least `immediateExecutionThreads + 1`; prefer `ASYNCHRONOUS` |
| Separate DataSource for durable-flow | Complete transaction isolation; no cross-DataSource atomicity | ✅ Cleanest isolation; cross-database atomicity requires XA (rarely needed) |
