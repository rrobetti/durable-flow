# Reliable Multi-Step Workflows in Java: An Introduction to durable-flow

## The Problem Nobody Wants to Talk About

Picture a common scenario: a customer places an order in your e-commerce application. Your service validates the order, checks inventory, charges the payment provider, and finally sends a confirmation email. Written naively, this looks like four sequential method calls inside a single request handler. Simple enough — until the service crashes between step two and step three, your payment provider returns a timeout, or your email relay goes down at 2 a.m. on a Saturday.

Suddenly you are facing a host of uncomfortable questions. Did the payment actually go through? Was inventory reserved before the crash? Should you retry the whole sequence from the start, or just pick up where you left off? If you retry from the start, do you risk charging the customer twice?

This class of problem — keeping a multi-step process reliable, resumable, and exactly-once — is deceptively hard, and most teams end up solving it in an ad hoc way: a tangle of try-catch blocks, a `status` column in the database that someone updates manually, and a cron job that nobody fully understands. `durable-flow` is a small Java library whose entire purpose is to provide a clean, production-quality answer to this problem.

---

## What Is durable-flow?

At its core, `durable-flow` is a workflow engine for Java 17+ that runs on top of a relational database. You describe a workflow as a named set of *steps*, where each step is just a Java function (a lambda or method reference). The library takes care of persisting the workflow state, executing steps in dependency order, handling retries when something goes wrong, and recovering in-flight work after a node restart.

The key guarantee the library offers is *durable execution*: before `receive()` returns to your code, the message and all of its pending steps have been committed to the database. This means that even if the JVM dies the instant after `receive()` returns, a background recovery scheduler will find the unfinished work and resume it — no data is lost.

Steps can depend on each other, forming a directed acyclic graph (DAG). A step that depends on two upstream steps will not start until both of them have succeeded. Outputs from upstream steps are automatically forwarded to downstream steps, so you can thread data through the pipeline without external coordination.

A *lease* mechanism prevents the same step from being executed twice across multiple nodes. When a node picks up a step, it atomically claims it by setting a timestamp and a lease owner in the database. If the lease expires without a success or failure record — because the node crashed — the recovery scheduler reclaims the step and makes it eligible again.

The library requires no Spring Framework, no Kafka, no Redis, and no external scheduler service. If you have a JDBC-accessible relational database, you have everything you need.

---

## When Should You Reach for It?

`durable-flow` fits naturally wherever you have a sequence of operations that must all succeed, in order, and that you want to be resilient to partial failures and restarts.

**Order and payment processing** is the canonical example. Validate the cart, reserve inventory, charge the card, and send the receipt. Each step is meaningful on its own, each can fail independently, and the order in which they must happen is fixed. Mapping these directly to workflow steps gives you automatic retry with configurable back-off, a clear audit trail in the database, and the ability to resume exactly where you left off after a failure.

**Asynchronous event handling from a REST API** is another sweet spot. When a client POSTs a request that kicks off a long-running process — document generation, data import, video transcoding — you want to acknowledge the request immediately and process the work in the background. With `durable-flow` in `ASYNCHRONOUS` mode you get exactly that: `receive()` returns a `messageId` to the caller right away (after durably persisting the request), while the steps run on a thread pool in the background.

**Notification pipelines** benefit from the same properties. Sending an email, pushing a push notification, and recording an audit log entry all need to happen as a consequence of some business event. If the push notification provider is temporarily down, you want the step to retry with exponential back-off rather than silently dropping the notification or blocking the whole request.

**Background jobs that must not run twice** are also a natural fit, especially in multi-node deployments where multiple instances of the service are running simultaneously. The lease-based claiming mechanism ensures that a given step is executed by exactly one node at a time, removing the need for a separate distributed lock.

---

## A Quick Tour of the API

Before diving into Spring Boot wiring, it is worth seeing what the core concepts look like in code.

You describe a workflow using a fluent builder. Here is a three-step pipeline that mirrors the order-processing scenario described earlier:

```java
WorkflowDefinition workflow = WorkflowDefinition.builder("order-processing")
    .beforeProcessing(ctx -> log.info("Starting workflow for message {}", ctx.getMessageId()))
    .step("validate",  ctx -> validate(ctx))
    .step("charge",    ctx -> charge(ctx)).dependsOn("validate")
        .retryPolicy(RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), true))
    .step("notify",    ctx -> notify(ctx)).dependsOn("charge")
    .afterProcessing(ctx -> log.info("Finished with state {}", ctx.getFinalState()))
    .build();
```

Each step handler receives a `StepContext` that carries the original message payload, any metadata attached to the message, and the byte-array outputs that upstream steps produced. A step signals success by returning a `StepResult` — either `StepResult.empty()` if it has no output for downstream steps, or `StepResult.of(bytes)` if it does.

Submitting a message for processing is a single call:

```java
InboundMessage message = new InboundMessage(
    "order-service",
    orderJson.getBytes(StandardCharsets.UTF_8),
    Map.of("correlationId", orderId)
);

ReceiveResult result = engine.receive(message, ReceiveOptions.of(workflow));
```

The `ReceiveResult` gives you the stable `messageId` you can store and use later to query the message's state. The library also deduplicates messages: if the same logical message is submitted twice (the same source, the same content), `receive()` returns the existing record with `duplicate = true` rather than creating a second workflow run.

---

## Using durable-flow in a Spring Boot Application

### Adding the Dependency

Start by adding `durable-flow-core` to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.durableflow</groupId>
    <artifactId>durable-flow-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Because the library has no Spring dependency of its own, this is all you need. Spring Boot's existing `spring.datasource.*` configuration and auto-configured `DataSource` bean will work just fine.

### Wiring the Engine as a Spring Bean

The recommended approach is a `@Configuration` class that creates the `DurableFlowEngine` and `DurableFlowConfig` beans. The engine implements `Closeable`, so declaring `destroyMethod = "close"` ensures it shuts down cleanly when the application context closes:

```java
@Configuration
public class DurableFlowConfiguration {

    @Bean(destroyMethod = "close")
    public DurableFlowEngine durableFlowEngine(DataSource dataSource,
                                               DurableFlowConfig config) {
        DurableFlowEngine engine = new DurableFlowEngine(dataSource, config);
        engine.start();
        return engine;
    }

    @Bean
    public DurableFlowConfig durableFlowConfig() {
        return DurableFlowConfig.builder()
                .nodeId(System.getenv().getOrDefault("HOSTNAME",
                        UUID.randomUUID().toString()))
                .leaseTimeoutSeconds(60)
                .recoveryIntervalSeconds(30)
                .immediateExecutionThreads(4)
                .schemaAutoMigrate(true)
                .executionMode(ExecutionMode.ASYNCHRONOUS)
                .build();
    }
}
```

A few things are worth pointing out here. The `nodeId` is used for lease tracking, so each JVM instance in a multi-node deployment should have a unique value — the `HOSTNAME` environment variable is a convenient choice in containerised environments. Setting `schemaAutoMigrate(true)` tells the library to run its own Flyway migrations on startup, which creates the `messages` and `message_steps` tables automatically.

### Calling Spring Beans from Step Handlers

Because the `DurableFlowEngine` is a plain Spring bean, you can inject it into any `@Service`. The workflow definition itself is typically built in the service's constructor, closing over the other injected Spring beans it needs:

```java
@Service
public class OrderWorkflowService {

    private final InventoryClient inventoryClient;
    private final PaymentGateway paymentGateway;
    private final NotificationService notificationService;
    private final DurableFlowEngine engine;
    private final WorkflowDefinition workflow;

    public OrderWorkflowService(InventoryClient inventoryClient,
                                PaymentGateway paymentGateway,
                                NotificationService notificationService,
                                DurableFlowEngine engine) {
        this.inventoryClient = inventoryClient;
        this.paymentGateway = paymentGateway;
        this.notificationService = notificationService;
        this.engine = engine;

        this.workflow = WorkflowDefinition.builder("order-processing")
            .step("validate",  this::validateOrder)
            .step("charge",    this::chargeOrder).dependsOn("validate")
                .retryPolicy(RetryPolicy.exponentialBackoff(
                    5, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), true))
            .step("notify",    this::sendConfirmation).dependsOn("charge")
            .build();
    }

    public String submit(Order order) {
        byte[] payload = serialize(order);
        ReceiveResult result = engine.receive(
            new InboundMessage("order-service", payload, Map.of()),
            ReceiveOptions.of(workflow));
        return result.messageId();
    }

    private StepResult validateOrder(StepContext ctx) throws Exception {
        Order order = deserialize(ctx.getPayload());
        inventoryClient.reserve(order);
        return StepResult.of(ctx.getPayload());
    }

    private StepResult chargeOrder(StepContext ctx) throws Exception {
        Order order = deserialize(ctx.getPreviousStepOutputs().get("validate"));
        paymentGateway.charge(order);
        return StepResult.of(ctx.getPayload());
    }

    private StepResult sendConfirmation(StepContext ctx) throws Exception {
        Order order = deserialize(ctx.getPayload());
        notificationService.sendEmail(order);
        return StepResult.empty();
    }
}
```

An important detail: step handlers run on the engine's internal thread pool (in `ASYNCHRONOUS` mode) or on the calling thread (in `SYNCHRONOUS` mode). Either way, there is no active Spring transaction on that thread when the handler is invoked. If you call a `@Transactional` Spring bean from inside a step handler, Spring will start a new transaction for that call, which is usually what you want — each step runs in its own transaction boundary.

### Choosing Between SYNCHRONOUS and ASYNCHRONOUS Mode

The execution mode controls when `receive()` returns relative to step execution. In `ASYNCHRONOUS` mode (the default), `receive()` returns a `ReceiveResult` as soon as the message is persisted to the database, and the steps execute in the background. This is the right choice for event-driven endpoints that need to respond quickly.

In `SYNCHRONOUS` mode, `receive()` blocks on the calling thread until all steps that can currently run have run. The returned `ReceiveResult.messageState()` reflects the actual outcome — `PROCESSED` if everything succeeded, `PARKED` if a step failed permanently. This mode is particularly useful in tests, where you want to assert the final state immediately, and in synchronous request handlers where the response body should reflect the outcome.

### A Note on Transactions When Calling receive() Inside @Transactional Methods

Because `durable-flow` manages its own JDBC connections independently of Spring's `PlatformTransactionManager`, calling `receive()` inside a `@Transactional` method can create a subtle ordering problem: the message is committed to the database by the library, but the surrounding Spring transaction — which may include the business record you wanted the workflow to process — has not been committed yet. If the application crashes at this point, the workflow will start executing steps against data that was never committed.

The clean solution is to defer the `receive()` call until after the Spring transaction commits, using `TransactionSynchronizationManager`:

```java
@Transactional
public String placeOrder(Order order) {
    orderRepository.save(order);

    TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                orderWorkflowService.submit(order);
            }
        }
    );

    return order.getId();
}
```

This pattern ensures that the workflow only starts once the business data it depends on is safely committed.

---

## Handling Failures and Retries

One of the most practical features of `durable-flow` is its per-step retry policy. Each step can declare how it wants to be retried when an exception is thrown. The library provides three ready-made factories: `RetryPolicy.noRetry()` for steps that should fail immediately, `RetryPolicy.fixedDelay(maxAttempts, delay)` for a simple fixed interval, and `RetryPolicy.exponentialBackoff(...)` for the classic pattern of doubling the wait time between attempts, optionally with jitter to spread load across nodes.

When a step exceeds its retry budget, the library marks it as `FAILED_FINAL` and moves the whole message into the `PARKED` state. A parked message is not retried automatically — it requires manual intervention via `engine.redrive(messageId)`, which resets the failed steps and makes them eligible for execution again. This makes it easy to fix a bug, deploy the fix, and then recover all the messages that failed before the fix without losing the work done by steps that already succeeded.

---

## Schema and Multi-Database Support

On startup, `durable-flow` uses Flyway to create the two tables it needs — `messages` and `message_steps` — along with indexes and constraints. This happens automatically when `schemaAutoMigrate` is set to `true`. If you are already using Flyway in your project and prefer to manage all migrations yourself, you can set `schemaAutoMigrate(false)` and copy the relevant migration scripts into your own Flyway location. The README has the details.

The library ships with dialect support for PostgreSQL, Oracle, MySQL, MariaDB, IBM DB2, and Microsoft SQL Server (MySQL and MariaDB share the same dialect implementation). In most cases the correct dialect is detected automatically from the JDBC metadata. If you need to pin it explicitly, `DurableFlowConfig.builder().dialect(OracleDialect.INSTANCE)` does the job.

---

## Observability

`durable-flow` logs structured events at appropriate levels using SLF4J, so its output will flow naturally into whatever logging infrastructure your application already uses. For metrics, it exposes the `MetricsListener` SPI: implement the interface, pass an instance to `DurableFlowConfig.builder().metricsListener(listener)`, and you will receive callbacks for every interesting event — message received, step started, step succeeded, step failed, message processed, message parked. Wiring this to Micrometer or Prometheus counters is straightforward.

---

## Summary

`durable-flow` is a focused, self-contained library that brings durable, exactly-once workflow execution to any Java application backed by a relational database. It handles the hard parts — persistence, deduplication, dependency ordering, lease-based concurrency, retries, and crash recovery — so that you can focus on the business logic inside each step. Its zero-framework-dependency design makes it a natural fit inside Spring Boot applications, where it slots in as a regular bean alongside your existing data source, services, and repositories.

If you have multi-step processes that today live in fragile try-catch chains or ad hoc status columns, `durable-flow` is worth a look.
