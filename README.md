# durable-flow

[![Java](https://img.shields.io/badge/Java-17-blue.svg)](https://openjdk.org/projects/jdk/17/)
[![Maven](https://img.shields.io/badge/Maven-3.9%2B-blue.svg)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A production-quality, durable workflow engine for Java. `durable-flow` provides exactly-once,
persistent step execution with DAG dependencies, lease-based concurrency control, automatic retries,
and background recovery — all backed by a PostgreSQL database.

---

## Table of Contents

1. [Features](#features)
2. [Architecture Overview](#architecture-overview)
3. [Getting Started](#getting-started)
4. [Core Concepts](#core-concepts)
   - [InboundMessage](#inboundmessage)
   - [WorkflowDefinition](#workflowdefinition)
   - [RetryPolicy](#retrypolicy)
   - [MessagePreprocessor SPI](#messagepreprocessor-spi)
   - [MetricsListener SPI](#metricslistener-spi)
5. [Deduplication](#deduplication)
6. [Persistence & Schema](#persistence--schema)
7. [Lease-Based Step Claiming](#lease-based-step-claiming)
8. [Background Recovery](#background-recovery)
9. [Configuration Reference](#configuration-reference)
10. [Examples](#examples)
11. [Running the Tests](#running-the-tests)
12. [Module Structure](#module-structure)

---

## Features

- **Durable execution** — every message and step is persisted before execution begins
- **Exactly-once delivery** — XXH3 128-bit hashing deduplicates messages by `(source, hash, length)`
- **DAG dependencies** — steps declare `dependsOn` relationships; the engine respects the order
- **Lease-based concurrency** — atomic `UPDATE … RETURNING` prevents double-execution across nodes
- **Configurable retry policies** — fixed, exponential back-off, jitter; per-step exception classifiers
- **Background recovery** — expired leases and retryable failures are picked up by a scheduler
- **Observability** — SLF4J structured logging + `MetricsListener` SPI for your own metrics system
- **Schema migrations** — Flyway manages the PostgreSQL schema automatically on startup
- **Extension points** — `MessagePreprocessor` for canonicalization / payload scrubbing

---

## Architecture Overview

```
InboundMessage
      │
      ▼
DurableFlowEngine.receive()
      │
      ├─ MessagePreprocessor.preprocess()
      │        └─ canonical bytes → XXH3 128-bit hash
      │
      ├─ INSERT messages (ON CONFLICT DO NOTHING)
      │        └─ duplicate? → return ReceiveResult(duplicate=true)
      │
      ├─ INSERT message_steps + message_step_dependencies
      │
      └─ commit → submit to ExecutorService
                        │
                        ▼
               WorkflowOrchestrator
                        │
               findEligibleSteps()  ◄──── RecoveryScheduler (background)
                        │                 (recovers expired leases)
                        ▼
                  claimStep()   (atomic UPDATE … RETURNING)
                        │
                        ▼
                StepHandler.execute()
                        │
               ┌────────┴────────┐
               │                 │
           succeed           fail
               │                 │
        markSucceeded()    markFailed()
               │                 │
        recalculate message state
```

---

## Getting Started

### Prerequisites

- Java 17+
- Maven 3.9+
- PostgreSQL 14+ (or any JDBC-compatible PostgreSQL-compatible database)

### Add dependency

```xml
<dependency>
    <groupId>io.github.durableflow</groupId>
    <artifactId>durable-flow-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Minimal usage

```java
// 1. Create a DataSource (HikariCP recommended)
HikariConfig cfg = new HikariConfig();
cfg.setJdbcUrl("jdbc:postgresql://localhost:5432/mydb");
cfg.setUsername("user");
cfg.setPassword("pass");
cfg.setAutoCommit(false);
DataSource dataSource = new HikariDataSource(cfg);

// 2. Create and start the engine
DurableFlowEngine engine = new DurableFlowEngine(dataSource, DurableFlowConfig.defaults());
engine.start(); // starts background recovery scheduler

// 3. Define a workflow
WorkflowDefinition workflow = WorkflowDefinition.builder("order-processing")
    .step("validate",  ctx -> StepResult.empty())
    .step("enrich",    ctx -> StepResult.empty()).dependsOn("validate")
        .retryPolicy(RetryPolicy.exponentialBackoff(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(30), true))
    .step("notify",    ctx -> StepResult.empty()).dependsOn("enrich")
    .build();

// 4. Receive a message
InboundMessage message = new InboundMessage("order-service", payloadBytes, headers);
ReceiveResult result = engine.receive(message, ReceiveOptions.of(workflow));

System.out.println("messageId: " + result.messageId());
System.out.println("duplicate: " + result.duplicate());

// 5. Clean up
engine.close();
```

---

## Core Concepts

### InboundMessage

```java
public record InboundMessage(String source, byte[] rawPayload, Map<String, String> headers) {}
```

- `source` — logical origin identifier (e.g. queue name, topic, system name)
- `rawPayload` — raw bytes to process
- `headers` — transport-layer metadata

### WorkflowDefinition

Describes an immutable DAG of steps. Built with a fluent API:

```java
WorkflowDefinition wf = WorkflowDefinition.builder("my-workflow")
    .step("step-a", ctx -> {
        // ... do work
        return StepResult.of(outputBytes);
    })
    .step("step-b", ctx -> {
        byte[] fromA = ctx.getPreviousStepOutputs().get("step-a");
        // ...
        return StepResult.empty();
    }).dependsOn("step-a")
    .retryPolicy(RetryPolicy.fixedDelay(5, Duration.ofSeconds(2)))
    .build();
```

Each step receives a `StepContext` containing:

| Field | Description |
|---|---|
| `messageId` | Stable message identifier |
| `stepName` | Name of the current step |
| `attemptCount` | 1-based attempt number |
| `payload` | Stored payload bytes (may be null for NO_PAYLOAD mode) |
| `metadata` | Map extracted by the preprocessor |
| `previousStepOutputs` | Map of stepName → output bytes from upstream steps |
| `nodeId` | Identifier of the processing node |

### RetryPolicy

```java
// Fixed delay
RetryPolicy.fixedDelay(3, Duration.ofSeconds(2))

// Exponential back-off with jitter
RetryPolicy.exponentialBackoff(5,
    Duration.ofMillis(500),   // initial delay
    2.0,                       // multiplier
    Duration.ofSeconds(30),   // max delay
    true);                     // jitter

// No retry
RetryPolicy.noRetry()
```

### MessagePreprocessor SPI

Implement this interface to control canonicalization and storage:

```java
public class MyPreprocessor implements MessagePreprocessor {
    @Override
    public PreprocessResult preprocess(InboundMessage message) {
        byte[] canonical = normalize(message.rawPayload()); // for hash
        byte[] stored    = encrypt(message.rawPayload());   // for storage

        return PreprocessResult.builder()
            .canonicalBytes(canonical)
            .storedPayload(stored)
            .payloadStorageMode(PayloadStorageMode.ENCRYPTED)
            .metadata(Map.of("origin", message.source()))
            .build();
    }
}

// Pass to receive options:
engine.receive(message, new ReceiveOptions(workflow, new MyPreprocessor()));
```

#### PayloadStorageMode

| Value | Description |
|---|---|
| `INLINE` | Stored as-is in `payload_data` (BYTEA) |
| `ENCRYPTED` | Stored encrypted inline |
| `NO_PAYLOAD` | Payload intentionally dropped; only hash and metadata retained |
| `EXTERNAL_REF` | Not stored inline; `payload_ref` contains an external URI |

### MetricsListener SPI

```java
public class PrometheusMetricsListener implements MetricsListener {
    @Override public void onMessageReceived() { counter("messages_received").inc(); }
    @Override public void onStepSucceeded(String step, Duration d) { histogram(step).observe(d); }
    // ...
}

DurableFlowEngine engine = new DurableFlowEngine(ds, config, new PrometheusMetricsListener());
```

---

## Deduplication

Deduplication scope: `(source, dedupe_hash, payload_length)`

The hash is computed using [zero-allocation-hashing](https://github.com/OpenHFT/Zero-Allocation-Hashing) XXH3 128-bit:

```
dedupe_hash = XXH3_128(canonicalBytes)  →  32-char lowercase hex string
```

The database enforces uniqueness via a `UNIQUE` constraint. When a duplicate is detected,
`receive()` returns immediately with `ReceiveResult(duplicate=true)` without inserting new rows.

---

## Persistence & Schema

The schema is managed by **Flyway** and applied automatically on engine startup (configurable).

### Key tables

| Table | Purpose |
|---|---|
| `messages` | One row per unique message |
| `message_steps` | One row per step per message |
| `message_step_dependencies` | DAG edges between steps |

### Message States

```
RECEIVED → IN_PROGRESS → PROCESSED
                      ↘ ERROR (retryable failures pending)
                      ↘ PARKED (permanent failure; requires redrive)
```

### Step States

```
PENDING → RUNNING → SUCCEEDED
               ↘ FAILED_RETRYABLE → (back to PENDING after delay)
               ↘ FAILED_FINAL
```

---

## Lease-Based Step Claiming

Step claiming uses an atomic `UPDATE … WHERE … RETURNING` to prevent double-execution:

```sql
UPDATE message_steps
SET step_state   = 'RUNNING',
    owner        = ?,          -- nodeId
    locked_until = ?,          -- NOW() + leaseTimeoutSeconds
    attempt_count = attempt_count + 1,
    updated_at   = NOW()
WHERE id = ?
  AND step_state IN ('PENDING', 'FAILED_RETRYABLE')
  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
  AND (locked_until IS NULL OR locked_until < NOW())
RETURNING id
```

If no row is returned, another node claimed the step first.

---

## Background Recovery

The `RecoveryScheduler` runs every `recoveryIntervalSeconds` (default 30s) and:

1. Resets RUNNING steps with expired leases back to `FAILED_RETRYABLE`
2. Scans for eligible steps and dispatches them to the executor

```sql
-- Recover expired leases
UPDATE message_steps
SET step_state = 'FAILED_RETRYABLE', locked_until = NULL, owner = NULL, next_retry_at = NOW()
WHERE step_state = 'RUNNING' AND locked_until < NOW()
```

---

## Configuration Reference

```java
DurableFlowConfig config = DurableFlowConfig.builder()
    .nodeId("my-node-1")                 // default: random UUID
    .leaseTimeoutSeconds(60)             // default: 60
    .recoveryIntervalSeconds(30)         // default: 30
    .immediateExecutionThreads(4)        // default: 4
    .schemaAutoMigrate(true)             // default: true
    .build();
```

| Property | Default | Description |
|---|---|---|
| `nodeId` | random UUID | Identifies this node in the `owner` column |
| `leaseTimeoutSeconds` | 60 | How long a step lease is held before recovery |
| `recoveryIntervalSeconds` | 30 | How often the recovery scheduler runs |
| `immediateExecutionThreads` | 4 | Thread pool size for post-receive step execution |
| `schemaAutoMigrate` | true | Whether to run Flyway migrations on startup |

---

## Examples

See the `durable-flow-example` module for runnable examples:

| Class | Description |
|---|---|
| `SequentialWorkflowExample` | 3 sequential steps: validate → enrich → notify |
| `ParallelWorkflowExample` | Parallel enrichment steps converging at aggregate |
| `NoPayloadExample` | Preprocessor that drops payload for privacy compliance |

Set environment variables before running:

```bash
export JDBC_URL=jdbc:postgresql://localhost:5432/durable_flow
export DB_USER=postgres
export DB_PASS=postgres
```

---

## Running the Tests

### Unit tests (no database required)

```bash
mvn test
```

### Integration tests (requires Docker for Testcontainers)

```bash
mvn verify -P integration
```

Or run all tests including integration:

```bash
mvn failsafe:integration-test failsafe:verify
```

Integration tests use [Testcontainers](https://testcontainers.com/) to spin up a PostgreSQL
container automatically. Docker must be available on the host.

---

## Module Structure

```
durable-flow/
├── pom.xml                          # Root POM (multi-module)
├── durable-flow-core/               # Main library
│   ├── src/main/java/io/github/durableflow/
│   │   ├── api/                     # Public API (records, enums, interfaces)
│   │   ├── spi/                     # Extension points
│   │   ├── engine/                  # Workflow orchestration internals
│   │   ├── persistence/             # JDBC repositories
│   │   ├── scheduler/               # Background recovery
│   │   ├── DurableFlowEngine.java   # Main entry point
│   │   └── DurableFlowConfig.java
│   └── src/main/resources/
│       └── db/migration/
│           └── V1__initial_schema.sql
└── durable-flow-example/            # Runnable examples
    └── src/main/java/io/github/durableflow/example/
```

---

## Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 17 | Language |
| Maven | 3.9+ | Build |
| SLF4J | 2.0 | Logging facade |
| HikariCP | 5.1 | JDBC connection pool |
| zero-allocation-hashing | 0.16 | XXH3 128-bit hashing |
| Flyway | 10.x | Schema migrations |
| Jackson | 2.17 | JSON metadata serialization |
| JUnit 5 | 5.10 | Unit testing |
| Testcontainers | 1.19 | Integration testing with PostgreSQL |
| Mockito | 5 | Mocking |
| PostgreSQL | 14+ | Primary persistence backend |