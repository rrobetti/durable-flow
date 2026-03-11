# Table Partitioning Guide

Partitioning the `messages`, `message_steps`, and `message_step_dependencies` tables is
**optional**. The default schema works well until tables reach tens of millions of rows.
Once tables grow large, partitioning limits the number of rows each query must scan and
makes it straightforward to purge old data by dropping entire partitions instead of
issuing expensive bulk `DELETE` statements.

This guide explains when to consider partitioning, what trade-offs each database imposes,
and provides ready-to-use SQL for every supported database. **None of this SQL is applied
automatically by durable-flow.** The engine works with any table layout — partitioned or
not — as long as the column names, data types, and indexes match the base schema.

---

## Table of Contents

1. [When to Consider Partitioning](#when-to-consider-partitioning)
2. [Partitioning Strategies](#partitioning-strategies)
3. [PostgreSQL](#postgresql)
4. [MySQL / MariaDB](#mysql--mariadb)
5. [Oracle](#oracle)
6. [Microsoft SQL Server](#microsoft-sql-server)
7. [IBM DB2](#ibm-db2)
8. [Partition Maintenance](#partition-maintenance)
9. [Migrating Existing Tables](#migrating-existing-tables)

---

## When to Consider Partitioning

Partitioning adds operational complexity. A useful rule of thumb:

| Approximate table size | Recommendation |
|---|---|
| < 10 million rows | Standard indexes are sufficient; partitioning is unlikely to help. |
| 10 – 100 million rows | Profile query latency first. Add indexes before considering partitioning. |
| > 100 million rows | Time-based range partitioning generally provides measurable benefit. |

Workloads that benefit the most:

- **Recovery scans** — `RecoveryScheduler` queries steps by `step_state` and `locked_until`.
  A partial/filtered index already prunes most rows; partitioning removes the tail.
- **Archival / data retention** — dropping a monthly partition is orders of magnitude
  faster than `DELETE FROM messages WHERE created_at < ?`.
- **Analytics queries** — if you run reports against the workflow tables, partition
  pruning ensures only relevant months are scanned.

---

## Partitioning Strategies

Two strategies suit durable-flow's access patterns:

| Strategy | Partition key | Best for | Notes |
|---|---|---|---|
| **Time-based range** | `created_at` (monthly or daily) | Archival, data retention, time-range queries | Requires schema changes on PostgreSQL and MySQL (see below). |
| **Hash** | `id` | Even I/O distribution across storage | Fixed number of partitions; no archival benefit. |

**Time-based range partitioning** is the most common choice and is the primary focus of
this guide. Most operators want to archive or delete messages that completed more than
N days or months ago, and range partitioning makes that a single `DROP TABLE` call
instead of a long-running delete.

---

## PostgreSQL

### Constraints

PostgreSQL **requires** that primary keys and unique constraints include all partition key
columns. For range partitioning by `created_at` this means:

- The primary key becomes `(id, created_at)`.
- The deduplication unique constraint must also include `created_at`.
- The foreign key from `message_steps.message_id → messages(id)` can no longer be a
  database-level constraint, because there is no standalone unique index on `messages.id`.
  Remove the `REFERENCES` clause and enforce referential integrity at the application layer.

If you want to keep the original primary key on `id` only, use **hash partitioning**
instead (see below). Hash partitioning does not need `created_at` in the primary key, but
it does not enable archival by date.

### Option A — Range partitioning by `created_at` (recommended for archival)

Replace the Flyway-managed schema with the DDL below **before** the first deployment.
For existing installations, see [Migrating Existing Tables](#migrating-existing-tables).

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- messages  (partitioned by created_at, monthly ranges)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE messages (
    id                   VARCHAR(36)   NOT NULL,
    source               VARCHAR(255)  NOT NULL,
    dedupe_hash          VARCHAR(64)   NOT NULL,
    payload_length       BIGINT        NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR(32)   NOT NULL DEFAULT 'INLINE',
    payload_data         BYTEA,
    payload_ref          VARCHAR(1024),
    message_state        VARCHAR(32)   NOT NULL DEFAULT 'RECEIVED',
    workflow_name        VARCHAR(255),
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error           TEXT,
    metadata_json        TEXT,
    -- Partition key (created_at) must be part of the primary key
    PRIMARY KEY (id, created_at),
    -- Deduplication constraint must also include the partition key
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_messages_state  ON messages (message_state);
CREATE INDEX idx_messages_source ON messages (source);

-- Create one partition per month; add more as needed (see Partition Maintenance)
CREATE TABLE messages_y2024m01 PARTITION OF messages
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE messages_y2024m02 PARTITION OF messages
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... continue for each month ...

-- ─────────────────────────────────────────────────────────────────────────────
-- message_steps  (partitioned by created_at, monthly ranges)
-- NOTE: REFERENCES messages(id) is removed because messages has a composite PK
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_steps (
    id                  VARCHAR(36)      NOT NULL,
    message_id          VARCHAR(36)      NOT NULL,   -- no FK constraint; enforced by app
    step_name           VARCHAR(255)     NOT NULL,
    step_state          VARCHAR(32)      NOT NULL DEFAULT 'PENDING',
    attempt_count       INTEGER          NOT NULL DEFAULT 0,
    max_attempts        INTEGER          NOT NULL DEFAULT 3,
    next_retry_at       TIMESTAMPTZ,
    locked_until        TIMESTAMPTZ,
    owner               VARCHAR(255),
    last_error          TEXT,
    result_data         BYTEA,
    retry_delay_ms      BIGINT           NOT NULL DEFAULT 1000,
    retry_multiplier    DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    retry_max_delay_ms  BIGINT           NOT NULL DEFAULT 60000,
    retry_jitter        BOOLEAN          NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at),
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_steps_message_id ON message_steps (message_id);
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at)
    WHERE step_state IN ('PENDING', 'FAILED_RETRYABLE');
CREATE INDEX idx_steps_expired    ON message_steps (locked_until)
    WHERE step_state = 'RUNNING';

CREATE TABLE message_steps_y2024m01 PARTITION OF message_steps
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE message_steps_y2024m02 PARTITION OF message_steps
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... continue for each month ...

-- ─────────────────────────────────────────────────────────────────────────────
-- message_step_dependencies  (partitioned by message_id hash to co-locate with steps)
-- Alternatively, leave this table unpartitioned — it is much smaller than the other two
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)  NOT NULL,
    step_name            VARCHAR(255) NOT NULL,
    depends_on_step_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

> **Tip:** Use [pg_partman](https://github.com/pgpartman/pg_partman) to automate the
> creation of future partitions and the detachment of old ones. It integrates with
> `pg_cron` or a standalone background worker.

### Option B — Hash partitioning by `id`

Hash partitioning spreads rows evenly and does not require changing the primary key or
dropping the foreign key. It does not allow archival by date.

```sql
CREATE TABLE messages (
    id               VARCHAR(36)   NOT NULL,
    source           VARCHAR(255)  NOT NULL,
    dedupe_hash      VARCHAR(64)   NOT NULL,
    payload_length   BIGINT        NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR(32) NOT NULL DEFAULT 'INLINE',
    payload_data     BYTEA,
    payload_ref      VARCHAR(1024),
    message_state    VARCHAR(32)   NOT NULL DEFAULT 'RECEIVED',
    workflow_name    VARCHAR(255),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error       TEXT,
    metadata_json    TEXT,
    PRIMARY KEY (id),
    -- NOTE: unique constraints must include the partition key (id) — which they
    -- already do implicitly here only for the PK. The deduplication constraint
    -- (source, dedupe_hash, payload_length) does NOT include id, so it cannot
    -- be enforced as a database constraint on a hash-partitioned table.
    -- Enforce deduplication in the application or use a separate lookup table.
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length, id)
) PARTITION BY HASH (id);

-- 4 partitions is a common starting point; use a power of 2 for easy resharding
CREATE TABLE messages_p0 PARTITION OF messages FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE messages_p1 PARTITION OF messages FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE messages_p2 PARTITION OF messages FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE messages_p3 PARTITION OF messages FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE INDEX idx_messages_state  ON messages (message_state);
CREATE INDEX idx_messages_source ON messages (source);

-- message_steps hashed by message_id to co-locate related rows
CREATE TABLE message_steps (
    id                  VARCHAR(36)      NOT NULL,
    message_id          VARCHAR(36)      NOT NULL REFERENCES messages(id),
    step_name           VARCHAR(255)     NOT NULL,
    step_state          VARCHAR(32)      NOT NULL DEFAULT 'PENDING',
    attempt_count       INTEGER          NOT NULL DEFAULT 0,
    max_attempts        INTEGER          NOT NULL DEFAULT 3,
    next_retry_at       TIMESTAMPTZ,
    locked_until        TIMESTAMPTZ,
    owner               VARCHAR(255),
    last_error          TEXT,
    result_data         BYTEA,
    retry_delay_ms      BIGINT           NOT NULL DEFAULT 1000,
    retry_multiplier    DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    retry_max_delay_ms  BIGINT           NOT NULL DEFAULT 60000,
    retry_jitter        BOOLEAN          NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, message_id),
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name)
) PARTITION BY HASH (message_id);

CREATE TABLE message_steps_p0 PARTITION OF message_steps FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE message_steps_p1 PARTITION OF message_steps FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE message_steps_p2 PARTITION OF message_steps FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE message_steps_p3 PARTITION OF message_steps FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE INDEX idx_steps_message_id ON message_steps (message_id);
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at)
    WHERE step_state IN ('PENDING', 'FAILED_RETRYABLE');
CREATE INDEX idx_steps_expired    ON message_steps (locked_until)
    WHERE step_state = 'RUNNING';

CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)  NOT NULL,
    step_name            VARCHAR(255) NOT NULL,
    depends_on_step_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

---

## MySQL / MariaDB

MySQL 8.0 and MariaDB 10.6 impose the same restriction as PostgreSQL: **all columns in a
unique key (including the primary key) must include the partition column** when using
`PARTITION BY RANGE`. The primary key must therefore change to `(id, created_at)`, and
the foreign key from `message_steps` must be removed.

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- messages  (partitioned by created_at, monthly ranges)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE messages (
    id                   VARCHAR(36)    NOT NULL,
    source               VARCHAR(255)   NOT NULL,
    dedupe_hash          VARCHAR(64)    NOT NULL,
    payload_length       BIGINT         NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR(32)    NOT NULL DEFAULT 'INLINE',
    payload_data         LONGBLOB,
    payload_ref          VARCHAR(1024),
    message_state        VARCHAR(32)    NOT NULL DEFAULT 'RECEIVED',
    workflow_name        VARCHAR(255),
    created_at           DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at           DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error           LONGTEXT,
    metadata_json        LONGTEXT,
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uq_messages_dedupe (source, dedupe_hash, payload_length, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    PARTITION p202401 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01 00:00:00')),
    PARTITION p202402 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01 00:00:00')),
    PARTITION p202403 VALUES LESS THAN (UNIX_TIMESTAMP('2024-04-01 00:00:00')),
    -- ... add partitions for each month ...
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

CREATE INDEX idx_messages_state  ON messages (message_state);
CREATE INDEX idx_messages_source ON messages (source);

-- ─────────────────────────────────────────────────────────────────────────────
-- message_steps  (partitioned by created_at, monthly ranges)
-- Foreign key to messages removed because messages has a composite primary key
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_steps (
    id                  VARCHAR(36)     NOT NULL,
    message_id          VARCHAR(36)     NOT NULL,   -- no FK constraint
    step_name           VARCHAR(255)    NOT NULL,
    step_state          VARCHAR(32)     NOT NULL DEFAULT 'PENDING',
    attempt_count       INT             NOT NULL DEFAULT 0,
    max_attempts        INT             NOT NULL DEFAULT 3,
    next_retry_at       DATETIME(6),
    locked_until        DATETIME(6),
    owner               VARCHAR(255),
    last_error          LONGTEXT,
    result_data         LONGBLOB,
    retry_delay_ms      BIGINT          NOT NULL DEFAULT 1000,
    retry_multiplier    DOUBLE          NOT NULL DEFAULT 2.0,
    retry_max_delay_ms  BIGINT          NOT NULL DEFAULT 60000,
    retry_jitter        TINYINT(1)      NOT NULL DEFAULT 0,
    created_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at          DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id, created_at),
    UNIQUE KEY uq_message_steps (message_id, step_name, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (UNIX_TIMESTAMP(created_at)) (
    PARTITION p202401 VALUES LESS THAN (UNIX_TIMESTAMP('2024-02-01 00:00:00')),
    PARTITION p202402 VALUES LESS THAN (UNIX_TIMESTAMP('2024-03-01 00:00:00')),
    PARTITION p202403 VALUES LESS THAN (UNIX_TIMESTAMP('2024-04-01 00:00:00')),
    -- ... add partitions for each month ...
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

CREATE INDEX idx_steps_message_id ON message_steps (message_id);
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at);
CREATE INDEX idx_steps_expired    ON message_steps (locked_until);

-- message_step_dependencies — leave unpartitioned or mirror the strategy above
CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)    NOT NULL,
    step_name            VARCHAR(255)   NOT NULL,
    depends_on_step_name VARCHAR(255)   NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

To add new monthly partitions, replace `p_future` with the new month and add a new
catch-all:

```sql
ALTER TABLE messages REORGANIZE PARTITION p_future INTO (
    PARTITION p202412 VALUES LESS THAN (UNIX_TIMESTAMP('2025-01-01 00:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
-- Repeat for message_steps
ALTER TABLE message_steps REORGANIZE PARTITION p_future INTO (
    PARTITION p202412 VALUES LESS THAN (UNIX_TIMESTAMP('2025-01-01 00:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

---

## Oracle

Oracle 12c Release 2+ supports **interval partitioning**, which automatically creates a
new monthly (or daily) partition whenever a row falls outside the existing partition
boundaries. This is the most operationally convenient option because you never need to
pre-create partitions.

Oracle does **not** require the partition key to be part of the primary key, so the
original `PRIMARY KEY (id)` and foreign key constraints are preserved.

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- messages  (interval-partitioned by created_at, auto monthly)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE messages (
    id                   VARCHAR2(36)              NOT NULL PRIMARY KEY,
    source               VARCHAR2(255)             NOT NULL,
    dedupe_hash          VARCHAR2(64)              NOT NULL,
    payload_length       NUMBER(19)                NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR2(32)              NOT NULL DEFAULT 'INLINE',
    payload_data         BLOB,
    payload_ref          VARCHAR2(1024),
    message_state        VARCHAR2(32)              NOT NULL DEFAULT 'RECEIVED',
    workflow_name        VARCHAR2(255),
    created_at           TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error           CLOB,
    metadata_json        CLOB,
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length)
)
PARTITION BY RANGE (created_at)
INTERVAL (INTERVAL '1' MONTH)
(
    -- At least one anchor partition is required; Oracle creates subsequent
    -- partitions automatically as data arrives
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00.000000 UTC')
);

CREATE INDEX idx_messages_state  ON messages (message_state) LOCAL;
CREATE INDEX idx_messages_source ON messages (source)        LOCAL;

-- ─────────────────────────────────────────────────────────────────────────────
-- message_steps  (interval-partitioned by created_at, auto monthly)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_steps (
    id                  VARCHAR2(36)              NOT NULL PRIMARY KEY,
    message_id          VARCHAR2(36)              NOT NULL REFERENCES messages(id),
    step_name           VARCHAR2(255)             NOT NULL,
    step_state          VARCHAR2(32)              NOT NULL DEFAULT 'PENDING',
    attempt_count       NUMBER(10)                NOT NULL DEFAULT 0,
    max_attempts        NUMBER(10)                NOT NULL DEFAULT 3,
    next_retry_at       TIMESTAMP WITH TIME ZONE,
    locked_until        TIMESTAMP WITH TIME ZONE,
    owner               VARCHAR2(255),
    last_error          CLOB,
    result_data         BLOB,
    retry_delay_ms      NUMBER(19)                NOT NULL DEFAULT 1000,
    retry_multiplier    FLOAT(53)                 NOT NULL DEFAULT 2.0,
    retry_max_delay_ms  NUMBER(19)                NOT NULL DEFAULT 60000,
    retry_jitter        NUMBER(1,0)               NOT NULL DEFAULT 0,
    created_at          TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name)
)
PARTITION BY RANGE (created_at)
INTERVAL (INTERVAL '1' MONTH)
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00.000000 UTC')
);

CREATE INDEX idx_steps_message_id ON message_steps (message_id) LOCAL;
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at) LOCAL;
CREATE INDEX idx_steps_expired    ON message_steps (locked_until)              LOCAL;

-- ─────────────────────────────────────────────────────────────────────────────
-- message_step_dependencies  (leave unpartitioned — much smaller table)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_step_dependencies (
    message_id           VARCHAR2(36)  NOT NULL,
    step_name            VARCHAR2(255) NOT NULL,
    depends_on_step_name VARCHAR2(255) NOT NULL,
    CONSTRAINT pk_step_deps PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

Use `LOCAL` indexes (partitioned indexes aligned with the table partitions) for the best
partition-pruning performance.

To drop an old partition (archival):

```sql
-- Drop data older than 2024-01
ALTER TABLE messages      DROP PARTITION p_initial;
ALTER TABLE message_steps DROP PARTITION p_initial;
```

Oracle will automatically create the next partition as new data arrives.

---

## Microsoft SQL Server

SQL Server uses **partition functions** and **partition schemes** that are defined once
and then referenced by the table and its indexes. SQL Server does **not** require the
partition key to be part of the primary key.

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1: Partition function — defines the boundary points (monthly)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE PARTITION FUNCTION pf_monthly (DATETIME2(6))
AS RANGE RIGHT FOR VALUES (
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01',
    '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01',
    '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
    '2025-01-01'
    -- Add future boundary points before data arrives (see Partition Maintenance)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 2: Partition scheme — maps each partition to a filegroup
-- Replace [PRIMARY] with dedicated filegroups for production workloads
-- ─────────────────────────────────────────────────────────────────────────────
CREATE PARTITION SCHEME ps_monthly
AS PARTITION pf_monthly ALL TO ([PRIMARY]);

-- ─────────────────────────────────────────────────────────────────────────────
-- messages  (placed on the partition scheme, partitioned by created_at)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE messages (
    id                   VARCHAR(36)        NOT NULL PRIMARY KEY,
    source               VARCHAR(255)       NOT NULL,
    dedupe_hash          VARCHAR(64)        NOT NULL,
    payload_length       BIGINT             NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR(32)        NOT NULL DEFAULT 'INLINE',
    payload_data         VARBINARY(MAX),
    payload_ref          VARCHAR(1024),
    message_state        VARCHAR(32)        NOT NULL DEFAULT 'RECEIVED',
    workflow_name        VARCHAR(255),
    created_at           DATETIME2(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           DATETIME2(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error           NVARCHAR(MAX),
    metadata_json        NVARCHAR(MAX),
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length)
) ON ps_monthly (created_at);

CREATE INDEX idx_messages_state  ON messages (message_state) ON ps_monthly (created_at);
CREATE INDEX idx_messages_source ON messages (source)        ON ps_monthly (created_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- message_steps  (partitioned by created_at)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_steps (
    id                  VARCHAR(36)         NOT NULL PRIMARY KEY,
    message_id          VARCHAR(36)         NOT NULL REFERENCES messages(id),
    step_name           VARCHAR(255)        NOT NULL,
    step_state          VARCHAR(32)         NOT NULL DEFAULT 'PENDING',
    attempt_count       INT                 NOT NULL DEFAULT 0,
    max_attempts        INT                 NOT NULL DEFAULT 3,
    next_retry_at       DATETIME2(6),
    locked_until        DATETIME2(6),
    owner               VARCHAR(255),
    last_error          NVARCHAR(MAX),
    result_data         VARBINARY(MAX),
    retry_delay_ms      BIGINT              NOT NULL DEFAULT 1000,
    retry_multiplier    FLOAT               NOT NULL DEFAULT 2.0,
    retry_max_delay_ms  BIGINT              NOT NULL DEFAULT 60000,
    retry_jitter        BIT                 NOT NULL DEFAULT 0,
    created_at          DATETIME2(6)        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME2(6)        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name)
) ON ps_monthly (created_at);

CREATE INDEX idx_steps_message_id ON message_steps (message_id) ON ps_monthly (created_at);

CREATE INDEX idx_steps_eligible ON message_steps (step_state, next_retry_at)
    WHERE step_state IN ('PENDING', 'FAILED_RETRYABLE')
    ON ps_monthly (created_at);

CREATE INDEX idx_steps_expired ON message_steps (locked_until)
    WHERE step_state = 'RUNNING'
    ON ps_monthly (created_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- message_step_dependencies  (leave unpartitioned — much smaller table)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)    NOT NULL,
    step_name            VARCHAR(255)   NOT NULL,
    depends_on_step_name VARCHAR(255)   NOT NULL,
    CONSTRAINT pk_step_deps PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

---

## IBM DB2

DB2 LUW 11.1+ uses `PARTITION BY RANGE` directly in the `CREATE TABLE` statement and
does **not** require the partition key to be part of the primary key.

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- messages  (partitioned by created_at, monthly ranges)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE messages (
    id                   VARCHAR(36)              NOT NULL PRIMARY KEY,
    source               VARCHAR(255)             NOT NULL,
    dedupe_hash          VARCHAR(64)              NOT NULL,
    payload_length       BIGINT                   NOT NULL WITH DEFAULT 0,
    payload_storage_mode VARCHAR(32)              NOT NULL WITH DEFAULT 'INLINE',
    payload_data         BLOB(10M),
    payload_ref          VARCHAR(1024),
    message_state        VARCHAR(32)              NOT NULL WITH DEFAULT 'RECEIVED',
    workflow_name        VARCHAR(255),
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL WITH DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL WITH DEFAULT CURRENT_TIMESTAMP,
    last_error           CLOB(1M),
    metadata_json        CLOB(64K),
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length)
)
PARTITION BY RANGE (created_at)
(
    STARTING FROM ('2024-01-01-00.00.00') ENDING AT ('2025-01-01-00.00.00')
    EVERY 1 MONTH
);

CREATE INDEX idx_messages_state  ON messages (message_state);
CREATE INDEX idx_messages_source ON messages (source);

-- ─────────────────────────────────────────────────────────────────────────────
-- message_steps  (partitioned by created_at, monthly ranges)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_steps (
    id                  VARCHAR(36)              NOT NULL PRIMARY KEY,
    message_id          VARCHAR(36)              NOT NULL REFERENCES messages(id),
    step_name           VARCHAR(255)             NOT NULL,
    step_state          VARCHAR(32)              NOT NULL WITH DEFAULT 'PENDING',
    attempt_count       INTEGER                  NOT NULL WITH DEFAULT 0,
    max_attempts        INTEGER                  NOT NULL WITH DEFAULT 3,
    next_retry_at       TIMESTAMP WITH TIME ZONE,
    locked_until        TIMESTAMP WITH TIME ZONE,
    owner               VARCHAR(255),
    last_error          CLOB(1M),
    result_data         BLOB(10M),
    retry_delay_ms      BIGINT                   NOT NULL WITH DEFAULT 1000,
    retry_multiplier    DOUBLE                   NOT NULL WITH DEFAULT 2.0,
    retry_max_delay_ms  BIGINT                   NOT NULL WITH DEFAULT 60000,
    retry_jitter        SMALLINT                 NOT NULL WITH DEFAULT 0,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL WITH DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL WITH DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name)
)
PARTITION BY RANGE (created_at)
(
    STARTING FROM ('2024-01-01-00.00.00') ENDING AT ('2025-01-01-00.00.00')
    EVERY 1 MONTH
);

CREATE INDEX idx_steps_message_id ON message_steps (message_id);
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at);
CREATE INDEX idx_steps_expired    ON message_steps (locked_until);

-- ─────────────────────────────────────────────────────────────────────────────
-- message_step_dependencies  (leave unpartitioned — much smaller table)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)  NOT NULL,
    step_name            VARCHAR(255) NOT NULL,
    depends_on_step_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
```

To extend the partition range into 2026:

```sql
ALTER TABLE messages
    ADD PARTITION
    STARTING FROM ('2025-01-01-00.00.00') ENDING AT ('2026-01-01-00.00.00')
    EVERY 1 MONTH;
-- Repeat for message_steps
ALTER TABLE message_steps
    ADD PARTITION
    STARTING FROM ('2025-01-01-00.00.00') ENDING AT ('2026-01-01-00.00.00')
    EVERY 1 MONTH;
```

---

## Partition Maintenance

Partitions must be created **before** data arrives in that range. Failing to do so causes
an error on most databases (PostgreSQL, MySQL) or fills a catch-all `p_future` partition
(MySQL) which must later be reorganised.

Automate partition creation with a scheduled job that runs at the start of each month
(or more frequently for daily partitions):

### PostgreSQL — monthly partition creation

```sql
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', NOW() + INTERVAL '1 month');
    end_date   DATE := start_date + INTERVAL '1 month';
    suffix     TEXT := TO_CHAR(start_date, 'YYYYMM');
BEGIN
    EXECUTE FORMAT(
        'CREATE TABLE IF NOT EXISTS messages_y%s PARTITION OF messages
         FOR VALUES FROM (%L) TO (%L)',
        suffix, start_date, end_date
    );
    EXECUTE FORMAT(
        'CREATE TABLE IF NOT EXISTS message_steps_y%s PARTITION OF message_steps
         FOR VALUES FROM (%L) TO (%L)',
        suffix, start_date, end_date
    );
END;
$$;
```

### SQL Server — split the last boundary point

```sql
-- Add next month's boundary before data arrives
ALTER PARTITION FUNCTION pf_monthly() SPLIT RANGE ('2025-02-01');
```

### Archiving / dropping old partitions

```sql
-- PostgreSQL: detach first (makes partition a regular standalone table for archiving),
-- then drop
ALTER TABLE messages      DETACH PARTITION messages_y2023m01;
ALTER TABLE message_steps DETACH PARTITION message_steps_y2023m01;
DROP TABLE messages_y2023m01;
DROP TABLE message_steps_y2023m01;

-- Oracle
ALTER TABLE messages      DROP PARTITION p_initial;
ALTER TABLE message_steps DROP PARTITION p_initial;

-- MySQL
ALTER TABLE messages      DROP PARTITION p202301;
ALTER TABLE message_steps DROP PARTITION p202301;

-- SQL Server: switch out to a staging table, then truncate or drop it
ALTER TABLE messages
    SWITCH PARTITION 1 TO messages_archive PARTITION 1;
TRUNCATE TABLE messages_archive;
```

---

## Migrating Existing Tables

Converting a non-partitioned table to a partitioned one requires a table rebuild. The
safest zero-downtime approach is:

1. Create the new partitioned table alongside the existing one (using a temporary name).
2. Copy existing rows in batches.
3. Cut over by renaming both tables atomically.
4. Drop the old table.

### PostgreSQL example

```sql
-- 1. Create partitioned table under a temporary name
CREATE TABLE messages_partitioned ( LIKE messages INCLUDING ALL )
    PARTITION BY RANGE (created_at);
-- Add PRIMARY KEY and UNIQUE constraints matching your chosen strategy (Option A or B above)

-- Create partitions covering the full historical date range + future months
CREATE TABLE messages_y2023m01 PARTITION OF messages_partitioned
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
-- ... etc. ...

-- 2. Copy data in batches (use a cursor or application-level paging to avoid lock contention)
INSERT INTO messages_partitioned SELECT * FROM messages;

-- 3. Swap tables (requires brief write lock)
BEGIN;
ALTER TABLE messages          RENAME TO messages_old;
ALTER TABLE messages_partitioned RENAME TO messages;
COMMIT;

-- 4. Drop the old table after verifying correctness
DROP TABLE messages_old;
```

For Oracle, SQL Server, and DB2, refer to each vendor's online documentation for
`DBMS_REDEFINITION` (Oracle), online table rebuild features (SQL Server Enterprise), and
`ADMIN_MOVE_TABLE` (DB2).
