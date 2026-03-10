-- Durable Flow initial schema (MySQL 8.0+ / MariaDB 10.6+)
-- Requires FOR UPDATE SKIP LOCKED support (MySQL 8.0+ / MariaDB 10.6+).
-- Set the JDBC connection timezone to UTC: ?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8mb4
-- DATETIME(6) stores with microsecond precision; all values stored as UTC.
-- Partial/filtered indexes are not supported; regular indexes are used.

CREATE TABLE messages (
    id                   VARCHAR(36)    NOT NULL PRIMARY KEY,
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
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_messages_state  ON messages(message_state);
CREATE INDEX idx_messages_source ON messages(source);

CREATE TABLE message_steps (
    id                  VARCHAR(36)     NOT NULL PRIMARY KEY,
    message_id          VARCHAR(36)     NOT NULL,
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
    CONSTRAINT uq_message_steps UNIQUE (message_id, step_name),
    CONSTRAINT fk_steps_message FOREIGN KEY (message_id) REFERENCES messages(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_steps_message_id ON message_steps(message_id);
CREATE INDEX idx_steps_eligible   ON message_steps(step_state, next_retry_at);
CREATE INDEX idx_steps_expired    ON message_steps(locked_until);

CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)    NOT NULL,
    step_name            VARCHAR(255)   NOT NULL,
    depends_on_step_name VARCHAR(255)   NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_deps_message ON message_step_dependencies(message_id);
