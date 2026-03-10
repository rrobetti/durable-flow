-- Durable Flow initial schema (IBM DB2 LUW 11.1+)
-- Uses DB2 DDL syntax: NOT NULL WITH DEFAULT for column defaults.
-- TIMESTAMP WITH TIME ZONE requires DB2 9.7+.
-- BOOLEAN requires DB2 11.1+; SMALLINT is used here for broader compatibility.
-- SKIP LOCKED DATA requires DB2 11.1+.
-- Partial/filtered indexes are not supported; regular indexes are used.

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
);

CREATE INDEX idx_messages_state  ON messages(message_state);
CREATE INDEX idx_messages_source ON messages(source);

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
);

CREATE INDEX idx_steps_message_id ON message_steps(message_id);
CREATE INDEX idx_steps_eligible   ON message_steps(step_state, next_retry_at);
CREATE INDEX idx_steps_expired    ON message_steps(locked_until);

CREATE TABLE message_step_dependencies (
    message_id           VARCHAR(36)  NOT NULL,
    step_name            VARCHAR(255) NOT NULL,
    depends_on_step_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies(message_id);
