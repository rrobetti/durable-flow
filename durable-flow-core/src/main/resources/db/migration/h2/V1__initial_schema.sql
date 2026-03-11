-- Durable Flow initial schema (H2 in PostgreSQL compatibility mode)
-- Uses TIMESTAMP WITH TIME ZONE, BYTEA, and TEXT for compatibility with H2.
-- Plain (non-partial) indexes are used because H2 does not support partial indexes.

CREATE TABLE messages (
    id               VARCHAR(36)   NOT NULL PRIMARY KEY,
    source           VARCHAR(255)  NOT NULL,
    dedupe_hash      VARCHAR(64)   NOT NULL,
    payload_length   BIGINT        NOT NULL DEFAULT 0,
    payload_storage_mode VARCHAR(32) NOT NULL DEFAULT 'INLINE',
    payload_data     BYTEA,
    payload_ref      VARCHAR(1024),
    message_state    VARCHAR(32)   NOT NULL DEFAULT 'RECEIVED',
    workflow_name    VARCHAR(255),
    created_at       TIMESTAMP WITH TIME ZONE   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP WITH TIME ZONE   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error       TEXT,
    metadata_json    TEXT,
    CONSTRAINT uq_messages_dedupe UNIQUE (source, dedupe_hash, payload_length)
);

CREATE INDEX idx_messages_state  ON messages(message_state);
CREATE INDEX idx_messages_source ON messages(source);

CREATE TABLE message_steps (
    id                VARCHAR(36)   NOT NULL PRIMARY KEY,
    message_id        VARCHAR(36)   NOT NULL REFERENCES messages(id),
    step_name         VARCHAR(255)  NOT NULL,
    step_state        VARCHAR(32)   NOT NULL DEFAULT 'PENDING',
    attempt_count     INTEGER       NOT NULL DEFAULT 0,
    max_attempts      INTEGER       NOT NULL DEFAULT 3,
    next_retry_at     TIMESTAMP WITH TIME ZONE,
    locked_until      TIMESTAMP WITH TIME ZONE,
    owner             VARCHAR(255),
    last_error        TEXT,
    result_data       BYTEA,
    retry_delay_ms    BIGINT        NOT NULL DEFAULT 1000,
    retry_multiplier  DOUBLE PRECISION NOT NULL DEFAULT 2.0,
    retry_max_delay_ms BIGINT       NOT NULL DEFAULT 60000,
    retry_jitter      BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMP WITH TIME ZONE   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP WITH TIME ZONE   NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
