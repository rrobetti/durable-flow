-- Durable Flow initial schema (Oracle Database 12c Release 2+)
-- VARCHAR2, BLOB, CLOB, TIMESTAMP WITH TIME ZONE; no BOOLEAN (NUMBER(1,0) used instead).
-- Partial indexes are not supported; regular indexes are created instead.

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
);

CREATE INDEX idx_messages_state  ON messages (message_state);
CREATE INDEX idx_messages_source ON messages (source);

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
);

CREATE INDEX idx_steps_message_id ON message_steps (message_id);
CREATE INDEX idx_steps_eligible   ON message_steps (step_state, next_retry_at);
CREATE INDEX idx_steps_expired    ON message_steps (locked_until);

CREATE TABLE message_step_dependencies (
    message_id           VARCHAR2(36)  NOT NULL,
    step_name            VARCHAR2(255) NOT NULL,
    depends_on_step_name VARCHAR2(255) NOT NULL,
    CONSTRAINT pk_step_deps PRIMARY KEY (message_id, step_name, depends_on_step_name)
);

CREATE INDEX idx_deps_message ON message_step_dependencies (message_id);
