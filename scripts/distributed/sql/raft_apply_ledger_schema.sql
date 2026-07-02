-- raft_apply_ledger_schema.sql
-- Database schema for the crash-safe Raft -> BCDB -> PostgreSQL recovery ledger

BEGIN;

CREATE SCHEMA IF NOT EXISTS ariabc_internal;

-- Schema version tracking metadata
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_schema_meta (
    schema_version integer PRIMARY KEY
);

-- Epoch registry table
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_epoch (
    epoch_id           bytea PRIMARY KEY,
    epoch_label        text NOT NULL,
    protocol_version   integer NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),

    CHECK (octet_length(epoch_id) = 32)
);

-- Raft application-level entry log/manifest table
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_entry (
    epoch_id           bytea NOT NULL,
    raft_log_index     bigint NOT NULL,
    entry_digest       bytea NOT NULL,
    expected_items     integer NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (epoch_id, raft_log_index),

    CHECK (octet_length(epoch_id) = 32),
    CHECK (octet_length(entry_digest) = 32),
    CHECK (raft_log_index > 0),
    CHECK (expected_items > 0)
);

-- Immutable manifest of each item contained within a Raft entry
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_entry_item (
    epoch_id           bytea NOT NULL,
    raft_log_index     bigint NOT NULL,
    item_ordinal       integer NOT NULL,
    item_digest        bytea NOT NULL,

    PRIMARY KEY (epoch_id, raft_log_index, item_ordinal),
    FOREIGN KEY (epoch_id, raft_log_index) REFERENCES ariabc_internal.raft_apply_entry(epoch_id, raft_log_index),

    CHECK (octet_length(epoch_id) = 32),
    CHECK (octet_length(item_digest) = 32),
    CHECK (item_ordinal >= 0)
);

-- Individual enqueued, claimed, or finalized item table
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_item (
    epoch_id               bytea NOT NULL,
    raft_log_index         bigint NOT NULL,
    item_ordinal           integer NOT NULL,

    entry_digest           bytea NOT NULL,
    item_digest            bytea NOT NULL,

    state                  smallint NOT NULL,

    result_format_version  integer,
    result_payload         bytea,

    error_format_version   integer,
    sqlstate_code          text,
    error_payload          bytea,

    terminal_digest        bytea,
    committed_at           timestamptz,

    failure_digest         bytea,
    failure_sqlstate       char(5),
    failure_class          text,
    failure_retryable      boolean,
    failure_format_version integer,
    failure_recorded_at    timestamptz,

    PRIMARY KEY (epoch_id, raft_log_index, item_ordinal),

    CHECK (octet_length(epoch_id) = 32),
    CHECK (octet_length(entry_digest) = 32),
    CHECK (octet_length(item_digest) = 32),
    CHECK (
        terminal_digest IS NULL
        OR octet_length(terminal_digest) = 32
    ),
    CHECK (item_ordinal >= 0),
    CHECK (
        failure_digest IS NULL
        OR octet_length(failure_digest) = 32
    ),
    CHECK (state IN (1, 2, 3, 4)),
    CHECK (
        state = 1
        OR
        (state = 2
         AND result_format_version IS NOT NULL
         AND result_payload IS NOT NULL
         AND terminal_digest IS NOT NULL
         AND failure_digest IS NULL
         AND failure_sqlstate IS NULL
         AND failure_class IS NULL
         AND failure_retryable IS NULL
         AND failure_format_version IS NULL
         AND failure_recorded_at IS NULL)
        OR
        (state = 3
         AND error_format_version IS NOT NULL
         AND sqlstate_code ~ '^[0-9A-Z]{5}$'
         AND error_payload IS NOT NULL
         AND terminal_digest IS NOT NULL
         AND failure_digest IS NULL
         AND failure_sqlstate IS NULL
         AND failure_class IS NULL
         AND failure_retryable IS NULL
         AND failure_format_version IS NULL
         AND failure_recorded_at IS NULL)
        OR
        (state = 4
         AND failure_digest IS NOT NULL
         AND octet_length(failure_digest) = 32
         AND failure_sqlstate ~ '^[0-9A-Z]{5}$'
         AND failure_class IS NOT NULL
         AND failure_retryable IS NOT NULL
         AND failure_format_version = 1
         AND failure_recorded_at IS NOT NULL
         AND sqlstate_code IS NULL
         AND terminal_digest IS NULL
         AND result_payload IS NULL
         AND error_payload IS NULL
         AND result_format_version IS NULL
         AND error_format_version IS NULL
         AND committed_at IS NULL)
    )
);

LOCK TABLE ariabc_internal.raft_apply_schema_meta,
           ariabc_internal.raft_apply_item
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE ariabc_internal.raft_apply_item
    ADD COLUMN IF NOT EXISTS failure_digest bytea,
    ADD COLUMN IF NOT EXISTS failure_sqlstate char(5),
    ADD COLUMN IF NOT EXISTS failure_class text,
    ADD COLUMN IF NOT EXISTS failure_retryable boolean,
    ADD COLUMN IF NOT EXISTS failure_format_version integer,
    ADD COLUMN IF NOT EXISTS failure_recorded_at timestamptz;

ALTER TABLE ariabc_internal.raft_apply_item
    DROP CONSTRAINT IF EXISTS raft_apply_item_state_check,
    DROP CONSTRAINT IF EXISTS raft_apply_item_state_contract;

ALTER TABLE ariabc_internal.raft_apply_item
    ADD CONSTRAINT raft_apply_item_state_contract CHECK (
        state IN (1, 2, 3, 4)
        AND
        (
            state = 1
            OR
            (state = 2
             AND result_format_version IS NOT NULL
             AND result_payload IS NOT NULL
             AND terminal_digest IS NOT NULL
             AND failure_digest IS NULL
             AND failure_sqlstate IS NULL
             AND failure_class IS NULL
             AND failure_retryable IS NULL
             AND failure_format_version IS NULL
             AND failure_recorded_at IS NULL)
            OR
            (state = 3
             AND error_format_version IS NOT NULL
             AND sqlstate_code ~ '^[0-9A-Z]{5}$'
             AND error_payload IS NOT NULL
             AND terminal_digest IS NOT NULL
             AND failure_digest IS NULL
             AND failure_sqlstate IS NULL
             AND failure_class IS NULL
             AND failure_retryable IS NULL
             AND failure_format_version IS NULL
             AND failure_recorded_at IS NULL)
            OR
            (state = 4
             AND failure_digest IS NOT NULL
             AND octet_length(failure_digest) = 32
             AND failure_sqlstate ~ '^[0-9A-Z]{5}$'
             AND failure_class IS NOT NULL
             AND failure_retryable IS NOT NULL
             AND failure_format_version = 1
             AND failure_recorded_at IS NOT NULL
             AND sqlstate_code IS NULL
             AND terminal_digest IS NULL
             AND result_payload IS NULL
             AND error_payload IS NULL
             AND result_format_version IS NULL
            AND error_format_version IS NULL
            AND committed_at IS NULL)
    )
);

DO $$
DECLARE
    current_version integer;
BEGIN
    SELECT schema_version
      INTO current_version
      FROM ariabc_internal.raft_apply_schema_meta
     FOR UPDATE;

    IF NOT FOUND THEN
        INSERT INTO ariabc_internal.raft_apply_schema_meta (schema_version)
        VALUES (2);
    ELSIF current_version = 1 THEN
        UPDATE ariabc_internal.raft_apply_schema_meta
           SET schema_version = 2
         WHERE schema_version = 1;
    ELSIF current_version = 2 THEN
        NULL;
    ELSE
        RAISE EXCEPTION 'unsupported raft_apply schema_version %', current_version;
    END IF;
END $$;

COMMIT;

-- A deferred row trigger is not used for the CLAIMED invariant.
-- The worker asserts terminal state directly before its top-level commit.

DROP TRIGGER IF EXISTS enforce_no_claimed_on_commit
ON ariabc_internal.raft_apply_item;

DROP FUNCTION IF EXISTS ariabc_internal.check_claimed_rows_trigger();
