-- raft_apply_ledger_schema.sql
-- Database schema for the crash-safe Raft -> BCDB -> PostgreSQL recovery ledger

CREATE SCHEMA IF NOT EXISTS ariabc_internal;

-- Schema version tracking metadata
CREATE TABLE IF NOT EXISTS ariabc_internal.raft_apply_schema_meta (
    schema_version integer PRIMARY KEY
);

-- Initialize version if empty
INSERT INTO ariabc_internal.raft_apply_schema_meta (schema_version)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM ariabc_internal.raft_apply_schema_meta);

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

    PRIMARY KEY (epoch_id, raft_log_index, item_ordinal),

    CHECK (octet_length(epoch_id) = 32),
    CHECK (octet_length(entry_digest) = 32),
    CHECK (octet_length(item_digest) = 32),
    CHECK (
        terminal_digest IS NULL
        OR octet_length(terminal_digest) = 32
    ),
    CHECK (item_ordinal >= 0),
    CHECK (state IN (1, 2, 3)),
    CHECK (
        state = 1
        OR
        (state = 2
         AND result_format_version IS NOT NULL
         AND result_payload IS NOT NULL
         AND terminal_digest IS NOT NULL)
        OR
        (state = 3
         AND error_format_version IS NOT NULL
         AND sqlstate_code ~ '^[0-9A-Z]{5}$'
         AND error_payload IS NOT NULL
         AND terminal_digest IS NOT NULL)
    )
);

-- A deferred row trigger is not used for the CLAIMED invariant.
-- The worker asserts terminal state directly before its top-level commit.

DROP TRIGGER IF EXISTS enforce_no_claimed_on_commit
ON ariabc_internal.raft_apply_item;

DROP FUNCTION IF EXISTS ariabc_internal.check_claimed_rows_trigger();
