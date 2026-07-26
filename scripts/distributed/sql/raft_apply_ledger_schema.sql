-- raft_apply_ledger_schema.sql
-- Database schema for the crash-safe Raft -> BCDB -> PostgreSQL recovery ledger

BEGIN;

CREATE SCHEMA IF NOT EXISTS ariabc_internal;

-- Existing clusters keep their old pg_proc rows across binary upgrades.
-- Register SQL wrappers for the native v8 recovery built-ins so bootstrap/recovery works
-- without requiring a destructive initdb.  On fresh clusters this is an
-- idempotent CREATE OR REPLACE of the catalog-defined functions.
CREATE OR REPLACE FUNCTION pg_catalog.merkle_recovery_status()
RETURNS text
AS 'merkle_recovery_status'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

-- Index-specific verify API (P0.6 fix: multi-Merkle-index verification).
CREATE OR REPLACE FUNCTION pg_catalog.merkle_verify_index(index_oid regclass)
RETURNS boolean
AS 'merkle_verify_index'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

-- Index-specific root hash API.
CREATE OR REPLACE FUNCTION pg_catalog.merkle_root_hash_index(index_oid regclass)
RETURNS text
AS 'merkle_root_hash_index'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

-- Dynamic Merkle inspection/recovery APIs.  Existing clusters acquire these
-- wrappers without initdb; access is revoked below because item summaries
-- contain canonical primary-key material.
CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_verify(index_oid regclass)
RETURNS boolean
AS 'merkle_dynamic_verify'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_get_partition_roots(index_oid regclass)
RETURNS TABLE(partition_id integer, prefix_len integer, prefix bytea,
              tuple_count bigint, data_xor bytea, is_leaf boolean)
AS 'merkle_dynamic_get_partition_roots'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_get_leaf_frontier(index_oid regclass)
RETURNS TABLE(partition_id integer, prefix_len integer, prefix bytea,
              tuple_count bigint, data_xor bytea, is_leaf boolean)
AS 'merkle_dynamic_get_leaf_frontier'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_get_ranges(index_oid regclass,
                                                                 ranges jsonb)
RETURNS TABLE(partition_id integer, prefix_len integer, prefix bytea,
              tuple_count bigint, data_xor bytea, is_leaf boolean)
AS 'merkle_dynamic_get_ranges'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_get_range_items(index_oid regclass,
                                                                      ranges jsonb)
RETURNS TABLE(partition_id integer, prefix_len integer, prefix bytea,
              key_data bytea, key_text text, route_digest bytea,
              tuple_hash bytea)
AS 'merkle_dynamic_get_range_items'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_dynamic_tree_stats(index_oid regclass)
RETURNS jsonb
AS 'merkle_dynamic_tree_stats'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION pg_catalog.merkle_native_partition_roots_at(
    index_oid regclass, sequence_domain smallint, sequence_epoch bigint,
    sequence_value bigint)
RETURNS TABLE(partition_id integer, visible_apply_seq bigint,
              visible_domain smallint, sequence_flags smallint,
              visible_epoch bigint, version_no bigint, creator_xid xid,
              frozen boolean, tuple_count bigint, data_xor bytea,
              structure_hash bytea)
AS 'merkle_native_partition_roots_at'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

-- Bootstrap pg_proc functions cannot change their OUT row type in place on
-- an existing cluster.  Keep roots_at ABI-compatible and expose the item
-- commitment through this versioned helper.
CREATE OR REPLACE FUNCTION pg_catalog.merkle_native_partition_commitments_at(
    index_oid regclass, sequence_domain smallint, sequence_epoch bigint,
    sequence_value bigint)
RETURNS TABLE(partition_id integer, visible_apply_seq bigint,
              visible_domain smallint, sequence_flags smallint,
              visible_epoch bigint, version_no bigint, creator_xid xid,
              frozen boolean, tuple_count bigint, data_xor bytea,
              content_xor bytea, structure_hash bytea)
AS 'merkle_native_partition_roots_at'
LANGUAGE internal VOLATILE PARALLEL UNSAFE;

-- Helper function to retrieve all keys belonging to a leaf given its leaf hash (data_xor hex)
CREATE OR REPLACE FUNCTION pg_catalog.merkle_get_keys_by_leaf_hash(
    p_index regclass,
    p_hash_hex text
)
RETURNS TABLE (
    partition_id int,
    prefix_len int,
    prefix_hex text,
    key_text text,
    route_digest_hex text,
    tuple_hash_hex text,
    leaf_data_xor_hex text
) AS $$
SELECT r.partition_id,
       r.prefix_len::int,
       encode(r.prefix, 'hex') AS prefix_hex,
       items.key_text,
       encode(items.route_digest, 'hex') AS route_digest_hex,
       encode(items.tuple_hash, 'hex') AS tuple_hash_hex,
       encode(r.data_xor, 'hex') AS leaf_data_xor_hex
  FROM merkle_dynamic_get_leaf_frontier(p_index) r
  CROSS JOIN LATERAL merkle_dynamic_get_range_items(
         p_index,
         jsonb_build_array(
           jsonb_build_object(
             'partition_id', r.partition_id,
             'prefix_length', r.prefix_len,
             'prefix_value', encode(r.prefix, 'hex')
           )
         )
       ) items
 WHERE encode(r.data_xor, 'hex') = lower(p_hash_hex)
 ORDER BY items.key_text;
$$ LANGUAGE sql STABLE PARALLEL SAFE;


-- Global ordering for crash-safe Merkle delta application.  Raft positions
-- are epoch-scoped; this counter supplies a database-wide, non-repeating
-- sequence.  Raft manifests reserve a range once per multi-item entry.
CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_apply_counter (
    singleton           boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    next_seq            bigint NOT NULL CHECK (next_seq >= 0),
    terminal_prefix_seq bigint NOT NULL DEFAULT 0 CHECK (terminal_prefix_seq >= 0)
);

-- Upgrade v3 before any v4 statement references the new column.
ALTER TABLE ariabc_internal.merkle_apply_counter
    ADD COLUMN IF NOT EXISTS terminal_prefix_seq
        bigint NOT NULL DEFAULT 0 CHECK (terminal_prefix_seq >= 0);

INSERT INTO ariabc_internal.merkle_apply_counter(singleton, next_seq, terminal_prefix_seq)
VALUES (true, 0, 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_apply_state (
    singleton    boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    applied_seq  bigint NOT NULL CHECK (applied_seq >= 0),
    state        smallint NOT NULL DEFAULT 0 CHECK (state IN (0, 1, 2, 3, 4)),
    error_text   text,
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO ariabc_internal.merkle_apply_state(singleton, applied_seq)
VALUES (true, 0)
ON CONFLICT (singleton) DO NOTHING;

-- WAL-logged dynamic-Merkle state.  A generation is the complete physical
-- RelFileNode identity, so REINDEX cannot attach queued transitions to stale
-- side-table rows even when the catalog OID remains unchanged.
CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_dynamic_state (
    index_oid           oid NOT NULL,
    rnode_spc           oid NOT NULL,
    rnode_db            oid NOT NULL,
    rnode_rel           oid NOT NULL,
    heap_oid            oid NOT NULL,
    partitions          integer NOT NULL CHECK (partitions > 0),
    logical_fanout      integer NOT NULL CHECK (
        logical_fanout BETWEEN 2 AND 32 AND
        (logical_fanout & (logical_fanout - 1)) = 0
    ),
    leaf_capacity       integer NOT NULL CHECK (leaf_capacity > 0),
    merge_threshold     integer NOT NULL CHECK (merge_threshold >= 0 AND merge_threshold < leaf_capacity),
    leaf_byte_capacity  integer NOT NULL CHECK (leaf_byte_capacity > 0),
    max_key_bytes       integer NOT NULL CHECK (max_key_bytes > 0 AND max_key_bytes <= 2000),
    build_complete      boolean NOT NULL DEFAULT false,
    applied_seq         bigint NOT NULL DEFAULT 0 CHECK (applied_seq >= 0),
    seen_pruned_seq     bigint NOT NULL DEFAULT 0 CHECK (seen_pruned_seq >= 0),
    item_count          bigint NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    item_bytes          bigint NOT NULL DEFAULT 0 CHECK (item_bytes >= 0),
    node_count          bigint NOT NULL DEFAULT 0 CHECK (node_count >= 0),
    leaf_count          bigint NOT NULL DEFAULT 0 CHECK (leaf_count >= 0),
    max_depth           integer NOT NULL DEFAULT 0 CHECK (max_depth BETWEEN 0 AND 256),
    max_leaf_items      integer NOT NULL DEFAULT 0 CHECK (max_leaf_items >= 0),
    split_count         bigint NOT NULL DEFAULT 0 CHECK (split_count >= 0),
    merge_count         bigint NOT NULL DEFAULT 0 CHECK (merge_count >= 0),
    structure_failures  bigint NOT NULL DEFAULT 0 CHECK (structure_failures >= 0),
    stats_dirty         boolean NOT NULL DEFAULT false,
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (index_oid, rnode_spc, rnode_db, rnode_rel)
);

ALTER TABLE ariabc_internal.merkle_dynamic_state
    ADD COLUMN IF NOT EXISTS seen_pruned_seq bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stats_dirty boolean NOT NULL DEFAULT false;

-- Upgrade the former fanout=32-only contract in existing installations.
ALTER TABLE ariabc_internal.merkle_dynamic_state
    DROP CONSTRAINT IF EXISTS merkle_dynamic_state_logical_fanout_check;
ALTER TABLE ariabc_internal.merkle_dynamic_state
    ADD CONSTRAINT merkle_dynamic_state_logical_fanout_check CHECK (
        logical_fanout BETWEEN 2 AND 32 AND
        (logical_fanout & (logical_fanout - 1)) = 0
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ariabc_internal.merkle_dynamic_state'::regclass
          AND conname = 'merkle_dynamic_state_seen_pruned_seq_check'
    ) THEN
        ALTER TABLE ariabc_internal.merkle_dynamic_state
            ADD CONSTRAINT merkle_dynamic_state_seen_pruned_seq_check
            CHECK (seen_pruned_seq >= 0);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_dynamic_node (
    index_oid       oid NOT NULL,
    rnode_spc       oid NOT NULL,
    rnode_db        oid NOT NULL,
    rnode_rel       oid NOT NULL,
    partition_id    integer NOT NULL CHECK (partition_id >= 0),
    prefix_len      smallint NOT NULL CHECK (prefix_len BETWEEN 0 AND 256),
    prefix_bytes    bytea NOT NULL CHECK (octet_length(prefix_bytes) = 32),
    is_leaf         boolean NOT NULL,
    tuple_count     bigint NOT NULL CHECK (tuple_count >= 0),
    subtree_bytes   bigint NOT NULL CHECK (subtree_bytes >= 0),
    data_xor        bytea NOT NULL CHECK (octet_length(data_xor) = 32),
    structure_hash  bytea NOT NULL CHECK (octet_length(structure_hash) = 32),
    last_seq        bigint NOT NULL CHECK (last_seq >= 0),
    PRIMARY KEY (index_oid, rnode_spc, rnode_db, rnode_rel,
                 partition_id, prefix_len, prefix_bytes)
);

-- Node summaries are mutable.  Reserving one HOT-update slot per build tuple
-- prevents the first sparse recovery after a bulk build from relocating every
-- touched ancestor and maintaining both identity indexes.
ALTER TABLE ariabc_internal.merkle_dynamic_node SET (fillfactor = 50);

CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_dynamic_leaf_item (
    index_oid       oid NOT NULL,
    rnode_spc       oid NOT NULL,
    rnode_db        oid NOT NULL,
    rnode_rel       oid NOT NULL,
    partition_id    integer NOT NULL CHECK (partition_id >= 0),
    prefix_len      smallint NOT NULL CHECK (prefix_len BETWEEN 0 AND 256),
    prefix_bytes    bytea NOT NULL CHECK (octet_length(prefix_bytes) = 32),
    key_data        bytea NOT NULL CHECK (octet_length(key_data) <= 2000),
    route_digest    bytea NOT NULL CHECK (octet_length(route_digest) = 32),
    tuple_hash      bytea NOT NULL CHECK (octet_length(tuple_hash) = 32),
    last_seq        bigint NOT NULL CHECK (last_seq >= 0),
    PRIMARY KEY (index_oid, rnode_spc, rnode_db, rnode_rel, key_data)
);

ALTER TABLE ariabc_internal.merkle_dynamic_leaf_item SET (fillfactor = 90);

CREATE INDEX IF NOT EXISTS merkle_dynamic_node_prefix_lookup_idx
    ON ariabc_internal.merkle_dynamic_node
       (index_oid, rnode_spc, rnode_db, rnode_rel,
        partition_id, prefix_bytes, prefix_len);

CREATE INDEX IF NOT EXISTS merkle_dynamic_leaf_lookup_idx
    ON ariabc_internal.merkle_dynamic_leaf_item
       (index_oid, rnode_spc, rnode_db, rnode_rel,
        partition_id, prefix_len, prefix_bytes, route_digest);

CREATE INDEX IF NOT EXISTS merkle_dynamic_route_lookup_idx
    ON ariabc_internal.merkle_dynamic_leaf_item
       (index_oid, rnode_spc, rnode_db, rnode_rel,
        partition_id, route_digest, key_data);

-- CREATE INDEX executes its access-method build callback in a security-
-- restricted context, where creating a per-build temporary table is forbidden.
-- This shared UNLOGGED relation is therefore the bounded build spool.  Every
-- row is generation-keyed, transactionally removed at build completion, and
-- never participates in recovery reads or durable tree state.
CREATE UNLOGGED TABLE IF NOT EXISTS ariabc_internal.merkle_dynamic_build_stage (
    index_oid       oid NOT NULL,
    rnode_spc       oid NOT NULL,
    rnode_db        oid NOT NULL,
    rnode_rel       oid NOT NULL,
    partition_id    integer NOT NULL CHECK (partition_id >= 0),
    key_data        bytea NOT NULL CHECK (octet_length(key_data) <= 2000),
    route_digest    bytea NOT NULL CHECK (octet_length(route_digest) = 32),
    tuple_hash      bytea NOT NULL CHECK (octet_length(tuple_hash) = 32)
);

CREATE INDEX IF NOT EXISTS merkle_dynamic_build_stage_route_idx
    ON ariabc_internal.merkle_dynamic_build_stage
       (index_oid, rnode_spc, rnode_db, rnode_rel,
        partition_id, route_digest, key_data);

CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_dynamic_seen (
    index_oid       oid NOT NULL,
    rnode_spc       oid NOT NULL,
    rnode_db        oid NOT NULL,
    rnode_rel       oid NOT NULL,
    apply_seq       bigint NOT NULL CHECK (apply_seq > 0),
    key_data        bytea NOT NULL CHECK (octet_length(key_data) <= 2000),
    PRIMARY KEY (index_oid, rnode_spc, rnode_db, rnode_rel,
                 apply_seq, key_data)
);

-- Generation existence is checked by every backend mutation/API path.  Use
-- one parent-row trigger for set-based cleanup rather than invoking a foreign-
-- key constraint trigger for each node/item mutation.  This preserves atomic
-- cascade semantics and keeps lifecycle cleanup independent of item count.
DO $$
DECLARE
    constraint_row record;
BEGIN
    FOR constraint_row IN
        SELECT namespace.nspname, child.relname, constraint_entry.conname
          FROM pg_constraint AS constraint_entry
          JOIN pg_class AS child
            ON child.oid = constraint_entry.conrelid
          JOIN pg_namespace AS namespace
            ON namespace.oid = child.relnamespace
         WHERE constraint_entry.contype = 'f'
           AND constraint_entry.confrelid =
               'ariabc_internal.merkle_dynamic_state'::regclass
           AND constraint_entry.conrelid IN (
               'ariabc_internal.merkle_dynamic_node'::regclass,
               'ariabc_internal.merkle_dynamic_leaf_item'::regclass,
               'ariabc_internal.merkle_dynamic_build_stage'::regclass,
               'ariabc_internal.merkle_dynamic_seen'::regclass
           )
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I DROP CONSTRAINT %I',
            constraint_row.nspname,
            constraint_row.relname,
            constraint_row.conname
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION ariabc_internal.merkle_dynamic_state_cascade()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, ariabc_internal
AS $$
BEGIN
    DELETE FROM ariabc_internal.merkle_dynamic_build_stage
     WHERE index_oid = OLD.index_oid
       AND rnode_spc = OLD.rnode_spc
       AND rnode_db = OLD.rnode_db
       AND rnode_rel = OLD.rnode_rel;
    DELETE FROM ariabc_internal.merkle_dynamic_seen
     WHERE index_oid = OLD.index_oid
       AND rnode_spc = OLD.rnode_spc
       AND rnode_db = OLD.rnode_db
       AND rnode_rel = OLD.rnode_rel;
    DELETE FROM ariabc_internal.merkle_dynamic_leaf_item
     WHERE index_oid = OLD.index_oid
       AND rnode_spc = OLD.rnode_spc
       AND rnode_db = OLD.rnode_db
       AND rnode_rel = OLD.rnode_rel;
    DELETE FROM ariabc_internal.merkle_dynamic_node
     WHERE index_oid = OLD.index_oid
       AND rnode_spc = OLD.rnode_spc
       AND rnode_db = OLD.rnode_db
       AND rnode_rel = OLD.rnode_rel;
    RETURN OLD;
END;
$$;

REVOKE ALL ON FUNCTION ariabc_internal.merkle_dynamic_state_cascade()
FROM PUBLIC;

DROP TRIGGER IF EXISTS merkle_dynamic_state_cascade_before_delete
ON ariabc_internal.merkle_dynamic_state;

CREATE TRIGGER merkle_dynamic_state_cascade_before_delete
BEFORE DELETE ON ariabc_internal.merkle_dynamic_state
FOR EACH ROW
EXECUTE FUNCTION ariabc_internal.merkle_dynamic_state_cascade();

-- Direct PostgreSQL transactions (outside the Raft safe-ledger path) store
-- one compact transaction batch here.  The row and heap DML commit atomically.
CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_local_delta (
    apply_seq      bigint PRIMARY KEY CHECK (apply_seq > 0),
    delta_version  integer NOT NULL,
    delta_blob     bytea,
    committed_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT merkle_local_delta_payload_check CHECK (
        (delta_version = 0 AND delta_blob IS NULL) OR
        (delta_version IN (1, 2) AND delta_blob IS NOT NULL)
    )
);

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
    merkle_apply_seq_base bigint NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (epoch_id, raft_log_index),

    CHECK (octet_length(epoch_id) = 32),
    CHECK (octet_length(entry_digest) = 32),
    CHECK (raft_log_index > 0),
    CHECK (expected_items > 0),
    CHECK (merkle_apply_seq_base > 0)
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

    merkle_apply_seq       bigint NOT NULL,
    merkle_delta_version   integer NOT NULL DEFAULT 0,
    merkle_delta_blob      bytea,

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
        (merkle_delta_version = 0 AND merkle_delta_blob IS NULL)
        OR
        (merkle_delta_version IN (1, 2) AND merkle_delta_blob IS NOT NULL)
    ),
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
           ariabc_internal.raft_apply_entry,
           ariabc_internal.raft_apply_item,
           ariabc_internal.merkle_apply_counter,
           ariabc_internal.merkle_apply_state,
           ariabc_internal.merkle_local_delta,
           ariabc_internal.merkle_dynamic_state,
           ariabc_internal.merkle_dynamic_node,
           ariabc_internal.merkle_dynamic_leaf_item,
           ariabc_internal.merkle_dynamic_build_stage,
           ariabc_internal.merkle_dynamic_seen
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE ariabc_internal.merkle_apply_state
    DROP CONSTRAINT IF EXISTS merkle_apply_state_state_check;
ALTER TABLE ariabc_internal.merkle_apply_state
    ADD CONSTRAINT merkle_apply_state_state_check CHECK (state IN (0, 1, 2, 3, 4));

ALTER TABLE ariabc_internal.merkle_local_delta
    ALTER COLUMN delta_blob DROP NOT NULL;
ALTER TABLE ariabc_internal.merkle_local_delta
    DROP CONSTRAINT IF EXISTS merkle_local_delta_delta_version_check;
ALTER TABLE ariabc_internal.merkle_local_delta
    DROP CONSTRAINT IF EXISTS merkle_local_delta_payload_check;
ALTER TABLE ariabc_internal.merkle_local_delta
    ADD CONSTRAINT merkle_local_delta_payload_check CHECK (
        (delta_version = 0 AND delta_blob IS NULL) OR
        (delta_version IN (1, 2) AND delta_blob IS NOT NULL)
    );

ALTER TABLE ariabc_internal.raft_apply_entry
    ADD COLUMN IF NOT EXISTS merkle_apply_seq_base bigint;

ALTER TABLE ariabc_internal.raft_apply_item
    ADD COLUMN IF NOT EXISTS failure_digest bytea,
    ADD COLUMN IF NOT EXISTS failure_sqlstate char(5),
    ADD COLUMN IF NOT EXISTS failure_class text,
    ADD COLUMN IF NOT EXISTS failure_retryable boolean,
    ADD COLUMN IF NOT EXISTS failure_format_version integer,
    ADD COLUMN IF NOT EXISTS failure_recorded_at timestamptz,
    ADD COLUMN IF NOT EXISTS merkle_apply_seq bigint,
    ADD COLUMN IF NOT EXISTS merkle_delta_version integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS merkle_delta_blob bytea;

-- Existing v2 manifests predate Merkle deltas.  Give them deterministic
-- sequence ranges and initialize the durable watermark past them: their
-- delta columns are necessarily empty, and existing v6 indexes must REINDEX
-- before this server accepts them as v7.
DO $$
DECLARE
    next_value bigint;
    r record;
BEGIN
    SELECT next_seq INTO next_value
      FROM ariabc_internal.merkle_apply_counter
     WHERE singleton
     FOR UPDATE;

    IF EXISTS (
        SELECT 1
          FROM ariabc_internal.raft_apply_entry e
          LEFT JOIN LATERAL (
              SELECT count(*) AS terminal_items
                FROM ariabc_internal.raft_apply_item i
               WHERE i.epoch_id = e.epoch_id
                 AND i.raft_log_index = e.raft_log_index
                 AND i.state IN (2, 3, 4)
          ) terminal ON true
         WHERE e.merkle_apply_seq_base IS NULL
           AND terminal.terminal_items <> e.expected_items
    ) THEN
        RAISE EXCEPTION 'cannot upgrade Merkle durability with incomplete legacy manifests'
            USING HINT = 'Complete Raft replay using the old binary, then retry the migration.';
    END IF;

    FOR r IN
        SELECT epoch_id, raft_log_index, expected_items
          FROM ariabc_internal.raft_apply_entry
         WHERE merkle_apply_seq_base IS NULL
         ORDER BY epoch_id, raft_log_index
    LOOP
        UPDATE ariabc_internal.raft_apply_entry
           SET merkle_apply_seq_base = next_value + 1
         WHERE epoch_id = r.epoch_id
           AND raft_log_index = r.raft_log_index;
        next_value := next_value + r.expected_items;
    END LOOP;

    UPDATE ariabc_internal.merkle_apply_counter
       SET next_seq = next_value
     WHERE singleton;

    IF EXISTS (
        SELECT 1 FROM ariabc_internal.raft_apply_entry
         WHERE merkle_apply_seq_base IS NULL
    ) THEN
        RAISE EXCEPTION 'failed to assign Merkle apply sequence to every existing Raft manifest';
    END IF;
END $$;

-- Materialize the globally ordered item position once.  The applier and
-- freshness path must not repeatedly derive it by joining the full manifest
-- history.  Existing v2/v3 rows are backfilled from their immutable entry
-- range and then made non-null/unique.
UPDATE ariabc_internal.raft_apply_item a
   SET merkle_apply_seq = e.merkle_apply_seq_base + i.item_ordinal::bigint
  FROM ariabc_internal.raft_apply_entry e
  JOIN ariabc_internal.raft_apply_entry_item i
    ON i.epoch_id = e.epoch_id
   AND i.raft_log_index = e.raft_log_index
 WHERE a.epoch_id = i.epoch_id
   AND a.raft_log_index = i.raft_log_index
   AND a.item_ordinal = i.item_ordinal
   AND a.merkle_apply_seq IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ariabc_internal.raft_apply_item
         WHERE merkle_apply_seq IS NULL
    ) THEN
        RAISE EXCEPTION 'cannot assign Merkle apply sequence to an existing ledger item';
    END IF;
END $$;

ALTER TABLE ariabc_internal.raft_apply_item
    ALTER COLUMN merkle_apply_seq SET NOT NULL;

ALTER TABLE ariabc_internal.raft_apply_item
    DROP CONSTRAINT IF EXISTS raft_apply_item_merkle_apply_seq_check;

ALTER TABLE ariabc_internal.raft_apply_item
    ADD CONSTRAINT raft_apply_item_merkle_apply_seq_check
    CHECK (merkle_apply_seq > 0);

CREATE UNIQUE INDEX IF NOT EXISTS raft_apply_item_merkle_apply_seq_uq
    ON ariabc_internal.raft_apply_item(merkle_apply_seq);

CREATE INDEX IF NOT EXISTS raft_apply_item_terminal_seq_idx
    ON ariabc_internal.raft_apply_item(merkle_apply_seq)
    WHERE state IN (2, 3, 4);

ALTER TABLE ariabc_internal.raft_apply_entry
    ALTER COLUMN merkle_apply_seq_base SET NOT NULL;

ALTER TABLE ariabc_internal.raft_apply_entry
    DROP CONSTRAINT IF EXISTS raft_apply_entry_merkle_apply_seq_base_check;

ALTER TABLE ariabc_internal.raft_apply_entry
    ADD CONSTRAINT raft_apply_entry_merkle_apply_seq_base_check
    CHECK (merkle_apply_seq_base > 0);

CREATE UNIQUE INDEX IF NOT EXISTS raft_apply_entry_merkle_seq_base_uq
    ON ariabc_internal.raft_apply_entry(merkle_apply_seq_base);

ALTER TABLE ariabc_internal.raft_apply_item
    DROP CONSTRAINT IF EXISTS raft_apply_item_merkle_delta_contract;

ALTER TABLE ariabc_internal.raft_apply_item
    ADD CONSTRAINT raft_apply_item_merkle_delta_contract CHECK (
        (merkle_delta_version = 0 AND merkle_delta_blob IS NULL)
        OR
        (merkle_delta_version IN (1, 2) AND merkle_delta_blob IS NOT NULL)
    );

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
        VALUES (5);
    ELSIF current_version IN (1, 2) THEN
        UPDATE ariabc_internal.raft_apply_schema_meta
           SET schema_version = 5
         WHERE schema_version = current_version;
        UPDATE ariabc_internal.merkle_apply_state
           SET applied_seq = (SELECT next_seq
                                FROM ariabc_internal.merkle_apply_counter
                               WHERE singleton),
               state = 2,
               error_text = 'schema upgrade requires v7 Merkle index rebuild',
               updated_at = clock_timestamp()
         WHERE singleton;
        -- P0.3 fix for v1/v2: these predate Merkle deltas so all positions
        -- up to next_seq are vacuously terminal.  Set terminal_prefix_seq =
        -- next_seq so the applied_seq <= target_seq invariant holds.
        UPDATE ariabc_internal.merkle_apply_counter
           SET terminal_prefix_seq = next_seq
         WHERE singleton;
    ELSIF current_version = 3 THEN
        -- P0.3 fix for v3: compute a correct contiguous prefix.  Start from
        -- applied_seq (which the v3 schema maintained correctly) and advance
        -- one step at a time across both queues, stopping at the first gap.
        -- Do NOT use MAX() + absence-of-nonterminal as proof of contiguity.
        DECLARE
            prefix_val bigint;
            next_pos   bigint;
            found_row  boolean;
        BEGIN
            SELECT applied_seq INTO prefix_val
              FROM ariabc_internal.merkle_apply_state
             WHERE singleton;

            LOOP
                next_pos := prefix_val + 1;
                SELECT EXISTS (
                    SELECT 1 FROM ariabc_internal.raft_apply_item
                     WHERE merkle_apply_seq = next_pos
                       AND state IN (2, 3, 4)
                    UNION ALL
                    SELECT 1 FROM ariabc_internal.merkle_local_delta
                     WHERE apply_seq = next_pos
                ) INTO found_row;
                EXIT WHEN NOT found_row;
                prefix_val := next_pos;
            END LOOP;

            UPDATE ariabc_internal.merkle_apply_counter
               SET terminal_prefix_seq = prefix_val
             WHERE singleton;
        END;
        UPDATE ariabc_internal.raft_apply_schema_meta
           SET schema_version = 5
         WHERE schema_version = 3;
    ELSIF current_version = 4 THEN
        UPDATE ariabc_internal.raft_apply_schema_meta
           SET schema_version = 5
         WHERE schema_version = 4;
    ELSIF current_version = 5 THEN
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

REVOKE ALL ON ariabc_internal.merkle_apply_counter,
                    ariabc_internal.merkle_apply_state,
                    ariabc_internal.merkle_local_delta,
                    ariabc_internal.merkle_dynamic_state,
                    ariabc_internal.merkle_dynamic_node,
                    ariabc_internal.merkle_dynamic_leaf_item,
                    ariabc_internal.merkle_dynamic_build_stage,
                    ariabc_internal.merkle_dynamic_seen
FROM PUBLIC;
-- Status and freshness checks are safe to expose read-only.  Mutation paths
-- execute inside trusted backend code and the applier functions remain
-- superuser-only, so ordinary users cannot forge sequence or delta rows.
GRANT SELECT ON ariabc_internal.merkle_apply_counter,
                    ariabc_internal.merkle_apply_state
TO PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_verify(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_verify_index(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_verify(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_get_partition_roots(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_get_ranges(regclass, jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_get_range_items(regclass, jsonb) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_get_leaf_frontier(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_dynamic_tree_stats(regclass) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_native_partition_roots_at(regclass, smallint, bigint, bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_catalog.merkle_native_partition_commitments_at(regclass, smallint, bigint, bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.merkle_recovery_status() TO PUBLIC;
