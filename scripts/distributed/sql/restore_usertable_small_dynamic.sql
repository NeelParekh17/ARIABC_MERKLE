-- Compatibility entry point. Keep the 12k-row dataset in one canonical file.
-- Suppress the obsolete static Merkle index: the dynamic setup below is the
-- sole authoritative index for this acceptance workload.
\set bench_enable_merkle 0
-- This entry point is guarded by ariabc.allow_destructive_benchmark_reset in
-- create_usertable_small_dynamic_index.sql.  Drop stale/corrupt benchmark
-- state directly instead of asking that old index to drain successfully.
\set bench_skip_pending_drain 1

-- An interrupted benchmark index may be too corrupt for the DDL
-- guard to inspect, and PostgreSQL intentionally refuses to DROP it in that
-- state.  REINDEX is the supported recovery operation and restores enough
-- metadata for the canonical DROP TABLE below to proceed safely.
SELECT to_regclass('public.usertable_small_dynamic_merkle_idx') IS NOT NULL
       AS stale_dynamic_index_exists
\gset
\if :stale_dynamic_index_exists
REINDEX INDEX public.usertable_small_dynamic_merkle_idx;
\endif

\ir ../../restore_usertable_small.sql

-- An interrupted build can leave WAL-logged dynamic generations behind even
-- after the benchmark index has been dropped.  This restore is an explicit
-- destructive reset, so remove those orphan generations before recreating the
-- authoritative index.  The reset guard in create_usertable_small_dynamic_index
-- still protects callers that did not opt into the benchmark reset.
DELETE FROM ariabc_internal.merkle_dynamic_state;
TRUNCATE ariabc_internal.merkle_dynamic_build_stage,
         ariabc_internal.merkle_dynamic_seen,
         ariabc_internal.merkle_dynamic_leaf_item,
         ariabc_internal.merkle_dynamic_node;

\ir create_usertable_small_dynamic_index.sql
