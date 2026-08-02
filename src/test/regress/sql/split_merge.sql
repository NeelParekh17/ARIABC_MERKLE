-- Test dynamic split and merge
\set ECHO none
SET client_min_messages = warning;
\i ../../../scripts/distributed/sql/raft_apply_ledger_schema.sql
SET merkle_read_lag_policy = apply;
RESET client_min_messages;
\set ECHO all
CREATE TABLE sm_test (id int);
CREATE INDEX sm_idx ON sm_test USING merkle (id) WITH (split_threshold=32, merge_threshold=8);

-- Insert 10 rows (below split threshold)
INSERT INTO sm_test SELECT generate_series(1, 10);
SELECT prefix_len, is_leaf, tuple_count FROM ariabc_internal.merkle_node WHERE index_oid = 'sm_idx'::regclass ORDER BY prefix_len, node_id;
SELECT merkle_verify('sm_test') AS initial_verify;

-- Insert 30 more rows (total 40 > 32). This will trigger a split on the root.
INSERT INTO sm_test SELECT generate_series(11, 40);
SELECT prefix_len, is_leaf FROM ariabc_internal.merkle_node WHERE index_oid = 'sm_idx'::regclass ORDER BY prefix_len, node_id;
SELECT merkle_verify('sm_test') AS verify_after_split;
SELECT node_id, prefix_len, is_leaf FROM merkle_get_descendants_batch('sm_idx'::regclass, '\x0000000000000000'::bytea, 0::smallint, 2);

-- Delete 35 rows (total 5 < 8). This will trigger a merge on the children.
DELETE FROM sm_test WHERE id > 5;
SELECT prefix_len, is_leaf, tuple_count FROM ariabc_internal.merkle_node WHERE index_oid = 'sm_idx'::regclass ORDER BY prefix_len, node_id;
SELECT merkle_verify('sm_test') AS verify_after_merge;

-- Clean up
DROP TABLE sm_test;
