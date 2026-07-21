CREATE TABLE merkle_native_test (
    id integer PRIMARY KEY,
    payload text NOT NULL
);
INSERT INTO merkle_native_test
SELECT g, 'v' || g FROM generate_series(1, 40) AS g;
CREATE INDEX merkle_native_test_idx ON merkle_native_test
USING merkle (id) WITH (dynamic=true, partitions=4, leaf_capacity=4,
                        merge_threshold=2, leaf_byte_capacity=1024,
                        max_key_bytes=128);
CREATE TEMP TABLE merkle_native_queue_before AS
SELECT COALESCE(pg_stat_get_tuples_inserted(
           to_regclass('ariabc_internal.merkle_local_delta')), 0) AS inserted;

SELECT merkle_verify('merkle_native_test') AS initial_verify;
SELECT merkle_dynamic_tree_stats('merkle_native_test_idx')->>'authority'
       AS authority;
SELECT merkle_dynamic_tree_stats('merkle_native_test_idx')->>'logical_fanout' = '32'
       AND merkle_dynamic_tree_stats('merkle_native_test_idx')->>'physical_node_fanout' = '2'
       AS fanout_contract;
SELECT (merkle_dynamic_tree_stats('merkle_native_test_idx')->>'max_depth')::int <= 2
       AS reduced_logical_height_contract;
SELECT (merkle_dynamic_tree_stats('merkle_native_test_idx') ? 'data_root')
       AND (merkle_dynamic_tree_stats('merkle_native_test_idx') ? 'structure_root')
       AND (merkle_dynamic_tree_stats('merkle_native_test_idx') ? 'combined_root')
       AND merkle_dynamic_tree_stats('merkle_native_test_idx')->>'layout_version' = '6'
       AND (merkle_dynamic_tree_stats('merkle_native_test_idx')->>'data_root') <>
           (merkle_dynamic_tree_stats('merkle_native_test_idx')->>'structure_root')
       AS commitment_provenance;
REINDEX INDEX merkle_native_test_idx;
SELECT merkle_dynamic_tree_stats('merkle_native_test_idx')->>'update_mode' =
       'synchronous_cow' AS reindex_preserves_durable_mode;
CREATE TEMP TABLE merkle_native_root AS
SELECT merkle_root_hash('merkle_native_test') AS root;

BEGIN;
UPDATE merkle_native_test SET payload='first' WHERE id=1;
UPDATE merkle_native_test SET payload='final' WHERE id=1;
UPDATE merkle_native_test SET id=101 WHERE id=2;
DELETE FROM merkle_native_test WHERE id BETWEEN 3 AND 10;
INSERT INTO merkle_native_test VALUES (102, 'inserted');
SAVEPOINT s;
INSERT INTO merkle_native_test VALUES (103, 'rolled back');
ROLLBACK TO s;
COMMIT;

SELECT merkle_verify('merkle_native_test') AS dml_verify,
       merkle_root_hash('merkle_native_test') <>
       (SELECT root FROM merkle_native_root) AS root_changed;
SELECT COALESCE(pg_stat_get_tuples_inserted(
           to_regclass('ariabc_internal.merkle_local_delta')), 0) =
       (SELECT inserted FROM merkle_native_queue_before)
       AS no_queue_writes;

TRUNCATE merkle_native_root;
INSERT INTO merkle_native_root
SELECT merkle_root_hash('merkle_native_test');
BEGIN;
UPDATE merkle_native_test SET payload='abort' WHERE id=1;
DELETE FROM merkle_native_test WHERE id=11;
ROLLBACK;
SELECT merkle_verify('merkle_native_test') AS abort_verify,
       merkle_root_hash('merkle_native_test') =
       (SELECT root FROM merkle_native_root) AS abort_root;

SELECT count(*) = 4 AS all_partition_roots
FROM merkle_dynamic_get_partition_roots('merkle_native_test_idx');
SELECT count(*) > 4 AS split_leaf_frontier
FROM merkle_dynamic_get_leaf_frontier('merkle_native_test_idx');
WITH requests AS (
  SELECT jsonb_agg(jsonb_build_object(
           'partition_id', partition_id,
           'prefix_length', prefix_len,
           'prefix_value', encode(prefix, 'hex'))) AS ranges
    FROM merkle_dynamic_get_leaf_frontier('merkle_native_test_idx')
)
SELECT count(*) = (SELECT count(*) FROM merkle_native_test)
       AS frontier_range_roundtrip
  FROM requests
 CROSS JOIN LATERAL merkle_dynamic_get_range_items(
   'merkle_native_test_idx', requests.ranges) AS item;
SELECT bool_and(visible_apply_seq >= 0 AND visible_domain = 2
                AND visible_epoch = 0
                AND octet_length(content_xor) = 32) AS typed_provenance
FROM merkle_native_partition_commitments_at('merkle_native_test_idx', 2::smallint,
                                     0::bigint, 9223372036854775807::bigint);

-- Each generated INSERT is an autocommit transaction.  Enough immutable
-- roots are published to cross multiple append-page boundaries; publication
-- must extend instead of selecting a page that lacks line-pointer space.
\set ECHO none
SELECT format('INSERT INTO merkle_native_test VALUES (%s, %L)',
              g, 'append-' || g)
FROM generate_series(1000, 1127) AS g
\gexec
\set ECHO all
SELECT count(*) = 161 AND merkle_verify('merkle_native_test')
       AS append_page_boundary_verify
FROM merkle_native_test;
SELECT (merkle_dynamic_tree_stats('merkle_native_test_idx')->>'max_depth')::int <= 2
       AS append_reduced_logical_height_contract;

-- A single statement produces one large transition batch.  Its new keys land
-- in multiple empty logical-slot runs separated by existing compressed
-- children.  Batch application must not bridge those runs and disconnect the
-- occupied children between them.
INSERT INTO merkle_native_test
SELECT g, 'batch-' || g FROM generate_series(2000, 2255) AS g;
SELECT count(*) = 417 AND merkle_verify('merkle_native_test')
       AS disjoint_empty_slot_batch_verify
FROM merkle_native_test;
WITH requests AS (
  SELECT jsonb_agg(jsonb_build_object(
           'partition_id', partition_id,
           'prefix_length', prefix_len,
           'prefix_value', encode(prefix, 'hex'))) AS ranges
    FROM merkle_dynamic_get_leaf_frontier('merkle_native_test_idx')
)
SELECT count(*) = (SELECT count(*) FROM merkle_native_test)
       AS batch_frontier_range_roundtrip
  FROM requests
 CROSS JOIN LATERAL merkle_dynamic_get_range_items(
   'merkle_native_test_idx', requests.ranges) AS item;

VACUUM merkle_native_test;
SELECT merkle_verify('merkle_native_test') AS post_vacuum_verify;
TRUNCATE merkle_native_test;
SELECT merkle_verify('merkle_native_test') AS truncate_verify;
DROP TABLE merkle_native_test;
