\set ON_ERROR_STOP on

DROP TABLE IF EXISTS merkle_atomicity_test;
CREATE TABLE merkle_atomicity_test (
    id bigint PRIMARY KEY,
    payload text NOT NULL,
    version integer NOT NULL
);

SELECT :'merkle_mode' = 'dynamic' AS use_dynamic_merkle \gset
\if :{?update_mode}
\else
\set update_mode synchronous_cow
\endif
\if :use_dynamic_merkle
CREATE INDEX merkle_atomicity_test_idx ON merkle_atomicity_test
USING merkle (id)
WITH (partitions=2, fanout=32, dynamic=on,
      leaf_capacity=4, merge_threshold=2,
      leaf_byte_capacity=4096, max_key_bytes=1024,
      update_mode=:'update_mode');
\else
CREATE INDEX merkle_atomicity_test_idx ON merkle_atomicity_test
USING merkle (id);
\endif

INSERT INTO merkle_atomicity_test
SELECT g, 'seed-' || g, 1
FROM generate_series(1, 8) AS g;

SELECT merkle_recovery_status();
SELECT merkle_verify('merkle_atomicity_test'::regclass) AS setup_verified;

\if :use_dynamic_merkle
SELECT (merkle_dynamic_tree_stats('merkle_atomicity_test_idx'::regclass)
        ->> 'max_leaf_items')::int <= 4 AS dynamic_leaf_bound;
SELECT count(*) > 0 AND sum(tuple_count) = 8 AS dynamic_frontier_conserves_rows
FROM merkle_dynamic_get_leaf_frontier('merkle_atomicity_test_idx'::regclass);
\endif
