-- Test multi-column Merkle index with dynamic partitioning
\set ECHO none
SET client_min_messages = warning;
\i ../../../scripts/distributed/sql/raft_apply_ledger_schema.sql
SET merkle_read_lag_policy = apply;
RESET client_min_messages;
\set ECHO all

CREATE TABLE merkle_mc_test (ts timestamptz, tag text, val int);

-- Create a dynamic Merkle index
CREATE INDEX merkle_mc_idx ON merkle_mc_test
    USING merkle (ts, tag);

INSERT INTO merkle_mc_test VALUES
    ('2024-01-01 10:00:00+00', 'alpha', 10),
    ('2024-01-01 11:00:00+00', 'beta',  20),
    ('2024-01-01 12:00:00+00', 'gamma', 30);

SELECT merkle_verify('merkle_mc_test') AS mc_initial_verify;

-- Check routes (now dynamic, so it might output different internal node IDs, but let's just get the count)
SELECT count(*) FROM ariabc_internal.merkle_node WHERE index_oid = 'merkle_mc_idx'::regclass AND is_leaf = true;

UPDATE merkle_mc_test SET val = val * 10 WHERE tag = 'alpha';

SELECT merkle_verify('merkle_mc_test') AS mc_payload_update_verify;

UPDATE merkle_mc_test SET tag = 'gamma' WHERE tag = 'beta';

SELECT merkle_verify('merkle_mc_test') AS mc_rk_update_verify;

-- Test nulls
INSERT INTO merkle_mc_test (ts, tag, val) VALUES ('2024-01-01 13:00:00+00', NULL, 40);

SELECT merkle_verify('merkle_mc_test') AS mc_null_insert_verify;

DELETE FROM merkle_mc_test;

SELECT merkle_verify('merkle_mc_test') AS mc_delete_verify;

DROP TABLE merkle_mc_test;
