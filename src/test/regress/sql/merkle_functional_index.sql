-- Dynamic Merkle functional index, canonical hashing, transaction, and DML verification tests.
\set ECHO none
SET client_min_messages = warning;
\i ../../../scripts/distributed/sql/raft_apply_ledger_schema.sql
SET merkle_read_lag_policy = apply;
RESET client_min_messages;
\set ECHO all

CREATE TABLE merkle_dyn_test (
    id bigint,
    payload text,
    nullable text,
    raw bytea,
    ts timestamptz,
    f float8
);
CREATE INDEX merkle_dyn_test_idx ON merkle_dyn_test USING merkle (id);

INSERT INTO merkle_dyn_test VALUES
    ('-9223372036854775808', '*null*', NULL, '\x00ff',
     '2026-01-02 03:04:05+00', '-Infinity'),
    (0, '', '*null*', '\x', '2026-01-02 03:04:05+05:30', 'NaN'),
    (1, 'a*b', '', '\x0102', '2026-07-10 12:00:00+00', 'Infinity');

SELECT merkle_apply_pending();

SELECT merkle_verify('merkle_dyn_test') AS initial_verify;

CREATE TEMP TABLE merkle_root_before AS
SELECT merkle_root_hash('merkle_dyn_test') AS root;

SET timezone = 'Asia/Kolkata';
SET datestyle = 'SQL, DMY';
SELECT merkle_verify('merkle_dyn_test') AS guc_independent_verify,
       merkle_root_hash('merkle_dyn_test') =
       (SELECT root FROM merkle_root_before) AS root_unchanged;
RESET timezone;
RESET datestyle;

BEGIN;
INSERT INTO merkle_dyn_test VALUES
    (9, 'rollback', NULL, '\x09', now(), 9);
ROLLBACK;
SELECT merkle_verify('merkle_dyn_test') AS rollback_verify,
       merkle_root_hash('merkle_dyn_test') =
       (SELECT root FROM merkle_root_before) AS rollback_root;

BEGIN;
SAVEPOINT merkle_savepoint;
UPDATE merkle_dyn_test SET payload = 'savepoint' WHERE id = 1;
ROLLBACK TO merkle_savepoint;
COMMIT;
SELECT merkle_verify('merkle_dyn_test') AS savepoint_verify,
       merkle_root_hash('merkle_dyn_test') =
       (SELECT root FROM merkle_root_before) AS savepoint_root;

UPDATE merkle_dyn_test SET payload = 'updated' WHERE id = 1;
UPDATE merkle_dyn_test SET id = 7 WHERE id = 0;
DELETE FROM merkle_dyn_test WHERE id = '-9223372036854775808';
SELECT merkle_apply_pending();
SELECT merkle_verify('merkle_dyn_test') AS dml_verify;

CREATE TABLE merkle_route_test (ts timestamptz, key_text text);
CREATE INDEX merkle_route_test_idx ON merkle_route_test USING merkle (ts, key_text);
INSERT INTO merkle_route_test VALUES ('2026-07-10 12:00:00+00', '*null*');

SET timezone = 'America/Los_Angeles';
SET datestyle = 'German, DMY';
SELECT merkle_apply_pending();
SELECT merkle_verify('merkle_route_test') AS canonical_route_verify;
RESET timezone;
RESET datestyle;
DROP TABLE merkle_route_test;

TRUNCATE merkle_dyn_test;
SELECT merkle_apply_pending();
SELECT merkle_verify('merkle_dyn_test') AS truncate_verify;
DROP TABLE merkle_dyn_test;

-- Test SQL functional hash functions and dynamic Merkle functional covering index
SELECT octet_length(merkle_key_hash(12345::bigint)) = 8 AS key_hash_len_8;
SELECT octet_length(merkle_tuple_hash(r)) = 32 AS tuple_hash_len_32 FROM (SELECT 1::int AS a, 'test'::text AS b) r;

CREATE TABLE functional_index_test (id bigint primary key, val text);
CREATE INDEX functional_covering_idx ON functional_index_test (merkle_key_hash(id), merkle_tuple_hash(functional_index_test.*), id);
INSERT INTO functional_index_test VALUES (100, 'hello'), (200, 'world');
UPDATE functional_index_test SET val = 'updated' WHERE id = 100;
DELETE FROM functional_index_test WHERE id = 200;
SELECT count(*) = 1 FROM functional_index_test;

-- EXPLAIN validation for functional index lookup (plan shape, no timing)
VACUUM functional_index_test;
EXPLAIN SELECT id, merkle_key_hash(id) FROM functional_index_test WHERE merkle_key_hash(id) = merkle_key_hash(100);
EXPLAIN SELECT merkle_key_hash(id), merkle_tuple_hash(functional_index_test.*), id FROM functional_index_test WHERE merkle_key_hash(id) = merkle_key_hash(100);

DROP TABLE functional_index_test;
