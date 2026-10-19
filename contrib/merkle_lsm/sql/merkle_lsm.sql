CREATE EXTENSION merkle_lsm;
CREATE TABLE ml_test (k int, v text) WITH (fillfactor=50);
CREATE INDEX ml_test_idx ON ml_test USING merkle_lsm(k) INCLUDE(v)
 WITH (levels=3, top_index_size=16, size_ratio=2);
INSERT INTO ml_test SELECT i, 'v' || i FROM generate_series(1,1000) i;
SET enable_seqscan=off;
SET enable_bitmapscan=off;
SELECT count(*), sum(k) FROM ml_test WHERE k>0;
UPDATE ml_test SET v='changed' WHERE k%10=0;
UPDATE ml_test SET k=k+1000 WHERE k%5=0;
DELETE FROM ml_test WHERE k%7=0;
VACUUM ml_test;
SELECT count(*), sum(k) FROM ml_test WHERE k>0;
SELECT merkle_lsm_config('ml_test_idx');
SELECT merkle_lsm_flush('ml_test_idx');
SELECT count(*), sum(k) FROM ml_test WHERE k>0;
SELECT level, count(*) AS components, sum(entries) AS physical_entries
 FROM merkle_lsm_components('ml_test_idx') GROUP BY level ORDER BY level;
ALTER INDEX ml_test_idx RENAME TO ml_test_renamed;
SELECT count(*) FROM ml_test WHERE k>0;
SET enable_indexscan=off;
SET enable_bitmapscan=on;
SELECT count(*), sum(k) FROM ml_test WHERE k>0;
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE ml_test;
SELECT count(*) FROM pg_class WHERE relname LIKE 'ml\_%' ESCAPE '\';
-- ERROR during bulk build must release the private smgr descriptor.
CREATE TABLE ml_build_error(k int);
INSERT INTO ml_build_error SELECT i FROM generate_series(1,20)i;
CREATE INDEX ml_error ON ml_build_error USING merkle_lsm((1/(k%17)));
CREATE INDEX ml_after_error ON ml_build_error USING merkle_lsm(k);
SELECT count(*) FROM ml_build_error WHERE k>0;
DROP TABLE ml_build_error;
-- VACUUM cardinality must include upper-level entries even with an empty base.
CREATE TABLE ml_stats(k int);
CREATE INDEX ml_stats_idx ON ml_stats USING merkle_lsm(k) WITH(levels=2);
INSERT INTO ml_stats SELECT i FROM generate_series(1,1000)i;
DELETE FROM ml_stats WHERE k<=100;
VACUUM ml_stats;
SELECT reltuples::bigint FROM pg_class WHERE oid='ml_stats_idx'::regclass;
DROP TABLE ml_stats;
DROP EXTENSION merkle_lsm;
