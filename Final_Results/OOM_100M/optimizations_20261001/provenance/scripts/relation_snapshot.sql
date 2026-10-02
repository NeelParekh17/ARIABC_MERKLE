-- Run on a dedicated remote instance before/after a separate profiling workload.
-- Wait for the expected table n_tup_upd/n_tup_ins increments to be published;
-- clear the snapshot and poll rather than assuming collector timing is exact.
\pset format csv
SELECT pg_stat_clear_snapshot();
SELECT pg_current_wal_insert_lsn() AS wal_insert_lsn;
SELECT c.oid::regclass AS relation, c.relfilenode, c.reloptions,
       pg_relation_size(c.oid) AS bytes,
       s.n_tup_upd, s.n_tup_hot_upd, s.n_tup_ins, s.n_tup_del,
       i.heap_blks_read, i.heap_blks_hit, i.idx_blks_read, i.idx_blks_hit
FROM pg_class c
LEFT JOIN pg_stat_all_tables s ON s.relid=c.oid
LEFT JOIN pg_statio_all_tables i ON i.relid=c.oid
WHERE c.oid IN ('usertable'::regclass, 'ariabc_internal.merkle_node_usertable'::regclass)
ORDER BY c.oid;
SELECT indexrelid::regclass AS index, idx_blks_read, idx_blks_hit
FROM pg_statio_all_indexes
WHERE relid IN ('usertable'::regclass, 'ariabc_internal.merkle_node_usertable'::regclass)
ORDER BY indexrelid;
SELECT pg_current_wal_flush_lsn() AS wal_flush_lsn;
