-- Read-only catalog evidence; execute only against the isolated remote DB.
\set ON_ERROR_STOP on
SELECT current_setting('data_directory') AS data_directory,
       current_setting('port') AS port,
       current_setting('default_transaction_isolation') AS default_isolation,
       current_setting('merkle_apply_synchronous_direct') AS synchronous_merkle;
SELECT count(*) AS heap_rows FROM public.usertable_small;
SELECT pg_get_indexdef(i.indexrelid) AS definition, c.reloptions
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am am ON am.oid = c.relam
WHERE i.indrelid = 'public.usertable_small'::regclass AND am.amname = 'merkle';
SELECT format($query$
SELECT count(*) AS nodes,
       count(*) FILTER (WHERE is_leaf) AS leaves,
       count(*) FILTER (WHERE NOT is_leaf) AS internal_nodes,
       count(*) FILTER (WHERE prefix_len = 0) AS partition_roots,
       count(*) FILTER (WHERE is_leaf AND tuple_count = 0) AS empty_leaves,
       min(tuple_count) FILTER (WHERE is_leaf AND tuple_count > 0) AS min_nonempty_occupancy,
       avg(tuple_count) FILTER (WHERE is_leaf AND tuple_count > 0) AS avg_nonempty_occupancy,
       max(tuple_count) FILTER (WHERE is_leaf) AS max_occupancy,
       max(prefix_len) AS max_prefix_bits,
       pg_total_relation_size(%L::regclass) AS node_storage_bytes
FROM ariabc_internal.%I;
SELECT prefix_len, is_leaf, count(*) AS nodes, sum(tuple_count) AS tuple_count_sum
FROM ariabc_internal.%I GROUP BY prefix_len, is_leaf ORDER BY prefix_len, is_leaf;
$query$, 'ariabc_internal.merkle_node_' || i.indexrelid,
         'merkle_node_' || i.indexrelid, 'merkle_node_' || i.indexrelid)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am am ON am.oid = c.relam
WHERE i.indrelid = 'public.usertable_small'::regclass AND am.amname = 'merkle'
\gexec
SELECT merkle_root_hash('public.usertable_small') AS root,
       merkle_verify('public.usertable_small') AS verified;
