\set ON_ERROR_STOP on

SELECT merkle_apply_pending() AS applied_through;
SELECT merkle_recovery_status() AS recovery_status;
SELECT merkle_recovery_status() LIKE '%"state":"READY"%'
       AS recovery_ready \gset
\if :recovery_ready
\else
    \echo 'Merkle recovery did not reach READY'
    SELECT 1 / 0;
\endif

SELECT merkle_verify('merkle_atomicity_test'::regclass)
       AS verified_before_reindex \gset
\if :verified_before_reindex
\else
    \echo 'Merkle verification failed before REINDEX'
    SELECT 1 / 0;
\endif

SELECT merkle_root_hash('merkle_atomicity_test'::regclass)
       AS root_before_reindex \gset
REINDEX INDEX merkle_atomicity_test_idx;
SELECT merkle_root_hash('merkle_atomicity_test'::regclass)
       AS root_after_reindex \gset
SELECT :'root_before_reindex' = :'root_after_reindex'
       AS reindex_root_equal \gset
\if :reindex_root_equal
\else
    \echo 'Recovered root differs from clean REINDEX root'
    SELECT 1 / 0;
\endif

SELECT merkle_verify('merkle_atomicity_test'::regclass)
       AS verified_after_reindex \gset
\if :verified_after_reindex
\else
    \echo 'Merkle verification failed after REINDEX'
    SELECT 1 / 0;
\endif

SELECT :'merkle_mode' = 'dynamic' AS use_dynamic_merkle \gset
\if :use_dynamic_merkle
SELECT merkle_dynamic_verify('merkle_atomicity_test_idx'::regclass)
       AS dynamic_verified \gset
\if :dynamic_verified
\else
    \echo 'Dynamic Merkle structural verification failed'
    SELECT 1 / 0;
\endif
SELECT (merkle_dynamic_tree_stats('merkle_atomicity_test_idx'::regclass)
        ->> 'max_leaf_items')::int <= 4 AS dynamic_leaf_bound \gset
\if :dynamic_leaf_bound
\else
    \echo 'Dynamic Merkle leaf capacity was exceeded'
    SELECT 1 / 0;
\endif
\endif

SELECT :'root_before_reindex' AS root_before_reindex,
       :'root_after_reindex' AS root_after_reindex,
       merkle_recovery_status() AS final_recovery_status;
