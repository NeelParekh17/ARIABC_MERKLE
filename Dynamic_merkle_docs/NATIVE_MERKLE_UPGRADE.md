# Upgrade and recovery

Native layout v5 is incompatible with earlier experimental native layouts.
Upgrade by installing the server, then running `REINDEX` for each dynamic
Merkle index.  The old relation is not interpreted as v5 and no physical
locator is migrated in place.  A failed build can be retried after dropping or
reindexing the affected index.

Downgrade is unsupported once a v5 index exists.  Replicas must run the same
route/row hash versions and compare roots only within the same layout and
sequence provenance.  Monitoring should record `authority`, `update_mode`,
`data_root`, `structure_root`, `combined_root`, `min_apply_seq` and
`max_apply_seq`, `sequence_domain`, `sequence_flags` and `sequence_epoch` from
`merkle_dynamic_tree_stats`. Distributed equality must use the typed
`merkle_native_partition_roots_at` marker helper rather than current-state
statistics alone.
