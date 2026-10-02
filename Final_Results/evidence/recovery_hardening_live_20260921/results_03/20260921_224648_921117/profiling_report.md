# Recovery Profiling Report

- recovery measurement boundary: `restore_repair_ms`
- audit measurement boundary: `audit_validation_ms`
- cache_mode: `uncontrolled normal benchmark state`
- track_io_timing: `on`
- PostgreSQL version: `b'13devel'`
- git commit: `c4d12723c68c59462b5790c9bafb625f4d7f9e8d`
- I/O timing available: `1`
- exact geometry:
  - manual-f32-s32-m8: fanout=32, split_threshold=32, merge_threshold=8, total_leaf_count=200, tree_depth=0

## Localization Access Observability

Per-run deltas for `pg_stat_all_indexes` and `pg_statio_all_indexes` around the native `merkle_node` localization call are in `localisation_index_stats.csv`.
These include index scans, index tuples read/fetched, buffer reads/hits, and relation/index sizes.
The diagnostic snapshot and asynchronous stats flush are reported as `localisation_stats_probe_ms` and `localisation_stats_flush_wait_ms`; both are excluded from `restore_repair_ms`.
The session planner/cache settings used by the campaign are in `runtime_settings.csv`.
Deep profiling additionally replays the exact localization frontiers with `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` in `localisation_plan_summary.csv` and `localisation_plan_profiles.jsonl`.

## Phase Medians And P95

| profile_label | tuple_count | fanout | split_threshold | merge_threshold | bad_leaf_count | corrupted_tuple_count | phase | median_ms | p95_ms |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | tree_localisation_ms | 10.981 | 11.465 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | candidate_row_fetch_ms | 3.778 | 3.921 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | row_comparison_ms | 0.032 | 0.033 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | repair_write_ms | 1.852 | 2.002 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | targeted_post_repair_confirmation_ms | 3.137 | 3.146 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | recovery_observability_ms | 0.000 | 0.001 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | recovery_orchestration_ms | 0.951 | 1.002 |
| manual-f32-s32-m8 | 1000 | 32 | 32 | 8 | 10 | 10 | restore_repair_ms | 20.730 | 21.551 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | tree_localisation_ms | 5.048 | 5.641 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | candidate_row_fetch_ms | 1.430 | 1.480 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | row_comparison_ms | 0.012 | 0.015 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | repair_write_ms | 0.781 | 0.796 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | targeted_post_repair_confirmation_ms | 3.091 | 3.096 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | recovery_observability_ms | 0.001 | 0.001 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | recovery_orchestration_ms | 1.185 | 1.197 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 1 | 1 | restore_repair_ms | 11.549 | 12.226 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | tree_localisation_ms | 5.322 | 5.408 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | candidate_row_fetch_ms | 1.862 | 1.996 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | row_comparison_ms | 0.013 | 0.014 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | repair_write_ms | 1.026 | 1.038 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | targeted_post_repair_confirmation_ms | 3.232 | 3.410 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | recovery_observability_ms | 0.001 | 0.001 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | recovery_orchestration_ms | 6.077 | 11.264 |
| manual-f32-s32-m8 | 1200 | 32 | 32 | 8 | 2 | 2 | restore_repair_ms | 17.533 | 22.862 |

## Growth Ratios

- manual-f32-s32-m8: insufficient 1M/5M data for growth ratios
