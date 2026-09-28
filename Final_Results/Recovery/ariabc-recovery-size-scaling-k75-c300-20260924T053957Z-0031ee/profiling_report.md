# Recovery Profiling Report

- recovery measurement boundary: `restore_repair_ms`
- audit measurement boundary: `audit_validation_ms`
- cache_mode: `uncontrolled normal benchmark state`
- track_io_timing: `on`
- PostgreSQL version: `13devel`
- git commit: `unavailable: Command '['/usr/bin/git', 'rev-parse', 'HEAD']' returned non-zero exit status 128.; source_snapshot.json records synced source provenance`
- I/O timing available: `1`
- exact geometry:
  - fanout_f32_l16: fanout=32, split_threshold=32, merge_threshold=8, total_leaf_count=6509270, tree_depth=3

## Localization Access Observability

Per-run deltas for `pg_stat_all_indexes` and `pg_statio_all_indexes` around the native `merkle_node` localization call are in `localisation_index_stats.csv`.
These include index scans, index tuples read/fetched, buffer reads/hits, and relation/index sizes.
The diagnostic snapshot and asynchronous stats flush are reported as `localisation_stats_probe_ms` and `localisation_stats_flush_wait_ms`; both are excluded from `restore_repair_ms`.
Statistics boundaries await a private scan-counter publication marker; setup and full-audit scans are drained outside measured recovery. Catalog counters require an isolated benchmark database.
The session planner/cache settings used by the campaign are in `runtime_settings.csv`.
Deep profiling additionally replays the exact localization frontiers with `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` in `localisation_plan_summary.csv` and `localisation_plan_profiles.jsonl`.

## Phase Medians And P95

| profile_label | tuple_count | fanout | split_threshold | merge_threshold | bad_leaf_count | corrupted_tuple_count | phase | median_ms | p95_ms |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 121.916 | 123.983 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 18.723 | 19.673 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.643 | 0.666 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 24.926 | 25.532 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.481 | 3.541 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.458 | 1.501 |
| fanout_f32_l16 | 1000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 171.406 | 173.985 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 126.202 | 132.147 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 35.033 | 35.844 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 1.399 | 1.453 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 21.495 | 33.689 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.528 | 3.564 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.713 | 1.740 |
| fanout_f32_l16 | 3000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 189.533 | 201.538 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 137.082 | 145.259 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 50.668 | 51.849 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 2.261 | 2.302 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 22.599 | 33.311 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.545 | 3.586 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.934 | 2.023 |
| fanout_f32_l16 | 5000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 218.318 | 230.253 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 162.266 | 177.253 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 33.638 | 34.646 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 1.341 | 1.377 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 25.997 | 37.992 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.610 | 3.627 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.692 | 1.827 |
| fanout_f32_l16 | 7000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 228.461 | 254.807 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 184.850 | 193.167 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 16.939 | 17.755 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.543 | 0.563 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 26.970 | 27.551 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.528 | 3.542 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.399 | 1.531 |
| fanout_f32_l16 | 10000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 234.245 | 242.833 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 184.795 | 198.147 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 17.448 | 18.320 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.569 | 0.611 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 27.504 | 39.402 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.601 | 3.668 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.413 | 1.438 |
| fanout_f32_l16 | 15000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 240.297 | 248.861 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 180.285 | 201.204 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 17.962 | 18.948 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.595 | 0.629 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 27.470 | 39.550 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.619 | 3.779 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.002 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.441 | 1.456 |
| fanout_f32_l16 | 20000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 231.435 | 253.223 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 185.216 | 194.205 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 19.482 | 20.159 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.654 | 0.682 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 26.820 | 39.442 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.584 | 3.638 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.432 | 1.553 |
| fanout_f32_l16 | 25000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 236.953 | 258.295 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 188.388 | 195.497 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 19.775 | 20.439 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.665 | 0.706 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 27.391 | 39.758 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.632 | 3.666 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.483 | 1.518 |
| fanout_f32_l16 | 30000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 241.507 | 260.858 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 177.443 | 187.349 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 21.945 | 22.822 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.756 | 0.792 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 26.494 | 37.339 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.609 | 3.644 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.481 | 1.508 |
| fanout_f32_l16 | 40000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 231.847 | 241.313 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | tree_localisation_ms | 178.610 | 191.544 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | candidate_row_fetch_ms | 24.439 | 25.083 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | row_comparison_ms | 0.872 | 0.911 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | repair_write_ms | 25.710 | 36.339 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | targeted_post_repair_confirmation_ms | 3.488 | 3.565 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | recovery_observability_ms | 0.001 | 0.001 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | recovery_orchestration_ms | 1.513 | 1.556 |
| fanout_f32_l16 | 50000000 | 32 | 32 | 8 | 75 | 300 | restore_repair_ms | 235.046 | 248.167 |

## Growth Ratios

- fanout_f32_l16: 5M / 1M median ratios
  - candidate_row_fetch_ms: 2.706
  - targeted_post_repair_confirmation_ms: 1.018
  - repair_write_ms: 0.907
  - tree_localisation_ms: 1.124

- highest-growing phase: `candidate_row_fetch_ms` for `fanout_f32_l16` at ratio `2.706`
