# CHANGELOG — Merkle-only refactor

## refactor/merkle-only-recovery  (2026-07-06)

### Archived before cleanup

- Tag `recovery-three-method-v2-baseline` and branch `archive/recovery-three-method-v2`
  preserve the full CTA / disk / Merkle three-method implementation.
  The full Figure 12 comparison (all three methods) remains reproducible from
  that branch.

### Removed (Phase 1)

| File / symbol                     | Reason                                      |
|-----------------------------------|---------------------------------------------|
| `recover_cta.py`                  | CTA re-export stub no longer needed         |
| `recover_disk.py`                 | Disk re-export stub no longer needed        |
| `repair_cta()` function           | CTA full-copy recovery removed              |
| `repair_disk()` function          | Binary COPY snapshot/restore removed        |
| `rebuild_indexes_for_table()`     | Only used by CTA/disk                       |
| `swap_recovered()`                | Only used by CTA/disk                       |
| `method_order()` rotation         | Only needed for three-method ordering       |
| `file_sha256()` for .copybin      | Disk snapshot SHA validation removed        |
| CTA/disk timing columns           | `cta_total_ms`, `disk_total_ms`, etc.       |
| `--keep-disk-snapshots` CLI flag  | Disk snapshots removed                      |
| `.copybin` generation/validation  | Removed with disk method                    |
| CTA/disk schema setup             | `recovery_cta`, `recovery_disk` schemas     |
| `run_recovery_benchmark.py`       | 1 639-line monolith; replaced by driver +   |
|                                   | `merkle_recovery/` package                  |
| `build_dataset.py` stub           | Re-export stub removed                      |
| `collect_metrics.py` stub         | Re-export stub removed                      |
| `corruption_manifest.py` stub     | Re-export stub removed                      |
| `verify_recovery.py` stub         | Re-export stub removed                      |

### Added (Phase 2 — modular split)

```text
run_merkle_recovery_benchmark.py   ← new slim entry point
merkle_recovery/
  __init__.py
  config.py        profiles, constants, BenchmarkConfig dataclass
  db.py            connection + SQL helpers
  dataset.py       schema/dataset/occupancy
  manifest.py      corruption manifest (all five modes)
  localisation.py  Merkle tree descent
  repair.py        candidate fetch, planner preflight, DML
  verification.py  audit, EXCEPT ALL, merkle_verify
  metrics.py       Metrics dataclass + timing contract
  reporting.py     CSV/JSON/progress helpers
tests/
  test_corruption_modes.py
```

### Added (Phase 3 — correctness coverage)

New `--corruption-mode` CLI flag with five modes:

- `paper-update-only` — original paper profile (default, unchanged)
- `update-only` — same semantics, distinct label
- `delete-only` — targeted rows deleted from damaged copy
- `insert-only` — spurious rows (negative keys) inserted into damaged copy
- `mixed` — deterministic 1/3-1/3-1/3 split

Nine assertions verified per mode in `tests/test_corruption_modes.py`.

### Added (Phase 4 — extended counters)

Every `runs.csv` row now includes:
`bad_partition_count`, `bad_leaf_count`, `tree_nodes_visited`,
`healthy_candidate_rows`, `damaged_candidate_rows`, `total_candidate_rows`,
`rows_inserted`, `rows_updated`, `rows_deleted`, `total_rows_repaired`,
`mean_rows_per_bad_leaf`, `p95_rows_per_bad_leaf`.

### Kept unchanged

- `create_source_snapshot.py`
- `verify_source_snapshot.py`
- `package_recovery_artifacts.py`
- `run_synced_remote_recovery_benchmark.sh`
- `fetch_synced_remote_recovery_results.sh`
- `python_requirements_contract.json`
- `verify_recovery_python_env.py`
- `write_host_info.py`
- `write_failure_json.py`
- `patch_config_json.py`
- `plot_recovery_results.py`
- All SQL files under `sql/`
- `create_schema.sql`, `create_merkle_indexes.sql`
- `benchmark_schema_version = 2`, `timing_contract_version = 1`
