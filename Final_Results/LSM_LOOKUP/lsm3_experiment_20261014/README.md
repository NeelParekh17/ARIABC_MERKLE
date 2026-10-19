# lsm3 lookup-index experiment (2026-10-14/15) — historical, superseded

This folder archives the first LSM experiment for the Merkle row-lookup index
(`usertable_merkle_lookup_idx`). It used a patched copy of Postgres Pro `lsm3`
(upstream 3bac1042): one base B-tree plus two alternating top B-trees,
coordinated through shared memory. That design is **replaced** by the in-tree
multi-level `contrib/merkle_lsm` access method (see `doc/LSM_LOOKUP_ARCHITECTURE.md`).
The old on-disk layout is not compatible with `merkle_lsm`.

Known defects of the old design, fixed in `merkle_lsm`:

- the insert path mutated `relam` in the relcache and used the index after `index_close`;
- a merger ERROR left `merge_in_progress` set and kept a dangling PGPROC pointer;
- merges were not idempotent (crash mid-merge could duplicate `(key,TID)` in the base);
- truncating a top dropped SERIALIZABLE predicate locks, and VACUUM could race a merge;
- top size was only checked every 65,536 inserts, so short runs never merged;
- components were found by name (`<index>_top0/1`) and the merge state lived only in shared memory.

## Contents

| Path | What |
|---|---|
| `REPORT.md`, `RESULTS.md`, `headline_table.md`, `activity_table.md` | YCSB A grid at ff90 (det / B-tree / lsm3, θ 0 and 0.99, 1–16 workers, 3 trials) |
| `CORRECTNESS.md`, `AUDIT.md`, `BASELINES.md`, `LSM_CHOICE.md`, `PATCH_NOTES.md` | correctness evidence, audit, baseline construction, why lsm3, patch notes |
| `data/` | per-trial CSVs, medians, ratios, audit JSON, TPS plot |
| `ycsb_d_run_20261015/` | workload D s0.99 w16, 2 trials B-tree vs lsm3 (20K statements each, ~1.8 s runs); summary.csv is the B-tree D baseline |
| `patches/`, `lsm3_source/` | the patched lsm3 module and the harness patches as used |
| `harness/` | controller, preparation and small-test scripts as run |
| `evidence/` | small correctness, baseline-identity and C++ build evidence (logs are gitignored) |
| `old_design_docs/` | the architecture documents written for this design, with their diagrams |

## Headline numbers

- Workload A, ff90: lsm3 vs B-tree −2.61% … +2.23% TPS; almost every update was HOT, so the lookup
  index received almost no inserts and no merge ever ran.
- Workload D, s0.99 w16 (2 trials, 20K statements): B-tree 10,293 / 10,793 TPS; lsm3 11,474 / 11,455 TPS.
  WAL fell 16.1 MB → 8.5 MB per run. Only ~1,024 inserts per run, so no merge happened in any run.
