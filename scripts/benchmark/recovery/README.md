# AriaBC Merkle Recovery Benchmark

This benchmark reproduces the paper's static Merkle-recovery configuration
and **Merkle series** for Figures 12 and 13.  The full three-method Figure 12
comparison (CTA + disk + Merkle) remains available in the
`archive/recovery-three-method-v2` branch (tag `recovery-three-method-v2-baseline`).

## Two schemas, one PostgreSQL instance

```text
healthy.usertable   ← reference (never mutated during benchmark)
damaged.usertable   ← replica under test (corrupted, then repaired)
```

## Quick start

```bash
# smoke run (~seconds, tiny row counts)
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile smoke

# preflight — paper-shaped geometry at 1 M rows, 1 repetition
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile preflight

# paper run — 1 M / 3 M / 5 M (Fig 12) + 3 M sweep (Fig 13), 5 repetitions
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile paper
```

## Paper profiles

### Figure 12 — Merkle

| Parameter            | Value                    |
|----------------------|--------------------------|
| Tuple counts         | 1 M, 3 M, 5 M            |
| Partitions           | 200                      |
| Leaves per partition | 16                       |
| Bad leaf nodes       | 10                       |
| Repetitions          | 5                        |
| Corruption mode      | `paper-update-only`      |

### Figure 13 — Merkle

| Parameter            | Value                    |
|----------------------|--------------------------|
| Tuple count          | 3 M                      |
| Partitions           | 100 and 200              |
| Leaves per partition | 16                       |
| Corrupt tuples       | 300                      |
| Sweep variable       | k (damaged leaf count)   |
| Repetitions          | 5                        |
| Corruption mode      | `paper-update-only`      |

## Corruption modes

The `--corruption-mode` flag selects the injection strategy.

| Mode               | Description                                               |
|--------------------|-----------------------------------------------------------|
| `paper-update-only`| **Paper profile.** Mutates `field9` of existing rows.    |
| `update-only`      | Same semantics; distinct label for correctness tests.     |
| `delete-only`      | Deletes targeted rows from the damaged copy.              |
| `insert-only`      | Inserts spurious rows (negative keys) into damaged copy.  |
| `mixed`            | Deterministic 1/3 update, 1/3 delete, 1/3 insert split.  |

Non-paper modes are correctness tests.  Do not use them for paper plots.

## Source layout

```text
scripts/benchmark/recovery/
├── run_merkle_recovery_benchmark.py   ← main entry point
├── merkle_recovery/
│   ├── config.py          ← profiles, constants, paper parameters
│   ├── db.py              ← DB connection and SQL helpers
│   ├── dataset.py         ← schema setup, data generation, occupancy
│   ├── manifest.py        ← corruption manifest (all five modes)
│   ├── localisation.py    ← Merkle tree descent
│   ├── repair.py          ← candidate fetch, planner preflight, DML
│   ├── verification.py    ← audit, EXCEPT ALL, merkle_verify
│   ├── metrics.py         ← Metrics dataclass, timing contract
│   └── reporting.py       ← CSV/JSON/progress helpers
├── tests/
│   └── test_corruption_modes.py
├── sql/
│   ├── recovery_helpers.sql
│   ├── targeted_merkle_lookup.sql
│   └── verification.sql
├── create_schema.sql
├── create_merkle_indexes.sql
└── README.md
```

## Output files

Each `results/<timestamp>/` directory contains:

```text
config.json                    benchmark parameters
environment.txt                git HEAD, Python version, platform
python_environment.json
corruption_manifest.json       exact corruption spec (reproducible)
runs.csv                       per-run timing + all Phase 4 counters
phase_timings.csv              per-phase breakdown
timing_contract.csv            timing boundary assertions
planner_checks.csv             EXPLAIN evidence for functional index use
schema_fidelity.csv
dataset_sizes.csv
bucket_consistency_summary.csv
verification_results.csv
plots/                         auto-generated SVGs
stdout.log / stderr.log
```

### Extended counters (Phase 4)

Every `runs.csv` row includes:

| Counter                   | Meaning                                        |
|---------------------------|------------------------------------------------|
| `bad_partition_count`     | Partitions whose roots differed                |
| `bad_leaf_count`          | Leaf nodes localised as corrupt                |
| `tree_nodes_visited`      | Internal nodes descended during localisation   |
| `healthy_candidate_rows`  | Rows fetched from healthy leaves               |
| `damaged_candidate_rows`  | Rows fetched from damaged leaves               |
| `total_candidate_rows`    | Sum of both                                    |
| `rows_inserted`           | INSERT DML issued during repair                |
| `rows_updated`            | UPDATE DML issued during repair                |
| `rows_deleted`            | DELETE DML issued during repair                |
| `total_rows_repaired`     | Sum of the three above                         |
| `mean_rows_per_bad_leaf`  | Mean candidate rows per bad leaf               |
| `p95_rows_per_bad_leaf`   | p95 candidate rows per bad leaf                |

Audit time is **excluded** from `paper_style_total_ms`.

## Correctness tests

```bash
pytest scripts/benchmark/recovery/tests/ \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" -v
```

## Archival note

The complete CTA / disk / Merkle three-method comparison implementation is
preserved at:

- **Tag:** `recovery-three-method-v2-baseline`
- **Branch:** `archive/recovery-three-method-v2`

To reproduce the full Figure 12 comparison (including CTA and disk baselines),
check out that branch and use the original `run_recovery_benchmark.py`.

## Benchmark schema version

All results produced by this driver are:

```text
benchmark_schema_version = 3
timing_contract_version  = 1
```

Version 3 is the Merkle-only static-recovery benchmark.
Version 2 was the archived three-method CTA/disk/Merkle benchmark.
Do not pool v2 and v3 results in one plot.
