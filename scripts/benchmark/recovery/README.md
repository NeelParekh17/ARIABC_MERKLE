# AriaBC Merkle State Recovery Benchmark Suite

This directory (`scripts/benchmark/recovery/`) contains the benchmark harness and automated analysis tools for testing Merkle state recovery across corrupted replicas in AriaBC.

It supports both static and **native Dynamic Merkle Index** configurations, benchmarking recovery performance across synthetic data volumes from **1 Million to 50 Million+ tuples**.

---

## 🏗️ Architecture & Recovery Phases

The benchmark operates using two side-by-side database schemas in a single PostgreSQL instance:

```text
healthy.usertable   ← Reference Table (Uncorrupted state)
damaged.usertable   ← Replica Table (Corrupted by harness, then repaired)
```

### The 5 Recovery Pipeline Phases

1. **Phase 1: Localisation**: Performs top-down Merkle tree comparison using array-based batching and frontier pruning to identify corrupt leaf nodes.
2. **Phase 2: Candidate Fetch**: Fetches healthy and damaged row candidates corresponding to corrupted leaf ranges (`repair_write_ms` attribution boundary).
3. **Phase 3: Row/Tuple Comparison**: In-memory alignment comparing healthy vs damaged tuples to isolate missing, extra, or updated attributes.
4. **Phase 4: Repair DML Execution**: Issues targeted batch `INSERT`, `UPDATE`, or `DELETE` SQL DML statements to align the damaged replica state with the reference table.
5. **Phase 5: Verification Audit**: Executes `merkle_verify()` and asserts zero remaining hash differences (`divergence_count = 0`).

---

## ⚡ Quick Start

```bash
# 1. Smoke Run (Fast test with minimal row counts)
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" \
  --profile smoke

# 2. Preflight Run (1M row test with paper geometry)
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" \
  --profile preflight

# 3. Full Paper Scale Run (1M, 3M, 5M, 10M..50M rows)
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" \
  --profile paper
```

---

## 📊 Benchmark Profiles & Configurations

### Dynamic Merkle Benchmark Geometry

| Parameter | Value / Range | Description |
|---|---|---|
| `Tuple Counts` | 1M, 3M, 5M, 10M, 25M, 50M | Target dataset scaling |
| `Fanout (F)` | 32 (or 16, 64) | Child slot capacity per node |
| `Split Threshold` | 1024 | Max tuples per dynamic leaf node |
| `Merge Threshold` | 256 | Min tuples per dynamic leaf node |
| `Corrupted Tuples` | 300 (or variable $k$) | Target corrupted tuple count |
| `Repetitions` | 5 | Benchmark repetitions per data point |

---

## 🧪 Synthetic Corruption Modes

The `--corruption-mode` flag selects the corruption injection strategy:

| Corruption Mode | Description | Benchmark Purpose |
|---|---|---|
| `paper-update-only` | Mutates `field9` attribute of existing rows | Standard research paper comparison profile |
| `update-only` | Modifies non-key tuple attributes | General value corruption test |
| `delete-only` | Deletes targeted rows from damaged replica | Missing tuple recovery test |
| `insert-only` | Injects spurious rows into damaged replica | Extra tuple purging test |
| `mixed` | Equal 1/3 update, 1/3 delete, 1/3 insert split | Real-world catastrophic corruption test |

---

## 📁 Source Layout

```text
scripts/benchmark/recovery/
├── run_merkle_recovery_benchmark.py   # Main CLI entry point
├── generate_analysis_plots.py         # Automated SVG/PNG plot generator for scaling reports
├── merkle_recovery/
│   ├── config.py          # Benchmark profiles, timing constants, geometry definitions
│   ├── db.py              # PostgreSQL database connections and SQL helpers
│   ├── dataset.py         # Schema creation, bulk load, occupancy calculation
│   ├── manifest.py        # Reproducible corruption manifest generator
│   ├── localisation.py    # Merkle tree descent and frontier pruning
│   ├── repair.py          # Candidate fetch, tuple comparison, and DML execution
│   ├── verification.py    # Merkle audit and EXCEPT ALL verification
│   ├── metrics.py         # Metrics data structures and timing contracts
│   └── reporting.py       # CSV, JSON, and progress logging handlers
├── tests/
│   └── test_corruption_modes.py # Unit and integration test suite
└── README.md              # Documentation (this file)
```

---

## 📈 Analysis & Plot Generation

To regenerate paper scaling plots (Tree Localisation, Candidate Fetch Phase, Row Comparison Phase, Leaf Geometry Occupancy, Total Repair Time) from raw benchmark CSV artifacts:

```bash
./.venv/bin/python3 scripts/benchmark/recovery/generate_analysis_plots.py \
  --results-dir scripts/bench_full_results/size-scaling-k75-c300 \
  --output-dir Dynamic_merkle_docs/plots
```

Generated plots will be saved as publication-ready SVGs/PNGs and referenced in [`Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md`](../../../Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md).

---

## 🧪 Running Automated Unit Tests

```bash
# Run corruption mode unit tests
pytest scripts/benchmark/recovery/tests/ \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" -v
```
