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

The synced remote wrapper has an EPYC default campaign. From the repository
root, run it with no arguments:

```bash
./scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh
```

This defaults to `protectdr@ranking.cse.iitb.ac.in`, remote root
`/home/protectdr/merkle_recovery_runs`, `/usr/bin/python3`, the release build,
the native `dynamic-size-scaling-k75-c300` profile, tuple counts 1M/3M/5M,
one repetition, profiling off, audit skipped, summary artifacts, and a kept
remote archive. Any of these can still be overridden with the corresponding
option.

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

# dynamic acceptance — 1 M / 3 M / 5 M, 5 repetitions, audit disabled by default
./.venv/bin/python3 scripts/benchmark/recovery/run_merkle_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile dynamic-size-scaling-k75-c300 \
  --audit-mode skip
```

## Dynamic Merkle acceptance profile

`dynamic-size-scaling-k75-c300` is separate from every static paper profile.
It creates Merkle indexes with `dynamic=on`, P=200, a configurable power-of-two
logical fanout from 2 through 32 (default 32), leaf capacity/split threshold 32,
and merge threshold 8. The physical split implementation remains binary and
the fixed v8 node record retains capacity for 32 logical slots. It never creates
the static `merkle_bucket_for_key(...)` expression indexes. Use `--fanout 4`
(or 2, 8, 16, 32) to select a different logical directory width.

The default campaign runs 1 M, 3 M, and 5 M tuples with 75 corrupted bounded
ranges, 300 update corruptions, and five repetitions. Audit is disabled by
default so recovery timings measure recovery only. Pass `--audit-mode full` to
run the optional post-recovery audit; its time is reported separately and is
never included in `paper_style_total_ms` or `restore_repair_ms`.
Recovery compares shape-independent logical prefix summaries
`(tuple_count,data_xor)`, descends MSB-first, fetches at most 4,800 key/hash
summary rows, fetches full healthy heap rows only for exact insert/update keys,
uses set-based repair DML, drains the durable applier, relocalises, and audits.

To include real network overhead, run the Python driver on a client machine
against a dedicated PostgreSQL benchmark database on another machine:

```bash
./scripts/benchmark/recovery/run_networked_recovery_benchmark.sh \
  --client-host recovery-client.example \
  --db-host merkle-db.example \
  --db-name merkle_recovery_bench \
  --fanout 4 \
  --allow-destructive-dataset-reset
```

All recovery phase timers then include their database round trips. The runner
also requires PostgreSQL to report distinct TCP client/server addresses and
stores baseline SQL RTT samples and endpoint identity in `network_probe.json`.
It uses libpq authentication on the client (normally `~/.pgpass`) and refuses
to run unless the destructive dedicated-database acknowledgement is present.

For a true split-host recovery, where the damaged native index is on
`admin123` and the healthy reference is on `user4`, use:

```bash
./scripts/benchmark/recovery/run_split_host_recovery_benchmark.sh \
  --allow-destructive-dataset-reset
```

Those placements are configurable but are the current defaults. The recovery
client runs on admin123, so damaged reads and repair writes are local while
healthy root/range/row fetches cross the admin123-to-user4 TCP path. The default
tuple series is `1M,3M,5M,10M`. Pass `--full-scale` for
`1M,3M,5M,7M,10M,15M,20M,25M,30M,40M,50M`, or use `--tuple-count CSV` for an
explicit series. By default, the wrapper starts isolated clusters on TCP port
55432 using each host's `/home/neel/Desktop/ariabc_install`; existing PGDATA is
reused and never deleted. Pass `--no-prepare-postgres` plus the host, port, and
database options to use externally managed dedicated databases instead.

When `--audit-mode full` is requested, dynamic acceptance additionally checks
both table differences, matching root hashes and counts,
`merkle_dynamic_verify()` on both sides, no remaining bad logical range, both
indexes `READY`, maximum leaf occupancy at most 32, exactly 300 repairs, and
no timed user-table sequential scan. With the default `skip` mode, only the
bounded recovery/repair confirmation is timed.

## Paper profiles

### Figure 12 — Merkle

| Parameter            | Value                    |
|----------------------|--------------------------|
| Tuple counts         | 1 M, 3 M, 5 M            |
| Partitions           | 200                      |
| Leaves per partition | 16                       |
| Bad leaf nodes       | 10                       |
| Corrupted tuples     | 300                      |
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
dynamic_logical_ranges.csv     every logical summary request/result
dynamic_summary_items.csv      bounded key/route/hash summaries
dynamic_exact_heap_rows.csv    exact full healthy rows fetched for repair
dynamic_tree_stats.csv         state/depth/occupancy/byte evidence plus distinct
                               index_build and recovery_execution split/merge stages
dynamic_index_plans.csv        heap EXPLAIN plus native-API authority proof
dynamic_preflight.json         resolved mode/geometry/API/index fail-fast proof
dynamic_crash_gate_summary.json destructive crash/lifecycle acceptance summary
postgres_memory.csv            remote PostgreSQL RSS/private-PSS samples
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

Dynamic schema-v7 runs use separate, unambiguous counters. Schema v7 binds
the configured logical localisation fanout to native index metadata while
reporting the binary physical node fanout (2) separately. The candidate
payload is canonical key/route/tuple-hash summaries, not full heap rows:

| Counter | Dynamic acceptance meaning |
|---|---|
| `dynamic_candidate_summary_items_fetched` | Both replicas combined; hard limit `2 * 75 * 32 = 4800` |
| `candidate_summary_bound_ok` | Must be `1` before comparison or repair starts |
| `candidate_summary_bytes` | Encoded key plus two 32-byte digests |
| `exact_healthy_heap_rows_fetched` | Full healthy heap rows; exactly 300 for update-only acceptance |
| `full_damaged_heap_rows_fetched` | Must remain `0` |
| `dynamic_storage_seq_scan_delta` | Timed node/leaf-item side-table scans; must remain `0` |
| `dynamic_native_api_check_count` | Healthy/damaged API authority checks; exactly `2` |
| `dynamic_native_api_authority_failures` | Native-index-page authority mismatches; must remain `0` |

For dynamic runs, `bad_leaf_count` remains the configured 75 physical
corruption-selection leaves. `bad_range_count` is the number of bounded
configured-fanout logical ranges produced by shape-independent localization; the two
counts need not be equal because logical boundaries may cut physical leaves.

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

Static results produced by this driver use:

```text
benchmark_schema_version = 3
timing_contract_version  = 1
```

Version 3 is the Merkle-only static-recovery benchmark.
Version 4 was the initial dynamic-recovery benchmark.
Version 5 introduced the fail-closed 4,800-summary bound and node/leaf-item
side-table plan evidence. Version 6 made recovery consume logical fanout 32
per localisation level and recorded physical node fanout 2 separately. Version
7 accepts configured logical fanouts 2, 4, 8, 16, or 32 and records SQL RTT and
client/server endpoint evidence in `network_probe.json`.
Version 2 was the archived three-method CTA/disk/Merkle benchmark.
Do not pool v2, v3, v4, v5, and v6 results without an explicit comparison design.
