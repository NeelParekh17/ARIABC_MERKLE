# AriaBC Merkle Recovery Benchmark

This directory contains a self-contained benchmark for the existing static partitioned Merkle index design. It compares:

- Merkle-guided selective logical repair
- CTA full-copy recovery
- Binary `COPY` disk snapshot plus full restore

The benchmark uses two schemas in one PostgreSQL instance:

```text
healthy.usertable
damaged.usertable
```

Generated output is written under `scripts/benchmark/recovery/results/<timestamp>/`.

For remote synced-tree validation, use:

```bash
scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh \
  --host admin123 \
  --ssh-user neel \
  --profile smoke \
  --artifact-mode summary
```

All result directories produced before benchmark schema version 2 are invalid
for paper plots because `paper_total_ms` included audit time for CTA and disk
but excluded audit time for Merkle. Do not reuse or replot existing pre-v2
CSV/SVG files.

New outputs record:

```text
benchmark_schema_version = 2
timing_contract_version = 1
```

## One-command smoke run

Use this for validation before a large paper-style run:

```bash
./.venv/bin/python3 scripts/benchmark/recovery/run_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile smoke
```

## One-command paper-style run

This runs the 1M, 3M, and 5M Figure 12-style experiment plus the 3M Figure 13-style sweep with five repetitions:

```bash
./.venv/bin/python3 scripts/benchmark/recovery/run_recovery_benchmark.py \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel" \
  --profile paper
```

## Output Files

Each result directory contains:

```text
config.json
environment.txt
python_environment.json
host_info.json
source_snapshot.json
dataset_sizes.csv
runs.csv
phase_timings.csv
timing_contract.csv
planner_checks.csv
bucket_consistency_summary.csv
schema_fidelity.csv
corruption_manifest.json
verification_results.csv
stdout.log
stderr.log
plots/
```

The synced remote flow packages a compact archive from an isolated run
directory and rejects pre-v2 benchmark results, `.copybin` disk snapshots, and
runtime/build directories such as `src/`, `install/`, `pgdata/`, and
`scratch/`.

The Merkle path uses two batched `merkle_get_partition_root_hashes` calls, then descends only through mismatching child hashes from `merkle_get_child_hashes`. Candidate rows are fetched through the functional B-tree indexes on `merkle_bucket_for_key(...)`.

`merkle_bucket_for_key(...)` is a static-geometry benchmark helper. For the lifetime of a functional leaf-lookup index, partitions, leaves per partition, and fanout must not change. Any geometry change requires dropping the functional lookup index, dropping the Merkle index, creating the new Merkle index, creating the new functional lookup index, and running `ANALYZE`.

The smoke profile uses the same code path with small row counts. The preflight profile runs the paper-shaped validation at 1M rows before the paper profile. Because catalog functions from `pg_proc.dat` are initialized into the system catalog, validation after adding a new built-in function requires a freshly initialized PostgreSQL data directory, not just a server restart.
