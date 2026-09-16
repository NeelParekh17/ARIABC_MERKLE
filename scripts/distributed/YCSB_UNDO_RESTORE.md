# Logical undo for the 100M-row YCSB workloads

`ycsb_undo.py` reads the exact workload before execution, saves the original
rows for every distinct write key, and emits a standalone `restore.sql`.
Repeated writes to one key need only one before-image. Keys absent at capture
time are recorded too, so inserts can be removed. Partial workload completion
and applying the same restore file twice are supported.

The September 15 Workload A files contain:

| Skew | Statements | Updates | Distinct write keys |
|---|---:|---:|---:|
| 0.0 | 20,000 | 9,983 | 9,983 |
| 0.99 | 20,000 | 10,008 | 6,900 |

## Generate and apply a restore file

Use a dedicated benchmark database. Capture before-images **before** executing
the workload, and stop/drain every workload writer before restoring. Keep the
capture directory for as long as the working database is reused.

Run these commands on the database host (adjust the binary path and port):

```bash
export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib
python3 ycsb_undo.py prepare \
  --psql /home/neel/Desktop/ariabc_install/bin/psql --port 5548 \
  --workload workload.sql --output-dir before_case

# Run the workload through PG, DET, or Merkle, then stop/drain its writers.

/home/neel/Desktop/ariabc_install/bin/psql \
  -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5548 -U postgres -d postgres \
  -f before_case/restore.sql

python3 ycsb_undo.py verify \
  --psql /home/neel/Desktop/ariabc_install/bin/psql --port 5548 \
  --output-dir before_case
```

The restore runs ordinary PostgreSQL SQL in one transaction, regardless of the
workload execution mode. Merkle maintenance stays enabled when a Merkle index
exists. It deletes newly inserted keys, updates changed rows, and reinserts
deleted rows. An exact bidirectional row comparison runs before commit; a
mismatch aborts the transaction. The log prints SHA-256 hashes of both saved
and restored affected rows. For Merkle indexes, capture also saves their roots;
the restore file checks those roots after commit, when staged tree updates have
been applied. A post-commit root failure stops psql with an error and the working
database must not be reused. `verify` independently compares the affected-row
COPY stream with its captured SHA-256 and checks any saved Merkle roots.

The parser deliberately accepts only literal-key, single-table YCSB statements.
Unknown SQL, primary-key updates, expressions, other tables, user triggers,
foreign keys, partitioned tables, and row-level security are rejected. Restoring
arbitrary SQL would require tracking all its side effects.

## Full verification

Before the first workload and after restoration, run:

```bash
python3 ycsb_undo.py hash \
  --psql /home/neel/Desktop/ariabc_install/bin/psql --port 5548
```

This streams every `usertable` key and field in primary-key order into SHA-256
with bounded client memory. Compare both `sha256` and `rows`. It covers logical
row contents; it does not hash PostgreSQL system catalogs or execution history.
The affected-row check cannot detect damage outside the captured keys. A full
hash can, but requires scanning the entire table and is timed separately.

For Merkle mode, also compare `merkle_root_hash('usertable')` with its baseline
and require `merkle_verify('usertable') = true`. The latter recomputes the hash
from the heap rather than trusting only the stored root.

## Reproduce the three-mode validation

The validator creates one independent copy of `pgdata_base` in a new directory,
runs the actual server/gateway with direct completion validation, and checks
the full-table hash after every undo. Merkle runs first; its indexes are then
removed for PG and DET. PostgreSQL restarts clear volatile DET state before
gateway request IDs are reused. All original data and artifacts are retained.

```bash
python3 scripts/distributed/validate_ycsb_undo.py \
  --remote-dir /tmp/ariabc_undo_validation_NEW \
  --output-dir scripts/bench_full_results/undo_validation_NEW \
  --workloads \
    scripts/bench_full_results/oom_100m_sweep/run_20260915_012215_3cb02623/workloads/ycsb_a_skew_0.0_20000.sql \
    scripts/bench_full_results/oom_100m_sweep/run_20260915_012215_3cb02623/workloads/ycsb_a_skew_0.99_20000.sql
```

Each case saves the actual restore file, original rows, hashes, timings, gateway
completion counters, settings, and logs. `COMPLETE.json` exists only after all
cases pass. This experiment does not drop global caches and is not a cold-cache
throughput benchmark. Restore wall time includes its transaction commit and
affected-row checks; full hashing and PostgreSQL restart are separate.

## Scope and benchmark implications

Logical undo does not recreate the original physical database: dead tuples,
WAL, transaction IDs, page placement, index growth, statistics, Merkle topology,
and execution history can differ. Repeated undo can therefore change subsequent
I/O and throughput. The existing OOM sweep retains its physical-copy reset
default. A one-second end-to-end reset is a measurement target, not a guarantee,
especially when it includes full-table verification or cold-cache preparation.

Tests use a disposable PostgreSQL instance:

```bash
YCSB_UNDO_TEST_PORT=5549 YCSB_UNDO_TEST_PSQL=/work/ARIABC/install/bin/psql \
  python3 -m unittest scripts.distributed.tests.test_ycsb_undo -v
```
