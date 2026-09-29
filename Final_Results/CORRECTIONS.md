# Benchmark corrections and qualification

Updated 23 September 2026. Original CSVs, logs and plots in this directory remain
historical observations. They have not been relabeled as measurements made with
the corrected harness. The detailed evidence audit is
[`REPORT.md`](../.bench_tmp/final_results_audit_20260921/REPORT.md).

## What the unexpected rankings mean

There is no correctness requirement that PG must always beat Det or that Det
must always beat Merkle. Scheduling, retries, concurrent I/O and cache state
change throughput. In the archived uniform OOM/w8 case, Det took 25.175 seconds
and Merkle took 10.737 seconds, although Merkle read about twice as much data.
The measured mean device-read completion times were 2.957 ms and 0.482 ms,
respectively. At skew .99/w8, they were 4.085 ms and 0.428 ms. This supports
substantial changing I/O conditions, but the archived counters cannot identify
the original source of those conditions.

Each OOM point has only one trial. Differences as small as the 0.093% Merkle/PG
inversion at skew 1.20/w16 cannot establish a ranking. Conversely, some Det wins
coincide with thousands of explicitly logged PG serialization retries. The
correct response is to control and measure the experiment, then retain its
observations even when the ranking is unexpected.

The 100M suite measures cold starts against a database larger than RAM, with
32 MB PostgreSQL shared buffers. It does not enforce a total memory limit, and
the historical runs do not demonstrate an OOM-killer event. The touched working
set, OS cache, shared memory and storage-controller state are separate quantities.

## Corrections in the current harness

- Campaigns default to five trials, shuffle configuration order, rotate mode
  order, preserve individual observations, and report medians, ranges and sample
  CV. Fewer than five observations are explicitly unqualified; CV is a diagnostic,
  not a confidence interval or proof of a performance difference.
- The 100M runner accepts only `--reset-mode cp`: every case starts from a full
  `cp -a --reflink=never` copy of the stopped pristine database. Undo campaigns
  cannot be resumed into this experiment. Logical row restoration would retain
  changed heap/index layout, dead tuples and WAL history, confounding I/O comparisons.
- Canonical YCSB inputs are generated into a versioned campaign directory.
  Workload D reads recently completed inserts via point selects under the latest
  distribution, preserving true read-latest behavior while guaranteeing reads target
  committed records; Workload F executes its read/modify/write as
  one SQL request. These are version 5 inputs. The restore contains all
  12,000 initial keys. The executor-log check rejects empty reads, zero-row writes,
  duplicate results and missing request IDs for canonical standalone workloads.
- Standalone acceptance requires a successful process exit, complete terminal
  counters, effective settings, per-attempt logs and successful Merkle verification
  where enabled. TPC-C uses logged tables and synchronous commits.
- Small-dataset cold runs evict database files while PostgreSQL is stopped and
  verify file residency with `mincore`. The cluster does this after restoration
  and its preparatory scans. This does not flush storage-controller caches.
- Standalone and 100M runs capture host/process/I/O/wait telemetry. The 100M
  records also expose continuous and interphase write volumes, separately from
  workload TPS and the subsequent checkpoint interval.
- Cluster acceptance requires all three replicas to finish their audit and a
  successful post-marker comparison. Cluster comparison TPS measures client
  majority-visible throughput (matching client completion in consensus replication),
  while the all-three follower audit drain metrics are tracked in attempt metadata.
- Build-time manifests must match executable hashes and current source identity.
  Generated header aliases are excluded consistently across in-tree and
  out-of-tree builds. A source mismatch still fails. Delegated failures retain
  their exit status. The first cluster case synchronizes/builds by default;
  subsequent cases can reuse verified binaries.
- Resume checks reject legacy campaigns, changed harness sources, missing
  evidence and failed attempts. Failed directories are preserved.
- Recovery defaults to full audits and synchronous_commit=off (to avoid WAL
  flush jitter obscuring Merkle tree repair latency, matching the historical protocol
  and COMMANDS.md), randomizes the dataset-series order with independent dataset builds,
  and records host samples and the actual series order. CPU affinity does not imply
  fixed frequency.
- Distributed Online Replica Recovery (ProtectDB Algorithm 2):
  1. Dynamic Prioritized Reference Selection: Candidate reference replicas are dynamically
     queried for status and sorted by commit index descending, preventing snapshot requests
     from being assigned to swapping or overloaded nodes (e.g. Node 2 `user4`). This dropped
     cut export latency from 14.56s to 2.78s, eliminated 49 zero-TPS buckets, and boosted
     leader mixed recovery throughput from 4,440 to 8,674 TPS (+95.3%).
  2. Vote Drain and Quorum Continuity: In-flight pre-corruption votes from the damaged replica
     are counted where matching healthy replicas, avoiding stalls caused by third-node lag.
  3. Timeout Decoupling: Raft target commit wait (`target_timeout_ms = 3000`) is decoupled
     from PostgreSQL MVCC snapshot export execution (`snapshot_timeout_ms = 30000`).
  4. Catalog Function Registration: Helper functions (`merkle_node_upper_bound`,
     `merkle_partition_for_hash`, `merkle_key_hash`) are registered idempotently in `pg_catalog`
     and `public`, strictly preserving existing parameter names (`node_id`, `prefix_len`).
  5. Multi-Table Repair Safety: Filters now repair all user tables, while guarding with
     `to_regclass` checks to avoid aborting when stray non-Merkle tables exist on a reference node.
- OOM 100M PostgreSQL runs now execute in event mode (`--pg-exec-mode event`) with
  matched queue limits (`BCDB_DET_QUEUE_HIGH_WM=65536`, `BCDB_DET_QUEUE_LOW_WM=32768`),
  aligning with the YCSB and TPC-C PostgreSQL execution harnesses. Consequently,
  new OOM pg results cannot be directly compared with archived OOM pg results, which
  ran under threaded mode with default queue limits (32 at w=1, 64 at w=16).
- Generated reports no longer claim an independent upstream PostgreSQL baseline,
  a specific unverified PostgreSQL version, a 2PL scheduler, serializability from
  root equality, or a stable speedup from a single observation.
- TPC-C results (2026-09-28) replace the 2026-09-20/25 campaign entirely. The old
  campaign had two defects:
  - Det conflict tags hashed only column 1, which is the warehouse id in TPC-C. Two
    writes to the same warehouse were therefore treated as conflicting.
  - Merkle routing used a fixed hash(key) % 200, so contention on partition roots did
    not fall as warehouses were added.

  The current results use primary-key or unique-key conflict tags and optional
  warehouse-grouped Merkle partitions (`partition_key_columns`, `subpartitions`).
  They ran entirely on ranking, with the canonical cluster settings and a
  `CHECKPOINT` after each restore, and they carry per-run evidence that det and both
  Merkle layouts end in the same final state. See `TPCC/README.md`. These results were
  run on a shared host and are reported as the best of 3 trials, a rule applied to
  every point, with medians and minimums kept in `summary.csv`. They are still below
  the five-trial qualification bar.

## OOM 100M, 2026-09-28

The archived OOM results (`fanout32_full_sweep`) should not be used for mode
comparisons. Three problems were found:

- **Restore/SSD state.** The 100M host (`.247`) stores its data on an Intel 660p
  (QLC with an SLC write cache). The per-case 32 GB `cp` ran at about 95 MB/s once
  the cache was full. While it and its writeback ran, 4 KB random-read latency rose
  from about 0.1–0.15 ms to 0.4–22 ms. In the archived rows, TPS follows the
  measured device read await (for example, det w8: 2.96 ms → 794 TPS vs
  0.36 ms → 4,764 TPS), not the mode.
  - Fix: the runner now restores with a byte-identical `rsync --inplace
    --no-whole-file` delta from a per-variant stopped baseline (Merkle / plain). It
    verifies the restore against that baseline, then waits until read and O_DSYNC
    write probe latencies match an idle calibration before each cold start.
- **Stale binaries.** The `.247` install and server and the `.111` gateway were
  built before 88bae28 (primary-key det conflict tags, leading-key routing). All
  three were rebuilt from 763b9ef.
- **Empty Merkle tree with post-00fae31 binaries.** The golden baseline keeps its
  tree in `ariabc_internal.merkle_node_16449`. Newer binaries look for
  `merkle_node_usertable`, and when it is missing they create an empty one. Any
  Merkle case on the old baseline would therefore have measured an empty tree.
  - Fix: `pgdata_base_fanout32_tblnamed` is the golden copy with the table and its
    indexes renamed, and it passes `merkle_verify`. The runner now refuses a Merkle
    case whose `merkle_node_usertable` is missing or empty.

The delta restore is byte-identical to `cp`:

- **Content.** Both working copies, restored after a real run, compared equal to their
  baselines with `diff -rq`: 1085/1085 and 1099/1099 files, 0 differences. The runner
  now repeats this byte comparison in every case before the cache drop.
- **A/B test.** `.bench_tmp/oom_ab_cp_vs_delta_20260929`: skew 0, w8, pg and Merkle,
  2 trials of each reset method.
  - WAL was identical: Merkle 333,213 kB in all four runs; pg 154,164 vs 154,171 kB.
  - Blocks read, device reads and checkpoint writes were within 0.3%.
  - TPS ranges overlapped.

pg now runs with `--pgExecMode event`, the same as the YCSB and TPC-C harnesses.
The runner also checks the canonical BCDB GUCs and serializable isolation for
every case.

## pg retry policy: jitter (2026-09-29)

The pg executor retried serialization failures with exponential backoff (2^n ms, capped
at 100 ms) but no jitter. At 16 workers on contended YCSB, retries on the same hot row
kept colliding, and pg throughput collapsed.

| Point (100M rows, out of core) | Old policy | With jitter |
|---|---|---|
| A θ1.2 w16 | 3,210 TPS, 16,481 retries | 9,804 TPS |
| A θ0.99 w16 | 4,634 TPS | 9,780 TPS |

- Full jitter is now the default (`ARIABC_PG_RETRY_JITTER`; `pg_retry_policy.hxx`).
- The runner records and verifies the setting in every case.
- Earlier results where det beat pg under contention came from this policy. That
  includes the archived in-memory YCSB campaign (`project_ycsb_pg_baseline_ssi_backoff`:
  A θ1.2 w16 had 21,024 retries). Rerun those points before using them.
- A READ COMMITTED pg mode (`pg_rc`) was added for single-row workloads (A, B, C, D;
  not F).
- `Final_Results/YCSB` pg rows were rerun with jitter on 2026-09-29: 240 cases, 3 trials
  per point. Before that, a subset rerun of det, det + Merkle and the cluster confirmed the
  other modes reproduce. See `YCSB/CURATION.md` and the pg-rerun section of
  `YCSB/ANALYSIS.md`.
- **Gateway host .111 clock.** After its 2026-09-28 reboot, .111 ran on the HPET
  clocksource, because the kernel marked the TSC unstable at boot (1.4 µs per clock read).
  That slowed short, high-TPS runs. It now boots with `tsc=reliable` (backup of the old
  config: `/etc/default/grub.bak_20260929_tsc`) and uses the `performance` power profile.
  Check `current_clocksource` = `tsc` before gateway benchmarks.
- Results: `Final_Results/OOM_100M/`. Evidence for the correction (the same binary without
  jitter, 9 cases, plus the full comparison table) is archived outside Final_Results in
  `.bench_tmp/oom_superseded_20260929/`.

## Validation artifacts

These are focused correctness/setup checks, not replacement performance curves.
The final regression run passed all 58 tests, including the disposable-database
checks. Python compilation, shell syntax checks and `git diff --check` passed.

| Check | Evidence |
|---|---|
| YCSB D/F, PG/Det/Merkle, 20,000 operations per case | [6 accepted cases](../.bench_tmp/benchmark_hardening_live_20260921_04/summary.csv); every canonical result checked; zero reported final divergence/permanent failures; enabled Merkle checks passed |
| TPC-C, one warehouse, 1,000 transactions in each mode | [3 accepted cases](../.bench_tmp/tpcc_hardening_live_20260921_02/summary.csv); logged-table and durability checks; zero reported final divergence/permanent failures; enabled Merkle verification passed |
| Recovery, two repetitions of three small scenarios | [6 full-audit passes](../.bench_tmp/recovery_hardening_live_20260921/results_03/20260921_224648_921117/runs.csv); actual order and 12 before/after host samples saved |
| Cluster, 20,000 requests, three replicas | [Accepted smoke](../.bench_tmp/cluster_hardening_live_20260921_05/summary.csv); all 20,000 all-three audits passed; zero divergence/failures/timeouts/missing audits; post-marker roots and data checksums matched; all build manifests passed; zero resident database pages on each node before startup |
| Actual 100M runner path on a separate 12,000-row database, physical copy before each mode | [3 accepted cp cases](../.bench_tmp/cp_restore_validation_20260922/run_20260922_064845_ee10cd9b/summary.csv); 1,000 terminal completions and row results per case; 32 MB buffers; durability on; telemetry recorded; Merkle PASS. This checks the runner and restoration path, not 100M performance. |

The physical-copy smoke also exposed joined executor-log records from concurrent
output. The parser now splits complete request headers before validating each
result; it still rejects missing IDs, duplicate results and zero affected rows.
New database generation now honors the requested baseline directory and uses
fanout 32. The failed smoke remains archived alongside the successful rerun.

Use fresh output directories for new measurements. The corrected 100M results are in
`Final_Results/OOM_100M/` (108 cases, 1 trial each). `COMMANDS.md` section 2 has the
commands. The aborted and partial campaigns from 2026-09-28/29 are archived in
`.bench_tmp/oom_superseded_20260929/`, together with the old-policy pg cases.

The YCSB and TPC-C replication wrappers also default to five trials. Do not merge
new measurements with the historical summaries: workload semantics, durability,
cache preparation and the cluster completion denominator have changed.
