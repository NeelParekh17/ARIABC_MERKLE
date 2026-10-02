# Benchmark corrections and qualification

## Primary TPC-C figures updated to v2 (2026-10-02)

`TPCC/tpcc_warehouses_scaling.png` and `TPCC/tpcc_workers_scaling.png` now show
the completed **v2** sweep: pg with retry jitter, det and synchronous Merkle,
all at fillfactor 90 and SERIALIZABLE. Merkle uses warehouse routing
16384/1/16, fanout 32, split 1024 / merge 256. The right panels explicitly
label the previous split-32 warehouse-routing and hash-%-200 curves as
different-configuration references. Old figures/README are preserved in
`TPCC/previous/`, and September CSVs/plot scripts are unchanged.

The sweep contains 89 accepted 20,000-transaction attempts over 36 distinct
points: initial pass with stall reruns, reverse-order second pass with reruns,
then two extra attempts each at det W20/32, Merkle W100/16 and det W100/64.
Lines show best of all accepted attempts, bands min–max; README tables include
best/median/min and attempt counts, with ratios using only new denominators.
The raw source `summary.md` still described the first pass; publication uses
the completed status history and accepted observations rather than that text.

`TPCC/tpcc_headline_ab_v2.png` shows C1..C5 mean TPS and decimal WAL kB/tx.
There are 11 accepted headline attempts; C1 trial 2 at 887.78 TPS is excluded
from the headline means as a ranking stall, leaving two included attempts per
configuration. Its evidence remains. HOT fractions pool the same included
attempts. Customer non-HOT falls to about 0.0061%, not exactly zero. Headline
C4/C5 apply FF90 to eight mutable tables and leave immutable `item` at default;
the subsequent sweeps apply FF90 to all nine tables. This distinction is
recorded rather than assuming identical physical configuration.

All 100 measured attempts have saved 20,000 terminal successes, divergence 0,
permanent failures 0 and gateway exit 0; all 35 measured Merkle attempts have
`merkle_verify=9:true`. Saved settings confirm SERIALIZABLE, synchronous
maintenance and durability. det/Merkle state checksums match per W, including
headline versus sweeps at W100. This is an eight-table count/64-bit-hash-sum
projection excluding timestamps and immutable `item`, not full row equality
or three-replica/recovery evidence. No canonical hash or recovery code changed.

Ranking intermittently stalled many attempts despite low observed host load.
The cause remains unknown; suspected NUMA placement is unconfirmed. The pg
W5 third attempt returned to the prior publication's throughput. Unequal
best-of-attempt counts and small headline samples prevent a stable or isolated
causal performance claim. Slow sweep attempts remain in medians/min–max bands.

Read-only SSH/rsync fetched 1,730 evidence files (87,743,209 bytes) with remote
SHA-256 checksums into `TPCC/v2_20261002/{sweeps,headline_ab}/`; no pgdata,
ptrace trees or bulk server logs were copied. A local saved-file audit checked
hashes, raw terminal logs, configurations, table options, WAL and state records.
No build, database instance or benchmark ran during publication. Reproduction
of the publication and previous plots is documented in
[COMMANDS.md, section 3](COMMANDS.md#3-tpc-c--v2-primary-publication-2026-10-02).

## Primary OOM publication updated to fresh v2 (2026-10-02)

`OOM_100M/figures/` now uses only the completed 2026-10-01 **fresh v2** campaign:
84 accepted SERIALIZABLE cases (pg with jitter, det, synchronous det + Merkle),
including read-only C at every worker count. All cases validated 20,000 terminal
results, with zero reported divergence, permanent failures and exhausted retries;
all 28 Merkle cases passed post-workload verification. The publication audit also
checked all 1,680,000 successful unique per-case server request IDs and all seven
published SQL hashes against the saved files. This is saved-evidence validation,
not a new remote run.

V2 uses a fresh 100M-row **fillfactor-90 heap for every mode**, 200 Merkle
partitions, fanout 32, split 1024 / merge 256, compact rebuild storage and the
frozen working-tree code including uncommitted Merkle changes. PG, server and
gateway have v2 build provenance; the final successful source snapshot hash is
`b376a4586ee96f263db61190f9dcdd69a330f2478cf3181b8e8135dc368cf171`.
The intermediate launch-report hash is not the successful build identity.
Merkle leaf/ancestor maintenance remains synchronous through the partition root
inside the transaction; canonical row-hash format remains version 1.

The primary series are updated because all three modes now have matched fresh
measurements on the current configuration. Ratios use **v2 det/pg**, rather than
mixing new Merkle with September denominators. A separate labeled comparison
shows previous split-32 and split-1024 observations, including the earlier rerun's
historical det/pg denominator caveat. Different datasets/configurations/builds
and one trial per point prevent single-change attribution or stable rankings.
The earlier paired fillfactor experiment observed Merkle benefits and modest det
costs; fillfactor 90 is an explicit configuration choice, not an isolated gain.
PG READ COMMITTED was intentionally not rerun.

The consolidated CSV preserves 136 historical rows' existing fields and appends
84 v2 rows (220 total), with `campaign`/`fillfactor` columns. Earlier primary
figures are retained under `OOM_100M/figures/previous/`, and the entire preceding
README content remains under "Previous results". Original old OOM pgdata was
deleted at the user's request, so it cannot reproduce those physical baselines.
Compact curated evidence, unchanged per-case files, path mapping and checksums
are in `OOM_100M/runs/v2_20261001/`. Duplicate SQL is linked by identical hash;
no source/build tarballs were copied. Their checksums and original .111 location
are recorded in `remote_bulk_sha256.txt` and `CURATION.md`.

Failed build preparation produced no data and is retained separately. An aborted
first load used an unattainable 12288MB MemAvailable guard; after the guard became
4608MB and the partial load was deleted, one clean generation completed. Stale
same-size source/build state on .247 was moved aside; snapshot rsync now uses
`--checksum`. These failures are preparation history, not failed measured cases.
Exact completed commands and repeatable publication checks are in
[COMMANDS.md, section 2](COMMANDS.md#2-oom-100m--fresh-v2-primary-result).

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
- **A/B test.** `Final_Results/evidence/oom_ab_cp_vs_delta_20260929/`: skew 0, w8, pg and Merkle,
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
- `Final_Results/TPCC` pg was rerun with jitter on ranking on 2026-09-29 (45 runs, including extra trials at 8 and 48 workers).
  - The server was rebuilt with only the jitter patch; det reproduced with the same restart counts.
  - pg's best-of-3 rose 15–23% at 5–30 warehouses and 15% at 64 workers; other points changed by −3% to +7%.
  - See `TPCC/README.md`, section "pg rerun".
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
  `10.129.27.111:~/claude_ctl/archive/bench_tmp_20261002/oom_superseded_20260929/` (moved off the workstation on 2026-10-02).

## OOM split geometry and method clarification (2026-10-01)

The published 2026-09-29 Merkle curve used **fanout 32, split threshold 32 and
merge threshold 8**. Fanout is not the split threshold. Its 108 cases remain
valid observations of that configuration; they are not superseded or relabeled.
The 2026-10-01 addition contains 24 Merkle cases with split 1024 / merge 256 and
optimized PostgreSQL, plus four canonical det drift controls: 136 consolidated
cases. Separate mode labels preserve both Merkle series and the published det
comparison denominator. Read-only C was not rerun.

The earlier README's attribution of the whole Merkle penalty to node-page I/O
was too strong: synchronous SQL/executor work, hashing, buffers, WAL and
contention also contribute. The new baseline is rebuilt and manually compacted
with `VACUUM FULL`; the October campaign changes geometry, code and physical
baseline together, so it does not isolate a code-only speedup. The campaign
handoff reports a separate 2–4% code-only microbenchmark, whose paired raw
observations are not included here. New/old TPS is 1.022–1.809× across matching
points; the four det controls range −6.60% to +13.88%. Single trials do not
establish stable rankings.

The published method uses `delta_content_check=sampled`: full byte comparisons
on first restore and every tenth restore, with size/mtime checks every case.
The earlier statement above that full byte comparison repeats in every case
is therefore superseded by the actual archived campaign settings. The source
identity for optimized PostgreSQL is handoff-reported HEAD 19562ae plus
uncommitted changes; per-case executable SHA-256 hashes identify what ran but
are not an immutable source snapshot. See [OOM_100M/README.md](OOM_100M/README.md)
and [COMMANDS.md, section 2a](COMMANDS.md#2a-oom-100m--split-1024--optimized-code-2026-10-01).

Historical READ COMMITTED data remains labeled and archived; it is omitted from
current SERIALIZABLE figures/campaigns and is not evidence that READ COMMITTED
in general provides SERIALIZABLE semantics.

## Validation artifacts

These are focused correctness/setup checks, not replacement performance curves.
The final regression run passed all 58 tests, including the disposable-database
checks. Python compilation, shell syntax checks and `git diff --check` passed.

| Check | Evidence |
|---|---|
| YCSB D/F, PG/Det/Merkle, 20,000 operations per case | [6 accepted cases](evidence/benchmark_hardening_live_20260921_04/summary.csv); every canonical result checked; zero reported final divergence/permanent failures; enabled Merkle checks passed |
| TPC-C, one warehouse, 1,000 transactions in each mode | [3 accepted cases](evidence/tpcc_hardening_live_20260921_02/summary.csv); logged-table and durability checks; zero reported final divergence/permanent failures; enabled Merkle verification passed |
| Recovery, two repetitions of three small scenarios | [6 full-audit passes](evidence/recovery_hardening_live_20260921/results_03/20260921_224648_921117/runs.csv); actual order and 12 before/after host samples saved |
| Cluster, 20,000 requests, three replicas | [Accepted smoke](evidence/cluster_hardening_live_20260921_05/summary.csv); all 20,000 all-three audits passed; zero divergence/failures/timeouts/missing audits; post-marker roots and data checksums matched; all build manifests passed; zero resident database pages on each node before startup |
| Actual 100M runner path on a separate 12,000-row database, physical copy before each mode | [3 accepted cp cases](evidence/cp_restore_validation_20260922/run_20260922_064845_ee10cd9b/summary.csv); 1,000 terminal completions and row results per case; 32 MB buffers; durability on; telemetry recorded; Merkle PASS. This checks the runner and restoration path, not 100M performance. |

The physical-copy smoke also exposed joined executor-log records from concurrent
output. The parser now splits complete request headers before validating each
result; it still rejects missing IDs, duplicate results and zero affected rows.
New database generation now honors the requested baseline directory and uses
fanout 32. The failed smoke remains archived alongside the successful rerun.

Use fresh output directories for new measurements. The corrected 100M results are in
`Final_Results/OOM_100M/` (108 cases, 1 trial each). `COMMANDS.md` section 2 has the
commands. The aborted and partial campaigns from 2026-09-28/29 are archived in
`10.129.27.111:~/claude_ctl/archive/bench_tmp_20261002/oom_superseded_20260929/` (moved off the workstation on 2026-10-02), together with the old-policy pg cases.

The YCSB and TPC-C replication wrappers also default to five trials. Do not merge
new measurements with the historical summaries: workload semantics, durability,
cache preparation and the cluster completion denominator have changed.
