# YCSB on a 100M-row database larger than memory (OOM)

**Published 2026-09-29; split-1024 campaign added 2026-10-01.** The consolidated
CSV contains 136 accepted cases: the original 108, 24 new Merkle cases, and four
det drift controls. All completed and validated 20,000 statements, with zero
reported divergences and permanent failures. All 52 Merkle cases passed full
`merkle_verify`. These are standalone direct-completion runs, not a replicated
cluster or recovery-fault test.

The split-32 results remain valid observations of the previous configuration.
The new series is labeled `bcdb_merkle_s1024`; controls are labeled `det_control`
and never replace published `bcdb_det`. There is no new read-only C measurement.

## Setup and synchronous integrity contract

| Item | Value |
|---|---|
| DB host | `neel@10.129.148.247`, about 15 GiB RAM, Intel 660p QLC NVMe |
| Gateway | `10.129.27.111`, 96 terminals and 96 deterministic client workers, direct completion |
| Data | `usertable`, 100,000,000 rows; 200 Merkle partitions, fanout 32 |
| Buffers/cache | `shared_buffers=32MB`; OS caches dropped before every case; subsequent OS page cache is unbounded |
| Durability | `fsync=on`, `full_page_writes=on`, `synchronous_commit=on` |
| Current benchmark isolation | SERIALIZABLE; PostgreSQL serialization failures retried by the client executor |
| Integrity maintenance | `merkle_apply_synchronous_direct=on`: leaf and ancestors through the partition root updated inside the user transaction, exact at commit |
| Work | Same hashed YCSB SQL inputs as the published run; 20,000 statements; workers 1/4/8/16 |
| Trials | One per point in both campaigns; controls at A/F θ0.99, workers 1/16 |

Historical `pg_rc` observations are retained as labeled archival data. They are
excluded from the default figures and from the new campaign; this publication
does not establish SERIALIZABLE semantics for READ COMMITTED workloads.
Canonical row-hash bytes and recovery semantics were required to remain unchanged.
The archived setup reports row-hash format version 1; verification confirms the
measured tree, but is not evidence of crash-recovery or replica-repair safety.

## What changed in the 2026-10-01 campaign

The baseline was copied from `pgdata_base_fanout32_tblnamed` into the separate
`/tmp/ariabc_oom_100m/pgdata_base_f32s1024`. The campaign handoff records dropping
`usertable_merkle_idx` and rebuilding with the optimized install using
`WITH (partitions=200, fanout=32)`. The new build defaults select split threshold
1024 and merge threshold 256, versus published split 32 / merge 8. Every new
case's `setup.json` independently records 211,400 nodes and 204,800 leaves before
the workload, about 488 rows per leaf on average (100,000,000 / 204,800).

The handoff also records `VACUUM FULL ariabc_internal.merkle_node_usertable`
after rebuild: the old build path left the dropped tree's rows as dead tuples;
node heap storage fell from approximately 780 MB to 24 MB, with a 6.5 MB primary
key index. This storage-size account is handoff evidence, not a value captured
by the case I/O counters. The new campaign therefore measures geometry,
optimized code, and the rebuilt/compacted physical baseline together.
The rebuild/compact scripts and log were not present under `.bench_tmp` at
publication; their roles and exact remote fetch paths are in
[`BASELINE_PROVENANCE.md`](campaign_s1024_20261001/BASELINE_PROVENANCE.md).

The optimized path includes routing TID hints, one traversal snapshot, lazy
executor index state, corrected `PG_TRY` handling, and cached send-function
lookups. The [campaign handoff](campaign_s1024_20261001/CAMPAIGN_HANDOFF.md)
reports only about 2–4% improvement from code optimizations in a separate `.247`
microbenchmark. Its raw paired observations are not included in this archive,
so that percentage is a reported result rather than a recomputation here.
Together with the smaller tree and lower measured I/O, this supports geometry
as the main explanation; this campaign does not isolate each optimization's
contribution or separate geometry from compaction.

## Binary and source provenance

The handoff records git HEAD **19562ae + uncommitted changes** for the optimized
PostgreSQL build. It is not a reproducible clean-commit identity or a complete
immutable source snapshot. The run's `preflight.txt` and every `setup.json`/
`result.json` record these executable SHA-256 values (identical across cases):

| Executable | SHA-256 |
|---|---|
| `.247` `/home/neel/claude_opt/install_opt/bin/postgres` | `30277981279b2b40ad88d8eb0b4585d82dae939161fbdaf3b2b02a6e29ed4165` |
| `.247` control `/home/neel/Desktop/ariabc_install/bin/postgres` | `6abec3b60f8643b32fc4686fd7a4fcb5ac1e31816d0fc5445963b7b2a05ff278` |
| `.247` canonical `ariabc_pg_server` | `2b5dd1197efed60a14b60d1965bcf39b95a1db3ae99ef2aae7bb59d27ac1e501` |
| `.111` canonical `ariabc_pg_gateway` | `03731bcba093cf8e6e02a0385481b592aaffc0d8c6a0cf83a6ba65e0471da15a` |

Published September results used the 763b9ef build with the server retry-jitter
patch. The October controls use the canonical install and the existing
`pgdata_base_fanout32_tblnamed_plain`; Merkle uses `install_opt` and the new
Merkle baseline. `campaign.json` saves harness/input hashes, while per-case
provenance saves baseline manifests and executable hashes.

## Method and timing boundaries

1. Restore from the stopped baseline for the selected variant using byte-identical
   `rsync --inplace --no-whole-file` delta copying. Check size/mtime each case;
   the campaign's `delta_content_check=sampled` compares full file contents on
   first restore and every tenth restore, not on every case.
2. Check relations, baseline identity, geometry and effective settings. Wait
   until QD1/QD8 reads and O_DSYNC write probes are within 1.25× idle calibration.
3. Drop OS caches, start PostgreSQL cold, run the SERIALIZABLE workload, and
   validate every statement's terminal result.
4. Measure a separate `CHECKPOINT`, then restart and perform full Merkle
   verification for Merkle cases.

TPS uses gateway workload wall time; restore, settle, separate checkpoint and
verification are excluded. Timed device counters exclude checkpoint and
verification but use their own sampling interval, which is slightly wider than
gateway wall time. Device counters include unrelated host traffic. PostgreSQL
`blks_read` counts buffer misses, which can hit OS cache rather than the device.
See [CORRECTIONS.md](../CORRECTIONS.md#oom-100m-2026-09-28) for delta-vs-copy validation.

## Merkle comparison: all 24 matching points

TPS is statements/s. Both Merkle/det ratios use **published September det**;
new/old compares October split 1024 against September split 32. Values are
rounded for display; exact values and I/O are in
[`comparison.csv`](campaign_s1024_20261001/comparison/comparison.csv).

| Workload | Workers | det TPS | Merkle split 32 TPS | Merkle split 1024 TPS | S32/det | S1024/det | S1024/S32 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A θ0.0 | 1 | 1,427.246 | 907.359 | 1,170.138 | 0.636 | 0.820 | 1.290 |
| A θ0.0 | 4 | 3,078.818 | 1,786.352 | 2,302.821 | 0.580 | 0.748 | 1.289 |
| A θ0.0 | 8 | 3,743.215 | 2,165.205 | 2,989.090 | 0.578 | 0.799 | 1.381 |
| A θ0.0 | 16 | 4,977.601 | 2,536.462 | 3,964.321 | 0.510 | 0.796 | 1.563 |
| A θ0.99 | 1 | 2,149.382 | 1,318.652 | 1,794.205 | 0.614 | 0.835 | 1.361 |
| A θ0.99 | 4 | 3,907.776 | 2,272.469 | 2,929.544 | 0.582 | 0.750 | 1.289 |
| A θ0.99 | 8 | 5,178.664 | 2,916.727 | 4,518.753 | 0.563 | 0.873 | 1.549 |
| A θ0.99 | 16 | 7,296.607 | 3,908.540 | 5,934.718 | 0.536 | 0.813 | 1.518 |
| A θ1.2 | 1 | 2,641.659 | 1,987.874 | 2,266.546 | 0.753 | 0.858 | 1.140 |
| A θ1.2 | 4 | 4,692.633 | 3,000.300 | 3,770.739 | 0.639 | 0.804 | 1.257 |
| A θ1.2 | 8 | 6,651.147 | 3,768.607 | 5,064.573 | 0.567 | 0.761 | 1.344 |
| A θ1.2 | 16 | 7,880.221 | 4,531.038 | 6,161.429 | 0.575 | 0.782 | 1.360 |
| B θ0.99 | 1 | 4,030.633 | 3,402.518 | 3,478.261 | 0.844 | 0.863 | 1.022 |
| B θ0.99 | 4 | 8,357.710 | 6,578.947 | 7,471.050 | 0.787 | 0.894 | 1.136 |
| B θ0.99 | 8 | 10,822.511 | 8,532.423 | 9,675.859 | 0.788 | 0.894 | 1.134 |
| B θ0.99 | 16 | 13,821.700 | 10,235.415 | 11,210.762 | 0.741 | 0.811 | 1.095 |
| D θ0.99 | 1 | 3,932.363 | 3,159.558 | 3,442.933 | 0.803 | 0.876 | 1.090 |
| D θ0.99 | 4 | 8,517.888 | 6,437.078 | 7,228.045 | 0.756 | 0.849 | 1.123 |
| D θ0.99 | 8 | 11,191.942 | 7,880.221 | 9,337.068 | 0.704 | 0.834 | 1.185 |
| D θ0.99 | 16 | 13,422.819 | 9,478.673 | 11,254.924 | 0.706 | 0.838 | 1.187 |
| F θ0.99 | 1 | 2,013.288 | 1,298.533 | 1,549.547 | 0.645 | 0.770 | 1.193 |
| F θ0.99 | 4 | 3,872.217 | 2,191.060 | 2,921.841 | 0.566 | 0.755 | 1.334 |
| F θ0.99 | 8 | 5,275.653 | 2,683.483 | 3,954.132 | 0.509 | 0.750 | 1.474 |
| F θ0.99 | 16 | 7,499.063 | 3,269.577 | 5,915.410 | 0.436 | 0.789 | 1.809 |

Across these points, split 1024 + optimized code reaches 1.022–1.809× split-32
TPS and 0.748–0.894× published det. On write-heavy A/F the new/old range is
1.140–1.809×. These are observed differences, not stable speedup estimates.
Read-only C retains its published curves only.

## Timed device I/O per statement

**KB = 1000 bytes.** Each value is `device_*_mib × 2^20 / (1000 × total_queries)`.
Checkpoint writes are separate and remain in the summary/comparison CSVs.

| Workload | Workers | det read KB | S32 read KB | S1024 read KB | det write KB | S32 write KB | S1024 write KB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A θ0.0 | 1 | 17.238 | 35.462 | 23.297 | 14.629 | 25.584 | 20.309 |
| A θ0.0 | 4 | 17.154 | 35.343 | 23.293 | 12.750 | 23.222 | 18.189 |
| A θ0.0 | 8 | 17.146 | 35.343 | 23.306 | 11.347 | 22.337 | 16.824 |
| A θ0.0 | 16 | 17.163 | 35.301 | 23.279 | 10.119 | 21.384 | 15.607 |
| A θ0.99 | 1 | 7.723 | 20.623 | 11.838 | 9.313 | 16.831 | 12.895 |
| A θ0.99 | 4 | 7.695 | 20.612 | 11.851 | 7.816 | 15.456 | 11.584 |
| A θ0.99 | 8 | 7.721 | 20.601 | 11.837 | 6.550 | 14.552 | 10.252 |
| A θ0.99 | 16 | 7.685 | 20.617 | 11.830 | 5.521 | 13.373 | 9.258 |
| A θ1.2 | 1 | 2.397 | 7.948 | 4.474 | 5.209 | 9.505 | 7.965 |
| A θ1.2 | 4 | 2.409 | 7.992 | 4.477 | 4.243 | 8.509 | 6.725 |
| A θ1.2 | 8 | 2.399 | 7.986 | 4.492 | 3.287 | 7.672 | 5.704 |
| A θ1.2 | 16 | 2.399 | 8.005 | 4.503 | 2.786 | 7.029 | 5.148 |
| B θ0.99 | 1 | 7.726 | 9.728 | 8.663 | 1.193 | 2.673 | 2.327 |
| B θ0.99 | 4 | 7.734 | 10.067 | 8.668 | 1.148 | 2.245 | 1.854 |
| B θ0.99 | 8 | 7.728 | 9.726 | 8.669 | 1.086 | 2.111 | 1.721 |
| B θ0.99 | 16 | 7.728 | 9.734 | 8.672 | 1.010 | 2.015 | 1.713 |
| D θ0.99 | 1 | 7.376 | 10.123 | 8.759 | 0.462 | 2.728 | 2.098 |
| D θ0.99 | 4 | 7.382 | 10.117 | 8.778 | 0.447 | 2.158 | 1.664 |
| D θ0.99 | 8 | 7.392 | 10.135 | 8.763 | 0.439 | 2.019 | 1.551 |
| D θ0.99 | 16 | 7.397 | 10.130 | 8.762 | 0.424 | 1.990 | 1.550 |
| F θ0.99 | 1 | 7.716 | 21.412 | 11.831 | 9.406 | 16.919 | 13.278 |
| F θ0.99 | 4 | 7.688 | 20.730 | 11.830 | 7.828 | 15.520 | 11.600 |
| F θ0.99 | 8 | 7.675 | 20.613 | 11.844 | 6.508 | 14.686 | 10.359 |
| F θ0.99 | 16 | 7.706 | 20.607 | 11.853 | 5.632 | 13.654 | 9.255 |

Across matching points, new timed device reads are 10.9–44.7% lower and writes
12.9–32.2% lower than split 32. Synchronous integrity work still adds CPU,
executor, buffer, WAL and contention cost; device I/O alone does not account
for the entire throughput difference.

## Fresh det drift controls

Controls change neither the published det curve nor the comparison denominator.

| Workload | Workers | Published det TPS | Control TPS | Change |
| --- | --- | --- | --- | --- |
| A θ0.99 | 1 | 2,149.382 | 2,157.730 | +0.39% |
| A θ0.99 | 16 | 7,296.607 | 8,309.098 | +13.88% |
| F θ0.99 | 1 | 2,013.288 | 1,880.406 | -6.60% |
| F θ0.99 | 16 | 7,499.063 | 7,920.792 | +5.62% |

The observed −6.60% to +13.88% range (about −7% to +14%) bounds these four
controls only. There are no fresh controls for B/D or intermediate workers.
One trial per configuration remains below the five-repeat qualification rule;
shared-host/SSD/cache variation can affect small differences. A broader matched,
repeated campaign is needed to establish stable rankings and attribute gains.

## Published 2026-09-29 observations (unchanged values)

The table below keeps all original modes, including historical READ COMMITTED.
Every `det + Merkle` row in this table is **split 32 / merge 8**.

| Workload | Mode | w1 | w4 | w8 | w16 |
|---|---|---:|---:|---:|---:|
| A θ0.0 | pg SERIALIZABLE | 1,454 | 2,942 | 4,373 | 5,961 (1) |
| A θ0.0 | pg READ COMMITTED | 1,450 | 2,984 | 4,068 | 5,983 |
| A θ0.0 | det | 1,427 | 3,079 | 3,743 | 4,978 |
| A θ0.0 | det + Merkle | 907 | 1,786 | 2,165 | 2,536 |
| A θ0.99 | pg SERIALIZABLE | 2,209 | 4,470 (66) | 6,961 (192) | 9,780 (586) |
| A θ0.99 | pg READ COMMITTED | 2,086 | 4,661 | 6,647 | 11,488 |
| A θ0.99 | det | 2,149 | 3,908 | 5,179 | 7,297 |
| A θ0.99 | det + Merkle | 1,319 | 2,272 | 2,917 | 3,909 |
| A θ1.2 | pg SERIALIZABLE | 2,732 | 5,850 (1,721) | 7,908 (5,042) | 9,804 (11,926) |
| A θ1.2 | pg READ COMMITTED | 2,770 | 6,398 | 9,960 | 11,186 |
| A θ1.2 | det | 2,642 | 4,693 | 6,651 | 7,880 |
| A θ1.2 | det + Merkle | 1,988 | 3,000 | 3,769 | 4,531 |
| B θ0.99 | pg SERIALIZABLE | 4,220 | 11,093 (1) | 15,773 (2) | 17,683 (7) |
| B θ0.99 | pg READ COMMITTED | 4,031 | 12,026 | 16,353 | 18,639 |
| B θ0.99 | det | 4,031 | 8,358 | 10,823 | 13,822 |
| B θ0.99 | det + Merkle | 3,403 | 6,579 | 8,532 | 10,235 |
| C θ0.99 | pg SERIALIZABLE | 4,512 | 15,773 | 23,866 | 32,949 |
| C θ0.99 | pg READ COMMITTED | 4,734 | 16,949 | 24,272 | 34,014 |
| C θ0.99 | det | 4,561 | 9,828 | 14,144 | 18,298 |
| C θ0.99 | det + Merkle | 4,244 | 10,101 | 14,225 | 19,029 |
| D θ0.99 | pg SERIALIZABLE | 4,145 | 12,217 | 15,823 | 18,034 |
| D θ0.99 | pg READ COMMITTED | 4,240 | 12,063 | 16,090 | 17,699 |
| D θ0.99 | det | 3,932 | 8,518 | 11,192 | 13,423 |
| D θ0.99 | det + Merkle | 3,160 | 6,437 | 7,880 | 9,479 |
| F θ0.99 | pg SERIALIZABLE | 1,937 | 4,501 (62) | 6,240 (212) | 8,768 (700) |
| F θ0.99 | det | 2,013 | 3,872 | 5,276 | 7,499 |
| F θ0.99 | det + Merkle | 1,299 | 2,191 | 2,683 | 3,270 |


## Contents and regeneration

| Path | Content |
|---|---|
| `summary.csv` | 136 cases; original columns preserved, plus geometry and baseline node/leaf counts where known |
| `runs/det_merkle/`, `runs/pg_serializable/`, `runs/pg_read_committed/` | Unchanged published September evidence |
| `runs/merkle_s1024/`, `runs/det_control_20261001/` | Complete October run directories, including SQL inputs, logs, I/O, checkpoints, telemetry, settings and provenance |
| `runs/*/CURATION.md` | Archive policy and original-to-published mode/path mapping for new runs |
| `runs/{merkle_s1024,det_control_20261001}/ARCHIVE_SHA256SUMS` | Checksums for every copied raw file; no files omitted |
| `campaign_s1024_20261001/` | Exact campaign shell, controller logs/status, comparison CSV/Markdown, handoff and baseline provenance |
| `figures/oom_scaling_all.png`, `figures/scaling_<wl>_<skew>.png` | pg SERIALIZABLE, published det, published Merkle split 32, new Merkle split 1024; new C absent |
| `figures/oom_relative_to_pg.png` | Curves relative to published pg SERIALIZABLE at workers 1 and 16 |
| `figures/oom_merkle_ratios_io.png`, `.csv` | Both Merkle/det ratios and det/old/new timed device read/write KB per statement, all matching workers |
| `figures/oom_tps_table.csv` | Unrounded TPS for every plotted series; blank for missing new C points |

New raw files retain their original mode names and `.bench_tmp` paths; only the
consolidated CSV assigns publication labels and archive `artifact_dir` paths.
Geometry columns describe the baseline before each workload, not final tree size.
Historical node counts are left blank because the saved setup lacks that field.

Regenerate with local data analysis only:

```bash
MPLCONFIGDIR=/tmp/ariabc-oom-matplotlib python3 scripts/distributed/plot_oom_figures.py
```

The default plots contain only SERIALIZABLE series. `--include-rc` optionally
shows retained historical READ COMMITTED observations; it launches no benchmark.
Campaign reproduction commands and missing remote evidence fetch instructions
are in [COMMANDS.md, section 2](../COMMANDS.md#2-oom-100m--pg-det-and-det--merkle).


## Optimization experiments (2026-10-01)

Two paired experiments used the same optimized install
`/home/neel/claude_opt/install_opt` on lab DB/server host `10.129.148.247`.
The archived executable hashes match across all treatments; the gateway ran on
`10.129.27.111`, as recorded in each manifest. Each point is one trial of
20,000 statements, 100M rows, 32MB PostgreSQL buffers, cold OS-cache drop after
delta restore, SERIALIZABLE with client-side serialization retries, and
`fsync=on`, `full_page_writes=on`, `synchronous_commit=on`. Merkle remains
synchronous through the partition root inside the user transaction
(`merkle_apply_synchronous_direct=on`), fanout 32, split 1024 / merge 256.
Canonical row-hash bytes and recovery semantics were unchanged by these
configuration experiments. This publication adds no database code changes.

All 24 unique measured cases have 20,000 validated terminal results,
`divergence_count=0`, `permanent_failures=0`, no exhausted retries, exit status 0,
and Merkle PASS for every Merkle case. The two experiments share the eight
WAL-off cases as fillfactor-100 controls: the
[summary CSV](optimizations_20261001/summary.csv) has 32 labeled rows, not 32
independent observations. Unrounded paired metrics and archive paths are in
[comparison.csv](optimizations_20261001/comparison.csv). Raw run directories,
SQL inputs, controller commands/status/logs, calibration, cache/settings,
checkpoints, telemetry and executable provenance are retained without
exclusions; see [archive policy](optimizations_20261001/CURATION.md) and
[checksums](optimizations_20261001/ARCHIVE_SHA256SUMS).

The [remaining-overhead report H](optimizations_20261001/provenance/report_H.md)
compares split 1024 against published September det. At A θ0.0 w1 it measures
about **308.4 µs/update-equivalent** of extra mixed-workload wall time, including
**211.7 µs/update-equivalent** of extra aggregate PostgreSQL read time. The
remaining **96.7 µs/update-equivalent** is an unattributed net residual containing
CPU, WAL/flush waits, locks, scheduling and changes to common work. These are
mixed-workload differences normalized by 9,983 UPDATEs in 20,000 statements,
not measured standalone UPDATE latencies. PG reads can hit the OS cache;
aggregate read time overlaps at multiple workers and cannot be added to wall
time there. The preserved [decomposition CSV](optimizations_20261001/provenance/overhead_analysis/decomposition.csv)
contains the exact inputs and all matching points.

A separate supplied plain-SQL probe summarized in report H attributes about
**61% of extra PG buffer misses** and **62% of extra full-page-image (FPI) bytes**
to the user lookup B-tree, roughly 60% of each. Those percentages are reported
probe evidence, not per-relation instrumentation in these gateway campaigns.
The tempting optimization of skipping an identical lookup-key insertion after
a non-HOT UPDATE was rejected as unsafe: its old index entry retains the old
TID, and a cross-page successor cannot be reached through a HOT chain. After
visibility changes, pruning or VACUUM this can hide the live row from split or
repair scans. Physical row-version reachability must be preserved.

![TPS, Merkle/det ratio and timed device I/O for both experiments](optimizations_20261001/optimization_experiments.png)

[PDF figure](optimizations_20261001/optimization_experiments.pdf).
TPS is gateway statements/s. **KB = 1000 bytes**;
`device_*_mib × 2^20 / (1000 × total_queries)` gives timed workload device I/O
per statement. Checkpoint writes are excluded from these table/figure I/O
values and remain separate in the CSV/raw evidence. Device windows are
slightly wider than gateway wall time and include other host traffic; these
writes are not a direct measurement of workload WAL bytes.

**WAL compression off → on.** Within det the run order was off then on;
within Merkle it was on then off. All heap baselines retain fillfactor 100.

| Point | Mode | Baseline TPS | Treatment TPS | TPS change | Read KB/stmt before → after | Write KB/stmt before → after | Write change | Merkle/det before → after |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A θ0.0 w1 | det | 1,587.932 | 1,295.337 | -18.43% | 16.892 → 16.899 | 14.453 → 9.866 | -31.73% | — |
| A θ0.0 w1 | Merkle | 1,147.447 | 965.577 | -15.85% | 23.297 → 23.318 | 20.417 → 15.528 | -23.95% | 0.723 → 0.745 |
| A θ0.0 w16 | det | 5,053.057 | 6,416.426 | +26.98% | 16.879 → 16.899 | 10.131 → 4.664 | -53.96% | — |
| A θ0.0 w16 | Merkle | 3,639.010 | 4,026.575 | +10.65% | 23.292 → 23.292 | 15.725 → 10.078 | -35.91% | 0.720 → 0.628 |
| F θ0.99 w1 | det | 2,025.111 | 1,757.624 | -13.21% | 7.449 → 7.449 | 9.406 → 7.280 | -22.60% | — |
| F θ0.99 w1 | Merkle | 1,588.941 | 1,503.759 | -5.36% | 11.844 → 11.824 | 13.240 → 10.597 | -19.97% | 0.785 → 0.856 |
| F θ0.99 w16 | det | 7,535.795 | 7,047.216 | -6.48% | 7.439 → 7.438 | 5.510 → 3.182 | -42.25% | — |
| F θ0.99 w16 | Merkle | 5,082.592 | 5,165.289 | +1.63% | 11.848 → 11.845 | 9.384 → 6.717 | -28.42% | 0.674 → 0.733 |

Compression reduced timed device writes **19.97–53.96%** (about 20–54%) across
all eight mode/point pairs. The narrower 25–54% shorthand does not cover the
recorded F w1 reductions (det 22.60%, Merkle 19.97%). TPS changed
**−18.43% to +26.98%** (about −18% to +27%), with regressions at both w1 points
and inconsistent changes at w16. Reads barely change. WAL compression is
**not adopted** as the campaign default: lower device writes did not establish
a consistent throughput improvement, and these artifacts do not isolate
compression CPU cost from run variation.

**User-table fillfactor 100 → 90, WAL compression off.** The physically
rewritten heap was prepared with
`prepare_fillfactor90_inroot.sh`; the [preparation log](optimizations_20261001/fillfactor90/prepare.log)
records heap growth from **25,600,008,192 to 28,248,276,992 bytes**
(25.60 → 28.25 GB, **10.34%**), PK **2,246,197,248 bytes** (2.25 GB), and lookup
**3,154,264,064 bytes** (3.15 GB). Both det and Merkle use that common rewritten
heap; the controls are the same-day WAL-off runs above with the same binaries.

| Point | Mode | Baseline TPS | Treatment TPS | TPS change | Read KB/stmt before → after | Write KB/stmt before → after | Write change | Merkle/det before → after |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A θ0.0 w1 | det | 1,587.932 | 1,431.947 | -9.82% | 16.892 → 17.160 | 14.453 → 10.518 | -27.23% | — |
| A θ0.0 w1 | Merkle | 1,147.447 | 1,300.052 | +13.30% | 23.297 → 20.665 | 20.417 → 12.184 | -40.33% | 0.723 → 0.908 |
| A θ0.0 w16 | det | 5,053.057 | 4,944.376 | -2.15% | 16.879 → 17.174 | 10.131 → 6.002 | -40.76% | — |
| A θ0.0 w16 | Merkle | 3,639.010 | 4,501.463 | +23.70% | 23.292 → 20.660 | 15.725 → 7.603 | -51.65% | 0.720 → 0.910 |
| F θ0.99 w1 | det | 2,025.111 | 1,878.111 | -7.26% | 7.449 → 7.899 | 9.406 → 7.954 | -15.43% | — |
| F θ0.99 w1 | Merkle | 1,588.941 | 1,614.726 | +1.62% | 11.844 → 11.369 | 13.240 → 9.696 | -26.77% | 0.785 → 0.860 |
| F θ0.99 w16 | det | 7,535.795 | 7,363.770 | -2.28% | 7.439 → 7.905 | 5.510 → 3.993 | -27.53% | — |
| F θ0.99 w16 | Merkle | 5,082.592 | 5,629.046 | +10.75% | 11.848 → 11.378 | 9.384 → 5.651 | -39.78% | 0.674 → 0.764 |

Merkle TPS increased **1.62–23.70%** (about +2% to +24%), while det decreased
**2.15–9.82%** (about −2% to −10%). At A θ0.0, Merkle/det moved
**0.723 → 0.908 at w1** and **0.720 → 0.910 at w16**, approximately 0.72 → 0.91.
Timed Merkle writes fell in every pair; its reads fell while det reads grew.
This is a **per-deployment trade-off** involving heap size, cache pressure and
write cost, rather than a universal default. **HOT fraction was not measured**:
the retained run artifacts lack per-user-table `n_tup_upd` and
`n_tup_hot_upd` before/after snapshots, so the results do not prove how much of
the change came from HOT.

Caveats: one trial per configuration cannot establish stable rankings. Allow
roughly **±10–15% potential run-to-run noise**, as a practical scale rather than
a confidence interval: the separate fresh det controls above ranged from
−6.60% to +13.88%, and are not repeats of these exact points. Fillfactor was
measured after the WAL campaign rather than counterbalanced with its controls.
The fillfactor-90 Merkle rebuild used the pre-fix path that DELETEs old node
rows, leaving dead tuples: its node relation total was **89,341,952 bytes**
(89.34 MB), a slight disadvantage to Merkle and an additional layout confound.
This is not an isolated HOT-only comparison. Published September det/pg used
**fillfactor 100**; do not replace their denominators with fillfactor-90 det or
relabel those published curves. No pg fillfactor-90 point, repeated-trial
qualification, crash or recovery experiment was measured here.

Regenerate from preserved evidence with local data analysis only:

```bash
MPLCONFIGDIR=/tmp/ariabc-oom-opt-matplotlib python3 scripts/distributed/oom_opt/plot_opt.py
```

The script checks raw completion, settings, Merkle verification, counter deltas,
workload hashes and matching executable hashes before generating summary,
paired comparison tables and PNG/PDF. It never contacts a host or starts a
workload. Fresh orchestrator reproduction commands are appended to
[COMMANDS.md](../COMMANDS.md#7-oom-optimization-experiments--2026-10-01).
