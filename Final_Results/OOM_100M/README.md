# YCSB on a 100M-row database larger than memory (OOM)

**Measured 2026-09-29.** 108 accepted cases.
- Every case completed and validated all 20,000 statements (yes).
- Divergences plus permanent failures: 0.
- Every det + Merkle case passed full `merkle_verify` of the 100M-row tree (yes).

## Setup

| Item | Value |
|---|---|
| Database host | `neel@10.129.148.247`: 15 GB RAM, Intel 660p NVMe (QLC) |
| Gateway host | `10.129.27.111`: 96 client terminals, direct completion path |
| Data | `usertable`, 100,000,000 rows, 31.4 GiB on disk; Merkle index with 200 partitions, fanout 32 |
| Memory | `shared_buffers = 32MB`; the OS page cache is dropped before every case |
| Durability | `fsync`, `full_page_writes`, `synchronous_commit = on` |
| Workload | YCSB, 20,000 statements per case, Zipf skew θ |
| Workers | 1 / 4 / 8 / 16: pg connection pool, or deterministic worker threads |
| Binaries | Built from commit 763b9ef; the server adds pg retry jitter |
| Trials | 1 per configuration |

**Modes**

| Mode | Isolation | Notes |
|---|---|---|
| pg SERIALIZABLE | SERIALIZABLE | PostgreSQL. Serialization failures are retried with exponential backoff and full jitter, capped at 100 ms |
| pg READ COMMITTED | READ COMMITTED | For single-row statements this gives the same serializable result. Not run for F, whose read-modify-write is not serializable at READ COMMITTED |
| det | Deterministic, serializable | Ordered-block execution |
| det + Merkle | Deterministic, serializable | det, plus synchronous Merkle tree maintenance |

## Method (every case)

1. Restore the database from a stopped pristine baseline with a byte-identical delta
   copy (`rsync --inplace`). pg and det use a baseline without the Merkle indexes.
2. Check the files against the baseline: metadata every case, full byte comparison
   periodically.
3. Check the relations and settings.
4. Wait until SSD read and write latency are back at the idle calibration.
5. Drop the OS caches, start PostgreSQL cold, run the workload.
6. Take a separate `CHECKPOINT`, restart, and read the I/O counters.
7. Verify the Merkle tree for Merkle cases.

Why a delta restore, and how it was validated against a full `cp`: see
`../CORRECTIONS.md` ("OOM 100M, 2026-09-28").

## Results (throughput in statements/s; pg SERIALIZABLE retry count in parentheses)

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

## Findings

- **One worker.** det is within 5% of pg SERIALIZABLE on every workload (0.95–1.04×).
- **Scaling.** From 4 workers up, pg scales further than det. At 16 workers det reaches
  0.74–0.86× of pg on A, B, D and F, and 0.56× on read-only C. det keeps scaling
  steadily up to 16 workers on every workload.
- **READ COMMITTED.** pg has no aborts and is fastest under contention (A θ0.99 w16:
  11,488; A θ1.2 w16: 11,186).
- **Cost of Merkle.**
  - Read-only C: nothing (Merkle ≈ det).
  - B and D: det + Merkle runs at 0.70–0.84× of det.
  - The 50%-write workloads A and F: 0.44–0.75× of det.
  - Where it comes from: extra node-page I/O. Merkle reads 2–3.3× and writes about 2×
    as much as det (`device_read_mib`, `device_write_mib`).
- **Precision.** Single trial per point. Earlier repeats of identical points differed by
  about 4% (median) and up to about 14%.

## Contents

| Path | Content |
|---|---|
| `summary.csv` | All 108 results with I/O, checkpoint, retry and validation columns; `source_run` names the raw run |
| `figures/oom_scaling_all.png` | Throughput vs workers, all workloads |
| `figures/scaling_<wl>_<skew>.png` | One chart per workload/skew |
| `figures/oom_relative_to_pg.png` | det and det + Merkle relative to pg SERIALIZABLE, at 1 and 16 workers |
| `figures/oom_tps_table.csv` | Every plotted value |
| `runs/det_merkle/` | Raw det and det + Merkle run: per-case logs, settings, I/O, telemetry, Merkle verification |
| `runs/pg_serializable/` | Raw pg SERIALIZABLE run (retry jitter on) |
| `runs/pg_read_committed/` | Raw pg READ COMMITTED run |

The figures show pg SERIALIZABLE, det and det + Merkle. pg READ COMMITTED is in `summary.csv`
and `runs/pg_read_committed/`, but is not plotted; `--include-rc` adds it back.

To regenerate the figures: `python3 scripts/distributed/plot_oom_figures.py`.
The commands that produced each run are in `../COMMANDS.md`, section 2.
