# Merkle fanout sweep 2..1024 — TPS and online recovery (2026-10-15)

How does Merkle fanout affect cluster throughput and the time online recovery spends in each phase?

- **Data:** 4,000,000-row YCSB `usertable_small`, 160,000 YCSB-A transactions, 96 client lanes, serializable.
- **Merkle:** 200 partitions, split threshold 1024, merge threshold 256. Only fanout varies: 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024.
- **Cluster:** gateway + controller on .247; replicas .247 / .246 / .248 (raft-kafka ordering).
- **Scenarios:**
  - **A**: no fault, recovery off.
  - **M_mixed**: 100 mixed (update/insert/delete) corrupted rows injected on follower .248 about 5 s in, then online recovery (detect → quarantine → cut → repair → replay).
- **Repetitions:** 3 per (fanout, scenario) = **60 runs, all accepted** (61 attempts). Before the matrix, two smoke runs qualified fanouts 1024 and 512 with a full online repair.
- **Correctness:** every accepted run passed the final check — same row count, Merkle root, data hash and `merkle_verify` on all 3 nodes — with 0 divergences.

## Build used (changes vs the 2026-10-14 sweep in `archive_20261014/`)

| Area | Change |
|---|---|
| Engine | Merkle fanout up to 1024. Splits now use up to 10 bits, with a uint16 child index (`merkle.h`, `merkle{,apply,build,verify}.c`). Tested on .247 at fanouts 2–1024: exactly `fanout` children per internal node; `merkle_verify` true after build, DML and REINDEX; incremental root == rebuilt root. |
| Repair transfer and apply | Run with hash and merge joins off, so they use the Merkle lookup index instead of reading all 4M rows. |
| Repair apply | The candidate temp table gets a primary-key index and `ANALYZE` (a codex review finding). With 62k candidates the DELETE went from 56.5 s to 0.35 s. |
| Repair verify | `heap_verify` defaults to 0: matching partition roots prove the repaired state, and the full-table `merkle_verify` is opt-in. |
| COPY | Cleanup can no longer loop on a broken connection. |
| Earlier fixes, still included | confirm-before-repair; settle-before-stop; split fault injector |

All changes were reviewed by codex `gpt-6.1-sol` (high reasoning) in three rounds; see `provenance/campaign_20261015/codex_review*.md`.

## Results (medians; times in seconds)

Medians of 3 runs. † The fault (M_mixed) columns at fanouts 2, 256 and 1024 use the 3 latest recheck runs (same build, same fault seeds) in place of the original matrix runs (`analyze.py --recheck replace`). The original matrix runs stay in the attempt tables; their numbers are in `results/fanout_summary_matrix_only.csv`.

| Fanout | TPS A | TPS M_mixed | Cut | Repair (drain..rebase) | Transfer | Apply | Verify | Replay (catch-up) | Total recovery | Rows read |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 † | 7573 | 6265 | 3.75 | 2.85 | 0.55 | 1.76 | 0.01 | 9.92 | 24.9 | 62,592 |
| 4 | 7048 | 6484 | 3.46 | 1.67 | 0.27 | 1.33 | 0.01 | 8.82 | 14.3 | 31,259 |
| 8 | 7670 | 7357 | 3.56 | 1.74 | 0.27 | 1.36 | 0.01 | 8.97 | 14.3 | 31,259 |
| 16 | 7663 | 7400 | 3.37 | 0.52 | 0.09 | 0.27 | 0.01 | 8.86 | 12.8 | 7,915 |
| 32 | 7543 | 6263 | 3.03 | 5.13 | 0.50 | 3.96 | 0.01 | 5.56 | 20.1 | 62,814 |
| 64 | 7720 | 7637 | 3.16 | 1.59 | 0.28 | 1.11 | 0.01 | 2.64 | 7.3 | 31,259 |
| 128 | 7740 | 7415 | 3.30 | 1.00 | 0.16 | 0.74 | 0.01 | 8.62 | 13.2 | 15,737 |
| 256 † | 7695 | 7592 | 3.18 | 0.63 | 0.07 | 0.27 | 0.01 | 8.38 | 12.9 | 7,915 |
| 512 | 7599 | 7361 | 3.52 | 0.54 | 0.04 | 0.21 | 0.01 | 7.43 | 11.9 | 3,956 |
| 1024 † | 7528 | 7431 | 3.64 | 0.42 | 0.03 | 0.08 | 0.01 | 8.42 | 12.8 | 2,043 |

Per-run detail for every sub-phase is in `results/matrix_enriched_attempts.csv`. The per-run reports written as the sweep ran are in `results/run_reports.md`.

## What the sweep shows

### Repair
- **Repair dropped from ~11 s (2026-10-14 sweep) to 0.45–5 s.** The full-table verify is gone (5.7 s → ~0.01 s) and transfer/apply now use the lookup index.
- **Repair cost now follows rows read**, not tree depth:
  - Rows read = differing leaves (~100) × leaf size.
  - Leaf size = ~20k rows per partition ÷ fanout^levels, with a split once a leaf passes 1024 rows.
  - So fanouts 2 and 32 read ~62k rows; fanouts 4, 8 and 64 ~31k; 16 and 256 ~7.9k; 512 ~4k; 1024 ~2k.
  - Apply dominates, at roughly 20–60 µs per row read.
  - Higher fanout means smaller leaves, so less to read and repair.
- **Localise** (comparing roots and leaves) grows slightly with very wide fanouts: 0.03–0.07 s, up to 0.24 s at 1024. That is still small.

### Cut
- **Cut is about 3.0–3.6 s at every fanout.** It is almost entirely "export": the snapshot waits for the replica's in-flight execution backlog of about 95 blocks, which comes from the client window of 96 threads × 256 in flight.
- Prepare and digest take under 0.03 s. The cut is independent of the Merkle tree.

### Replay (catch-up)
- **Catch-up has to keep up with live traffic.** It replays every block from the cut to the live tip while the cluster keeps committing. The replica executes about 38 blocks/s against about 26 blocks/s incoming, so it closes the gap at only about 12 blocks/s and usually reaches the tip only when the workload ends.
- **Its time therefore depends on when the fault is detected, not on fanout:**
  - Reps 1 and 3 detect mid-run, around block 285 → catch-up ≈ 9 s.
  - Rep 2 detects late, around block 485 → catch-up ≈ 2.5 s.
  - f8 rep 2 and f64 rep 1 were detected at the end of the workload → catch-up ≈ 0.
  - The medians mix these cases. See `figures/replay_vs_fanout.png`.

### Throughput
- **Throughput without a fault is flat at 7.0–7.7k TPS** across all ten fanouts. Even fanout 2, with 16 tree levels, shows no penalty.
- **TPS with a fault was lower at fanout 2 (~4.6k) and fanout 1024 (~5.7k).** A recheck explains both (see below): f1024 was .247 disk noise, while f2 has a real replay-speed penalty (~17 %). The other fanouts lose 1–6 %.

### Recheck of the low fault-run TPS at fanouts 2 and 1024 (`recheck_20261015/`)

**Mechanism.** A transaction completes when 2 of 3 replicas return matching results. While .248 (node4) is quarantined, repaired and replaying, every commit needs **both** node1 (.247) and node2 (.246). Any slowness on those two then hits throughput directly.

**What happened in the original runs:**
- In the slow runs, node1 fell up to 10.7 s behind. .247's QLC NVMe went into its slow mode: ~90% busy at less than half its normal write ops/s, with 11–16 ms latency spikes. .247 also hosts the gateway and Kafka.
- About 10k transactions then waited on node1 at the end of the workload. In f2 rep 3, completions sat at 149,045 → 149,186 for 5 s.
- That slow mode also appears at other fanouts (f16, f256, f512, f1024 no-fault), and .247's write volume does not depend on fanout.

**Recheck setup:** the 3 fault seeds were rerun for f2 and f1024, interleaved with f256 as a control.

| M_mixed TPS | rep 1 | rep 2 | rep 3 | median | original sweep median |
|---|---:|---:|---:|---:|---:|
| f256 (control) | 7592 | 7658 | 7388 | 7592 | 7427 |
| f1024 | 7431 | 7533 | 6467 | 7431 | 5714 |
| f2 | 6752 | 6265 | 5523 | 6265 | 4638 |

**f1024 dip — noise.** In the recheck, f1024 matches the control. Its one lower rep coincides with 3 saturated-disk seconds on .247.

**f2 dip — partly real.** f2 stays ~17 % below the control even in reps with no disk slow mode. The reason is replay speed:

| Fanout | Replay rate (blocks/s, median) |
|---|---:|
| 2 | ~22 (individual runs 12–34) |
| Every other fanout | ~38 |

At f2, replay is slower than the cluster's ~26 blocks/s incoming rate, so catch-up cannot converge until the workload ends (catch-up 9.9 s and 18.8 s). Repair is also longer (~2.4 s, 62k rows read). Together these keep the cluster in the both-nodes-needed state longest. The likely cause of the slower replay is the deeper Merkle tree at f2 (more levels updated per transaction during re-execution). That still needs profiling to confirm.

**The original 4.6k at f2** was this effect plus the .247 disk slow mode.

## Figures (`figures/`, PNG + PDF)

| File | Shows |
|---|---|
| `cut_breakdown_vs_fanout` | Cut: prepare / export (backlog wait + snapshot) / digest / other |
| `repair_breakdown_vs_fanout` | Repair: drain / localise / transfer / apply / verify / rebase / other |
| `replay_vs_fanout` | Catch-up time, blocks replayed, and blocks behind when replay started |
| `recovery_time_vs_fanout` | Total recovery stacked by every phase |
| `recovery_share_vs_fanout` | Each phase as a % of that run's total |
| `recovery_phases_vs_fanout` | Each phase as a line (median, dots = runs) |
| `tps_vs_fanout` | TPS, A vs M_mixed |
| `repair_work_vs_fanout` | Rows read by repair, and tree depth |
| `fanout_overview` | All of the above on one page |

Stacked bars show each fanout's median-total run, so its components add up exactly. Lines and dots show medians and individual runs.
At fanouts 2/256/1024, the figures' fault-run data are the latest recheck runs. The figures from the original 60 matrix runs are kept in `figures_matrix_only/`.

## Attempts and caveats

- **f64 M_mixed rep 3, attempt 1 — REJECTED.** The collector reported "missing fetched rows". Two threads wrote to the server log at the same moment, so the replay-completion line was spliced into the RECOVER result line and could not be parsed. The repair and final check of that attempt passed. Attempt 2 was accepted.
- **Smoke f1024, attempt 1 — NOISE_EXCLUDED.** .248 swapped more than 64 MiB because a desktop Firefox session was open. Firefox and Remmina were closed and the retry passed. On .246, shalini's Chrome and VS Code were closed (her Antigravity was kept).
- **Still open:**
  - Why replay is slower at f2 (~22 vs ~38 blocks/s); needs profiling.
  - Recheck f2 rep 2: the cut took 28 s because the fault was detected only at the end of the workload (L=627).
  - A rare, transient all-node digest disagreement ("without a strict majority"), seen again in f64 rep 3. Confirm-before-repair keeps it from causing a false repair.

## Layout

| Path | Contents |
|---|---|
| `results/` | `fanout_summary.csv` (per-fanout medians, including every sub-phase), `matrix_summary.csv`, `REPORT.md`, `run_reports.md`, `matrix_attempts.jsonl` (all 61 attempts), `matrix_enriched_attempts.csv`, `smoke_*`, completion audit |
| `figures/` | Plots (matrix runs; fault runs at f2/f256/f1024 replaced by the latest recheck runs) |
| `figures_matrix_only/` | The same plots from the 60 matrix runs only |
| `recheck_20261015/` | Recheck ledger, controller logs and runs (f2/f256/f1024 M_mixed × 3) |
| `controller/` | Controller and acceptance log for every attempt |
| `runs/` | Per-run evidence for all attempts. `tx_latency.csv` is omitted; it is still in the .247 workspace. |
| `harness/` | Exact harness: `campaign.json`, `run_case.sh`, controller, collector, `campaign_supervise.sh`, injector, and `recovery_fanout/{analyze,finish,run_report}.py` |
| `provenance/` | Build and binary hashes, deploy logs, `campaign_20261015/` (pre-change copies, source diff, codex reviews) |
| `archive_20261014/` | The earlier 30-run sweep (fanouts 16–256, previous build: ~11 s repair) |

Live workspace on .247: `~/ariabc_data/fanout_sweep_20261014`. The current campaign's results are in `results/` there; the earlier campaign is in `results_20261014/`.
