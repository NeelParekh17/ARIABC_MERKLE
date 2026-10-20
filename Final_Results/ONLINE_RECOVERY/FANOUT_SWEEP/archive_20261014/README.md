# Merkle fanout sweep — TPS and online recovery (2026-10-14)

How does Merkle fanout affect cluster throughput and online recovery time?

- **Data:** 4,000,000-row YCSB `usertable_small`, 160,000 YCSB-A transactions, 96 client lanes, serializable.
- **Merkle:** 200 partitions, split threshold 1024, merge threshold 256; only fanout varies.
- **Cluster:** gateway + controller on .247; replicas .247 / .246 / .248 (raft-kafka ordering).
- **Scenarios:**
  - **A**: no fault, recovery off (throughput baseline).
  - **M_mixed**: 100 mixed (update/insert/delete) corrupted rows injected on follower .248, 5 s into the run, with online recovery (detect → cut → Merkle repair → catch-up).
- **Repetitions:** 3 per (fanout, scenario), rotated order. Medians are reported; figures also show every run.
- **Engine:** HEAD `9cf6c70a` plus the uncommitted recovery fixes (confirm-before-repair, ledger-replay xid registration, cut self-check, pending-suspect settle). Every accepted run used one identical source/binary/harness identity (`results/matrix_completion_audit.json`).

## Status

| Fanouts | State |
|---|---|
| 16, 32, 64, 128, 256 | **Done.** 30/30 accepted on one identity. |
| 2, 4, 8 | **Pending.** 18 runs, waiting for .246 to have ≥ 9.5 GB free. |

Because `campaign.json` already lists all 8 fanouts, `results/REPORT.md` currently says *INCOMPLETE 30/48*. Results and figures here will be refreshed when the 2/4/8 runs finish.

## Results (fanouts 16–256, medians of 3)

| Fanout | TPS, A | TPS, M_mixed | Cut | Repair | Catch-up | Total recovery | Rows read by repair |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | 7659 | 7104 | 3.3 s | 11.2 s | 9.1 s | 24.3 s | 7.9k |
| 32 | 7761 | 7413 | 3.2 s | 12.6 s | 18.4 s | 34.6 s | 62.6k |
| 64 | 7605 | 6189 | 3.2 s | 11.5 s | 9.0 s | 23.9 s | 31.3k |
| 128 | 7728 | 5559 | 3.0 s | 11.3 s | 9.0 s | 23.5 s | 15.6k |
| 256 | 7781 | 7320 | 3.2 s | 11.1 s | 9.0 s | 23.6 s | 7.9k |

- **Throughput without a fault is flat** across fanouts (~7.6–7.8k TPS, CV < 3 %).
- **Cut and repair time do not depend on fanout.** Cut is ~3.2 s and repair ~11 s everywhere, although rows read vary 8×. Repair is dominated by fixed cost, not by rows read.
- **Rows read = differing leaves (~100) × leaf size.** Each partition holds ~20k rows, and a leaf splits once it passes 1024 rows, so leaf size is 20k / fanout^levels:
  - 32 → one level, ~625 rows per leaf
  - 16 → two levels, ~78 rows per leaf
  - 64 → ~312, 128 → ~156, 256 → ~78
- **Catch-up time and throughput under a fault are noisy.** The low M_mixed points (3.3k–4.4k TPS, catch-up up to 35 s) line up with short cluster-wide completion stalls (`empty_100ms_buckets`, `max_completion_gap_ms` in `results/matrix_enriched_attempts.csv`), not with fanout. Medians at 64/128 fall because of those outliers.
- Confirm-before-repair adds about one compare round (~4 s) to detection latency, the same at every fanout.

## Figures (`figures/`, PNG + PDF)

Each line is the median of 3 runs; each dot is an individual run.

| File | Shows |
|---|---|
| `fanout_overview` | All six panels below on one page |
| `tps_vs_fanout` | TPS, A vs M_mixed |
| `recovery_time_vs_fanout` | Total recovery stacked by every step: cut, drain, repair (localise / transfer / apply / verify), rebase, catch-up, other |
| `repair_breakdown_vs_fanout` | Merkle repair only: localise / transfer / apply / verify |
| `recovery_share_vs_fanout` | Same steps as a % of total recovery time |
| `recovery_phases_vs_fanout` | Each step as its own line, with individual runs |
| `repair_work_vs_fanout` | Rows read by repair, and tree levels below the partition root |

Repair sub-step timings come from the repaired node's `RECOVER` line in `runs/*/server_node*.log`.
They are recorded per run in `results/matrix_enriched_attempts.csv` as `localise_ms`, `transfer_ms`, `apply_ms`, `verify_ms`, `rebase_ms` and `drain_ms_server`.

**Where the time goes at 4M rows** (medians):

| Step | Time | Share of ~24 s | What it is |
|---|---|---|---|
| Catch-up | ~9 s | ~38 % | Replays every block missed since the cut |
| Verify | ~5.7 s | ~24 % | `merkle_verify` rehashes the full table |
| Apply | ~4 s | ~16 % | The DELETE walks all 4M rows (planner skips the lookup index) |
| Cut | ~3.2 s | ~13 % | Waits for the replica's ~95-block in-flight backlog |
| Transfer | ~1.5 s | ~6 % | Same full-table walk on the reference replica |
| Localise | 24–70 ms | <1 % | Root and leaf comparison |
| Drain + rebase | <0.6 s | small | |

A fix for transfer and apply is in `ariabc_pg/src/replica_repair.cxx`. It is uncommitted and not yet measured on the cluster.

## Layout

| Path | Contents |
|---|---|
| `results/` | Summary and report (`fanout_summary.csv`, `matrix_summary.csv`, `REPORT.md`) |
| `results/` | Per-attempt tables: `matrix_attempts.jsonl` keeps every attempt, including rejected, noisy and superseded ones |
| `results/` | Completion audit (`matrix_completion_audit.json`) |
| `figures/` | Plots |
| `controller/` | Controller and acceptance log for each attempt |
| `runs/` | Per-run evidence, 54 attempts. `tx_latency.csv` is omitted (13 MB each); it is still in the .247 workspace. |
| `harness/` | Exact harness used: `campaign.json`, `run_case.sh`, controller, collector, injector, analysis and plotting scripts |
| `provenance/` | Build and binary hashes, deploy logs, and mid-sweep change records |

The `provenance/` change records cover three mid-sweep changes:
- `rebuild_settle_20261014`: gateway settle fix
- `injector_retries_20261014`: injector retries raised
- `injector_split_20261014`: injector split per fault type
- `fanout_extend_20261014`: adds fanouts 2/4/8

Live workspace on .247: `~/ariabc_data/fanout_sweep_20261014`.

## Attempt history

54 attempts produced 30 accepted targets:

- **18 superseded.** These runs were accepted before the final gateway and injector fixes. They are marked `SUPERSEDED_PROVENANCE` and were re-run on the final identity. The original ledger is `results/matrix_attempts.pre_supersede_20261014.jsonl`.
- **5 rejected:**
  - injector conflicts
  - an unconfirmed fault that outlived the workload
  - an injector failure, twice
  - a fault that landed after the workload had finished
- **1 noise-excluded:** paging on .246.

Every failure was diagnosed, fixed, and its case retried.

Earlier 150-run Codex campaign (superseded): see `../FANOUT_CAMPAIGN.md`.
