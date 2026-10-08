# Det-mode protocol optimisations — results (2026-10-07/08)

Branch: `detopt/combined` (worktree `/work/ARIABC/AriaBC.worktrees/detopt_combined`, head `918ff30e`). It is not merged into main and not pushed.

## Contents

| Lever | Branch | Env switch (default) | Kept? |
|---|---|---|---|
| Early abort + incremental (delta) validation via per-tx digest ring + lookahead hand-off | detopt/early_validate | `BCDB_DT_EARLY_VALIDATE` (on), `BCDB_GATE_LOOKAHEAD` (on) | yes; this is the win |
| Exact tag dedup + cached hashes | detopt/tag_dedup | `BCDB_DT_TAG_DEDUP` (on) | yes; TPS-neutral, cuts read probes 57% |
| Trace fixes (second-boundary overflow, tx_id read after delete_tx, opt-in fine timers `BCDB_PHASE_TRACE_FINE`) | detopt/trace_rot | — | yes |
| Off-turn write-map rotation | detopt/trace_rot | `BCDB_DT_EARLY_ROTATION` (now **off**) | code kept, default off (measured slightly negative) |
| Pre-snapshot commit-set + targeted retry wait | detopt/commit_set | `BCDB_DT_COMMIT_SET` | **not merged**; only 0.05% of conflicts were false |

Not done, per the user's decision: relaxing commit durability, and dependency-aware scheduling.

## Method

- Ranking host 10.129.7.57. The v2 TPC-C workload is 20k tx, seed 42, det window 65536, SERIALIZABLE, fsync on, synchronous_commit on, phase trace OFF.
- Harness: `~/claude_checks/detopt_20261007/harness/ab.sh`. It adds `SYNC_BEFORE_MEASURE=1` and captures per-run `iostat`.
- The root NVMe (Crucial P3 QLC) randomly switches to a slow mode, with flush latency rising from 1.9 to 6–29 ms. In that mode TPS falls 2–3× for any binary. The numbers below are medians of **fast-mode runs only** (f_await < 2.1 ms). Slow-mode runs are listed in `ab_status.txt` and excluded.
- Correctness: every run (90+) reproduced the reference det state hash (W5 `fdb9545f…`, W30 `dcb895e5…`, W100 `e82921e1…`) with divergence_count=0 and permanent_failures=0.

## Results — 32 workers (TPS, fast-mode median)

| W | base | dedup only | early-validate only | EV + dedup | EV + dedup + rotation | final build |
|---|---|---|---|---|---|---|
| 5 | 983 | 977 (−0.6%) | 1176 (+19.7%) | **1182 (+20.3%)** | 1175 (+19.6%) | 1174 |
| 30 | 2102 | 2093 (−0.4%) | 2415 (+14.9%) | **2390 (+13.7%)** | 2372 (+12.8%) | 2405 |
| 100 | 3107 | 3111 (+0.1%) | 3256 (+4.8%) | **3275 (+5.4%)** | 3212 (+3.4%) | 3263 / 3315 |

## Results — worker scaling at W100 (TPS, fast-mode median)

| Workers | base | EV + dedup | Gain |
|---|---|---|---|
| 32 | 3107 | 3275 | +5.4% |
| 48 | 3133 | **3695** (final build: 3627) | +17.9% |
| 64 | 2874 | 3476 | +21.0% |

- Base gets slower beyond 32 workers, because the serial turn is its ceiling.
- With the turn shortened, det now scales to 48 workers.
- Best-vs-best at W100 is 3133 → 3695 (+18%).

## Trace-off effect (lever 5, measurement only)

Base with trace off vs on: W5 974 vs 897 (+9%), W30 2100 vs 812–1853 (noisy), W100 2843 vs 2774 (+2.5%). Both the v2 and v3 harnesses enable `BCDB_PHASE_TRACE` for det and not for pg.

## Mechanism evidence (traced runs, early_validate report)

- 80–92% of conflicts are now caught before the turn.
- Time spent on the conflict check while holding the turn: 115 µs → 4 µs (W100) and 160 µs → 1.4 µs (W5).
- Serial-slot wait at W100: 5.77 ms → 3.84 ms.
- Restarts rise at W5 (14.8k → 32k). Early checks catch older conflicts first, so these extra retries are paid off the turn.

## Open items / risks

- **Pre-existing hazard** (found by the commit_set agent; it exists in main, not introduced here). An apply-time whole-transaction retry after the turn has been released can re-publish different tags. Readers that validated in between may miss it. apply_retry_count was 0 in all TPC-C traces.
- Validation covered only v2 TPC-C on a single node.
  - Not yet run: YCSB/OOM, 4-node Raft/Kafka cluster with Merkle consistency, v3 TPC-C.
  - Not yet exercised: ledger replay and terminal-error paths.
- Shared memory: the digest ring is now 8192 × (384 × 8 B) ≈ 24 MiB. Any id outside the ring falls back to the full map check.
- `src/backend/utils/hash/dynahash.c` gained two helpers. They are used only by the rotation path, which is off by default.
- The W100/32 gain is small because 32 workers become the limit once the turn is short. Report det at 48 workers, or sweep workers again.
