# trace_rot — phase-trace fixes and early shard rotation

Worktree: `/work/ARIABC/AriaBC.worktrees/detopt_trace_rot`, branch `detopt/trace_rot`.
Base: `8d0968d4c2d657f873a6c27c5b4a5ecc4b8ed532`. Commit: `b704fe8a75e38b8487b6b3020c8242a8db9fc542` (committed locally; not pushed).

## Implementation

- `src/backend/bcdb/worker.c:692`: calculate the entire timespec difference as signed int64 nanoseconds, then divide and convert; monotonic phase durations no longer wrap at second boundaries.
- `src/backend/bcdb/worker.c:2594`: capture the transaction ID before entering PG_TRY and before shared-entry deletion/reuse. All three emit sites use that value (completion-only SELECT, normal completion, expected business-abort completion).
- `src/backend/bcdb/worker.c:606`, `src/include/bcdb/worker.h:92`, `src/backend/bcdb/shm_transaction.c:1439` and `:3668`: fine lock/hash timings require `BCDB_PHASE_TRACE_FINE=1`. Coarse phase and microphase timers and all existing counters remain. Fine fields stay in the CSV but are zero by default.
- `src/backend/bcdb/worker.c:3082`: append `conflict_turn_hits` and `conflict_turn_checks` to separate serial-turn validation conflicts from apply retries. Append `early_rotation_count` and `early_rotation_skipped_count` to verify actual early-clear execution and conservative fallback.
- `src/backend/bcdb/shm_transaction.c:3363`: cached `BCDB_DT_EARLY_ROTATION` switch; default ON. At offset `2*workers` of epoch x-1, publication marks a request for the inactive shard of epoch x before handing off the turn (`:3689`). After gate release, the scheduling worker runs the clear (`worker.c:3137`, `shm_transaction.c:3463`). The epoch-x boundary publisher waits for completion, or uses the original in-turn clear if no request succeeded (`shm_transaction.c:3511`, `:3595`, `:3628`). Thresholds <= `2*workers` use the original clear.
- `src/backend/bcdb/shm_transaction.c:3387`: register each speculative snapshot baseline in its tx-pool entry under `tx_pool_lock`, and stop tracking it after final conflict validation. The early worker checks both the contiguous watermark and all registered baselines (`:3408`) before clearing. This avoids assuming that worker count alone bounds the age of a snapshot when workers can stall. A failed safety check cancels the request and preserves the boundary fallback.
- `src/backend/utils/hash/dynahash.c:384`: detach only that partition's bucket heads under its existing shard lock. After all partitions are detached and previous readers have released their locks, rebuild allocator lists without holding the shard's partition locks (`:408`). No insert into that shard can occur until completion. No bucket masks or allocator mutexes are reinitialized by the early path.
- `src/backend/bcdb/shm_transaction.c:3445`: cancel an unfinished owned request on the worker PG_CATCH path. Recovery's existing quiescent clear also resets rotation markers (`:501`).

## Conflict invariant and synchronization argument

Let T be the epoch length and x the next epoch. Its inactive shard contains entries from epoch x-2 or earlier; every such writer has ID <= C = (x-1)*T-1. The scheduler is publisher P=(x-1)*T+2*workers, and only schedules when `T>2*workers`.

Before clearing, the early worker requires contiguous commit watermark >= C and every snapshot still awaiting conflict validation to have baseline >= C. Snapshot registration and the retirement scan share `tx_pool_lock`: an existing snapshot is included, and a future registration observes a watermark at least as new as the scanner's watermark. Newly inserted tx-pool entries are initialized as idle before releasing that lock. Registrations persist across conflict waits and are refreshed for re-execution; entries are marked idle only after validation/publication or on an exception that aborts validation. Already validated apply-stage transactions have no remaining conflict window to probe; an actual re-execution registers its new baseline again.

Consequently no cleared writer ID can satisfy `baseline < writer_id < own_id` for any pending or future conflict check. This is a direct check of the window property, independent of arbitrary worker stalls. If the check cannot establish this fact, nothing is cleared early; the unchanged baseline epoch-boundary behavior applies. The baseline threshold requirement (`T>=2*workers-1`) remains enforced.

Partial clears are safe: a probe before its partition is detached sees the old entries; a probe afterward sees empty buckets, whose writers are already outside its relevant window. Each partition lock excludes traversals while its heads are detached. Once all partition locks have been acquired/released, no probe retains a pointer to an old entry, so rebuilding allocator links cannot race with traversal. The completion atomic is published only after allocator reset, with a preceding write barrier and a matching read barrier in the boundary publisher (plain PostgreSQL atomic reads/writes supply no barrier themselves). Epoch-x publishers wait before inserting, and epoch x-1 publishes only to the other shard. `mapB_nonempty` stays set through partial clearing, resets only after mapB is completely cleared, and cannot race with a new mapB publisher because the latter waits for completion. Reused epoch markers distinguish subsequent rotations; quiescent recovery resets them.

The new path leaves the MAX-writer lookup semantics, conflict retry gate, publication ordering, apply/commit order and all workload/harness settings intact. The existing unsafe read-only bypass is not enabled or introduced by this change.

## Switches and trace audit

- `BCDB_DT_EARLY_ROTATION`: unset/default or `1` enables early retirement. `0` (also false/FALSE/no/NO) disables registration, scheduling, waiting and early clear and runs the original in-turn clear. Trace fixes still apply.
- `BCDB_PHASE_TRACE_FINE=1`: enables per-probe lock/hash clock reads. Unset/default disables them. `BCDB_PHASE_TRACE` remains the existing trace file prefix.
- Reviewed `bcdb_ptrace_now_us`, inline timer start/stop and `bcdb_get_time`: they convert absolute seconds/nanoseconds to microseconds before subtracting and do not have the negative-nanosecond cast bug. `bcdb_get_time` still uses CLOCK_REALTIME, so its legacy profiling deltas remain susceptible to clock adjustments; changing that unrelated global clock is outside scope.
- Harness `total_restarts` is summed from trace CSV column 2 (`sweep_run.sh:215`). Its zero value with tracing off means no trace rows were collected, not that execution had no retries. Actual retry/conflict totals are validated in the trace-on audit.
- `publish_hash_clear_us` now includes off-turn clears. It is not solely serial-turn time and must not be subtracted from `publish_ws_us` indiscriminately. `publish_rotation_lock_us` retains boundary lock timing and includes epoch-completion waiting. `early_rotation_count` identifies off-turn clears.

## Builds and benchmarks

Only the ranking host was used for builds and database runs. Harness scripts were read but not changed. Source changes alone were synced to `~/claude_checks/detopt_20261007/trace_rot/src`; all three builds succeeded. The first build was superseded before any run. W5 trial 1 used postgres SHA-256 `874e25218ffbc8ac003f79a047d121db0dad2713f8c116f59382081f46a60816`. Explicit publication/acquisition memory barriers were then added around the rotation-completion marker; the remaining runs use final postgres SHA-256 `6fcb5716269708d74db3346ee62067099269ff9d7f3f79c2685938663dfa1acf`. The final six local/remote source digests were compared and matched; manifests are retained in the variant directory as `SOURCES.txt` and `BINARIES.txt`. W5 is repeated on the final binary.

### Run commands and exact status lines

All commands use `ssh protectdr@10.129.7.57`, ports `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101`, worker count 32, and the unchanged harness defaults.

1. `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101 BCDB_DT_EARLY_ROTATION=1 ~/claude_checks/detopt_20261007/harness/bench.sh trace_rot 5 32 1 off`

```text
2026-10-07T21:25:07+05:30 RUN trace_rot det W=5 k=32 t=1 ptrace=off window=65536
2026-10-07T21:26:43+05:30 DONE trace_rot det W=5 k=32 t=1 ptrace=off rc=0 completed_tps=487.76 divergence_count=0 permanent_failures=0 total_restarts=0  state=fdb9545f1c588209
```

W5 reference hash matched. TPS is -49.91% against base trial 1 (973.67 TPS), -49.89% against base trial 2 (973.36 TPS). This is a regression in this attempt; the cause is not established.

2. `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101 BCDB_DT_EARLY_ROTATION=1 ~/claude_checks/detopt_20261007/harness/bench.sh trace_rot 30 32 1 off`

```text
2026-10-07T21:39:11+05:30 RUN trace_rot det W=30 k=32 t=1 ptrace=off window=65536
2026-10-07T21:41:47+05:30 DONE trace_rot det W=30 k=32 t=1 ptrace=off rc=0 completed_tps=2061.91 divergence_count=0 permanent_failures=0 total_restarts=0  state=dcb895e5961ab8e8
```

W30 reference hash matched with the switch on. TPS is -1.80% against base trial 1 (2099.71 TPS). The substantially slower base trial 2 is unsuitable for interpreting a stable speedup.

3. `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101 BCDB_DT_EARLY_ROTATION=0 ~/claude_checks/detopt_20261007/harness/bench.sh trace_rot 30 32 2 off`

```text
2026-10-07T21:41:48+05:30 RUN trace_rot det W=30 k=32 t=2 ptrace=off window=65536
2026-10-07T21:44:24+05:30 DONE trace_rot det W=30 k=32 t=2 ptrace=off rc=0 completed_tps=733.81 divergence_count=0 permanent_failures=0 total_restarts=0  state=dcb895e5961ab8e8
```

W30 reference hash matched with the switch off. TPS is -65.05% against base trial 1 (2099.71 TPS). The ON run was 180.99% faster than this OFF run in these individual attempts, but neither this ratio nor the baseline comparisons establish a stable speedup given the host's observed variation.

4. `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101 BCDB_DT_EARLY_ROTATION=1 ~/claude_checks/detopt_20261007/harness/bench.sh trace_rot 5 32 2 off`

```text
2026-10-07T21:47:41+05:30 RUN trace_rot det W=5 k=32 t=2 ptrace=off window=65536
2026-10-07T21:49:58+05:30 DONE trace_rot det W=5 k=32 t=2 ptrace=off rc=0 completed_tps=389.49 divergence_count=0 permanent_failures=0 total_restarts=0  state=fdb9545f1c588209
```

Final-binary W5 reference hash matched. TPS is -60.00% against base trial 1 (973.67 TPS). Both W5 ON attempts underperform the baseline substantially; this change is not established as a throughput win. The final reviewer should investigate with controlled A/B before selecting it for throughput.

5. `PORT=55441 CLIENT_PORT=18101 RAFT_PORT=19101 BCDB_DT_EARLY_ROTATION=1 BCDB_PHASE_TRACE_FINE=0 ~/claude_checks/detopt_20261007/harness/bench.sh trace_rot 100 32 1 on`

```text
2026-10-07T22:10:39+05:30 RUN trace_rot det W=100 k=32 t=1 ptrace=on window=65536
2026-10-07T22:16:07+05:30 DONE trace_rot det W=100 k=32 t=1 ptrace=on rc=0 completed_tps=3036.31 divergence_count=0 permanent_failures=0 total_restarts=1475  state=e82921e1ebb1b7ab
```

W100 reference hash matched with normal tracing. TPS is +6.81% against base trace-off trial 1 (2842.82 TPS), and +9.46% against the observed base trace-on run (2773.80 TPS). The trace-off comparison includes different instrumentation overhead; neither is a controlled multi-trial speedup claim.

### W100 trace audit

Parsed all 32 files in `~/claude_checks/tpcc_sweep_v2_detopt_20261007/trace_rot/det_pon_k32_t1_w100/ptrace_keep/`. Every file has 51 columns and well-formed numeric rows.

- Rows: **20,000**. Unique IDs: **20,000**, exactly **0..19999**; no missing or duplicate IDs.
- All durations are nonnegative and below `1e12` us. The maximum across all duration fields is **47,507 us**. Coarse and required microphase totals remain present and positive.
- All five per-probe/publish fine timing fields are **zero** with `BCDB_PHASE_TRACE_FINE=0`; counters remain populated.
- Conflict hits: **1,475**, validation attempts: **21,475**, restarts: **1,475**, apply retries: **0**.
- Early clears: **14**, safety-check skips: **0**. Total clears: **15**, comprising the initial in-turn tx-0 clear and 14 clears at `64 + 1500*n` for `n=0..13`. The last prepares the next epoch beyond this 20k workload.
- Off-turn clear durations range from **14,344 to 34,449 us**. They are absent from the corresponding `publish_ws_us`; for example tx 7564 cleared for **34,449 us** but published in **10 us**. No later boundary needed a fallback clear.
- Mean accumulated conflict microphases per completed row: **27.22 us**; mean `publish_ws_us`: **8.88 us**, including the initial clear. These are instrumented totals including validation retries, not independent TPS clocks.
- Independently computed full state SHA-256: `e82921e1ebb1b7abe7c1e53c1c1c8d4524fa60ea665e9cb376325a1edd45bd8c`.

Full maxima, counter totals and per-clear rows are retained at `~/claude_checks/detopt_20261007/trace_rot/TRACE_AUDIT_W100_T1.json`.

Remaining W100 trace-off run: pending.

## Risks and open questions

- The observed off-turn clears still delay their scheduling transaction before apply/commit by 14-34 ms. This can hold back the contiguous commit watermark and affect false-positive retries or turn-held retry waits, especially at high contention; it is a candidate to investigate for W5, not an established cause of the measured regression.
- Extra per-snapshot lock/atomic work and one tx-pool scan per epoch may offset the removed serial clear cost. Compare the same binary with the switch off; final controlled A/B belongs to the merging reviewer.
- Safety-check failure can retain an in-turn rotation stall. An epoch-boundary publisher also waits if its scheduling worker is delayed. PostgreSQL worker exceptions cancel unfinished requests; postmaster crash/recovery still uses the existing process/recovery semantics.
- The partition/allocation helpers require the fixed, preallocated, partitioned DT hash tables and the no-insert-until-completion protocol. They are not general-purpose clear functions for growing hash tables.
- Existing global baseline fallback behavior and its threshold assumption are preserved, rather than strengthened, when early retirement is unavailable.
- Baseline TPS has substantial variation already in `status.txt` (W30 trial 1: 2099.71, trial 2: 550.12; W100 trial 1: 2842.82, trial 2: 868.94). Single-trial comparisons cannot establish a stable speedup. Other agents' concurrent builds are a plausible source of measurement noise, not a verified cause.
