# early_validate — 2026-10-07

Status: six required workloads accepted with exact reference hashes; one additional W30 startup failure preserved. Performance is mixed, including a substantial W100 trace-OFF regression; hold performance merge pending investigation.
Commit: `2b0b3868` (source fixed throughout validation).
Branch: `detopt/early_validate`, based on `8d0968d4c2d657f873a6c27c5b4a5ecc4b8ed532`.

## Change

`src/backend/bcdb/shm_transaction.c:146–203,733–743,850–861,3373–3490,3552–3705`: shared digest ring, cached runtime switches, process-local checked-tag hash set, and incremental validation. Ring has 131072 slots (twice harness window 65536), each containing 384 64-bit tag hashes, a count/overflow indicator, and release-published owning tx ID. Allocation is 403701760 bytes (385 MiB), included in `tx_pool_size()`; allocated for both switch states so server startup layout does not depend on the switch. Hashing uses this PostgreSQL tree's `DatumGetUInt64(hash_any_extended(..., 0))` over all 16 bytes of `PREDICATELOCKTARGETTAG`.

`src/backend/bcdb/worker.c:1302–1562,3085–3117`: normal speculative execution prepares its checked ws/rs hash set, then validates during the existing serial-slot wait. An early hit returns from that wait and enters the existing conflict retry branch. The final at-turn incremental check remains authoritative. Terminal deterministic errors skip validation. Cached `BCDB_DT_EARLY_VALIDATE` defaults ON; `0` disables early checks, digest publication, incremental final checking, and lookahead. `BCDB_GATE_LOOKAHEAD` defaults ON but only takes effect with the primary switch ON. Its `0` retains the original spin/yield/condition-variable wait while retaining early validation.

`src/backend/bcdb/shm_block.c:987–1008`: with lookahead ON, publication signals both tx+1 and tx+2. A far waiter sleeps with a 1-ms timed CV wait, validates on wake, and spins when `published+2 >= own`. This tree exposes integer millisecond CV timeouts, so a positive 200–500-us CV timeout cannot be represented. The 5-second watchdog and fresh-server startup guard remain.

Declarations are in `src/include/bcdb/shm_transaction.h:302`; trace counter IDs are in `src/include/bcdb/worker.h:69`. Four counters appended to ptrace CSV: `early_conflict_hits`, `turn_conflict_hits`, `ring_fallbacks`, `incremental_txs_checked`.

## Conflict invariant and state audit

A transaction's checked set includes every ws and rs tag that the original full check probes. Every published digest includes every tag inserted into the original map: checked write tags plus publish-only tags. Equal original tags necessarily have equal 64-bit hashes; collisions can only introduce extra retries. Original hash maps remain populated and retain their original rotation logic.

The first pre-turn full map check acquires P0 before probing. Every predecessor through P0 has finished map publication. No successor may publish while our transaction waits for/owns its turn. A clean result records only P0, even if the probes observed a newer predecessor. Subsequent validation covers every ID from max(snapshot watermark, validated prefix)+1 to the acquire-loaded publication prefix. Ring ownership is acquire-checked before and after atomic payload loads, with a read barrier before the final ownership check. Missing/reused slot or overflow falls back to the original full map check; a clean fallback records only the supplied prefix. If the transaction arrives directly at its turn, the ring validates from the snapshot watermark (falling back on unavailable history). No clean incremental check omits an ID. At the turn the prefix is own-1, covering every predecessor not visible at snapshot baseline.

Early conflicts occur before `published_max_advanced` is set; no write publication, application, or commit has occurred. `init=false` has already been set. The existing retry branch drains optimistic writes, resets transaction memory (including the local validation hash set), aborts, resets transaction command state, clears portal/queryDesc/sxact and snapshot-holder state, waits for the conflicting ID's contiguous commit watermark, refreshes the snapshot baseline, and reexecutes. Validation state is rebuilt after execution. `parse_barrier_done` remains true across retries, preventing duplicate barrier registration. Gate telemetry is finished on the early-return path, and the serial wait timer is stopped normally. The optimistic-worker flag was already cleared before entering the gate. Ledger claim work is inside the same PostgreSQL transaction aborted by the existing retry branch and is redone on reexecution. Terminal deterministic-error outcomes retain the original no-retry/no-op path and are excluded from early and at-turn validation. The existing unsafe read-only-gate switch has not been reintroduced or enabled.

The argument assumes ordered publication, with the unsafe legacy read-only skip-gate flag OFF, as required by the brief and the harness. Full-map fallbacks rely on the existing dual-map retention/rotation contract, as baseline does. The ring is independent of rotation and never replaces map population.

## Build and commands

Only ranking was used to build or execute PostgreSQL. First build failed because this older tree lacks `common/hashfn.h`; corrected to its native `utils/hashutils.h`/`hash_any_extended` API. Rebuild returned `BUILD_OK early_validate`. All harness files and workload settings remain unchanged. Ports: `55442/18102/19102`.

Commit: `2b0b3868`. The full run matrix, exact status lines, and comparisons follow below.

W5 ON accepted run:

```text
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 5 32 1 on
2026-10-07T21:20:33+05:30 RUN early_validate det W=5 k=32 t=1 ptrace=on window=65536
2026-10-07T21:21:43+05:30 DONE early_validate det W=5 k=32 t=1 ptrace=on rc=0 completed_tps=1178.60 divergence_count=0 permanent_failures=0 total_restarts=32299  state=fdb9545f1c588209
```

Matched trace-on base:

```text
2026-10-07T21:25:07+05:30 DONE base det W=5 k=32 t=1 ptrace=on rc=0 completed_tps=896.90 divergence_count=0 permanent_failures=0 total_restarts=14758  state=fdb9545f1c588209
```

W5 early ON versus base trace ON: 1178.60/896.90 = +31.41% TPS (single run). Versus base trace OFF t1 973.67: +21.05%, but trace modes differ.

## W5 trace comparison

Each run has 20000 transaction rows. Base W5 versus early W5 ON:

| Metric | Base | Early ON |
|---|---:|---:|
| Total restarts | 14758 | 32299 |
| Early conflict hits | n/a | 29824 |
| Turn conflict hits | n/a | 2475 |
| Ring fallbacks | n/a | 0 |
| Incremental predecessor IDs checked | n/a | 415600 |
| serial_slot_wait_us, mean per transaction | 30405.09 | 17306.50 |
| conflict_us, mean per transaction | 159.69 | 1.35 |
| conflict_ws_us, mean per transaction | 53.22 | 104.76 |
| conflict_rs_us, mean per transaction | 106.66 | 86.97 |
| publish_ws_us, mean per transaction | 24.30 | 20.08 |

92.34% of detected conflicts were caught before the turn. Early plus turn hits exactly equal total restarts. Increased restarts are a cost: early validation can retry against older conflicts before a later predecessor is published. `conflict_ws_us` and `conflict_rs_us` in the early run include the full-map checks performed inside the gate wait; they are not measurements of time occupying the serial turn. The final incremental check is inside `conflict_us`; its work has moved to `serial_slot_wait_us` when performed early. Durations sum across concurrent transactions and must not be added to estimate wall time. No invalid >=1e12 cells occurred in the listed W5 metrics.


## Risks and open questions

- Added 385 MiB shared memory, including OFF mode; it is fixed per server and not per backend. Consider a smaller ring only if the supported maximum in-flight window is formally reduced; ownership mismatch always has a safe full-map fallback.
- Wide transactions overflow 384 tags and fall back to full checking. Duplicate tags consume ring capacity too.
- Hash intersections may cause false positive retries. Early retries can choose an earlier conflict than the max-writer map, potentially increasing the number of speculative attempts while reducing gate occupation.
- Timed CV wake scheduling is not guaranteed at exactly 1 ms. Lookahead provides a separate runtime switch for later measurement.
- Extra signals and scans impose overhead; trace-on versus trace-off measurements must be compared separately.
- No separate A/B isolates lookahead from early validation, and the acceptance workloads did not exercise digest overflow/reuse or Raft replay/terminal-error branches. Those safety branches are retained by code inspection rather than dedicated fault injection.
- Coarse ptrace phase cells >= 1e12 are invalid due to the pre-existing second-boundary overflow; ignore those cells. Microphase counters use a separate monotonic timer.
- Single-run timings do not establish stable speedup, especially given base W30 t1=2099.71 and t2=550.12 TPS with identical correct state hashes.

## Startup path failure and infrastructure workaround

W30 ON trial 3 failed before workload execution: `postgres.log` reports the campaign run directory's Unix socket pathname exceeds the 107-byte limit. This is not an accepted run and produced no state hash. All its files remain preserved.

Without changing any harness script, the installed `early_validate/install/bin/pg_ctl` is now an owned wrapper; its original executable remains `pg_ctl.original`. For `start`/`restart`, it invokes the original with `-o "-c unix_socket_directories=/home/protectdr/claude_checks/detopt_20261007/early_validate/sockets"`; other operations forward unchanged. Both paths stay inside the assigned ranking variant directory. The harness and restore/workload clients already use TCP `127.0.0.1`, so this changes only unused Unix socket placement and allows long run directory names (W30/W100, and trace-OFF labels) to start. No worker, gate, durability, buffer, transaction, or measurement settings were altered. The wrapper must be retained/recreated after a rebuild for this long variant name; `make install` may overwrite it. No PostgreSQL source change was made for this workaround.

```text
2026-10-07T21:28:06+05:30 RUN early_validate det W=30 k=32 t=3 ptrace=on window=65536
2026-10-07T21:28:45+05:30 DONE early_validate det W=30 k=32 t=3 ptrace=on rc=1  state=
```

## W30 and W100 trace detail

Mean durations are computed over valid cells per metric; cells >= 1e12 are excluded independently.

| Metric | W30 base trace ON | W30 early OFF | W30 early ON | W100 published v2 | W100 early ON |
|---|---:|---:|---:|---:|---:|
| restarts (total) | 4334 | 4548 | 5043 | 1494 | 1497 |
| early_conflict_hits (total) | n/a | 0 | 4309 | n/a | 1195 |
| turn_conflict_hits (total) | n/a | 4548 | 734 | n/a | 302 |
| ring_fallbacks (total) | n/a | 0 | 0 | n/a | 0 |
| incremental_txs_checked (total) | n/a | 0 | 340793 | n/a | 297004 |
| serial_slot_wait_us (mean us) | 24869.15 | 16362.28 | 19960.63 | 5773.87 | 3844.73 |
| conflict_us (mean us) | 136.52 | 134.17 | 4.27 | 114.64 | 4.33 |
| conflict_ws_us (mean us) | 45.16 | 43.90 | 88.49 | 36.26 | 94.01 |
| conflict_rs_us (mean us) | 91.59 | 90.58 | 81.67 | 78.65 | 52.99 |
| publish_ws_us (mean us) | 25.17 | 26.58 | 27.73 | 22.85 | 20.78 |

W30: 85.45% of conflicts found early; 4309+734=5043 total retries. The ON run is +6.18% versus base trace ON, but -32.26% versus the earlier OFF run of the same binary. W100: 79.83% found early; 1195+302=1497 retries, versus 1494 in the published v2 trace. W100 early serial wait is 33.41% lower than published v2, and its final conflict phase is 96.22% lower. These are trace-based mechanism observations, not stable TPS rankings.

Excluded conflict_us cells: W30 base=4, W30 early OFF=3, W30 early ON=0, W100 published v2=2, W100 early ON=0. Other listed microphase cells have no invalid values. The published v2 W100 source is `~/claude_checks/tpcc_sweep_v2_20261002_130000/det_matrix_k32_t1_w100/ptrace_keep`. Current-campaign base W100 trace is pending as of this extraction.

Trace-OFF harness `total_restarts=0` is an artifact of its trace-derived AWK summary with no trace rows; it does not prove zero actual retries. Use traced runs for retry counts.

## Final run matrix and exact status lines

All six completed workloads satisfy rc=0, divergence_count=0, permanent_failures=0, and the expected reference hash. The seventh attempt (W30 t3) failed before workload startup and is explicitly excluded. SHA-256 was independently recomputed from every accepted state.hash. Successful pgdata directories were removed by the unchanged harness; result/hash/trace/log artifacts remain. Exactly two W100 attempts were made.

| W | Trial | EARLY_VALIDATE | Trace | TPS | Base reference TPS | Difference |
|---|---:|---:|---|---:|---:|---:|
| 5 | 1 | 1 | on | 1178.60 | 896.90 | 31.41% |
| 5 | 2 | 0 | on | 663.75 | 896.90 | -26.00% |
| 30 | 3 | 1 | on | startup failed | n/a | n/a |
| 30 | 4 | 0 | on | 1273.35 | 812.40 | 56.74% |
| 30 | 5 | 1 | on | 862.61 | 812.40 | 6.18% |
| 100 | 6 | 1 | on | 3307.88 | 2842.82 (trace OFF) | 16.36% |
| 100 | 7 | 1 | off | 585.90 | 2842.82 | -79.39% |

W5/W30 references are base trace-ON t1, matching each variant run's tracing state. W100's current-campaign trace-ON base was unavailable at final extraction, so the 3307.88 comparison against 2842.82 is across tracing modes and cannot establish a matched speedup. Against the brief's approximate published v2 trace-ON 2815 TPS, the W100 traced run is about +17.51%. The W100 trace-OFF run is -79.39% against base OFF t1 (2842.82), and -32.57% against base OFF t2 (868.94). Same-binary switch ON/OFF comparisons: W5 +77.57%, W30 -32.26%; each is a single run with different execution time, not a paired controlled repeat.

Commands below were issued via ssh to protectdr@10.129.7.57:

```bash
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 5 32 1 on
BCDB_DT_EARLY_VALIDATE=0 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 5 32 2 on
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 30 32 3 on
BCDB_DT_EARLY_VALIDATE=0 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 30 32 4 on
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 30 32 5 on
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 100 32 6 on
BCDB_DT_EARLY_VALIDATE=1 PORT=55442 CLIENT_PORT=18102 RAFT_PORT=19102 ~/claude_checks/detopt_20261007/harness/bench.sh early_validate 100 32 7 off
```

Exact campaign status entries for every attempt:

```text
2026-10-07T21:20:33+05:30 RUN early_validate det W=5 k=32 t=1 ptrace=on window=65536
2026-10-07T21:21:43+05:30 DONE early_validate det W=5 k=32 t=1 ptrace=on rc=0 completed_tps=1178.60 divergence_count=0 permanent_failures=0 total_restarts=32299  state=fdb9545f1c588209
2026-10-07T21:26:43+05:30 RUN early_validate det W=5 k=32 t=2 ptrace=on window=65536
2026-10-07T21:28:06+05:30 DONE early_validate det W=5 k=32 t=2 ptrace=on rc=0 completed_tps=663.75 divergence_count=0 permanent_failures=0 total_restarts=14762  state=fdb9545f1c588209
2026-10-07T21:28:06+05:30 RUN early_validate det W=30 k=32 t=3 ptrace=on window=65536
2026-10-07T21:28:45+05:30 DONE early_validate det W=30 k=32 t=3 ptrace=on rc=1  state=
2026-10-07T21:32:05+05:30 RUN early_validate det W=30 k=32 t=4 ptrace=on window=65536
2026-10-07T21:35:54+05:30 DONE early_validate det W=30 k=32 t=4 ptrace=on rc=0 completed_tps=1273.35 divergence_count=0 permanent_failures=0 total_restarts=4548  state=dcb895e5961ab8e8
2026-10-07T21:45:38+05:30 RUN early_validate det W=30 k=32 t=5 ptrace=on window=65536
2026-10-07T21:47:40+05:30 DONE early_validate det W=30 k=32 t=5 ptrace=on rc=0 completed_tps=862.61 divergence_count=0 permanent_failures=0 total_restarts=5043  state=dcb895e5961ab8e8
2026-10-07T21:49:58+05:30 RUN early_validate det W=100 k=32 t=6 ptrace=on window=65536
2026-10-07T21:55:16+05:30 DONE early_validate det W=100 k=32 t=6 ptrace=on rc=0 completed_tps=3307.88 divergence_count=0 permanent_failures=0 total_restarts=1497  state=e82921e1ebb1b7ab
2026-10-07T21:55:19+05:30 RUN early_validate det W=100 k=32 t=7 ptrace=off window=65536
2026-10-07T22:01:33+05:30 DONE early_validate det W=100 k=32 t=7 ptrace=off rc=0 completed_tps=585.90 divergence_count=0 permanent_failures=0 total_restarts=0  state=e82921e1ebb1b7ab
```

Exact base status entries available at report completion:

```text
2026-10-07T21:04:45+05:30 DONE base det W=5 k=32 t=1 ptrace=off rc=0 completed_tps=973.67 divergence_count=0 permanent_failures=0 total_restarts=0  state=fdb9545f1c588209
2026-10-07T21:06:25+05:30 DONE base det W=30 k=32 t=1 ptrace=off rc=0 completed_tps=2099.71 divergence_count=0 permanent_failures=0 total_restarts=0  state=dcb895e5961ab8e8
2026-10-07T21:10:35+05:30 DONE base det W=100 k=32 t=1 ptrace=off rc=0 completed_tps=2842.82 divergence_count=0 permanent_failures=0 total_restarts=0  state=e82921e1ebb1b7ab
2026-10-07T21:11:49+05:30 DONE base det W=5 k=32 t=2 ptrace=off rc=0 completed_tps=973.36 divergence_count=0 permanent_failures=0 total_restarts=0  state=fdb9545f1c588209
2026-10-07T21:14:08+05:30 DONE base det W=30 k=32 t=2 ptrace=off rc=0 completed_tps=550.12 divergence_count=0 permanent_failures=0 total_restarts=0  state=dcb895e5961ab8e8
2026-10-07T21:18:51+05:30 DONE base det W=100 k=32 t=2 ptrace=off rc=0 completed_tps=868.94 divergence_count=0 permanent_failures=0 total_restarts=0  state=e82921e1ebb1b7ab
2026-10-07T21:25:07+05:30 DONE base det W=5 k=32 t=1 ptrace=on rc=0 completed_tps=896.90 divergence_count=0 permanent_failures=0 total_restarts=14758  state=fdb9545f1c588209
2026-10-07T21:39:10+05:30 DONE base det W=30 k=32 t=1 ptrace=on rc=0 completed_tps=812.40 divergence_count=0 permanent_failures=0 total_restarts=4334  state=dcb895e5961ab8e8
```

Full independently recomputed state.hash SHA-256 values:

- W5: `fdb9545f1c588209556c090351f39a0bfa49da418633f1d5aa9b5264750440e4`
- W30: `dcb895e5961ab8e84091fbf3d3723531593ec1ba9db18214b3209f1c59c9a049`
- W100: `e82921e1ebb1b7abe7c1e53c1c1c8d4524fa60ea665e9cb376325a1edd45bd8c`

## Recommendation and material unresolved performance risk

The required acceptance workloads are correct and traces show validation/retries moving before the serial turn. Do not treat this as a demonstrated production throughput improvement or merge for speed without the coordinator's controlled A/B. W100 trace OFF is a substantial measured regression, and W30 ON is slower than the same binary's OFF run. Timing variability in the base campaign is real, but it does not establish that this regression is noise. The OFF run's pre-run host snapshot had load average 1.24/1.57/1.80; no in-measurement CPU/I/O attribution was collected here, so external interference is not proven.

Possible contributors requiring measurement include contention from concurrent initial full-map probes (which were serialized before), extra speculative attempts, ring hash-intersection CPU work, and the 1-ms timed-wake/lookahead policy. Tracing changes pacing and cannot substitute for trace-OFF throughput evidence. No component-isolation runs were performed. The requested W100 budget is exhausted; no third W100 run was started. Investigate with allowed W5/W30 diagnostic runs or the coordinator's final campaign rather than accepting the trace-ON speedup as proof.

Source provenance: git checksum dry-run confirmed all five changed files match ranking; local worktree is clean at 2b0b38689bdcf9073135c02225c2ac3fe46d167d. No push or other-branch mutation was performed.
