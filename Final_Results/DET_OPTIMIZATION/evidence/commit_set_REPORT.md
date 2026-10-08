# commit_set — work in progress, 2026-10-07

Latest commit: 235ed41431af5de3e2b609f734ca644c27832e5c. Initial implementation: 99ed6155942506274bf87eff3627fea3f266bdcc. Branch: detopt/commit_set. Main/reference: 8d0968d4c2d657f873a6c27c5b4a5ecc4b8ed532.
Runtime switch: BCDB_DT_COMMIT_SET; absent/empty enables it, 0/false/FALSE/no/NO disables it. One cached read per backend.

Implemented pre-snapshot bounded process-local commit bitmap, separate checked-writer and publish-only-writer maxima, individual commit-flag retry waiting for checked conflicts (with contiguous-prefix fallback), and trace counters/timers. Removed unused bcdb_xid_preceded_snapshot. Baseline mode takes Max of the two fields, retains the original snapshot baseline and contiguous retry wait. Trace-only sampling in baseline mode measures eligibility without changing its conflict decisions.

Validation is pending. Do not treat this interim report as acceptance.

## Merge safety question — unresolved

The requested checked-writer induction is valid for ordered, fixed-coverage publication. The existing worker also has whole-transaction apply retries after handoff: worker.c:3270-3290 sets rw_conflicts=1 after published_max was advanced; worker.c:2740-2819 discards the lists, aborts, and reruns SQL against a fresh snapshot. There is no enforcement in this path that the new write tags are a subset of the first publication's tags.

Commit 235ed414 stores all publications after handoff in the unchecked maximum. This prevents treating the retrying writer's out-of-turn check as an inductive witness. It does not retroactively protect a reader that already passed before the retry introduced a new tag. Possible interleaving: T1 initially publishes A and stalls on apply; T2 checks/publishes B and commits; T3 samples T2, then skips its checked B entry; T1 reruns and first publishes B. If no existing publish-only predicate covers this change, T3 missed an earlier writer. The original contiguous retry triggered by T2 would have waited for T1. This is a source-derived possible interleaving, not a reproduced benchmark failure.

Do not merge this as generally safe unless supported workloads guarantee frozen publication coverage, or the post-handoff whole-transaction retry path is made compatible with that contract. Required TPC-C runs can show identical state hashes but cannot prove this unexercised path safe. A clarification about the workload contract has been requested; no guarantee has been assumed.

## Correctness argument for ordered, fixed-coverage publication

The sampled prefix and bitmap are fixed before GetTransactionSnapshot(). A successful transaction's release commit flag is stored after finish_xact_command(); observing that exact id with an acquire load before snapshot creation means its PostgreSQL writes are visible. A ring mismatch, missing block, or candidate outside the bounded range never supplies evidence to skip a conflict. The sample is rebuilt on every speculative execution and retry and occupies 256 process-local bytes (compile-time capacity 2048 ids).

A checked writer c performed the same tag check before publishing. By induction, it either waited for earlier conflicting writers or proved their writes visible through its own earlier sample. Its snapshot precedes its own commit, which precedes our sample and snapshot. Therefore skipping the checked maximum cannot conceal an unseen older writer. An unchecked prefix has no such argument, so its separate maximum always retains the original watermark rule. Terminal deterministic errors bypass conflict_checkDT; their tags are explicitly classified as unchecked even when they came from ws_table_record.

A checked conflict's retry wait accepts the exact candidate commit flag or the contiguous watermark having passed that candidate. The existing interruptible polling/backoff, five-second hang watchdog, and gate telemetry remain. Prefix conflicts and switch-off execution continue waiting for the contiguous watermark. The subsequent execution always refreshes the sample and snapshot before using the flag to skip anything. A flag read during the wait never changes the old snapshot's conflict decision.

## Commit-flag store audit

- worker success path: finish_xact_command() precedes bcdb_finish_terminal_item, which writes the flag in raft_apply_ledger.c.
- worker completion-only SELECT shortcut: no business SQL is executed and no write tags are published; its flag cannot claim visibility of business writes.
- worker generic error publication: SQL is in error state and will abort on rethrow, so uncommitted business writes cannot become visible. If error occurs after successful PostgreSQL commit, that commit already precedes flag publication. Any tags published before such an error were already checked; terminal errors that bypass checks are classified unchecked by this patch.
- expected TP001 user abort: AbortCurrentTransaction() precedes completion; no business writes survive.
- ledger replay completion: finish_xact_command() precedes completion publication; existing business writes were already committed by the original execution.
- ledger nonterminal failure publication: business execution is aborted first, a failure receipt transaction is committed, then the flag is stored; no business writes survive.
- shm_block.c's two initialization/reset assignments write -1, never a candidate id, and cannot create a positive sample.

## Counter definitions

baseline_conflict_checks counts the first entry per at-turn conflict_checkDT invocation (including retries) whose combined maximum would trigger the original watermark check. baseline_sampled_conflicts counts those first candidates known committed before the snapshot (including the freshly sampled contiguous prefix in off-mode telemetry). baseline_safe_conflicts is the subset from a checked maximum with no in-window unchecked prefix; these candidates are provably skippable under the induction. These are per-check counts, not distinct transactions or all tag probes. commit_set_skipped counts skipped checked tag probes and can count the same writer repeatedly across different read/write tags. checked_conflicts and prefix_conflicts identify the actual retry reason when enabled. The two timers record total sampling and retry commit-wait time per transaction.

W5 switch-off trace measures baseline decisions directly. W100 switch-on trace measures counterfactual original-rule candidates along the enabled execution, whose timing and snapshots differ from a separate baseline run; this distinction must be retained when interpreting its fraction. Only two W100 attempts are authorized, reserved for enabled trace-on and trace-off validation.

## Risks and open questions

- The proof requires the existing serial publish gate; BCDB_DT_SKIP_READONLY_GATE remains disabled by default and is not used or reintroduced by this patch.
- Maxima conservatively retain prefix conflicts even if the newest prefix writer was sampled committed. Prefix-heavy workloads may have little eligibility.
- The bounded sample may omit committed candidates beyond the runtime ring range, causing extra retries rather than a missed conflict. Existing result-ring retirement and generation-id ownership assumptions still apply.
- Shared entry layout changes require restarting PostgreSQL. All hash declarations use sizeof(WSTableEntry); tx_pool_size now estimates both write shards and the read shard, instead of only one.
- Generic errors, replay, and ring-wrap behavior are audited in source; the required TPC-C runs do not exhaustively exercise every failure/recovery interleaving.
- Baseline untraced result.txt total_restarts=0 is not evidence of zero executor restarts; real restart comparisons require per-transaction traces. W5 off-mode supplies one such comparison; W100 exact baseline restart counts require separate base trace artifacts from the reviewer.
- Builds are remote-only. Host-wide flock serializes DB runs but does not serialize other agents' builds; observed baseline trial 2 W30/W100 throughput dropped substantially, so timings during concurrent compilation are not clean ranking evidence.

## First eligibility measurement (W5, switch off, trace on)

20,000 trace rows; 14,804 restarts across 13,436 retried transactions (67.18%). Original-rule at-turn conflict checks: 14,804. Known pre-snapshot candidates: 8 (0.0540%); provably skippable checked-writer candidates with no prefix conflict: 4 (0.0270%). commit_set_skipped=0 confirms baseline mode. Sampling cost summed to 17,038 us across all executions/retries; retry commit waits summed to 55,536,065 us. Thus stale-watermark false positives were rare in this W5 control, whereas waiting remained substantial. These two added timers use the existing monotonic microsecond timer.

The first off-mode trial achieved 281.64 TPS versus base traced W5 896.90 TPS (14,758 restarts) and base untraced W5 approximately 973 TPS. This does not support a speed claim: timings vary strongly across neighboring host runs. The report will include all enabled results when complete.

Trace quality caveat: the inherited phase duration columns contain huge unsigned values when a timespec nanosecond delta is negative; do not use their sums for phase attribution. There are 20,000 valid CSV rows but 19,999 distinct emitted tx ids in this control; counters/restarts are aggregated by row, avoiding assumptions about one-to-one emitted ids. Existing emit code reads tx->tx_id after delete_tx; that is outside this optimization's scope. State-hash acceptance is independent of these trace caveats.

## Source references

All references below are in /work/ARIABC/AriaBC.worktrees/detopt_commit_set at commit 235ed414 (the original implementation is 99ed6155).

- src/backend/bcdb/shm_transaction.c:246 — bounded commit sampling before snapshots; 256-byte process-local bitmap.
- src/backend/bcdb/worker.c:2819 — sample immediately before the sole GetTransactionSnapshot() in the deterministic retry loop.
- src/include/bcdb/shm_transaction.h:209 — checked and unchecked writer maxima.
- src/backend/bcdb/shm_transaction.c:1406 — candidate eligibility counters, checked skipping, and conservative prefix conflicts in both shards.
- src/backend/bcdb/shm_transaction.c:3419 — publish both maxima; terminal-error tags that bypassed checks are stored as unchecked.
- src/backend/bcdb/worker.c:3142 — identifies whether publication followed a successful check while still owning the ordered turn; terminal errors and post-handoff retries are unchecked.
- src/backend/bcdb/shm_transaction.c:1574 and :1607 — non-DT readers use the combined maximum to preserve their original behavior.
- src/backend/bcdb/shm_transaction.c:855 — shared-memory estimate covers the two write hashes and one read hash; both hash constructors use sizeof(WSTableEntry).
- src/backend/bcdb/worker.c:1673 and :1691 — individual flag predicate and existing watchdog/backoff wait; :3126 selects it only for enabled checked conflicts.
- src/backend/bcdb/worker.c:482 — cached environment switch, default on.
- src/backend/bcdb/worker.c:692 and src/include/bcdb/worker.h:71 — appended trace columns and counter ids.

Build and source provenance: ranking ~/claude_checks/detopt_20261007/commit_set/{COMMIT.txt,SOURCE_SHA256.txt,BINARIES.txt,build_*.log}. All three remote builds passed; no DB run used the first intermediate binary. Trials t1, t2 and t3 used 99ed6155; subsequent trials use 235ed414. An additional final-binary W5 on-mode validation is queued as t8, and a repeated off-mode W5 control as t7. The final four source SHA-256 values were independently matched against the local committed files. git diff --check passes; the worktree is clean. No push or merge was performed. Main-tree graphify query was used for navigation; graphify update was not run, per the shared brief.

Hash sizing audit: PREDICATELOCKTARGETTAG has four uint32 fields (16 bytes), BCTxID is int32. WSTableEntry grows from 20 to 24 bytes. dynahash.c:688 and :838 allocate MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(entrysize); with the ranking host's 8-byte maximum alignment, both entry payloads occupy 24 aligned bytes, so the extra maximum consumes existing padding rather than increasing hash-element allocation size. The estimator change separately corrects the existing one-hash estimate to cover all three actually created hashes; this increases reserved shared-memory accounting, not the number of allocated hash entries. The per-backend bitmap has no shared-memory allocation.

## Exact status lines captured so far

Trial mapping: t1 W5 BCDB_DT_COMMIT_SET=0, trace on; t2 W30 BCDB_DT_COMMIT_SET=0, trace off. Both use PORT=55443 CLIENT_PORT=18103 RAFT_PORT=19103. Pending enabled trials: t3 W5 trace on, t4 W30 trace off, t5 W100 trace on, t6 W100 trace off, all BCDB_DT_COMMIT_SET=1.

```text
2026-10-07T21:21:43+05:30 RUN commit_set det W=5 k=32 t=1 ptrace=on window=65536
2026-10-07T21:23:54+05:30 DONE commit_set det W=5 k=32 t=1 ptrace=on rc=0 completed_tps=281.64 divergence_count=0 permanent_failures=0 total_restarts=14804  state=fdb9545f1c588209
2026-10-07T21:30:08+05:30 RUN commit_set det W=30 k=32 t=2 ptrace=off window=65536
2026-10-07T21:32:04+05:30 DONE commit_set det W=30 k=32 t=2 ptrace=off rc=0 completed_tps=2090.32 divergence_count=0 permanent_failures=0 total_restarts=0  state=dcb895e5961ab8e8
```

W30 off-mode is 0.45% below base t1 W30 (2099.71 TPS); both are trace off. This is consistent with baseline behavior, subject to single-trial noise.
