# Post-publication full restart: source-based correctness review

Date: 2026-10-07. Checkout HEAD: `51daba6d3844ca0b704b5fe060d0a02578bfa5ce`.

## Finding and evidence boundary

**The reported gap is real in the publication-based deterministic gate.** A transaction can release successors, then abort its physical PostgreSQL transaction and re-execute its business SQL using a fresh snapshot. The conflict check rejects only smaller deterministic transaction IDs; it does not reject values or keys supplied by already-completed successors. There is no preserved first-execution footprint/value comparison in this retry path.

This is a source-derived correctness finding, not a cluster reproduction. I did not execute a fault-injection scenario, establish that a historical divergence came from this path, or measure its frequency. No implementation, configuration, animation, or benchmark logic was changed. No database, build, test, or benchmark was started. Existing user changes and benchmark artifacts were preserved. This directory contains only this report and the saved-log survey evidence.

The central invariant is stronger than an unchanged write-key set: after successors are released against an earlier transaction's published effects, that earlier transaction must not subsequently adopt successor-dependent semantics. Re-executing SQL with the same deterministic ID but a new physical transaction does not preserve that invariant automatically.

## 1. What the worker actually does

The relevant path is `bcdb_worker_process_tx_dt()` in `src/backend/bcdb/worker.c:2537`.

| Stage | Implementation | Consequence |
|---|---|---|
| Simulate business SQL | `get_write_set()`, `worker.c:2054`; supplied snapshot passed to `PortalStart()` at 2283 and SQL executed by `PortalRun()` at 2332 | Read results, chosen keys, deferred tuple values, and branches come from this execution. |
| Wait for deterministic gate | `worker.c:3041`; source 0 uses `bcdb_wait_for_serial_slot()`, source 1 uses predecessor commit | With source 0, predecessor publication is sufficient. |
| Check local reads and writes | `conflict_checkDT()`, `shm_transaction.c:3223` | Both sets are checked against a shared map of writers. |
| Publish writes | `worker.c:3084`, `publish_ws_tableDT()` | Reads are not published into that shared map. |
| Release successors | `worker.c:3100` advances `published_max` before apply | Successors can check, apply, and physically commit before this transaction commits. |
| Apply deferred writes | `worker.c:3111`, `bcdb_apply_optim_writes_with_retry()` | There are apply-only retries and fallbacks to a whole transaction rerun. |
| Finish physical transaction | `finish_xact_command()`, `worker.c:3481` | Physical commit is separate from the earlier publication. |

The gate predicate is `(get_published_max_txid(tx) + 1) >= tx->tx_id` (`worker.c:1366`). `published_max_advanced` is initialized outside the retry loop (`worker.c:2556`). Whole retries do not reset it or retract the publication. A retry of an old deterministic ID therefore passes the already-advanced gate; it does not recover an exclusive place ahead of successors.

On `rw_conflicts == 1`, the loop:

1. Reinitializes `ws_table_record`, `ws_table_publish_record`, and `rs_table_record` (`worker.c:2685`).
2. Drains deferred writes and resets their memory context (`worker.c:2705`).
3. Calls `AbortCurrentTransaction()` and `reset_xact_command()` (`worker.c:2712`).
4. Starts a new transaction and calls `GetTransactionSnapshot()` (`worker.c:2757`, 2764).
5. Re-executes the original SQL and builds new deferred writes.

The baseline `tx_id_committed` is refreshed from the contiguous committed watermark (`worker.c:2746`). That integer is a conflict-check baseline, **not a restriction on which PostgreSQL XIDs the snapshot can see**. Snapshot creation uses the ordinary serializable snapshot wrapper (`snapmgr.c:343`), which calls `GetSnapshotData()` (`predicate.c:1725`). `GetSnapshotData()` assembles physical PostgreSQL visibility (`procarray.c:1504`); it does not impose an upper bound on BCDB transaction IDs.

## 2. Reads are checked, but not published

The statement “read tags are never put in the map” is accurate for shared DT publication. It should not be expanded into “reads are never checked.”

- `rs_table_reserveDT()` (`shm_transaction.c:1195`) records local read tags.
- Logical row-key reads and some leading-equality scan-prefix reads are also recorded (`shm_transaction.c:1077`, 1108).
- `conflict_checkDT()` walks both local write and read lists against the shared writer map (`shm_transaction.c:3247`, 3271).
- `publish_ws_tableDT()` publishes the checked write tags and publish-only write-prefix tags, not the read list (`shm_transaction.c:3472`).

The check is explicitly directional. Both map probes in `table_checkDT()` use:

```c
entry->tx_id < activeTx->tx_id &&
entry->tx_id > activeTx->tx_id_committed
```

See `shm_transaction.c:1431` and 1465. A writer with ID 12 is ignored when transaction 10 rechecks, even if its write is visible in transaction 10's new snapshot.

This direction makes sense during an initial ordered validation: an earlier read followed by a later write is compatible with the prescribed order. It ceases to justify safety when the earlier transaction restarts after that later write has committed.

Republishing does not repair the schedule. It happens after the new simulation and cannot invalidate a completed successor. Within a shard, an entry is updated only when the publishing ID is larger than the stored ID (`shm_transaction.c:3489`). Transaction 10 does not replace a stored writer ID of 12 on a newly touched key.

## 3. An unchanged logical footprint is not enough

This is a conditional semantic counterexample, not a recorded trace or an executed SQL reproducer. Let A and B be independent logical records, initially zero. Transaction 10 reads A and computes B from it; assume another deferred operation encounters one of the supported apply failures that triggers a whole rerun.

| Step | Transaction 10 | Transaction 12 | Visible consequence |
|---|---|---|---|
| 1 | Reads `A=0`; prepares `B=0` | | Local read set includes A; published write set includes B and any other actual writes. |
| 2 | Publishes its writes and releases successors | | A is read-only in transaction 10 and is not a published write reservation. |
| 3 | Has not committed | Writes `A=1` and physically commits | There is no original transaction-10 write to A for transaction 12 to conflict with. |
| 4 | Supported apply failure causes full abort/rerun | | Transaction 10 gets a new snapshot. |
| 5 | Reads `A=1`; prepares `B=1` | | The business result changes. |
| 6 | Recheck ignores writer 12; applies and commits | | Final state includes `A=1, B=1`. |

The specified order 10 then 12 produces `A=1, B=0`. The restarted execution can instead produce `A=1, B=1`. **The logical read and write key sets can be identical across both executions.** A key-set comparison alone would therefore not detect this case.

This result could be valid under a different serial order, 12 then 10. That does not make it correct for a deterministic executor whose agreed order is 10 then 12.

If another replica does not take the full rerun, or takes its replacement snapshot before transaction 12 commits, its final B can remain zero. Identical ordered SQL input does not then guarantee identical state. Actual replica divergence requires the relevant scheduling/trigger differences; it is a possible consequence, not an observation from this investigation.

For data-dependent SQL, a fresh read can also change which record is selected, whether a row exists, a range's membership, an aggregate, a control-flow branch, or a generated identifier. New writes are not protected by the first published footprint. Already-committed larger writers remain invisible to the directional conflict check. This is the second reported failure class.

## 4. Which retries matter

Apply-only retries run inside `BeginInternalSubTransaction("bcdb_apply_retry")` (`worker.c:899`). They reapply the stored optimistic queue; they do not themselves rerun all business SQL against a new top-level snapshot. An apply-only retry count is therefore not evidence of the reported whole-rerun hazard.

Whole-rerun branches identified in the caller:

| Log signature | Source | Meaning |
|---|---|---|
| `apply_unique_conflict_full_restart` | `worker.c:3191` | Unresolved unique conflict, outside the special terminal/no-op cases, sets `rw_conflicts=1` and continues the outer loop. |
| `apply_retry_restart` | `worker.c:3213` | Apply helper returns false without the nonretryable flag; outer loop reruns. |
| `apply_unique_settled_retry_restart` | `worker.c:3164` | Apply still requests a retry after the predecessor-settled attempt; also a whole rerun. |

There is a meaningful correction to the claim that this necessarily requires a rare exhausted-retry fallback: when `apply_failed_unique` is true and SQL contains `_proc`, the helper immediately returns false with `nonretryable_error=false` (`worker.c:1025`). It logs `apply_proc_unique_violation_fast_retry` and the caller reaches `apply_retry_restart`. This can happen with zero apply retries; exhausting `BCDB_APPLY_RETRY_MAX=64` is not required.

The TPC-C generator emits names such as `new_order_proc_exec`, `payment_by_name_proc_exec`, and `delivery_proc_exec` (`generate_tpcc_workload.py:120`, 160, 189), which match that substring test. This identifies applicability of the branch, not an observed TPC-C failure rate.

Not every SQL error uses these retries. The helper treats unique violations specially. Other non-whitelisted exceptions are logged and rethrown (`worker.c:988`); the saved deadlock described below follows that fatal path. Waiting for all predecessors on some unique-conflict branches also cannot remove a successor that has already committed.

## 5. Other mechanisms do not supply the missing guarantee

**Serializable isolation:** A replacement physical transaction has a replacement snapshot. Even ordinary PostgreSQL SERIALIZABLE would not force this new transaction to precede an already-committed transaction just because its external BCDB ID is smaller.

There are additional BCDB-specific changes: normal transaction construction sets `pred_lock=false` (`middleware.c:1222`, 1402). `PredicateLockAcquire()` records local DT reads and returns before acquiring normal SSI predicate locks in this mode (`predicate.c:2432`). For two BCDB transactions, `FlagRWConflict()` sets BCDB flags and returns before normal SSI conflict-edge processing (`predicate.c:4658`). The inspected DT worker retry/commit path does not test those flags to reject successor-dependent reruns. SSI cannot be assumed to rescue this path.

**Row locks:** Apply uses waiting tuple updates, which protect the written row while applying. They do not lock an unrelated row read earlier by ordinary SELECT, or reconstruct the original read value after abort/restart. Explicit row-locking SQL can constrain individual schedules, but it is not a universal protection in this executor.

**Committed watermark and result order:** A later physical commit can occur while the contiguous committed prefix still stops before transaction 10. The watermark is not a snapshot visibility ceiling. Ordering result delivery or advancing the completion prefix later does not undo database effects already adopted by a rerun.

**Ledger:** The Raft ledger claims/deduplicates an item and records terminal outcomes (`raft_apply_ledger.c:495`). It does not pin the business snapshot to the item's deterministic position. A failed top-level transaction rolls back its claim and business writes; a replacement attempt can execute the same item identity with different read values.

**Parse barrier:** It synchronizes initial simulation readiness and carries a `parse_barrier_done` flag across retries. It does not retract the publication or stop already-released successors on a full rerun.

**CUT export:** CUT hides PostgreSQL writer XIDs beyond the boundary (`recovery.c:65`), after waiting for the committed prefix. If an included earlier writer has already computed B from a later transaction's A, hiding the later writer's own XID does not remove that dependency from B. An exact writer-visibility cut does not independently prove that included execution obeyed the deterministic serial order. This is an implication of the conditional counterexample, not a reproduced recovery failure.

## 6. Boundaries and workload qualifications

With `bcdb_serial_gate_source=1`, a successor must pass the predecessor-commit gate (`worker.c:3042`) before applying. Among transactions obeying that gate, transaction 12 cannot physically commit while transaction 10 is still in a whole retry. That prevents the specific later-commit schedule above; it is not a comprehensive correctness certification of the mode. Effectively serial execution likewise removes this concurrency condition.

With tracking enabled and the original write reservations retained, later transactions touching transaction 10's original write keys are expected to detect the predecessor and wait/retry. That explains why many ordinary co-write schedules are handled. The gap concerns read-only dependencies and effects outside that original write footprint, plus changed semantics even when logical keys stay equal.

YCSB needs a narrower statement than “point keys make this safe.” Fixed existing-row point accesses usually have stable logical key sets and avoid the insert/unique trigger. The current paper-YCSB helper has a further protection against state-dependent output: reads do not feed update values; updates use a per-transaction seeded random sequence and return void (`generate_paper_ycsb.py:108`). Those properties can make this particular state-divergence mechanism hard to exercise. A different point-key procedure that reads A and writes a computed value to B is still vulnerable in principle. Stable keys alone are not a proof.

TPC-C has data-dependent selections, but individual dependencies require scrutiny:

- NewOrder increments and returns `district.d_next_o_id` (`restore_tpcc_procs.sql:54`). The district row is also written and published; a later NewOrder in the same district cannot simply bypass that original write reservation. The fact that an order ID is read from a row is not sufficient to prove this exact pair fails.
- Delivery selects an oldest `new_order` record (`restore_tpcc_procs.sql:342`) and follows related order/customer records. Customer-by-name selection uses ordering/median logic (`restore_tpcc_procs.sql:276`). These are examples of data-dependent access patterns, not demonstrated exploit pairs.
- This review did not establish a concrete failing pair of the supplied TPC-C procedures with a measured apply-restart trigger. It also did not verify each archived run's procedure/binary provenance against today's source.

## 7. Saved-log survey

I read saved files named `postgres.log`, `postgres_workload.log`, `server.log`, or `postgres_node*.log` within the scopes below. Counts are per file, not per independent run; backups and repeated node logs can be included. This was not an exhaustive survey of all machines or all historical archives.

| Saved scope | Files | Bytes | Files containing `[BCDB_FLOW]` |
|---|---:|---:|---:|
| `Final_Results/TPCC` | 190 | 80,394,034 | 0 |
| `Final_Results/ONLINE_RECOVERY` | 60 | 118,353 | 0 |
| `scripts/bench_full_results` | 186 | 529,791 | 0 |
| `.bench_tmp/tpcc_v3_w5_20261003` | 8 | 24,167,574 | 0 |
| `.bench_tmp/paper_ycsb_determinism_20261003_6eXkna` | 19 | 10,182,886 | 10 |
| `.bench_tmp/paper_ycsb_cluster_20261003_dFaUWmHp` | 599 | 344,464,791 | 0 |
| **Total** | **1,062** | **459,857,429** | **10** |

All three full-restart signatures had zero occurrences. `apply_proc_unique_violation_fast_retry` and `apply_retry_exhausted` also had zero occurrences. The ten files containing flow logs contained 18,690 `[BCDB_FLOW]` lines; no investigated restart marker was found in that limited subset.

**Zero markers cannot establish zero historical restarts.** `BCDB_FLOW_LOG` is conditional on `BCDB_FLOW_DEBUG`, off by default (`worker.c:390`, 561; runner at 508). The canonical collector defaults to `POSTGRES_LOG_MODE=compact`; its filter retains selected profiles, errors, fatal/panic messages and startup markers, but drops ordinary LOG restart markers (`run_4node_raft_cluster.sh:1702`). Most surveyed files had no flow lines at all.

One unconditional `SAFE_APPLY_EXCEPTION` was found:

`.bench_tmp/paper_ycsb_determinism_20261003_6eXkna/default_1/postgres.log:6286`

It reports SQLSTATE `40P01`, followed by `SAFE_NONDETERMINISTIC_SQL_ERROR` and `BCDB_FATAL worker_tx_error` for transaction 517. This is a deadlock/fatal-error event, not evidence of a successful post-publication internal full rerun. Its saved settings also had DT conflict tracking off; it should not be used as a demonstration of the tracking-enabled gap.

The per-file counts are in `log_counts.csv`; scope totals and sample records are in `log_summary.json`.

## 8. Assessment of the suggested guard

A comparison of first-run versus rerun **logical** read/write keys could detect footprint expansion or changes. It would not detect the unchanged-footprint A-to-B example, where only a read value and resulting write value change. Logging a difference would diagnose some cases without preventing them; refusing a changed footprint would cover those cases but would still not establish deterministic correctness for matching footprints. The original local sets are cleared by this retry path, so no such baseline comparison exists here today.

This is an assessment of the suggested guard, not a repair proposal or implementation. The request was analysis only.

## Source provenance

The core reviewed files were clean relative to HEAD before review. SHA-256:

| File | SHA-256 |
|---|---|
| `src/backend/bcdb/worker.c` | `e35357c5134dcec1f33e25ce626614f1151ecc142377c40f3104e9a404b0e4e6` |
| `src/backend/bcdb/shm_transaction.c` | `5196db55b51e60c8d68f334c6a3829610ca9a5031bb015c2335521f93d56f4a5` |
| `src/backend/storage/lmgr/predicate.c` | `4b2552f7055ac6d95e7b2634de438ff24c8ef4badf25e4faa73cf40d2da2d61d` |

Graphify was used to navigate the worker, conflict, snapshot, ledger, recovery, and workload components; conclusions were verified by reading the implementation, not by trusting graph-inferred edges or markdown descriptions. No implementation edits were made, so no AST graph update was needed for this review.
