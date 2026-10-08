# Post-publication re-execution: analysis, fix and evidence (2026-10-08)

Branch: `detopt/settle` (worktree `/work/ARIABC/AriaBC.worktrees/detopt_settle`). It is based on `detopt/combined` (early validation + tag dedup + trace fixes). Not merged, not pushed.

## 1. The bug (confirmed)

With `bcdb_serial_gate_source = 0` (every published det result uses it), a transaction releases its serial turn (`set_published_max_txid`) BEFORE it applies and commits. Three apply-failure branches then aborted the physical transaction and **re-executed the business SQL on a fresh snapshot**:
- `apply_unique_settled_retry_restart`
- `apply_unique_conflict_full_restart`
- `apply_retry_restart`, also reached with zero retries through the `apply_failed_unique && strstr(sql, "_proc")` fast path

By then, successors could have validated, applied and committed. The fresh snapshot sees their writes. The re-check ignores writers with a larger tx_id, and successors that only *read* our keys never waited for us. So tx N could compute its writes from tx N+k's values: an order other than the prescribed one, and a different one on each replica.

Comparing key sets (Codex's suggested guard) would not catch it: the same keys can carry different values.

Two more defects found while fixing it:
- **Coverage gap.** Write tags covered only one key index per table (`bcdb_keytag_index`: the PK, else the smallest unique index). A successor that inserted the same value of *another* unique index did not conflict. If it applied first, the earlier transaction's insert failed with 23505: the later transaction won.
- **Deadlock in early validation** (detopt/combined, only reachable through the old restart path). On a re-run after its own publication, `published_max >= own id`. The incremental ring check then matched the transaction's own digest and waited for its own commit forever. Reproduced: tx 0 stuck, `last_committed=-1`.

Also: with the old code, a genuine 23505 inside a `*_proc` call restarts forever, because every re-run fails the same way.

## 2. Design (as implemented)

**Invariant: once a transaction has released its turn, it never re-executes business SQL and never takes a new snapshot.** Its validated deferred write set is final.

`BCDB_DT_POST_PUBLISH_SETTLE` is default on; `0` restores the historical branches for A/B and reproduction only. The pieces, in worker.c unless noted:
- **Apply helper** `bcdb_apply_optim_writes_with_retry` (~L960):
  - uses 8 quick retries in settle mode (64 before);
  - the `_proc` fast restart is disabled;
  - 40001/40P01/55P03 raised during apply are retried instead of rethrown (the subtransaction rollback releases the locks);
  - a whitelisted deterministic error returns at once.
- **Settle** (worker loop, ~L3305): when an apply fails after release, the transaction waits until every predecessor has committed (`bcdb_wait_for_prev_committed`) and re-applies the SAME writes. If the failure persists, the outcome is deterministic:
  - unique violation + `INSERT … ON CONFLICT DO NOTHING` → idempotent no-op;
  - unique violation otherwise → terminal 23505, no business writes (what the statement does at its serial position);
  - whitelisted deterministic SQLSTATE → that terminal error;
  - anything else → `BCDB_INVARIANT_POST_PUBLISH_APPLY` WARNING, counter, terminal `BC001`.
- **Why the settled outcome is the same on every replica.** After all predecessors commit, the remaining interference would have to come from successors. A successor that writes any key we published (now including every unique-index value) sees our map entry (`baseline < our id < its id`) and waits for our commit. So the re-apply sees exactly the prefix state.
- **All-unique-index tags** (shm_transaction.c, `bcdb_keytag_collect_unique` ~L1060, `bcdb_reserve_write_key_tags` ~L1287):
  - each additional plain unique index gets its own checked write tag (prefix field `0x100 | ncols`, hash seeded with the index OID, NULL keys skipped);
  - partial, expression, or more than 8 unique indexes → one relation-wide write tag (conservative);
  - TPC-C has exactly one unique index per table, so its tag count is unchanged.
- **Read-only gate skip** (`BCDB_DT_SKIP_READONLY_GATE`) is ignored in settle mode, because it releases the turn before validation.
- **Self-conflict fix** (shm_transaction.c ~L3807): `bcdb_dt_validate_published` caps the scan at own−1.
- **Test hooks:**
  - failpoint `BCDB_FAILPOINT_POST_PUBLISH_APPLY=<N>`: the first apply call of each tx with `tx_id % N == 0` fails as a 23505;
  - ptrace columns `post_publish_settles`, `post_publish_terminal_unique`, `post_publish_invariant`.

**Remaining `rw_conflicts = 1` sites after release, with settle on:** none. The three historical ones sit in the `else` branches of the settle check. The PG_CATCH error path (an exception that escapes after publication) still publishes an error result. Settle removed the timing-dependent lock/serialization causes from it; a backend-fatal error there remains a replica-local failure, as before.

## 3. Evidence: correctness

Reproducer: `scripts/distributed/post_publish_repro/`, run on .247.
- Each run compares det (16 workers, server + gateway, same flags as the TPC-C v2 sweep) against the same 20k statements executed serially through psql.
- `ab_proc` reads `acct[a]` and folds it into `outt[b]`; `bump_proc` writes `acct` (8 hot keys); `uq_proc` writes a table with two unique indexes.

| Run | det = serial? | Settles | Terminal 23505 |
|---|---|---|---|
| main protocol (settle 0, early-validate 0) + failpoint 7, ×3 | **NO, NO, NO** | – | – |
| main + early validation + failpoint (after the self-conflict fix) | **NO** (no hang) | – | – |
| main, no failpoint | yes | – | – |
| settle + failpoint 7, ×3 | **yes ×3** | 2,858 each | 0 |
| settle, uq workload, ×2 | **yes ×2** | 1,309 | 1,309 (= serial's 1,309 errors) |
| settle, uq workload + failpoint, ×2 | **yes ×2** | 3,988 | 1,309 |

Every run had divergence_count = 0 and permanent_failures = 0. Artifacts are in `~/claude_checks/detopt_settle_20261008/runs/m2_*` on .247; the matrix log is `matrix.txt`. Runs from a garbled first attempt (two matrix instances ran concurrently) were moved to `runs_garbled/` and are not used.

TPC-C v2 on ranking: every settle run reproduced the reference state hash (W5 `fdb9545f…`, W30 `dcb895e5…`, W100 `e82921e1…`) with rc = 0.

## 4. Evidence: TPS (TPC-C v2, ranking, fast-disk-mode medians)

| W / workers | final (detopt/combined) | settle | Δ |
|---|---|---|---|
| 5 / 32 | 1,174 | 1,181 | +0.6% |
| 30 / 32 | 2,409 | 2,394 | −0.6% |
| 100 / 32 | 3,335 | 3,231 | −3.1% (first queue) |
| 100 / 48 | 3,703 | 3,621 | −2.2% |

**Is the W100 gap the fix?** No.
- A traced settle run at W100/32 (20,000 tx, 1,481 conflict restarts) recorded **post_publish_settles = 0**, terminal_unique = 0 and invariant = 0. The settle logic never executes on TPC-C, and TPC-C has one unique index per table, so it adds no tags either.
- In the one alternating triple where the disk stayed in fast mode (t51): final 3,343; settle 3,245; **the same settle binary with BCDB_DT_POST_PUBLISH_SETTLE=0: 3,280**. Most of the gap is present with the logic switched off, i.e. build/run variance.
- The other three triples hit the QLC slow-flush mode (f_await 5–8 ms) and are not comparable.
- Summary: the fix costs ≈0 on the hot path; the measurable band at W100 is within the host's run-to-run noise (final itself ranged 3,258–3,343).

## 5. Risks / open items

- The micro reproducer covers the deferred-write procedure path. The ledger (`raft_ledger_enabled`) path and Merkle mode were not exercised by it. Settle keeps them on the existing terminal-outcome code (`bcdb_raft_ledger_finalize_error`, subtransaction-aware Merkle staging).
- Phantom reads through a secondary unique index (a SELECT by unique value that finds no row) are still covered only by existing read tags. This is unchanged and outside this fix.
- Not yet run on the 4-node Raft/Kafka cluster with Merkle consistency checks.
- .247 has no C++ compiler any more. The reproducer used the Oct 6 server/gateway binaries; later C++ changes are gateway pacing only.
