# Expected NewOrder rollback and outcome accounting

The v3 function raises a real PostgreSQL exception with SQLSTATE **TP001** and
exact primary message **TPC-C expected NewOrder rollback: invalid item**. Only
the last line with the reserved unused item ID 100001 produces this outcome.
The function has already incremented d_next_o_id, inserted oorder/new_order,
and attempted each earlier stock update and order-line insert. It does not
catch the exception, prevalidate the items, return an artificial rollback
status, or commit those earlier effects. PostgreSQL aborts the entire SQL
transaction. There is no fallback/deviation from B8's real rollback requirement.

## Source investigation and changes

- `ariabc_pg/src/pg_executor.cxx`, `exec_sql`: normal pg executes the whole
  SELECT through libpq. SQLSTATE 40001/40P01/57014 use the existing bounded retry
  and exponential backoff/full-jitter paths. Other SQL errors previously
  returned a canonical ERROR, set is_error=true and led to failed completion.
  TP001 plus the exact message now yields the stable USER_ABORT receipt below,
  is_error=false, no retry, and one user_aborts increment at terminal emission.
  The event-mode libpq path applies the identical rule; no retry policy changed.
- `ariabc_pg/src/ariabc_pg_server.cxx`: the state machine's successful executor
  callback allows WAIT_RESULTS, WAIT_RESULT_ID and fused wait responses to
  return COMPLETED. A failed callback makes these waits return failure, and
  the gateway counts a permanent failure. These direct responses omit actual
  row payloads, so input inspection or Kafka-only counting would be insufficient.
  Added `__ARIABC_CTRL_GET_USER_ABORTS` on both Raft and bypass-Raft control paths,
  returning the actual executor counter, and user_aborts in PROFILE_SERVER.
- `src/backend/bcdb/worker.c`, `bcdb_worker_process_tx_dt`: NewOrder runs during
  get_write_set with optimistic writes staged in optim_write_list. Normally the
  worker then enters the parse barrier, serial gate, conflict check, write-set
  publication, write apply, transaction finish, and result publication. The
  old outer PG_CATCH logs BCDB_FATAL, publishes an ERROR SQLSTATE receipt,
  advances published/committed watermarks and block num_finished, then rethrows.
  PostgreSQL's outer PostgresMain handler performs AbortCurrentTransaction.
  Thus the legacy path DOES advance the serial gate for a failed non-ledger
  det transaction, but its result/watermark can precede full rollback.
- Added a narrow non-ledger catch for the reserved code **and exact message**:
  end optimistic-worker telemetry, drain staged writes, AbortCurrentTransaction,
  reset the command and stale portal/queryDesc/sxact pointers, enter the parse
  barrier and serial slot, mark the publish gate ready, wait for predecessor
  completion, publish the stable abort outcome via bcdb_finish_terminal_item,
  and advance the committed watermark/block completion once. The SQL rollback
  completes BEFORE either handoff. No aborted write set is published/applied.
  Background J workers continue their existing queue loop. Inline `s <txid>`
  callers rethrow the original error after publishing the ordered receipt,
  preserving a valid libpq ErrorResponse; the executor classifies it identically.
- `src/backend/access/merkle/merkledelta.c`, existing merkle_delta_xact_callback:
  XACT_EVENT_ABORT/parallel abort invoke merkle_delta_reset. Calling full
  AbortCurrentTransaction therefore clears any staged Merkle deltas as well as
  undoing heap/index mutations. This callback was inspected, not modified.
- `src/backend/bcdb/middleware.c`, bcdb_middleware_submit_block_results: default
  completion-only mode normally omits row payloads even when a slot is ready.
  Preserve this exact USER_ABORT receipt in that mode; otherwise executor abort
  accounting silently loses every block-mode rollback. Ordinary successful
  payload suppression is unchanged. Executor scalar completion-only handling
  likewise retains the reserved abort receipt instead of clearing it.
- `ariabc_pg/src/pg_error_result.hxx` defines the shared exact receipt classifier.
  Wrong SQLSTATE, wrong message and trailing text remain ordinary errors. Added
  focused assertions to `ariabc_pg/tests/pg_error_result_test.cxx`; compilation
  and execution are delegated to the integrator's remote plan.

The changes above apply to the v2-style single-node benchmark's pg (dbType=0),
det (dbType=1), and Merkle (dbType=1 plus Merkle indexes) paths, including direct
and block submission and event/threaded executors. `--safedb 1` is preserved;
it is distinct from the optional `--raft-apply-ledger safe` protocol. Safe apply
ledger uses its own business subtransaction, deterministic SQLSTATE whitelist,
durable ERROR/NONTERMINAL_FAILURE envelope and verification paths. TP001 is NOT
added to that whitelist or relabeled in a committed ledger envelope: its errors
remain fail-closed. This campaign does not claim safe-ledger/recovery support for
the new abort outcome. All validation uses the same legacy-ledger bypass-Raft
configuration as sweep_run.sh. The generic legacy det error path, including its
completion-only suppression, is otherwise unchanged; it should not be used as
evidence that arbitrary det SQL errors are business aborts or successful work.

## Harness interface (Agent B / integrator)

Canonical backend/executor outcome:

```
USER_ABORT sqlstate=TP001 message=TPC-C expected NewOrder rollback: invalid item
```

Gateway `PROGRESS_GATEWAY_DET` lines (shared for ordered pg/det) and final lines
print **user_aborts=N**. `completed` **includes** expected user aborts: it counts
all terminal business outcomes. For N inputs acceptance is **completed=N**,
**user_aborts=FILE.meta.json.expected_rollbacks**, permanent_failures=0 and
divergence_count=0. Do **not** add user_aborts to completed again. Committed
NewOrders are the district next-ID delta; it must equal generated NewOrders
minus expected_rollbacks. NOPM uses that committed delta, not total completed.

`__ARIABC_CTRL_GET_USER_ABORTS` returns a server-lifetime cumulative count.
The gateway samples the first configured replica, subtracts its pre-workload
baseline and polls at each progress/final report. It never derives an abort
count from query text. The dedicated benchmark server must run only this
workload: simultaneous other clients would contaminate this replica counter.
Replica counters are not summed because every replica executes the same inputs.
`user_abort_counter_supported=1` and `user_abort_poll_failures=0` are required;
older binaries can still run legacy workloads, but print unsupported=0 and
cannot provide v3 rollback evidence. Polling is a small extra control round
trip per progress sample (Agent B supplies --progressIntervalMs=1000).

PostgreSQL logs for det/Merkle expected aborts contain:

```
[BCDB_USER_ABORT] txid=N sqlstate=TP001 rollback_complete=1
```

Expected aborts do not produce BCDB_FATAL. Existing result signing, Kafka
majority comparison and ordered task completion are preserved: identical
expected-abort receipts vote identically and no failed-task callback fires.
Only unexpected failures count as permanent failures.

## Data, SQL and reproducibility contract

Run `tpcc_schema.sql` into a fresh database, then `tpcc_load.py`. Tables are
UNLOGGED, column order/names match the reference dump, timestamp defaults are
removed, c_ytd_payment is exact numeric, and ol_amount has enough precision for
NewOrder totals. The loader refuses nonempty or LOGGED tables, streams COPY
without per-row INSERTs, and requires psycopg2 or psycopg only for actual DB work.
Its bounded-memory generators use exact 10% BC/ORIGINAL selections, a fresh
permutation of 3000 customers per district's initial orders, and a fixed load
timestamp. Optional gzip cache entries are keyed by (format_version,W,seed),
written atomically under a file lock, and checksum-verified before reuse.
Numeric strings avoid binary floating-point population differences. No legacy
snapshot or legacy generator is read at runtime.

The workload generator streams one SELECT *_exec call per line. Metadata stores
mix/subtype counts, expected_rollbacks, C_LOAD/C_RUN, timestamp base/step and SQL
SHA256. The C_LAST load/run absolute delta is 65..119 except 96/112; workload
customer/item/last-name choices are NURand. Rollback probability is 1% **of
NewOrders**, not of the full mix. Remote supply probability is independently 1%
per line when W>1; Payment remote customer is 15%; Payment/Order-Status by name
is 60%. W=1 remote proportions are zero. Function Order-Status reads names and
balance plus every latest-order line's required fields and returns their digest
to stay within BCDB's 1024-byte receipt limit. Stock-Level includes the last
20 orders, and stock refill uses quantity+10 as required.

`consistency_check.sql` checks clause 3.3.2 conditions 1–4 plus eight named
additional invariants adapted to this initial population. It emits
consistency_ok=t/f and raises on a failure (psql ON_ERROR_STOP gives nonzero rc).
It does not label the additional checks as verbatim spec conditions 5–12.
`state_hash.sql` covers ALL columns of ALL nine tables, with ISO/UTC serialization
and order-independent count/sum(hashtextextended) checksums. This is a diagnostic
checksum, not a cryptographic proof; det/Merkle equality must be accompanied by
consistency and native Merkle verification.

The Merkle branch retains the nine expression b-tree lookup indexes in addition
to the nine native Merkle indexes and ordinary TPC-C b-tree indexes. **Agent B
must disclose these lookup indexes as part of measured Merkle cost in README
and results.** Index variables/defaults match restore_tpcc_procs.sql; SET LOGGED
inside tpcc_procs.sql occurs only in the Merkle branch, as contracted. The caller
still sets every table LOGGED in all modes. Keep enable_seqscan=off: relation-wide
SERIALIZABLE SIREAD/coarse deterministic read tags otherwise change contention.

## Evidence status

Local py_compile, generator statistical/input self-test and full W=1 COPY-file
population self-test passed; exact outputs are in STATIC_CHECKS_A.json. W=5 COPY-file generation alone took
34.88 seconds locally, without a database; this is not actual COPY/load timing. No local
PostgreSQL, build, server or gateway was started. Native builds, transaction
rollback, gate liveness, all-mode counts, det/Merkle final-state equality and
loader wall-clock performance are **pending remote execution**, not proven by
these static checks. VALIDATION_A.md and validate_remote_a.sh specify the ordered
remote build and smoke acceptance commands, including an isolated abort whose
entire state hash must remain unchanged and whose successor must complete.
