# Online replica recovery (integrated into gateway + server)

ProtectDB Algorithm 2 (paper §5.3) running inside the replicated service.
A corrupted replica is repaired from a healthy replica's snapshot at an exact
Raft boundary and then replays the Raft log from that boundary.  Nothing else
pauses: submitters keep running, healthy replicas keep executing, and the
damaged replica keeps participating in Raft (it may even be the leader).

Runbook, cluster layout and the other files in this directory:
[FAULT_INJECTION_AND_ONLINE_RECOVERY_SETUP.md](FAULT_INJECTION_AND_ONLINE_RECOVERY_SETUP.md).

## Modes (`--recovery-mode`, gateway `--recoveryMode`)

| Mode | Detection | What triggers a recovery |
|---|---|---|
| `off` | none | – |
| `active` | per-transaction result votes | a result hash that diverges from the majority (`reason=result_divergence`), or the all-node audit finding a single minority replica (`reason=audit_mismatch`) |
| `passive` | aligned Merkle state comparison every `--recovery-interval-ms` | a replica whose digest differs from the majority at the same Raft index (`reason=merkle_compare`); a result divergence or audit mismatch only starts a comparison immediately |
| `both` | both of the above | whichever fires first |

Recovery after detection is identical in all modes.

## Flow

```
detect ──► QUARANTINE D ──► CUT healthy H at L ──► RECOVER D ──► D replays L+1.. ──► LIVE
           (stop applying,     (exact prefix,        (sparse Merkle   (from D's own
            keep Raft role)     no pause)             repair + rebase)  Raft log store)
```

| Step | Where | What happens |
|---|---|---|
| Detection (active) | gateway vote store | see Modes |
| Detection (passive) | gateway coordinator | every interval all healthy replicas `CUT target=T` at the same future Raft index and return a per-table Merkle digest; a minority digest is a corrupted replica.  Each export waits (in its keeper backend only) until that replica has executed up to T, so under load a round lasts about one execution backlog of the slowest replica (~3–4 s at 8.8k TPS).  When idle and all replicas have applied the same commit point they cut there instead |
| Quarantine | `pg_state_machine` | `commit()` records entries but no longer applies them.  Entries D had already started still finish and publish: the gateway counts D's votes only where they match another replica, so the majority never has to wait for the slowest healthy replica alone |
| Cut | healthy replica | the keeper transaction is opened from the Raft commit thread before entry L+1 is dispatched; `bcdb_cut_snapshot_export(B)` exports an MVCC snapshot that hides every transaction > B that already committed out of order (xids recorded pre-commit), so the snapshot is exactly the prefix 0..B |
| Boundary rule | gateway + server | L must be ≥ the last entry D executed (`last_enqueued` from QUARANTINE): the coordinator waits for the reference's commit index to reach it, and `RECOVER` rejects a smaller L (`ERR boundary_behind_local`), so D never re-executes an entry it already voted on |
| Repair | damaged replica's server | `replica_repair.cxx`: compare partition roots and leaves with the snapshot, stream only rows of differing leaves (`COPY`), one set-oriented `DELETE` + `INSERT .. ON CONFLICT`, verify roots and `merkle_verify()`; full-table copy fallback; non-Merkle tables compared by checksum.  All tables in `public` are covered |
| Rebase | damaged replica's PostgreSQL | `bcdb_recovery_rebase(B)` resets BCDB watermarks, result ring, tx pool and write-set tables so tx B+1 executes next |
| Replay | damaged replica's server | entries L+1.. are read from the local Raft log store and applied through the normal commit path (bounded in-flight window), then the replica switches back to live commits under the commit lock |
| Rejoin | gateway | D is covered for entries ≤ L (installed from the snapshot; its pre-repair votes there are ignored by the all-node audit and cannot raise new divergence reports); its replayed votes for entries > L are audited normally |

## Control verbs (`__ARIABC_CTRL_RECOVERY ...` on the server client port)

```
CUT target=<idx|0> hold=0|1 digest=0|1 timeout_ms=N [target_timeout_ms=N]
                                                        -> OK L= B= snapshot= digest=
RELEASE snapshot=<id>
QUARANTINE                                              -> OK mode=QUARANTINED last_enqueued= ...
RECOVER ref_host= ref_port= snapshot= L= B= [wait_live_ms=] [heap_verify=0|1] [allow_rewind=0|1]
STATUS                                                  -> OK mode=LIVE|QUARANTINED|REPLAYING ...
```

`CUT` waits up to `target_timeout_ms` for entry T to commit and up to
`timeout_ms` for the replica's execution to reach the boundary.
`allow_rewind=1` lets a replica recover from its own older cut (single-node test only).

`ariabc_recovery_tool ctl <host:client_port> STATUS` sends them by hand;
`ariabc_recovery_tool repair|digest` run the repair and digest directly.

## Running

```
R=scripts/distributed/recovery/run_recovery_cluster_test.sh
$R --recovery-mode off                                     # baseline (builds)
$R --recovery-mode both --skip-build                       # overhead, no fault
$R --recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 \
   --inject-fault-delay-sec 5 --skip-build                 # fault run
```

Phase 7 prints `RECOVERY_EVENT ...`, `TPS_TIMELINE` / `TPS_RECOVERY_WINDOW`
(per-100 ms client throughput from `tx_latency.csv`), `recovery_compare_rounds`
and the final cross-replica digest check; Phase 8 independently compares Merkle
roots on all nodes.

Gateway environment knobs: `ARIABC_RECOVERY_COMPARE_MARGIN` (entries ahead for
aligned cuts, default 16), `ARIABC_RECOVERY_COMPARE_TIMEOUT_MS` (export wait per
compare cut, default 30000), `ARIABC_RECOVERY_CATCHUP_WAIT_MS` (default 120000).
Server: `ARIABC_RECOVERY_REPLAY_WINDOW` (replay in-flight entries, default 64),
`ARIABC_RECOVERY_CUT_TTL_MS` (held-cut expiry, default 300000).

## Verified results (2026-09-27, 3 nodes, YCSB 160k tx, 96 lanes)

Fault = 100 tuples of `usertable_small` corrupted 5 s into the run.
Result directories: `scripts/bench_full_results/cluster4_final_*_180113`,
`cluster4_mode_M_passive_181118`, `cluster4_mode_M_active_r{1,2}_183115`,
`cluster4_test_leader_184843`, `cluster4_test_mixed_utkarsh_184953`,
`cluster4_test_leader_mixed_185108`.

| Run | Node | Fault | Mode | TPS | vs A | Detected by | Ref | Recovery | Rows Repaired | Empty 100 ms | Phase 8 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A | – | none | off | 8,850 | – | – | – | – | – | 0 | PASS |
| B | – | none | both | 8,829 | −0.2% | 3 compare rounds | – | – | – | 0 | PASS |
| C | utkarsh | update | both | 8,693 | −1.8% | result divergence | 1 | 10.8 s | 31 | 0 | PASS |
| M1 | utkarsh | update | active | 8,601 | −2.8% | result divergence | 1 | 11.2 s | 154 | 0 | PASS |
| M1' | utkarsh | update | active | 8,700 | −1.7% | result divergence | 1 | 11.2 s | 29 | 0 | PASS |
| M2 | utkarsh | update | passive | 8,773 | −0.9% | Merkle compare (L=289) | 1 | 7.3 s | 7 | 0 | PASS |
| **L1 (initial)** | **admin123 (Leader)** | **update** | **both** | **8,052** | **−9.0%** | **result divergence** | **2 (lagging)** | **13.0 s** | **31** | **3** | **PASS** |
| **M_mix** | **utkarsh** | **mixed (upd+del+ins)** | **both** | **8,686** | **−1.9%** | **result divergence** | **1** | **11.0 s** | **34 del, 43 ups** | **0** | **PASS** |
| **L_mix (initial)** | **admin123 (Leader)** | **mixed (upd+del+ins)** | **both** | **4,440** | **−49.8%** | **result divergence** | **2 (swapping)** | **15.0 s** | **34 del, 33 ups** | **49** | **PASS** |
| **L1 (prioritized ref)** | **admin123 (Leader)** | **update** | **both** | **8,321** | **−6.0%** | **result divergence** | **4 (utkarsh)** | **12.6 s** | **164 ups** | **0** | **PASS** |
| **L_mix (prioritized ref)** | **admin123 (Leader)** | **mixed (upd+del+ins)** | **both** | **8,674** | **−2.0%** | **result divergence** | **4 (utkarsh)** | **10.4 s** | **34 del, 167 ups** | **0** | **PASS** |

In every fault run the recovered replica ended with the same Merkle root as the
others, the all-node audit was valid and there were no permanent failures.
No transaction took longer than 4 s in these runs; the only dip is one 100 ms
bucket when the repaired replica resumes.  An earlier active run started right
after the U22 rebuild on user4 (`cluster4_mode_M_active_181118`, 8,197 TPS) had
user4 ~7 s behind: the 82 transactions on which the two fast replicas disagreed
waited ~10 s for its vote, detection came ~7 s after the fault, and two of them
finished 1.4 s after the rest of the workload, which lowered the TPS figure.
Build in a separate run and benchmark with `--skip-build`.  Rows repaired varies because the workload overwrites most
corrupted rows before the repair's boundary.

### Leader Corruption (L1, L_mix) Analysis & Prioritized Reference Selection

In initial runs `cluster4_test_leader_184843` and `cluster4_test_leader_mixed_185108`, leader corruption caused throughput to drop (down to 4,440 tx/s in L_mix with 49 empty 100 ms buckets):
1. **Disagreement Tie-Breaker Wait**: Corrupted Node 1 and fast Node 4 returned divergent Blake3 hashes. In a 3-node cluster, neither hash had a 2-node majority, forcing the gateway to wait for Node 2 (`user4`) to cast the tie-breaking vote for every affected batch.
2. **Suboptimal Reference Replica Selection**: When Node 1 was quarantined, `healthy_nodes_locked()` returned candidate reference replicas in static node-ID order `[2, 4]`. The gateway unconditionally picked Node 2 (`user4`), which was experiencing memory pressure and swap thrashing. Taking an MVCC snapshot cut on Node 2 timed out 10 times (`cut_target_timeout`), taking **14.56 s** for `cut_ms`. While Node 1 was quarantined and Node 2 was tied up taking cuts, client transaction completion stalled.
3. **Fix — Prioritized Reference Selection**: In `gateway_recovery_manager.hxx`, candidate reference replicas are dynamically queried for `STATUS` and sorted descending by `last_commit` before attempting a cut. This automatically selects the most responsive and up-to-date node (`utkarsh`, Node 4) over lagging nodes.
4. **Outcome**:
   - Snapshot cut time (`cut_ms`) dropped from 14.56 s to **2.78 s**.
   - Zero-TPS empty buckets dropped from 49 to **0**.
   - **L_mix visible throughput jumped from 4,440 tx/s to 8,673.50 tx/s (+95.3%)**, with only 2.0% overhead vs fault-free execution.
   - **L1 visible throughput reached 8,320.77 tx/s** with 0 empty buckets.
   - All runs verified with 100% identical Merkle state (`root=80566f71...`), 0 permanent failures, and valid all-3 audit.

## Limits

- At most ⌊(n-1)/2⌋ replicas recover at once; with 3 nodes both healthy
  replicas are needed for the majority while one recovers.
- With 3 replicas, a fault is attributed only once the third replica's vote or
  digest arrives, and a transaction on which the two fast replicas disagree
  waits for it.  Detection latency and those transactions' latency therefore
  follow the slowest replica's lag (user4: ~1.5 s when healthy, 7–12 s when its
  memory is under pressure).
- Passive detection lags by one comparison round (execution backlog of the
  slowest replica); active detection lags by that replica's result lag.
- A cut fails (and is retried) if more than 2048 transactions beyond the
  boundary commit before it is materialised (pre-commit xid ring size), which
  is also why a cut cannot be taken far behind the execution frontier.
- A recovered replica catches up only as fast as it can execute; it needs
  execution headroom over the live workload.  `catchup_ms` / `RECOVERY_REPLAY_LIVE`
  measure the time until every missed entry has been handed back to the
  executor (the node then applies commits live again), not until it has
  finished executing them.
- A Merkle table missing on the damaged replica fails the repair; a plain
  table that exists only on the reference is skipped (logged as
  `REPLICA_REPAIR skip_table=`).
- Safe-ledger mode (`--raft-apply-ledger safe`) is not covered.
