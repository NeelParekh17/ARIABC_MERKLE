# Fault injection and online recovery: setup and runbook

How to inject corruption into one replica during a cluster benchmark and watch
the online recovery repair it.  The recovery design (detection modes, the
QUARANTINE → CUT → RECOVER → replay flow, control verbs, limits) is described
in [ONLINE_RECOVERY.md](ONLINE_RECOVERY.md); this file covers the cluster, the
tools in this directory and how to run and read a test.

## 1. Cluster

| Role | Host | Raft id | Client port | DB port | Notes |
|---|---|---|---|---|---|
| Gateway / runner | `10.129.27.111` | – | – | – | builds binaries, runs `ariabc_pg_gateway` |
| admin123 | `10.129.148.247` | 1 | 8000 | 5438 | usually Raft leader and recovery reference |
| user4 | `10.129.148.246` | 2 | 8000 | 5438 | Ubuntu 22.04; slowest replica (shared desktop, little free memory) |
| utkarsh | `10.129.148.248` | 4 | 8001 | 5438 | default fault target |

Replicas are compared with `usertable_small` (12,001 rows, Merkle index,
200 partitions).  Kafka (KRaft) runs co-located on the three nodes.

Before a benchmark, check that no other user's workload is running on the
nodes (load, iowait, swap-in).  user4's own BCDB PostgreSQL holds ~7 GB of
shared memory, so other desktop sessions on it quickly push it into swap; a
lagging user4 slows fault attribution and any transaction on which the other
two replicas disagree (see ONLINE_RECOVERY.md, Limits).

## 2. Files in this directory

| File | Status | Purpose |
|---|---|---|
| `ONLINE_RECOVERY.md` | current | design, modes, control verbs, results, limits |
| `run_recovery_cluster_test.sh` | current | benchmark wrapper (8 workers, 96 lanes, raft-kafka, `majority_async_all3`) around `run_4node_raft_cluster.sh` |
| `fault_injector.py` | current | corrupts N tuples on one replica (`update`/`delete`/`insert`/`mixed`); used by the runner's `--inject-fault-*` flags |
| `remote_db.py`, `compare_states.py` | current (helpers) | connections and Merkle root queries used by `fault_injector.py`; `compare_states.py --once --check-only` is a manual cross-replica root check |
| `tps_timeline.py` | current | per-100 ms client throughput from `tx_latency.csv` (`TPS_TIMELINE`, `TPS_RECOVERY_WINDOW` in Phase 7) |
| `ONLINE_RECOVERY_END_TO_END_REPORT.md` | historical | report written by another agent; read the corrections block at its top |
| `recovery_engine.py`, `active_recovery_hook.py`, `compare_states.py --auto-recover` | legacy | the earlier external repair that patched rows on the live database while the workload ran; not used by the gateway any more and unsafe under load (it does not repair to an exact log boundary).  Use only on an idle cluster |
| `corrupt_during_phase6.py`, `corrupt_during_phase6.sh` | legacy | standalone watcher that injects a fault when Phase 6 starts; superseded by `--inject-fault-*` |
| `provision_and_cleanup_nodes.py` | do not use | one-off script that kills all `ariabc_pg` processes and drops TPC-C tables on every node; contains a hardcoded password |

Recovery itself lives in the C++ binaries: `ariabc_pg/src/gateway_recovery_manager.hxx`
(coordinator), the vote store in `ariabc_pg_gateway.cxx`,
`pg_state_machine_recovery.cxx` and `replica_repair.cxx` (server side),
`ariabc_recovery_tool.cxx` (manual control), and `src/backend/bcdb/recovery.c`
(snapshot cut and rebase inside PostgreSQL).

## 3. Running a test

All runs go through the wrapper; extra flags are passed to
`run_4node_raft_cluster.sh`.  The first run after a code change must build
(omit `--skip-build`); later runs can reuse the binaries.

```bash
cd /work/ARIABC/AriaBC
R=scripts/distributed/recovery/run_recovery_cluster_test.sh

# A: baseline, recovery off
$R --recovery-mode off
# B: recovery on, no fault (overhead)
$R --recovery-mode both --skip-build
# C: recovery on, 100 corrupted tuples on utkarsh 5 s into the workload
$R --recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 \
   --inject-fault-delay-sec 5 --skip-build
```

`--recovery-mode` is `off`, `active`, `passive` or `both` (see ONLINE_RECOVERY.md).
Fault flags: `--inject-fault-node NAME|ID`, `--inject-fault-count N` (default 100),
`--inject-fault-type update|delete|insert|mixed` (default update),
`--inject-fault-delay-sec S` (default 3).  The comparison interval is
`RECOVERY_INTERVAL_MS` (wrapper default 1000).

Results land in `scripts/bench_full_results/<run id>/` (set the id with
`CLUSTER_RUN_ID=...`).

## 4. Reading the results

`runner.log`, Phase 7:

| Line | Meaning |
|---|---|
| `TPS_majority_visible` | client throughput |
| `TPS_TIMELINE ... steady_min_tps= empty_buckets= max_completion_gap_ms=` | per-100 ms throughput; empty buckets / long gaps mean a stall |
| `TPS_RECOVERY_WINDOW ... min_bucket_tps_inside= empty_buckets_inside=` | the same, restricted to fault injection → recovery done |
| `RECOVERY_EVENT node= reason= result=PASS ...` | one recovery: `reason` is `result_divergence` / `audit_mismatch` (active) or `merkle_compare` (passive); `cut_ms`, `repair_ms`, `rows_upserted`, `replay_from`, `total_ms` |
| `RECOVERY_FINAL_CHECK result=PASS` | end-of-run digest comparison of all replicas at the same Raft index |
| `recovery_compare_rounds=` | aligned state comparisons completed during the run (passive / both) |
| `All-3 audit valid` | every transaction's result was confirmed by all replicas (a repaired replica is covered by its snapshot up to the boundary) |

Phase 8 independently compares Merkle roots, row counts and `merkle_verify()`
on all nodes (`usertable_small consistency: PASS`).

Per node, `server_node*.log` has one `RECOVERY_CTRL node= verb= result=` line
per control call (QUARANTINE, CUT, RECOVER with its phase timings) and
`RECOVERY_REPLAY_LIVE` when the repaired replica is back on live commits.
`fault_injection.log` shows the target's root before and after corruption.

## 5. Manual checks on an idle cluster

```bash
# Merkle roots of all replicas (read-only)
python3 scripts/distributed/recovery/compare_states.py \
  --nodes "admin123=10.129.148.247:5438,user4=10.129.148.246:5438,utkarsh=10.129.148.248:5438" \
  --table usertable_small --once --check-only

# Corrupt 50 tuples on utkarsh
python3 scripts/distributed/recovery/fault_injector.py \
  --target-node 10.129.148.248:5438 --table usertable_small --fault-type update --count 50

# Server state / control (while ariabc_pg_server is running; the client port from §1)
ariabc_pg/build/bin/ariabc_recovery_tool ctl 10.129.148.248:8001 STATUS
```

`compare_states.py` recovers automatically unless `--check-only` is given; do
not run it without `--check-only` while a workload is running.

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `recovery_compare_rounds=0` under load | compare cuts timed out before every replica executed up to the boundary; raise `ARIABC_RECOVERY_COMPARE_TIMEOUT_MS` (default 30000) or check for a lagging replica |
| `CUT ... ERR cut_target_timeout` after the workload ends | expected: no new Raft entries arrive, the round is skipped; the final check cuts at the common commit point |
| `RECOVER ... ERR boundary_behind_local` | the reference's cut was older than what the damaged replica already executed; the coordinator retries with a newer cut |
| `function merkle_node_upper_bound(bytea, integer) does not exist` | data directory older than the catalog entry; the server registers the helper functions itself on first use |
| Long transaction latencies / late detection after a fault | a lagging third replica (usually user4): transactions on which the two fast replicas disagree wait for its vote |
| `No module named 'psycopg'` on the gateway host | `export PYTHONPATH=/home/neel/Desktop/ariabc_cluster/.venv/lib/python3.12/site-packages:$PYTHONPATH` |
