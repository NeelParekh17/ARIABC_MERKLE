# Online Merkle Fault Injection and Autonomous Recovery Setup

This document provides a complete guide to the **ProtectDB Online Concurrency and Merkle Recovery Subsystem** in AriaBC. It details the architecture, component interaction, fault injection procedures, and step-by-step instructions for reproducing online data healing during active Phase 6 benchmark execution.

---

## 1. System Overview & Architecture

AriaBC is a deterministic transaction processing system with synchronized Merkle tree indexing across all cluster replicas. During execution, transactions are ordered via Raft-Kafka and dispatched deterministically to PostgreSQL backends.

```
                              [ Client / Workload Generator ]
                                             │ (Phase 6)
                                             ▼
                             ┌───────────────────────────────┐
                             │       ariabc_pg_gateway       │
                             │  (10.129.27.111 / Controller) │
                             │                               │
                             │   ┌───────────────────────┐   │
                             │   │ GatewayRecoveryManager│   │
                             │   │  (Passive thread, 1s) │   │
                             │   └───────────┬───────────┘   │
                             └───────────────┼───────────────┘
                                             │ Merkle Root Check
                                             │ (every interval)
                      ┌──────────────────────┼──────────────────────┐
                      │                      │                      │
                      ▼                      ▼                      ▼
             ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
             │ Node 1: admin123│    │  Node 2: user4  │    │ Node 4: utkarsh │
             │ 10.129.148.247  │    │ 10.129.148.246  │    │ 10.129.148.248  │
             │   (Raft Leader) │    │ (Raft Follower) │    │ (Target Replica)│
             └─────────────────┘    └─────────────────┘    └────────▲────────┘
                      │                      │                      │
                      └────── Majority Quorum Hash ─────────────────┘
                               (2 of 3 nodes match)                 │
                                                                    │ External Fault
                                                                    │ Injection
                                                      ┌─────────────┴─────────────┐
                                                      │  corrupt_during_phase6.sh │
                                                      │  (Modifies live tuples)   │
                                                      └───────────────────────────┘
```

### Key Principles:
1. **Dynamic Merkle State**: Each replica maintains an incremental Merkle index tree on the active tables (e.g. `usertable_small`). Any committed `UPDATE`, `INSERT`, or `DELETE` immediately updates the partition and table-level root hashes.
2. **Autonomous Quorum Detection**: The gateway's `GatewayRecoveryManager` periodically polls the Merkle root hash across all cluster nodes using `compare_states.py`.
3. **Outlier Localization**: When one node's root hash deviates from the 2-node majority quorum, `compare_states.py` triggers `recovery_engine.py`.
4. **Sub-second Fine-Grained Repair**: The engine traverses the Merkle tree hierarchy (Root $\to$ 200 Partitions $\to$ Leaves $\to$ Tuples) to locate and heal *only* the corrupted rows from the reference replica, without stopping or blocking the ongoing transaction pipeline.

---

## 2. Directory Structure & Components

All scripts and engines for fault injection and recovery reside under [`scripts/distributed/recovery/`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/):

| File | Type | Description |
| :--- | :--- | :--- |
| [`compare_states.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/compare_states.py) | Python CLI / Daemon | Multi-node Merkle state comparison daemon. Polls root hashes across all replicas, calculates quorum agreement, and triggers healing for outlier nodes. |
| [`recovery_engine.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/recovery_engine.py) | Python Engine | ProtectDB Algorithm 2 implementation. Performs hierarchical localization (table $\to$ partition $\to$ leaf) and repairs tuples via batched DML. |
| [`fault_injector.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/fault_injector.py) | Python CLI / Lib | Controlled data corruption tool. Injects `update`, `delete`, `insert`, or `mixed` faults with `READ COMMITTED` isolation and retry semantics. |
| [`corrupt_during_phase6.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/corrupt_during_phase6.py) | Python Watcher | Orchestrates fault injection during live Phase 6 runs. Waits for gateway startup, introduces a delay, injects corruption, and monitors convergence. |
| [`corrupt_during_phase6.sh`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/corrupt_during_phase6.sh) | Bash Wrapper | Convenient bash entrypoint for running the Phase 6 corruption watcher with proper `PYTHONPATH`. |
| [`remote_db.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/remote_db.py) | Python Module | Database connection and catalog introspection abstraction using `psycopg` with optimized connection parameters. |
| [`active_recovery_hook.py`](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/active_recovery_hook.py) | Python Bridge | Hook script executed by the C++ gateway process when active recovery triggers are received. |

---

## 3. Cluster Nodes Configuration

| Node Name | IP Address | DB Port | Raft Port | OS / Environment | Default Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gateway / Controller** | `10.129.27.111` | - | - | Ubuntu 24.04 (`neel`) | Gateway & Benchmark Runner |
| **admin123** | `10.129.148.247` | `5438` | `8000` | Ubuntu 24.04 (`admin123`) | Node 1 (Raft Leader / Quorum Ref) |
| **user4** | `10.129.148.246` | `5438` | `8000` | Ubuntu 22.04 (`neel`) | Node 2 (Raft Follower / Quorum Ref) |
| **utkarsh** | `10.129.148.248` | `5438` | `8001` | Ubuntu 24.04 (`utkarsh`) | Node 4 (Raft Follower / Fault Target) |

> [!NOTE]
> Database credentials across all nodes default to user `postgres`, database `postgres`, port `5438` (or default `5432` if unmapped).

---

## 4. How Online Recovery Operates in Phase 6

When the benchmark executes Phase 6, the following sequence occurs:

1. **Gateway Initialization**:
   `ariabc_pg_gateway` starts with `--recovery-mode passive --recovery-interval-ms <N> --recovery-table usertable_small`.
2. **Background Polling**:
   Every $N$ milliseconds (e.g., 1000ms), the gateway runs a non-blocking `compare_states.py` check.
3. **External Fault Injection**:
   An external process (`corrupt_during_phase6.sh`) modifies $K$ tuples in `utkarsh` while the gateway is submitting thousands of update transactions across all 3 nodes.
4. **Divergence Detection**:
   On the next check tick:
   - Node 1 (`admin123`): Root `0c6509f9...`
   - Node 2 (`user4`):    Root `0c6509f9...`
   - Node 4 (`utkarsh`):  Root `e879329e...` (DIVERGED!)
   - **Quorum Result**: 2 of 3 nodes agree on `0c6509f9...`. Node `utkarsh` is flagged as damaged.
5. **Hierarchical Localization**:
   - Compares 200 partition root hashes between `utkarsh` and `admin123`.
   - Identifies only the few partitions containing the corrupted tuples (e.g., 5 partitions).
   - Localizes down to the exact 4-leaf tree nodes.
6. **In-Flight Tuple Repair**:
   - Queries the genuine tuples from `admin123` for the localized keys.
   - Executes batched `UPDATE` statements on `utkarsh` under `READ COMMITTED` isolation.
   - `utkarsh`'s Merkle tree automatically updates.
7. **Re-synchronization & Convergence**:
   - `utkarsh`'s Merkle root converges back to the live quorum hash.
   - Total healing time: **~150 ms** for recovery execution, **~2 to 3.5 seconds** end-to-end including poll interval.

---

## 5. Step-by-Step Execution Guide

To reproduce the live external corruption and autonomous healing, follow this two-terminal workflow.

### Workflow: 2-Terminal Live Test

#### Terminal 1: Launch External Corruption Watcher
Start the corruption watcher **first** so it is ready and listening for Phase 6 startup:

```bash
cd /work/ARIABC/AriaBC

./scripts/distributed/recovery/corrupt_during_phase6.sh \
  --target utkarsh \
  --count 100 \
  --fault-type update \
  --table usertable_small \
  --delay-sec 2.0
```

**Parameters Explained:**
- `--target utkarsh`: Injects corruption specifically into node `utkarsh` (`10.129.148.248:5438`).
- `--count 100`: Corrupts 100 random tuples in the table.
- `--fault-type update`: Modifies row fields (can also be `delete`, `insert`, or `mixed`).
- `--delay-sec 2.0`: Waits 2.0 seconds after Phase 6 starts to ensure the workload is running at full pipeline throughput before injecting the fault.

*Terminal 1 output will display:*
```text
[INFO] [compare_states] Monitoring for Phase 6 workload start (gateway_host=10.129.27.111)...
[INFO] [compare_states] Still waiting for Phase 6 to begin...
```

---

#### Terminal 2: Launch the 4-Node Cluster Benchmark
In Terminal 2, start the cluster benchmark with passive online recovery enabled:

```bash
cd /work/ARIABC/AriaBC

./scripts/distributed/run_4node_raft_cluster.sh \
  --skip-build \
  --threads 8 \
  --server-exec-workers 8 \
  --server-pg-connections 8 \
  --pool-size 8 \
  --bcdb-workers 8 \
  --bcdb-init-block-size 8 \
  --bcdb-decouple-workers 1 \
  --ordering-mode raft-kafka \
  --raft-ordering-policy leader-assigned \
  --kafka-completion-mode majority_async_all3 \
  --det-window 65536 \
  --recovery-mode passive \
  --recovery-interval-ms 1000 \
  --recovery-table usertable_small
```

**Key Recovery Options:**
- `--recovery-mode passive`: Enables the background online recovery daemon in the gateway.
- `--recovery-interval-ms 1000`: Runs the compare state check every **1 second** (set to `200` for 200ms high-frequency checks).
- `--recovery-table usertable_small`: Specifies the target table with Merkle index.
- `--skip-build`: Skips recompilation since binaries are already built on all nodes.

---

### What to Observe During Execution

#### In Terminal 1 (Corruption Watcher Output):
```text
[INFO] [compare_states] >>> Phase 6 is ACTIVE! Found running ariabc_pg_gateway (PID 394713)
[INFO] [compare_states] Workload running. Waiting 2.00s delay before fault injection...
[INFO] [compare_states] >>> INJECTING CORRUPTION: Corrupting 100 tuples on utkarsh (10.129.148.248:5438)...
[INFO] [compare_states] Before injection: table 'usertable_small' root hash = d55efc5f...
[INFO] [compare_states] Injecting UPDATE corruption on 100 tuples (sample key 1878, attempt 1)
[INFO] [compare_states] After corruption: table 'usertable_small' root hash = 8f9b29f8... (changed: True, total corrupted: 100)
[INFO] [compare_states] Corruption successfully injected! Monitoring online auto-recovery...
[INFO] [compare_states] Reference node root hash: e43a5398...
[INFO] [compare_states] *** HEALED! Target node root hash matches reference quorum (408c6870...) in 3488.78 ms (polls: 33)! ***
[INFO] [compare_states] === Phase 6 Online Recovery Demonstration: COMPLETE (PASS) ===
```

#### In Terminal 2 (Cluster Log Output):
```text
[recovery_mgr] Initialized ProtectDB Alg 2 Online Recovery: mode=passive interval=1000ms db_port=5438 table=usertable_small ...
...
recovery_triggered_count=1
recovery_success_count=1
recovery_failure_count=0
recovery_total_ms=147
...
[recovery_mgr] Phase 6 workload complete: running final Merkle drain & consistency verification...
[recovery_mgr] Table 'usertable_small': PASS (root=bc6e9749... across all 3 nodes)
[recovery_mgr] Final cluster Merkle verification: PASS - all replicas fully synchronized with 0 corruption remaining.
...
[18:36:19]   [admin123] rows=12001 root=bc6e9749... data_md5=016e2324... merkle_verify=t
[18:36:19]   [user4]    rows=12001 root=bc6e9749... data_md5=016e2324... merkle_verify=t
[18:36:19]   [utkarsh]  rows=12001 root=bc6e9749... data_md5=016e2324... merkle_verify=t
[18:36:19]   usertable_small consistency: PASS rows=12001 root=bc6e9749...
```

---

## 6. Standalone / Offline Verification Commands

You can also test state comparison, fault injection, and recovery manually on a running or idle cluster:

### A. Check Merkle Root Hash Consistency Across All Replicas
```bash
python3 scripts/distributed/recovery/compare_states.py \
  --nodes "admin123=10.129.148.247:5438,user4=10.129.148.246:5438,utkarsh=10.129.148.248:5438" \
  --table usertable_small \
  --once
```

### B. Manually Inject Corruption on a Single Replica
```bash
# Corrupt 50 tuples on utkarsh
python3 scripts/distributed/recovery/fault_injector.py \
  --target-node 10.129.148.248:5438 \
  --table usertable_small \
  --fault-type update \
  --count 50
```

### C. Manually Trigger Autonomous Localization & Healing
```bash
python3 scripts/distributed/recovery/compare_states.py \
  --nodes "admin123=10.129.148.247:5438,user4=10.129.148.246:5438,utkarsh=10.129.148.248:5438" \
  --table usertable_small \
  --once \
  --auto-recover
```

Expected output:
```text
[INFO] Table 'usertable_small': Quorum root bc6e9749... on nodes ['admin123', 'user4']. Damaged outlier node(s): ['utkarsh']
[INFO] Triggering Online Recovery for node utkarsh on table 'usertable_small' against reference admin123...
[INFO] Table 'usertable_small' localisation: 5 mismatched partitions ([6, 25, 77, 82, 198])
[INFO] Partition 6 localized down to 1 differing leaves (out of 4 total leaves)
...
[INFO] Table 'usertable_small' recovery SUCCEEDED in 125.03 ms
rows_updated: 50, localisation_ms: 27.5ms, diff_ms: 60.9ms, dml_ms: 6.2ms, total_ms: 125.1ms
```

---

## 7. Troubleshooting & Common Pitfalls

| Issue | Root Cause | Solution |
| :--- | :--- | :--- |
| `could not serialize access due to read/write dependencies` | PostgreSQL SSI (`SERIALIZABLE`) detects rw-antidependency cycles during concurrent transactions. | Ensure `fault_injector.py` and `remote_db.py` use `SET default_transaction_isolation = 'read committed'`. Already configured in current scripts. |
| `No module named 'psycopg'` on Gateway | System Python on `10.129.27.111` lacks psycopg package. | Prepend the virtualenv site-packages path to `PYTHONPATH`: `export PYTHONPATH="/home/neel/Desktop/ariabc_cluster/.venv/lib/python3.12/site-packages:${PYTHONPATH:-}"`. |
| Script hangs waiting for Phase 6 | Gateway PID detection was checking regex matching its own bash args. | Use exact `pidof ariabc_pg_gateway` check, as implemented in `corrupt_during_phase6.py`. |
| `Permission denied` on remote node | SSH username mismatch. | Node 2 (`10.129.148.246`) uses user `neel`. Node 1 uses `admin123`, Node 4 uses `utkarsh`. |
