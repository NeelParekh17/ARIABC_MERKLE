Ran command: `graphify query "gateway leader assigned mode sequencer raft flags"`
Ran command: `graphify query "ariabc_pg gateway leader assigned mode"`
Ran command: `graphify query "gateway_mode"`
Listed directory ariabc_pg
Listed directory src
Viewed ARIABC_PG_ARCHITECTURE.md:1-100
Viewed ARIABC_PG_ARCHITECTURE.md:101-200
Viewed ARIABC_PG_ARCHITECTURE.md:201-300
Viewed ARIABC_PG_ARCHITECTURE.md:301-407
Searched for "bypassRaft"
Searched for "bypassRaft"
Searched for "broadcast"
Viewed ariabc_pg_gateway.cxx:145-185
Viewed ariabc_pg_gateway.cxx:210-240
Searched for "ordering-mode"
Viewed run_4node_raft_cluster.sh:20-60
Searched for "ORDERING_MODE"
Viewed run_4node_raft_cluster.sh:1180-1230
Searched for "BYPASS_RAFT"
Searched for "GATEWAY_BROADCAST_TO_ALL"
Viewed run_4node_raft_cluster.sh:4070-4115
Viewed run_4node_raft_cluster.sh:3835-3880
Searched for "ordering_mode"
Searched for "ordering-mode"
Searched for "bypass"
Viewed run_all_modes_gateway_sweep.py:880-945
Searched for "ARIABC_RAFT_ORDERING_POLICY"
Viewed ariabc_pg_server.cxx:285-325
Searched for "raft_leader_assigned_ordering_enabled"
Viewed ariabc_pg_server.cxx:990-1040
Viewed ariabc_pg_server.cxx:1040-1070
Searched for "detStartSeq"
Searched for "format_deterministic"
Searched for "det_start_seq"
Viewed ariabc_pg_gateway.cxx:5415-5450
Viewed ariabc_pg_gateway.cxx:6265-6300
Searched for "leader_assigned"
Viewed pg_state_machine.cxx:530-570
Searched for "bypass_raft"
Viewed ariabc_pg_server.cxx:1570-1620
Viewed ariabc_pg_server.cxx:1620-1680
Viewed ariabc_pg_server.cxx:1681-1750
Viewed ariabc_pg_server.cxx:2130-2170
Searched for "direct_enqueue_ordered"
Viewed ariabc_pg_server.cxx:1461-1520
Searched for "RAFT_ORDERING_POLICY"
Viewed run_4node_raft_cluster.sh:1235-1255

Yes, **it is completely possible and already implemented in AriaBC**.

In AriaBC, this execution path is known as the **"kafka-only" / "bypass-Raft" / "preordered direct broadcast"** mode.

---

### 1. How Ordering Works: Leader-Assigned vs. Preassigned

* **Leader-Assigned Mode** (`--raft-ordering-policy leader-assigned`):
  The gateway submits unsequenced queries (`s <SQL>`), and the **Raft leader/server state machine** assigns deterministic sequences in Raft commit delivery order, rewriting queries to `s <assigned_seq> <SQL>`.
* **Preassigned Mode** (`--dbType 1` with `--detStartSeq`):
  The **gateway determines the deterministic execution order upfront** and prefixes each statement with an 8-digit sequence (`s 00000001 <SQL>`).

When you want to **skip Raft consensus entirely and determine the order earlier**, the gateway acts as the centralized sequencer and broadcasts the pre-ordered stream directly to the replica nodes.

---

### 2. Available Flags

Depending on whether you run via the cluster runner script or invoke the binaries directly, the relevant flags are:

#### A. In Cluster Scripts ([`run_4node_raft_cluster.sh`](file:///work/ARIABC/AriaBC/scripts/distributed/run_4node_raft_cluster.sh#L1187-L1192))
The high-level flag to skip Raft is:
```bash
./scripts/distributed/run_4node_raft_cluster.sh --ordering-mode kafka-only
```
Setting `--ordering-mode kafka-only` automatically:
1. Sets `BYPASS_RAFT=1` on all server nodes.
2. Sets `GATEWAY_BROADCAST_TO_ALL=1` on the gateway.
3. Sets `ORDERING_PATH="preordered_direct_broadcast"`.

---

#### B. Direct Binary Flags

##### 1. On `ariabc_pg_server` ([`ariabc_pg_server.cxx`](file:///work/ARIABC/AriaBC/ariabc_pg/src/ariabc_pg_server.cxx#L2138-L2169))
* **`--bypassRaft 1`**:
  * Skips NuRaft initialization and Raft consensus altogether.
  * Launches the direct TCP client handler ([`handle_client_fd_direct`](file:///work/ARIABC/AriaBC/ariabc_pg/src/ariabc_pg_server.cxx#L2166)).
  * Uses [`direct_enqueue_ordered()`](file:///work/ARIABC/AriaBC/ariabc_pg/src/ariabc_pg_server.cxx#L1461-L1509) to parse the preassigned sequence range (`first_seq` .. `last_seq`) sent by the gateway, queues them locally in a `direct_orderer`, and feeds them straight into the database executor ([`pg_state_machine`](file:///work/ARIABC/AriaBC/ariabc_pg/src/pg_state_machine.cxx) / [`pg_executor`](file:///work/ARIABC/AriaBC/ariabc_pg/src/pg_executor.cxx)).

##### 2. On `ariabc_pg_gateway` ([`ariabc_pg_gateway.cxx`](file:///work/ARIABC/AriaBC/ariabc_pg/src/ariabc_pg_gateway.cxx#L157-L177))
* **`--dbType 1` & `--detStartSeq <n>`**:
  * Tells the gateway to pre-assign deterministic sequences monotonically to all SQL statements (`s <seq> <SQL>`) before sending.
* **`--broadcastToAll 1`**:
  * Tells the gateway to broadcast each ordered request directly to every server replica in the cluster (instead of searching for and sending only to the Raft leader).
* **Quorum & Pipelining Controls**:
  * `--broadcastAcceptQuorum <n>`: The number of replica accept responses the gateway waits for before pipelining subsequent requests (e.g., `1` for fast pipelining, or a majority).
  * `--broadcastResultQuorum <n>`: For direct completion mode, the number of replicas required to finish executing the batch before marking it complete.
  * `--broadcastDrainInTimedRun 0|1`: Whether to include the final late-accept drain in the reported workload wall time.
  * `--completionPath direct|kafka_majority`: Choose between waiting for direct server execution ACKs or Kafka result topic consensus.

---

### Summary Architecture Comparison

| Feature | Raft-Kafka Mode (`--ordering-mode raft-kafka`) | Bypass-Raft Broadcast (`--ordering-mode kafka-only`) |
|---|---|---|
| **Ordering Source** | Raft consensus log / Raft leader | Gateway preassigned sequence (`--dbType 1`) |
| **Replication Path** | NuRaft log replication among servers | Gateway broadcast (`--broadcastToAll 1`) |
| **Server Engine** | Raft state machine commit delivery | Direct queue ([`direct_enqueue_ordered`](file:///work/ARIABC/AriaBC/ariabc_pg/src/ariabc_pg_server.cxx#L1461)) |
| **Consensus Overhead** | Raft leader election, RPCs, disk/log flush | **None (Zero Raft overhead)** |
| **Server Flag** | `--bypassRaft 0` | `--bypassRaft 1` |
| **Gateway Flag** | `--broadcastToAll 0` (sends to leader) | `--broadcastToAll 1` |