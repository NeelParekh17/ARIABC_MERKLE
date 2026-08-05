# AriaBC Distributed Architecture & Merkle Pipeline

This document details the topology, software components, database schemas, and data flow of the AriaBC deterministic concurrency control cluster running the Dynamic Merkle index engine.

---

## 1. Physical Node & Network Topology

The cluster consists of **4 machines** (1 Gateway controller machine + 3 Database/Raft server nodes):

```
+---------------------------------------------------------------------------------------------------+
|                                     GATEWAY MACHINE                                               |
|                                     IP: 10.129.27.111                                             |
|  +---------------------------------------------------------------------------------------------+  |
|  | ariabc_pg_gateway (Binary)                                                                 |  |
|  |   - 96 Client Worker Threads (Deterministic Lanes)                                         |  |
|  |   - Kafka Result Consumer / Async Validator                                                 |  |
|  +---------------------------------------------------------------------------------------------+  |
+-----------------------------------------------+---------------------------------------------------+
                                                | Client Connections (TCP :8000)
                                                v
+---------------------------------------------------------------------------------------------------+
|                                  NODE 1: admin123 (Leader)                                        |
|                                  IP: 10.129.148.247 | Ubuntu 20.04                                |
|                                                                                                   |
|  +-----------------------------------+   +-----------------------------------------------------+  |
|  | Kafka Broker (KRaft mode)         |   | ariabc_pg_server (Raft Leader, Node ID=1)           |  |
|  |   - Topic: ariabc_results         |   |   - Client Port: 8000 | Raft Port: 9000                 |  |
|  |   - Port: 9092                    |   |   - NuRaft Engine & Log Replication                    |  |
|  +-----------------------------------+   +--------------------------+--------------------------+  |
|                                                                     | libpq / SPI                 |
|                                                                     v                             |
|                                          +-----------------------------------------------------+  |
|                                          | PostgreSQL Database Instance                        |  |
|                                          |   - Port: 5438                                      |  |
|                                          |   - Engine: BCDB Deterministic Executor             |  |
|                                          |   - Index: Dynamic Merkle Access Method             |  |
|                                          +-----------------------------------------------------+  |
+----------------------+----------------------------------------------------------------------------+
                       |                                       ^
                       | NuRaft Log Sync (:9000)               | NuRaft Log Sync (:9000)
                       v                                       |
+--------------------------------------------------+ +----------------------------------------------+
|             NODE 2: user4 (Follower)             | |          NODE 3: utkarsh (Follower)          |
|             IP: 10.129.148.246 | Ubuntu 22.04    | |          IP: 10.129.148.248 | Ubuntu 20.04  |
|                                                  | |                                              |
|  +--------------------------------------------+  | |  +---------------------------------------+  |
|  | ariabc_pg_server (Node ID=2)               |  | |  | ariabc_pg_server (Node ID=4)          |  |
|  |   - Client Port: 8000 | Raft Port: 9000    |  | |  |   - Client Port: 8001 | Raft Port: 9000 |  |
|  +---------------------+----------------------+  | |  +-------------------+-------------------+  |
|                        | libpq / SPI             | |                      | libpq / SPI          |
|                        v                         | |                      v                      |
|  +--------------------------------------------+  | |  +---------------------------------------+  |
|  | PostgreSQL Database Instance               |  | |  | PostgreSQL Database Instance          |  |
|  |   - Port: 5438                             |  | |  |   - Port: 5438                        |  |
|  |   - Engine: BCDB Deterministic Executor    |  | |  |   - Engine: BCDB Deterministic Executor|  |
|  |   - Index: Dynamic Merkle Access Method    |  | |  |   - Index: Dynamic Merkle Access Method|  |
|  +--------------------------------------------+  | |  +---------------------------------------+  |
+--------------------------------------------------+ +----------------------------------------------+
```

---

## 2. Detailed Inside-Node Architecture

Each server node (`admin123`, `user4`, `utkarsh`) executes the exact same software stack for deterministic equivalence:

```
+---------------------------------------------------------------------------------------------------+
| SERVER NODE ARCHITECTURE                                                                          |
|                                                                                                   |
|  +---------------------------------------------------------------------------------------------+  |
|  | ariabc_pg_server Process                                                                    |  |
|  |                                                                                             |  |
|  |   +--------------------------+   +----------------------------+   +----------------------+  |
|  |   | Client RPC Listener      |   | NuRaft Consensus Core      |   | Kafka Result         |  |
|  |   | (Port 8000 / 8001)       |-->| (Leader/Follower Sync)     |-->| Publisher (Port 9092|  |
|  |   +--------------------------+   +----------------------------+   +----------------------+  |
|  |                                                |                                            |
|  |                                                v                                            |
|  |                                 +------------------------------+                            |
|  |                                 | BCDB Deterministic Scheduler |                            |
|  |                                 | ($E Worker Queues: 1..16)    |                            |
|  |                                 +--------------+---------------+                            |
|  +------------------------------------------------|--------------------------------------------+  |
|                                                   | Local SPI / Shared Memory Connection          |
|                                                   v                                               |
|  +---------------------------------------------------------------------------------------------+  |
|  | PostgreSQL Backend Process (port 5438)                                                      |  |
|  |                                                                                             |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  |   | Application Workload Schema (`public`)                                               |  |  |
|  |   |   - usertable_small (YCSB data table, primary key: ycsb_key)                        |  |  |
|  |   |   - usertable_small_dynamic_merkle_idx (Index using AM `merkle`)                     |  |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  |                                                                                             |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  |   | Internal Ledger & Merkle State Schema (`ariabc_internal`)                           |  |  |
|  |   |   - merkle_node          (Dynamic prefix tree node storage & XOR aggregate hashes)   |  |  |
|  |   |   - merkle_apply_counter (Singleton sequence tracking for consensus alignment)      |  |  |
|  |   |   - merkle_apply_state   (State machine: READY / CATCHING_UP / BLOCKED_ON_GAP)        |  |  |
|  |   |   - raft_apply_entry     (Raft transaction entry ledger batch)                       |  |  |
|  |   |   - raft_apply_entry_item(Item mapping within Raft entry batches)                      |  |  |
|  |   |   - raft_apply_item      (Ordered item state log)                                    |  |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  |                                                                                             |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  |   | Synchronous Dynamic Merkle Engine (`src/backend/access/merkle/`)                    |  |  |
|  |   |                                                                                      |  |  |
|  |   |   DML Operation (INSERT/UPDATE/DELETE)                                              |  |  |
|  |   |      |                                                                               |  |  |
|  |   |      v                                                                               |  |  |
|  |   |   Staged Delta Accumulator (Memory hash table in backend)                           |  |  |
|  |   |      |                                                                               |  |  |
|  |   |      v [Transaction Pre-Commit]                                                      |  |  |
|  |   |   `merkle_apply_staged_synchronous_safe()`                                           |  |  |
|  |   |      |                                                                               |  |  |
|  |   |      +---> Route lookup in route cache / search `ariabc_internal.merkle_node`        |  |  |
|  |   |      +---> Atomically update leaf XOR hash & tuple count                             |  |  |
|  |   |      +---> Check split threshold (e.g. >32) or merge threshold (<8)                  |  |  |
|  |   |      +---> Propagate parent XOR hash modifications up to root                        |  |  |
|  |   +--------------------------------------------------------------------------------------+  |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Dynamic Merkle Tree Structure & Execution Lifecycle

Instead of static partition boundaries, the Dynamic Merkle implementation dynamically splits and merges binary radix prefix nodes inside `ariabc_internal.merkle_node`:

```
                       +-----------------------------------+
                       | ROOT NODE                         |
                       | node_id: \x0000000000000000       |
                       | prefix_len: 0 | is_leaf: false    |
                       | hash: XOR(Child_0, Child_1)       |
                       +-----------------+-----------------+
                                         |
                  +----------------------+----------------------+
                  | 0                                           | 1
                  v                                             v
    +---------------------------+                 +---------------------------+
    | LEFT LEAF                 |                 | RIGHT LEAF                |
    | node_id: \x0000...        |                 | node_id: \x8000...        |
    | prefix_len: 1             |                 | prefix_len: 1             |
    | is_leaf: true             |                 | is_leaf: true             |
    | tuple_count: 24           |                 | tuple_count: 18           |
    | hash: XOR(Tuples in Left) |                 | hash: XOR(Tuples in Right)|
    +---------------------------+                 +---------------------------+
```

### End-to-End Execution & Verification Flow

The end-to-end execution lifecycle in `scripts/distributed/run_sweep.sh` (which delegates to `run_4node_raft_cluster.sh`) follows a 5-stage transaction pipeline across the 4 physical machines:

```text
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| STAGE 1: TRANSACTION INGESTION, SHARDING & CLUSTER SUBMISSION (run_sweep.sh Default Campaign Mode)                                                |
|                                                                                                                                                   |
|   GATEWAY MACHINE (proposed-gw: 10.129.27.111)                                                                                                    |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | ariabc_pg_gateway Process (--threads 96 --det-client-workers 96 --det-client-inflight 16 --det-window 65536)                              |  |
|   |   WORKLOAD FILE: scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt (Total 20,000 YCSB SQL queries: idx 0..19999)         |  |
|   |                                                                                                                                            |  |
|   |   IN-MEMORY TRACKING TABLE (vote_store):                                                                                                   |  |
|   |   +-----------------------------------------------------------------------------------------------------------------------------------+    |  |
|   |   | Maps req_id -> { worker_thread_id, det_seq, node1_hash, node2_hash, node4_hash, status: IN_FLIGHT }                                |   |  |
|   |   +-----------------------------------------------------------------------------------------------------------------------------------+    |  |
|   |                                                                                                                                            |  |
|   |   96 PARALLEL C++ WORKER THREADS (HORIZONTALLY STRIDED & INDEPENDENT SOCKETS):                                                             |  |
|   |   +------------------------------------+  +------------------------------------+       +------------------------------------+              |  |
|   |   | WORKER THREAD #0                   |  | WORKER THREAD #1                   |  ...  | WORKER THREAD #95                  |              |  |
|   |   | - Shard: idx = 0, 96, 192...       |  | - Shard: idx = 1, 97, 193...       |  ...  | - Shard: idx = 95, 191, 287...    |               |  |
|   |   | - Req ID: client-0                 |  | - Req ID: client-1                 |  ...  | - Req ID: client-95                |              |  |
|   |   | - Det Tag: s 00000000 UPDATE ...   |  | - Det Tag: s 00000001 UPDATE ...   |  ...  | - Det Tag: s 00000095 UPDATE ...   |              |  |
|   |   | - Action: Register vote_store      |  | - Action: Register vote_store      |  ...  | - Action: Register vote_store      |              |  |
|   |   | - OS Source Port: :49152           |  | - OS Source Port: :49153           |  ...  | - OS Source Port: :49247           |              |  |
|   |   +-----------------+------------------+  +-----------------+------------------+       +-----------------+------------------+              |  |
|   +---------------------|---------------------------------------|--------------------------------------------|--------------------------------+   |
|                         |                                       |                                            |                                    |
|                         | Socket Stream #0                      | Socket Stream #1                           | Socket Stream #95                  |
|                         | (Payload: req_id + det_seq)           | (Payload: req_id + det_seq)                | (Payload: req_id + det_seq)        |
|                         v                                       v                                            v                                    |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
                                                          |                                            |
                                                          | TCP 4-Tuple Connections to Port 8000       |
                                                          v                                            v
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| STAGE 2: 96 SERVER RPC THREADS, RAFT LEADER ORDERING (raft_orderer), DURABLE LOG & REPLICATION                                                     |
|                                                                                                                                                   |
|   NODE 1: admin123 (Raft Leader, Node ID=1 | IP: 10.129.148.247)                                                                                  |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | ariabc_pg_server Process (Client Port 8000 | Raft Port 9000)                                                                              |   |
|   |                                                                                                                                           |   |
|   |   96 DEDICATED SERVER RPC LISTENER THREADS (RUNNING IN PARALLEL):                                                                         |   |
|   |   +------------------------------------+  +------------------------------------+       +------------------------------------+             |   |
|   |   | SERVER RPC THREAD #0               |  | SERVER RPC THREAD #1               |  ...  | SERVER RPC THREAD #95              |             |   |
|   |   | - Socket FD = 7                    |  | - Socket FD = 8                    |  ...  | - Socket FD = 102                  |             |   |
|   |   | - Read Frame: client-0, det_seq: 0 |  | - Read Frame: client-1, det_seq: 1 |  ...  | - Read Frame: client-95, det_seq: 95|            |   |
|   |   | - Direct ACK back -> FD = 7        |  | - Direct ACK back -> FD = 8        |  ...  | - Direct ACK back -> FD = 102      |             |   |
|   |   | - Action: append(entry) to table   |  | - Action: append(entry) to table   |  ...  | - Action: append(entry) to table   |             |   |
|   |   +-----------------+------------------+  +-----------------+------------------+       +-----------------+------------------+             |   |
|   |                     |                                       |                                            |                                |   |
|   |                     +---------------------------------------+--------------------------------------------+                                |   |
|   |                                                             v                                                                             |   |
|   |   SHARED `raft_orderer` IN-MEMORY TRACKING TABLE (`orderer.pending` Map):                                                                 |   |
|   |   +----------------------------------------------------------------------------------------------------------------------------+          |   |
|   |   | KEY (`det_seq`) | PRODUCER THREAD        | REQUEST ID (`req_id`) | SQL PAYLOAD STATEMENT                   | STATUS        |          |   |
|   |   | ----------------+------------------------+-----------------------+-----------------------------------------+-------------- |          |   |
|   |   | 0               | Server RPC Thread #0   | client-0              | s 00000000 UPDATE usertable_small ...   | READY / DRAIN |          |   |
|   |   | 1               | Server RPC Thread #1   | client-1              | s 00000001 UPDATE usertable_small ...   | READY / DRAIN |          |   |
|   |   | ...             | ...                    | ...                   | ...                                     | READY / DRAIN |          |   |
|   |   | 95              | Server RPC Thread #95  | client-95             | s 00000095 UPDATE usertable_small ...   | READY / DRAIN |          |   |
|   |   +----------------------------------------------------------------------------------------------------------------------------+          |   |
|   |                                                             |                                                                             |   |
|   |                                                             | `drain_ready()` Pipeline Execution:                                         |   |
|   |                                                             |  - Sorts keys & extracts contiguous ready batch [seq 0..95]                 |   |
|   |                                                             v                                                                             |   |
|   |   SINGLE SHARED DURABLE LOG STORE ON DISK (File: run_raft/node_1/log/segment_00000000000000000001_g00000000000000000001.log):             |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   |   | SEGMENT HEADER (16 Bytes) : magic: 'RAFT_SEG' | format_ver: 2 | reserved: 0                                               |           |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   |   | RECORD #0 (44B Header + Payload) : raft_log_idx: 1 | raft_term: 1 | crc32: 0xa8f... | payload: "client-0 s 00000000..."  |            |   |
|   |   | RECORD #1 (44B Header + Payload) : raft_log_idx: 2 | raft_term: 1 | crc32: 0xb9e... | payload: "client-1 s 00000001..."  |            |   |
|   |   | ...                                                                                                                       |           |   |
|   |   | RECORD #95(44B Header + Payload) : raft_log_idx: 96 | raft_term: 1 | crc32: 0xc1d... | payload: "client-95 s 00000095..." |           |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   |                                                             |                                                                             |   |
|   |                                                             v                                                                             |   |
|   |   NURAFT `AppendEntries` REPLICATION RPC PAYLOAD GENERATOR (TCP Port 9000):                                                               |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   |   | RPC MESSAGE HEADER : term: 1 | leader_id: 1 | prev_log_idx: 0 | prev_log_term: 0 | commit_idx: 0 | entry_count: 96        |           |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   |   | LOG ENTRY ARRAY    :                                                                                                      |           |   |
|   |   |  - Entry [0] : term: 1 | log_val_type: app_log (1) | payload: "client-0 s 00000000 UPDATE usertable_small SET ..."        |           |   |
|   |   |  - Entry [1] : term: 1 | log_val_type: app_log (1) | payload: "client-1 s 00000001 UPDATE usertable_small SET ..."        |           |   |
|   |   |  - ...                                                                                                                    |           |   |
|   |   |  - Entry [95]: term: 1 | log_val_type: app_log (1) | payload: "client-95 s 00000095 UPDATE usertable_small SET ..."       |           |   |
|   |   +---------------------------------------------------------------------------------------------------------------------------+           |   |
|   +-------------------------------------------------------------+-----------------------------------------------------------------------------+   |
|                                                                 |                                                                                 |
|                                 +-------------------------------+-------------------------------+                                                 |
|                                 | ASIO TCP Socket Stream        | ASIO TCP Socket Stream        |                                                 |
|                                 | to user4:9000                 | to utkarsh:9000               |                                                 |
|                                 v                               v                               v                                                 |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | NODE 2: user4 (Follower, ID=2 | 10.129.148.246)                     NODE 3: utkarsh (Follower, ID=4 | 10.129.148.248)                     |   |
|   | - ASIO TCP Socket Listener (Socket FD = 9)                          - ASIO TCP Socket Listener (Socket FD = 9)                            |   |
|   | - Reads `AppendEntries` RPC frame from Socket FD = 9                - Reads `AppendEntries` RPC frame from Socket FD = 9                  |   |
|   |                                                                                                                                           |   |
|   | LOCAL FOLLOWER DURABLE LOG STORE ON DISK                            LOCAL FOLLOWER DURABLE LOG STORE ON DISK                              |   |
|   | (File: `run_raft/node_2/log/segment_...log`)                        (File: `run_raft/node_4/log/segment_...log`)                          |   |
|   | +---------------------------------------------------------+         +---------------------------------------------------------+           |   |
|   | | SEGMENT HEADER (16B) : magic: RAFT_SEG | ver: 2         |         | SEGMENT HEADER (16B) : magic: RAFT_SEG | ver: 2          |          |   |
|   | +---------------------------------------------------------+         +---------------------------------------------------------+           |   |
|   | | RECORD #0 (44B+N) : idx: 1 | payload: "client-0..."     |         | RECORD #0 (44B+N) : idx: 1 | payload: "client-0..."      |          |   |
|   | | RECORD #1 (44B+N) : idx: 2 | payload: "client-1..."     |         | RECORD #1 (44B+N) : idx: 2 | payload: "client-1..."      |          |   |
|   | | ...                                                     |         | ...                                                      |          |   |
|   | | RECORD #95(44B+N) : idx: 96| payload: "client-95..."    |         | RECORD #95(44B+N) : idx: 96| payload: "client-95..."     |          |   |
|   | +---------------------------------------------------------+         +---------------------------------------------------------+           |   |
|   |                                                                                                                                           |   |
|   | - Sends `AppendEntries` ACK back to Leader via Socket FD = 9        - Sends `AppendEntries` ACK back to Leader via Socket FD = 9          |   |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
                                                        |
                                                        v
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| STAGE 3: DETERMINISTIC BCDB ENGINE EXECUTION, CONFLICT TRACKING & SYNCHRONOUS MERKLE STAGING (8 PARALLEL WORKERS)                                 |
|                                                                                                                                                   |
|   EXECUTED CONCURRENTLY ON ALL 3 CLUSTER NODES: admin123 (10.129.148.247), user4 (10.129.148.246), utkarsh (10.129.148.248)                       |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | ariabc_pg_server & BCDB Deterministic Scheduler                                                                                           |   |
|   |   BCDB Scheduler: Dispatches committed Raft entries into 8 Partitioned Worker Queues (--bcdb-workers 8)                                   |   |
|   |   +-------------------+  +-------------------+  +-------------------+        +-------------------+  +-------------------+                 |   |
|   |   | TX QUEUE #0       |  | TX QUEUE #1       |  | TX QUEUE #2       |  ...   | TX QUEUE #6       |  | TX QUEUE #7       |                 |   |
|   |   | [tx0, tx8, tx16]  |  | [tx1, tx9, tx17]  |  | [tx2, tx10, tx18] |  ...   | [tx6, tx14, tx22] |  | [tx7, tx15, tx23] |                 |   |
|   |   +---------+---------+  +---------+---------+  +---------+---------+        +---------+---------+  +---------+---------+                 |   |
|   +-------------|----------------------|----------------------|------------------------------|----------------------|-------------------------+   |
|                 |                      |                      |                              |                      |                             |
|                 v                      v                      v                              v                      v                             |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | 8 PARALLEL BCDB WORKER BACKENDS (PostgreSQL Engine Processes on Port 5438)                                                                |   |
|   |                                                                                                                                           |   |
|   |   STEP 1: OPTIMISTIC PARALLEL EXECUTION & PROCESS-LOCAL RW-SET STAGING                                                                    |   |
|   |   +--------------------------+  SQL Execution  +----------------------------+  Access Hooks  +------------------------------------+       |   |
|   |   | Worker Backend Process   | -------------> | PostgreSQL Heap AM Engine    | ------------> | Backend Memory Context             |       |   |
|   |   | (Queue Worker #0..#7)    |                  | (usertable_small UPDATE)   |                | (bcdb_tx_context: zero shm lock)   |      |   |
|   |   +--------------------------+                  +----------------------------+                +-----------------+------------------+      |   |
|   |                                                                                                             |                             |   |
|   |                                                                                                             v                             |   |
|   |                                                                               +---------------------------------------------------+       |   |
|   |                                                                               | PROCESS-LOCAL SINGLY-LINKED LISTS                 |       |   |
|   |                                                                               | - rs_table_record: [TAG1] -> [TAG2] -> NULL       |       |   |
|   |                                                                               |   (Tag: relOid=16384, page=12, offset=4)          |       |   |
|   |                                                                               | - ws_table_record: [TAG1, CMD_UPDATE, Slot, CID]  |       |   |
|   |                                                                               +---------------------------------------------------+       |   |
|   |                                                                                                                                           |   |
|   |   STEP 2: DETERMINISTIC CONFLICT TRACKING & DUAL HASH SHARD PROBING (conflict_checkDT)                                                    |   |
|   |   +------------------------------------+          Probes Tuple Tags           +---------------------------------------------------+       |   |
|   |   | Local RW Lists (Step 1)            | -----------------------------------> | SHARED WSTable DUAL SHARDS (Ping-Pong Scheme)     |       |   |
|   |   | - rs_table_record                  |                                      | +-----------------------+ +---------------------+ |       |   |
|   |   | - ws_table_record                  |                                      | | Active Shard (map)    | | Secondary (mapB)    | |       |   |
|   |   +------------------------------------+                                      | +-----------------------+ +---------------------+ |       |   |
|   |                                                                               +-------------------------+-------------------------+       |   |
|   |                                                                                                         |                                 |   |
|   |                                                                                                         v                                 |   |
|   |                                                                               +---------------------------------------------------+       |   |
|   |                                                                               | CONFLICT EVALUATION ENGINE (cand_txid < self_txid)|       |   |
|   |                                                                               | - RAW Probe : ws_table tag matches local rs_record|       |   |
|   |                                                                               | - WAR Probe : rs_table tag matches local ws_record|       |   |
|   |                                                                               | - WAW Probe : ws_table tag matches local ws_record|       |   |
|   |                                                                               +-------------------------+-------------------------+       |   |
|   |                                                                                                         |                                 |   |
|   |                                                                    +------------------------------------+--------------------------------+       |   |
|   |                                                                    | PASS (Zero Conflict)                                                | FAIL (Conflict Hit)
|   |                                                                    v                                                                     v   |   |
|   |                                                   +----------------------------------+                                  +----------------+   |   |
|   |                                                   | Proceed to Publish & Serial Gate |                                  | Defer / Retry  |   |   |
|   |                                                   +----------------------------------+                                  +----------------+   |   |
|   |                                                                                                                                           |   |
|   |   STEP 3: WRITE-SET PUBLISHING & SERIAL COMMIT GATE PIPELINE                                                                              |   |
|   |   +------------------------------------+  publish_ws_tableDT()  +---------------------------------------------------------------------+   |   |
|   |   | Local ws_table_record              | ---------------------> | SHARED ACTIVE SHARD (ws_table->mapActive)                           |   |   |
|   |   | [TAG1, CMD_UPDATE, Slot, CID]      |                        | Atomically commits Write-Set tags for future conflict probes        |   |   |
|   |   +------------------------------------+                        +----------------------------------+----------------------------------+   |   |
|   |                                                                                                    |                                          |   |
|   |                                                                                                    v                                          |   |
|   |                                                                 +---------------------------------------------------------------------+   |   |
|   |                                                                 | bcdb_wait_for_serial_slot() (SERIAL GATE WATERMARK)                 |   |   |
|   |                                                                 | Enforces: tx_id == last_committed_tx_id + 1                         |   |   |
|   |                                                                 | Blocks out-of-order commits; advances watermark on commit completion|   |   |
|   |                                                                 +---------------------------------------------------------------------+   |   |
|   |                                                                                                                                           |   |
|   |   STEP 4: SYNCHRONOUS MERKLE TREE STAGING & ATOMIC RADIX BUBBLE                                                                           |   |
|   |   +--------------------------+  Heap Modification  +----------------------------+  Key Delta Stage  +-------------------------------+   |   |
|   |   | Modified Tuple           | ------------------> | merkledelta.c Access Hook  | ----------------> | Process HTAB Memory Buffer    |   |   |
|   |   | (usertable_small key=42) |                     | Compute TupleKeyHash(42)   |                   | Map: Key -> Delta (XOR Hash)  |   |   |
|   |   +--------------------------+                     +----------------------------+                   +---------------+---------------+   |   |
|   |                                                                                                                     |                         |   |
|   |                                                                                                                     v PRE_COMMIT Hook         |   |
|   |                                                                               +---------------------------------------------------+   |   |
|   |                                                                               | merkle_apply_staged_synchronous_safe()            |   |   |
|   |                                                                               | Lookup Node in ariabc_internal.merkle_node        |   |   |
|   |                                                                               +-------------------------+-------------------------+   |   |
|   |                                                                                                         |                             |   |
|   |                                                                                                         v                             |   |
|   |   DYNAMIC BINARY RADIX PREFIX MERKLE TREE STRUCTURE:                                                                                      |   |
|   |                                               +---------------------------------------+                                                   |   |
|   |                                               | ROOT NODE (Parent Level 0)            |                                                   |   |
|   |                                               | node_id: \x0000000000000000           |                                                   |   |
|   |                                               | prefix_len: 0  | is_leaf: false       |                                                   |   |
|   |                                               | root_hash: Child_0_Hash ^ Child_1_Hash|                                                   |   |
|   |                                               +-------------------+-------------------+                                                   |   |
|   |                                                                   |                                                                       |   |
|   |                                       +---------------------------+---------------------------+                                           |   |
|   |                                       | Bit 0 = 0                                             | Bit 0 = 1                                 |   |
|   |                                       v                                                       v                                           |   |
|   |                       +-------------------------------+                       +-------------------------------+                           |   |
|   |                       | LEFT LEAF NODE                |                       | RIGHT LEAF NODE               |                           |   |
|   |                       | node_id: \x0000000000000000   |                       | node_id: \x8000000000000000   |                           |   |
|   |                       | prefix_len: 1 | is_leaf: true |                       | prefix_len: 1 | is_leaf: true |                           |   |
|   |                       | tuple_count: 24 (Threshold 32)|                       | tuple_count: 18 (Threshold 32)|                           |   |
|   |                       | hash: XOR(Tuples in Left)     |                       | hash: XOR(Tuples in Right)    |                           |   |
|   |                       +-------------------------------+                       +-------------------------------+                           |   |
|   |                                       |                                                       |                                           |   |
|   |                                       +---------------------------+---------------------------+                                           |   |
|   |                                                                   |                                                                       |   |
|   |   TREE REBALANCING ENGINE:                                        v                                                                       |   |
|   |   +-----------------------------------+   +------------------------------------+   +--------------------------------------------------+   |   |
|   |   | 1. Leaf XOR Update                |   | 2. Rebalancing Rule                |   | 3. Atomic Parent Bubble                          |   |   |
|   |   | leaf_hash = leaf_hash ^ delta_hash|   | Split if tuple > 32 (prefix_len+1) |   | Propagate XOR modifications up parent nodes      |   |   |
|   |   | tuple_count = tuple_count + 1     |   | Merge if tuple < 8 with sibling    |   | up to ROOT (\x0000000000000000)                  |   |   |
|   |   +-----------------------------------+   +------------------------------------+   +--------------------------------------------------+   |   |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
                                                        |
                                                        v
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| STAGE 4: ASYNC KAFKA RESULT STREAMING & BACKGROUND vote_store VALIDATION                                                                          |
|                                                                                                                                                   |
|   ALL 3 SERVER NODES: admin123 (10.129.148.247), user4 (10.129.148.246), utkarsh (10.129.148.248)                                                 |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | librdkafka Producer inside ariabc_pg_server                                                                                               |   |
|   |   - Publishes result record: { req_id: "client-0", node_id: 1, status: OK, merkle_root: 0xa3f... } to Kafka Broker                        |   |
|   +---------------------------------------------------|-----------------------------------------------------------------------------------+   |
|                                                       | Kafka TCP Produce (Port 9092)                                                         |
|                                                       v                                                                                       |
|   NODE 1: admin123 (10.129.148.247:9092)                                                                                                          |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | Kafka Broker (KRaft Mode | Topic: ariabc_results)                                                                                           |   |
|   +---------------------------------------------------|-----------------------------------------------------------------------------------+   |
|                                                       | Kafka TCP Consume (Port 9092)                                                         |
|                                                       v                                                                                       |
|   GATEWAY MACHINE (proposed-gw: 10.129.27.111)                                                                                                    |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | Background Kafka Result Consumer Thread (inside ariabc_pg_gateway)                                                                        |   |
|   |   1. Asynchronously polls result records for `req_id: client-0` from topic `ariabc_results`                                               |   |
|   |   2. Looks up `vote_store["client-0"]` in background without blocking Worker Thread #0                                                    |   |
|   |   3. Accumulates replica root hash returns (Node 1, Node 2, Node 4) for result metrics tracking                                           |   |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
                                                        |
                                                        v
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| STAGE 5: POST-WORKLOAD BARRIER & REPLICA CONSISTENCY VERIFICATION (Phase 8 in run_4node_raft_cluster.sh / run_sweep.sh)                          |
|                                                                                                                                                   |
|   1. BARRIER MARKER SUBMISSION:                                                                                                                   |
|      - Gateway submits final barrier transaction marker (`key = 99999999`) across cluster to flush all pending blocks.                            |
|                                                                                                                                                   |
|   2. CONCURRENT SSH MERKLE READBACK:                                                                                                              |
|      - Runner script executes concurrent psql queries on port 5438 across admin123, user4, and utkarsh:                                          |
|        * `SELECT count(*) FROM usertable_small;`                                                                                                  |
|        * `SELECT merkle_root_hash('usertable_small');`                                                                                            |
|        * `SELECT merkle_verify('usertable_small');`                                                                                               |
|                                                                                                                                                   |
|   3. IN-DATABASE INTEGRITY CHECK (`src/backend/access/merkle/merkleverify.c`):                                                                  |
|      - `merkle_verify()` scans table tuples, recalculates bottom-up leaf XOR hashes, verifies tree topology, and checks                        |
|        against stored root in `ariabc_internal.merkle_node`. Returns `t` (true).                                                                 |
|                                                                                                                                                   |
|   4. CONSISTENCY PASS/FAIL VALIDATION:                                                                                                            |
|      - Script validates: POST_COUNTS[1] == POST_COUNTS[2] == POST_COUNTS[4]                                                                       |
|                      AND POST_ROOTS[1]  == POST_ROOTS[2]  == POST_ROOTS[4]                                                                       |
|                      AND POST_VERIFY    == 't' on all nodes                                                                                       |
|      - Outcome: Zero Hash Divergence (`divergence_count=0`, `permanent_failures=0`, `POST_PASS=1`).                                               |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

#### Detailed Pipeline Breakdown

1. **Transaction Ingestion, Sharding, Tagging & Tracking (Gateway -> Leader)**:
   - `ariabc_pg_gateway` runs on the Gateway Controller (`10.129.27.111`) with 96 worker threads (`--threads 96 --det-client-workers 96 --det-client-inflight 16 --det-window 65536`).
   - Ingests 20,000 YCSB SQL queries from `scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt` (`idx = 0..19999`).
   - **Strided Sharding**: Queries are partitioned across the 96 worker threads round-robin:
     - Worker 0 handles `idx = 0, 96, 192, 288...`
     - Worker 1 handles `idx = 1, 97, 193, 289...`
     - Worker 95 handles `idx = 95, 191, 287, 383...`
   - **Dual Identifier Payload Tagging & Leader-Assigned Ordering**:
     - **Protocol Request ID (`req_id`)**: Every query gets assigned a unique tracking string (e.g. `client-0`, `client-1`, ..., `client-19999`).
     - **Deterministic Sequence Tag (`det_seq`) & Policy**: Queries carry a sequence tag `"s <seq>"`. Under `run_sweep.sh` default (`--raft-ordering-policy leader-assigned`), the Raft Leader assigns/validates global deterministic execution order at admission while batching (`--raft-ordered-batch-append 1`, `--raft-ordered-batch-target-entries 64`, `--raft-ordered-batch-linger-us 1000`) and coalescing log entries (`--raft-ordered-coalesce-log 1`).
   - **In-Memory Tracking (`vote_store`)**: Before transmitting a query over the socket, the worker thread registers `req_id` into the shared in-memory `vote_store` map.
   - **Parallel TCP Socket Streaming**: Each worker thread maintains its own dedicated outbound socket (`TCP Socket #0` to `#95`) to Node 1 (`admin123:8000`), using unique Linux OS ephemeral ports (`:49152`..`:49247`).

2. **Connection Pooling (--pool-size $E), 96 Dedicated Server RPC Threads, Leader Raft Ordering & Replication**:
   - `ariabc_pg_server` on Node 1 (`admin123`) runs a main listen loop on port 8000.
   - **Per-Socket Dedicated Server Threads**: As the 96 Gateway sockets connect, Node 1 calls `accept()` on port 8000 and spawns **96 dedicated Server RPC Listener Threads** (`std::thread th([fd...] { handle_client_fd(fd...); })`).
     - Server RPC Thread #0 manages `Socket FD = 7` (connected to Gateway Worker Thread #0).
     - Server RPC Thread #1 manages `Socket FD = 8` (connected to Gateway Worker Thread #1).
     - Server RPC Thread #95 manages `Socket FD = 102` (connected to Gateway Worker Thread #95).
   - **Full Leader-Assigned Raft Ordering Pipeline (`append_raft_ordered` & `raft_orderer`)**:
     1. **Leader Sequence Assignment (`--raft-ordering-policy leader-assigned`)**: When request frames arrive, the Raft Leader (`admin123`) assigns global monotonically increasing sequence tags (`orderer.next_assigned_seq`) upon admission and tracks `orderer.req_id_to_seq` for idempotent deduplication against gateway retries.
     2. **Orderer Queueing (`orderer.pending`)**: Each assigned request entry is stored in the thread-safe `orderer.pending` map sorted by sequence number.
     3. **Contiguous Batch Collection & Coalescing (`drain_ready`)**: `drain_ready()` extracts contiguous ready sequence ranges starting at `orderer.next_seq`. Under `--raft-ordered-batch-append 1`, it accumulates batches up to `--raft-ordered-batch-target-entries 64` (or waits up to `--raft-ordered-batch-linger-us 1000` µs). When `--raft-ordered-coalesce-log 1` is set, ready batches are fused into a single multi-item Raft log entry.
     4. **Local Durable Log Store Append**: The Raft Leader serializes and appends the ordered log batch into Node 1's local durable NuRaft log store (`run_raft/node_1/log/segment_...log`) with `ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1`.
     5. **NuRaft Consensus Replication**: NuRaft streams `AppendEntries` RPC frames over TCP Port 9000 to Follower nodes: Node 2 (`user4:9000`) and Node 3 (`utkarsh:9000`). Once a majority quorum acknowledges replication, the entry index is committed across the cluster.
     6. **Leader Completion ACK**: Under `run_sweep.sh` default (`--kafka-completion-mode majority`), completion waits for 3-node consensus before client receipt; under `--kafka-completion-mode async`, an ACK frame is returned immediately over `Socket FD = 7` upon local Raft admission.

3. **Deterministic BCDB Engine Execution, Conflict Tracking & Merkle Staging**:
   - On commit, `ariabc_pg_server` on all 3 nodes dispatches the ordered
     transactions to the BCDB Deterministic Scheduler across 8 partitioned
     transaction queues (`tx_queues[0..7]`) in shared memory
     (`--bcdb-workers 8`, with `--server-exec-workers 8` and
     `--server-pg-connections 8`).
   - **Step 1: Optimistic Parallel Execution & Process-Local RW Staging**:
     - 8 parallel BCDB worker processes dequeue transactions and execute
       DML statements (`UPDATE`, `INSERT`, `DELETE`) on
       `public.usertable_small` (Port 5438).
     - During execution, `rs_table_reserveDT()` and `ws_table_reserveDT()`
       intercept reads/writes and append tuple tags (`PREDICATELOCKTARGETTAG`)
       to process-local linked lists (`rs_table_record` and `ws_table_record`)
       in `bcdb_tx_context`. No shared memory locks are acquired.
   - **Step 2: Deterministic Conflict Tracking (`conflict_checkDT()`)**:
     - Shared memory maintains dual Write-Set hash tables (`WSTable` with
       shards `map` and `mapB`) in a **Ping-Pong Scheme** to allow zero-lock
       table clear during block rotations.
     - Workers execute `conflict_checkDT()`, checking local RW sets against
       `ws_table` and `rs_table` for earlier transactions
       (`cand_txid < self_txid`):
       * **RAW (Read-After-Write)**: Probes if an earlier transaction wrote
         a tuple read by this worker.
       * **WAR (Write-After-Read)**: Probes if an earlier transaction read
         a tuple written by this worker.
       * **WAW (Write-After-Write)**: Probes if an earlier transaction
         wrote a tuple written by this worker.
    - **Step 3: Write-Set Publishing & Serial Commit Gate**:
      - `publish_ws_tableDT()` atomically commits local write-set entries
        into the active shard (`ws_table->mapActive`).
      - `bcdb_wait_for_serial_slot()` enforces sequential commit watermarks
        (`tx_id == last_committed_tx_id + 1`) for deterministic ordering.
    - **Step 4: Synchronous Merkle Tree Staging & Radix Update**:
      - PostgreSQL heap hooks intercept modified tuples, calculate
        `TupleKeyHash(ycsb_key)`, and stage key deltas in memory (`HTAB`).
      - At transaction `PRE_COMMIT`, `merkle_apply_staged_synchronous_safe()`
        updates `ariabc_internal.merkle_node`:
        * Leaf aggregate XOR hash update: `leaf_hash = leaf_hash ^ delta_hash`.
        * Rebalancing: Splits leaf when `tuple_count > 32` (prefix_len + 1);
          Merges with sibling when `tuple_count < 8`.
        * Bubbles modified XOR hashes up parent radix nodes to ROOT
          (`node_id: \x0000000000000000`).
   - **Transaction Commit Protocol & Index Maintenance Timeline**:
     1. **Optimistic Phase — Normal Heap Mutation**:
        Worker executes DML (`UPDATE`/`INSERT`/`DELETE`). PostgreSQL Heap AM
        modifies tuple in buffer pool (`table_tuple_update`/`insert`).
     2. **Optimistic Phase — Normal B-Tree Index Update**:
        PostgreSQL immediately inserts/updates tuple pointers in standard
        B-Tree indexes (e.g. `usertable_small_pkey` on `ycsb_key`) via
        `execIndexing.c` during SQL execution.
     3. **Optimistic Phase — RW Set Local Staging**:
        `rs_table_reserveDT()` / `ws_table_reserveDT()` append tuple tags to
        process-local linked lists (`rs_table_record` / `ws_table_record`).
     4. **Optimistic Phase — Merkle Delta Staging (`merkledelta.c`)**:
        Table access hooks compute `TupleKeyHash(ycsb_key)` and stage key XOR
        deltas into a backend process-local `HTAB` buffer. The Merkle tree
        table (`ariabc_internal.merkle_node`) is NOT modified yet.
     5. **Validation Phase — BCDB Conflict Check (`conflict_checkDT()`)**:
        Worker probes shared dual `WSTable` shards (`map` and `mapB`) for
        RAW, WAR, and WAW conflicts against earlier-ordered transactions.
     6. **Validation Phase — Write-Set Publishing (`publish_ws_tableDT()`)**:
        On zero conflicts, local Write-Set entries are published to active shard
        `ws_table->mapActive` for downstream conflict checking.
     7. **Commit Gate Phase — Serial Slot Watermark**:
        `bcdb_wait_for_serial_slot()` blocks execution until
        `tx_id == last_committed_tx_id + 1`.
     8. **PRE_COMMIT Hook Phase — Merkle Index Tree Apply (`merkleapply.c`)**:
        Transaction commit protocol triggers `XACT_EVENT_PRE_COMMIT`.
        `merkle_apply_staged_synchronous_safe()` reads staged deltas from `HTAB`,
        mutates leaf nodes in `ariabc_internal.merkle_node` (`leaf_hash =
        leaf_hash ^ delta_hash`), triggers dynamic split/merge rebalancing,
        and bubbles XOR aggregate hashes up to ROOT (`\x0000000000000000`).
     9. **Finalization Phase — WAL Record & Commit Completion**:
        PostgreSQL writes transaction commit record to WAL, releases locks,
        resets backend memory context, and advances serial commit watermark.

4. **Async Kafka Result Streaming & Background Validation**:
   - `ariabc_pg_server` processes on all 3 nodes use `librdkafka` to publish
     execution result records (`req_id`, `node_id`, `status`, `merkle_root`)
     to Kafka Broker on Node 1 (`10.129.148.247:9092`, Topic `ariabc_results`).
   - **Asynchronous Background Tracking**: The Gateway Kafka Result Consumer
     thread polls topic `ariabc_results` in background, matching `req_id`
     in `vote_store` for hash integrity without blocking Gateway workers.

5. **Post-Workload Barrier & Replica Verification (`Phase 8`)**:
    - `run_4node_raft_cluster.sh` submits a barrier marker (`key=99999999`) across the cluster.
   - Upon completion, the script executes SSH `psql` commands on port 5438 across all 3 nodes:
     - `SELECT count(*) FROM usertable_small;`
     - `SELECT merkle_root_hash('usertable_small');`
     - `SELECT merkle_verify('usertable_small');`
   - In `merkleverify.c`, `merkle_verify()` scans table tuples, recalculates expected leaf XOR hashes bottom-up, checks radix tree topology, and compares against the root stored in `ariabc_internal.merkle_node`. It returns `t` (true).
   - The test script asserts:
     - `POST_COUNTS[1] == POST_COUNTS[2] == POST_COUNTS[4]`
     - `POST_ROOTS[1]  == POST_ROOTS[2]  == POST_ROOTS[4]`
     - `POST_VERIFY    == 't'` on all nodes
   - Verification succeeds only when `divergence_count=0` and `permanent_failures=0`.

6. **Full `run_sweep.sh` Campaign Environment & CLI Flag Configuration**:
   - **Environment Variables Injected**:
     - `FORCE_BUILD=0`, `SKIP_RDKAFKA_SETUP=1`
     - `ARIABC_PREFERRED_LEADER_ID=1` (Pins Raft leadership priority to Node 1 `admin123`)
     - `ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1` (Async flush for local durable log store)
     - `ARIABC_RAFT_STREAM_GAP=512` (Pipeline gap control for NuRaft streaming)
     - `ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1` (Async background result publishing)
     - `BCDB_DET_QUEUE_HIGH_WM=65536` / `BCDB_DET_QUEUE_LOW_WM=32768` (Admission queue watermarks)
   - **Cluster CLI Arguments Passed**:
     - `--ordering-mode raft-kafka`, `--enable-merkle-index 1`, `--raft-apply-ledger-mode off`
     - `--threads 96`, `--det-client-workers 96`, `--det-client-inflight 16`, `--det-window 65536`
     - `--server-exec-workers $E`, `--server-pg-connections $E`, `--pool-size $E`, `--bcdb-workers $E`, `--bcdb-init-block-size $E` (swept for $E \in \{1, 2, 4, 8, 12, 16\}$)
     - `--bcdb-decouple-workers 1`, `--conn-fanout 1`, `--raft-ordered-fanout 1`
     - `--raft-ordering-policy leader-assigned`, `--raft-ordered-batch-append 1`
     - `--raft-ordered-batch-target-entries 64`, `--raft-ordered-batch-linger-us 1000`, `--raft-ordered-coalesce-log 1`
     - `--kafka-completion-mode majority` (default campaign completion mode)

---

### Local Durable NuRaft Log Store Architecture & Binary Record Format

The local durable log engine (`ariabc_raft::durable_log_store` in `ariabc_pg/src/durable_log_store.cxx`) persists incoming Raft log entries to disk before fast ACKs or replication.

```text
+-------------------------------------------------------------------------------------------------------------------------------+
| LOCAL DURABLE NURAFT LOG STORE ARCHITECTURE & BINARY RECORD FORMAT                                                            |
| Location on Disk: run_raft/node_1/log/ (configured via --raft-log-dir)                                                       |
|                                                                                                                               |
|   DIRECTORY LAYOUT:                                                                                                           |
|   run_raft/node_1/log/                                                                                                        |
|   ├── manifest.bin                                    (Stores active segment list & generation counter next_gen_id)           |
|   ├── watermark.bin                                   (Stores last_durable_index for crash recovery)                          |
|   └── segment_00000000000000000001_g00000000000000000001.log  (64 MiB segmented binary append-only log file)                  |
|                                                                                                                               |
|   BINARY SEGMENT FILE FORMAT (Little-Endian):                                                                                 |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | SEGMENT HEADER (16 Bytes):                                                                                            |   |
|   |  [0..7]   Magic Header  : 'RAFT_SEG' (0x5345475F54464152)                                                             |   |
|   |  [8..11]  Format Version: uint32 (2)                                                                                  |   |
|   |  [12..15] Reserved      : uint32 (0)                                                                                  |   |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | EACH LOG RECORD: [44-Byte Binary Record Header] + [Payload Bytes]                                                     |   |
|   |                                                                                                                       |   |
|   |  OFFSET   FIELD NAME       TYPE     SIZE      DESCRIPTION / SAMPLE VALUE                                              |   |
|   |  ------------------------------------------------------------------------------------------------------------------   |   |
|   |  [0..3]   magic            uint32   4 Bytes   Record Magic ID (0xAB1CFACE)                                            |   |
|   |  [4..7]   format_ver       uint32   4 Bytes   Format Version (2)                                                      |   |
|   |  [8..11]  record_type      uint32   4 Bytes   Type: ENTRY = 1, TRUNCATE_FROM = 2                                      |   |
|   |  [12..15] record_length    uint32   4 Bytes   Total Record Size (44 + payload_len)                                    |   |
|   |  [16..23] raft_log_idx     uint64   8 Bytes   Raft Log Sequence Index (e.g., 1, 2, 3...)                              |   |
|   |  [24..31] raft_term        uint64   8 Bytes   Raft Term ID (e.g., 1)                                                  |   |
|   |  [32..35] payload_len      uint32   4 Bytes   Length of transaction payload                                           |   |
|   |  [36..39] payload_crc32    uint32   4 Bytes   CRC32 Checksum of payload bytes                                         |   |
|   |  [40..43] header_crc32     uint32   4 Bytes   CRC32 Checksum of first 40 header bytes                                 |   |
|   |  ------------------------------------------------------------------------------------------------------------------   |   |
|   |  [44..N]  payload bytes    binary   N Bytes   Payload: "client-0 s 00000000 UPDATE usertable_small SET ..."           |   |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
+-------------------------------------------------------------------------------------------------------------------------------+
```

#### Append Mechanics & Index Lookup
1. **Append Execution**: Calling `durable_log_store::append(entry)` formats the 44-byte binary record header, calculates header & payload CRC32 checksums, appends the record to the active segment file descriptor, and updates the in-memory index map (`index_locations_[raft_log_idx] -> {segment_seq, file_offset, record_size}`).
2. **Segment Rotation**: Segment files auto-rotate when reaching 64 MiB (`SEGMENT_MAX_BYTES`). Each new segment increments generation ID (`gen_id`) and updates `manifest.bin`.
3. **Crash Recovery & Integrity**: On node startup, `scan_and_recover()` reads `manifest.bin`, scans segment records, validates header CRC32 and payload CRC32, and truncates incomplete or un-synced tail records to guarantee full durable consistency.


