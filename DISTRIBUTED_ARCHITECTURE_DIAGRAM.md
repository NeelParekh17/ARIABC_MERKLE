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
|                                  IP: 10.129.148.247 | Ubuntu 20.04                                   |
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
|  |                                 | (8 Worker Queues)            |                            |
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
| STAGE 1: TRANSACTION INGESTION, SHARDING & DIRECT SOCKET SUBMISSION (run_sweep.sh Default Mode)                                                   |
|                                                                                                                                                   |
|   GATEWAY MACHINE (proposed-gw: 10.129.27.111)                                                                                                    |
|   +-------------------------------------------------------------------------------------------------------------------------------------------+   |
|   | ariabc_pg_gateway Process (--threads 96 --det-client-workers 96 --det-client-inflight 1)                                                   |  |
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
| STAGE 2: 96 SERVER RPC THREADS, SHARED `raft_orderer` MEMORY TABLE, DURABLE LOG & REPLICATION                                                     |
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
+-------------------------------------------------------------------------------------------------------------------------------+
| STAGE 3: DETERMINISTIC DB EXECUTION & SYNCHRONOUS MERKLE TREE STAGING                                                         |
|                                                                                                                               |
|   EXECUTED CONCURRENTS ON ALL 3 NODES: admin123 (10.129.148.247), user4 (10.129.148.246), utkarsh (10.129.148.248)            |
|                                                                                                                               |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | ariabc_pg_server Process                                                                                              |   |
|   |   - BCDB Deterministic Scheduler dispatches committed Raft entries across 8 Worker Queues (--bcdb-workers 8)        |   |
|   +---------------------------------------------------|-------------------------------------------------------------------+   |
|                                                       | Local SPI / libpq Pool Connection (Port 5438)                         |
|                                                       v                                                                       |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | PostgreSQL Backend Engine (Port 5438 | postgres process)                                                              |   |
|   |                                                                                                                       |   |
|   |   1. Workload Execution:                                                                                              |   |
|   |      - DML executed on `public.usertable_small` (INSERT / UPDATE / DELETE)                                            |   |
|   |                                                                                                                       |   |
|   |   2. In-Memory Staging (`src/backend/access/merkle/merkledelta.c`):                                                   |   |
|   |      - PostgreSQL heap access hooks intercept modified tuples                                                         |   |
|   |      - Compute `TupleKeyHash` and stage delta in backend memory HTAB buffer                                           |   |
|   |                                                                                                                       |   |
|   |   3. Transaction Pre-Commit Hook (`merkle_apply_staged_synchronous_safe` in `merkleapply.c`):                         |   |
|   |      - Reads staged HTAB delta buffer at PRE_COMMIT                                                                   |   |
|   |      - Route lookup in `ariabc_internal.merkle_node` prefix radix tree                                                |   |
|   |      - Atomically updates leaf node XOR aggregate hash & tuple_count                                                  |   |
|   |      - Checks dynamic node split threshold (>32 tuples) or merge threshold (<8 tuples)                               |   |
|   |      - Bubbles XOR hash modifications up parent radix nodes to ROOT (\x0000000000000000)                              |   |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
+-------------------------------------------------------------------------------------------------------------------------------+
                                                        |
                                                        v
+-------------------------------------------------------------------------------------------------------------------------------+
| STAGE 4: ASYNC KAFKA RESULT STREAMING & BACKGROUND vote_store VALIDATION                                                      |
|                                                                                                                               |
|   ALL 3 SERVER NODES: admin123, user4, utkarsh                                                                                |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | librdkafka Producer inside ariabc_pg_server                                                                           |   |
|   |   - Publishes result record: { req_id: "client-0", node_id: 1, status: OK, merkle_root: 0xa3f... } to Kafka Broker    |   |
|   +---------------------------------------------------|-------------------------------------------------------------------+   |
|                                                       | Kafka TCP Produce (Port 9092)                                         |
|                                                       v                                                                       |
|   NODE 1: admin123 (10.129.148.247:9092)                                                                                      |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | Kafka Broker (KRaft Mode | Topic: ariabc_results)                                                                       |   |
|   +---------------------------------------------------|-------------------------------------------------------------------+   |
|                                                       | Kafka TCP Consume (Port 9092)                                         |
|                                                       v                                                                       |
|   GATEWAY MACHINE (proposed-gw: 10.129.27.111)                                                                               |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
|   | Background Kafka Result Consumer Thread (inside ariabc_pg_gateway)                                                    |   |
|   |   1. Asynchronously polls result records for `req_id: client-0` from topic `ariabc_results`                           |   |
|   |   2. Looks up `vote_store["client-0"]` in background without blocking Worker Thread #0                                |   |
|   |   3. Accumulates replica root hash returns (Node 1, Node 2, Node 4) for result metrics tracking                        |   |
|   +-----------------------------------------------------------------------------------------------------------------------+   |
+-------------------------------------------------------------------------------------------------------------------------------+
                                                        |
                                                        v
+-------------------------------------------------------------------------------------------------------------------------------+
| STAGE 5: POST-WORKLOAD BARRIER & REPLICA CONSISTENCY VERIFICATION (Phase 8 in run_4node_raft_cluster.sh / run_sweep.sh)      |
|                                                                                                                               |
|   1. BARRIER MARKER SUBMISSION:                                                                                               |
|      - Gateway submits final barrier transaction marker (`key = 99999999`) across cluster to flush all pending blocks.        |
|                                                                                                                               |
|   2. CONCURRENT SSH MERKLE READBACK:                                                                                          |
|      - Runner script executes concurrent psql queries on port 5438 across admin123, user4, and utkarsh:                      |
|        * `SELECT count(*) FROM usertable_small;`                                                                              |
|        * `SELECT merkle_root_hash('usertable_small');`                                                                        |
|        * `SELECT merkle_verify('usertable_small');`                                                                           |
|                                                                                                                               |
|   3. IN-DATABASE INTEGRITY CHECK (`src/backend/access/merkle/merkleverify.c`):                                               |
|      - `merkle_verify()` scans table tuples, recalculates bottom-up leaf XOR hashes, verifies tree topology, and checks    |
|        against stored root in `ariabc_internal.merkle_node`. Returns `t` (true).                                             |
|                                                                                                                               |
|   4. CONSISTENCY PASS/FAIL VALIDATION:                                                                                        |
|      - Script validates: POST_COUNTS[1] == POST_COUNTS[2] == POST_COUNTS[4]                                                   |
|                      AND POST_ROOTS[1]  == POST_ROOTS[2]  == POST_ROOTS[4]                                                   |
|                      AND POST_VERIFY    == 't' on all nodes                                                                   |
|      - Outcome: Zero Hash Divergence (`divergence_count=0`, `permanent_failures=0`, `POST_PASS=1`).                            |
+-------------------------------------------------------------------------------------------------------------------------------+
```

#### Detailed Pipeline Breakdown

1. **Transaction Ingestion, Sharding, Tagging & Tracking (Gateway -> Leader)**:
   - `ariabc_pg_gateway` runs on the Gateway Controller (`10.129.27.111`) with 96 worker threads (`--threads 96 --det-client-workers 96 --det-client-inflight 1`).
   - Ingests 20,000 YCSB SQL queries from `scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt` (`idx = 0..19999`).
   - **Strided Sharding**: Queries are partitioned across the 96 worker threads round-robin:
     - Worker 0 handles `idx = 0, 96, 192, 288...`
     - Worker 1 handles `idx = 1, 97, 193, 289...`
     - Worker 95 handles `idx = 95, 191, 287, 383...`
   - **Dual Identifier Payload Tagging**:
     - **Protocol Request ID (`req_id`)**: Every query gets assigned a unique tracking string (e.g. `client-0`, `client-1`, ..., `client-19999`).
     - **Deterministic Sequence Tag (`det_seq`)**: Queries are prepended with `"s <seq>"` (e.g. `s 00000000 UPDATE usertable_small SET ...`) to enforce global deterministic execution order across the Raft cluster.
   - **In-Memory Tracking (`vote_store`)**: Before transmitting a query over the socket, the worker thread registers `req_id` into the shared in-memory `vote_store` map.
   - **Parallel TCP Socket Streaming**: Each worker thread maintains its own dedicated outbound socket (`TCP Socket #0` to `#95`) to Node 1 (`admin123:8000`), using unique Linux OS ephemeral ports (`:49152`..`:49247`).

2. **Connection Pooling (--pool-size 256), 96 Dedicated Server RPC Threads, Local Durability & Direct ACK**:
   - `ariabc_pg_server` on Node 1 (`admin123`) runs a main listen loop on port 8000.
   - **Per-Socket Dedicated Server Threads**: As the 96 Gateway sockets connect, Node 1 calls `accept()` on port 8000 and spawns **96 dedicated Server RPC Listener Threads** (`std::thread th([fd...] { handle_client_fd(fd...); })`).
     - Server RPC Thread #0 manages `Socket FD = 7` (connected to Gateway Worker Thread #0).
     - Server RPC Thread #1 manages `Socket FD = 8` (connected to Gateway Worker Thread #1).
     - Server RPC Thread #95 manages `Socket FD = 102` (connected to Gateway Worker Thread #95).
   - **Exact Per-Thread Execution Sequence on Leader**:
     1. **Read Request Frame**: Server RPC Thread #0 reads `req_id: client-0` & `det_seq: s 00000000` from `Socket FD = 7`.
     2. **Local Raft Log Durability**: Server RPC Thread #0 invokes `raft->append_entries({log})`, serializing and writing the entry into Node 1's local durable NuRaft log store.
     3. **Fast Direct Socket ACK**: Under `run_sweep.sh` default (`--kafka-completion-mode async`), Server RPC Thread #0 immediately writes a submission ACK frame back over `Socket FD = 7` to Gateway Worker Thread #0, unblocking it to send query `idx = 96` (`s 00000096 UPDATE ...`).
     4. **Asynchronous Replication**: NuRaft replicates log entries asynchronously via `AppendEntries` RPC over TCP port 9000 to Followers: Node 2 (`user4`) and Node 3 (`utkarsh`).

3. **Deterministic DB Execution & Merkle Staging (PostgreSQL / BCDB)**:
   - On commit, `ariabc_pg_server` on all 3 nodes dispatches the ordered transactions to the BCDB Deterministic Scheduler (`--bcdb-workers 8`).
   - PostgreSQL backend (`port 5438`) executes DML on `public.usertable_small`.
   - **Staging (`merkledelta.c`)**: Table hooks intercept inserted/updated/deleted tuples, hash the tuple keys (`TupleKeyHash`), and stage deltas into a per-backend in-memory hash table (`HTAB`).
   - **Pre-Commit (`merkleapply.c`)**: `merkle_apply_staged_synchronous_safe()` executes at transaction `PRE_COMMIT`. It looks up affected prefix radix tree nodes in `ariabc_internal.merkle_node`, updates leaf aggregate XOR hashes, splits leaves if tuple count exceeds 32 (or merges if below 8), and bubbles modified XOR hashes up to the root node (`node_id: \x0000000000000000`).

4. **Async Kafka Result Streaming & Background Validation**:
   - `ariabc_pg_server` processes on all 3 nodes use `librdkafka` to publish execution result records (`req_id`, `node_id`, `status`, `merkle_root`) to the Kafka Broker on Node 1 (`10.129.148.247:9092`, Topic `ariabc_results`).
   - **Asynchronous Background Tracking**: The Gateway's Kafka Result Consumer thread polls topic `ariabc_results` in the background, matching `req_id` in `vote_store` for hash integrity metrics without blocking the Gateway worker threads.

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


