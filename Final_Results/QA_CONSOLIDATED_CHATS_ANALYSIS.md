# AriaBC: Consolidated Q&A Analysis & Technical Findings

> **Sessions Analyzed**:
> - **Chat 1**: `04c3e522-93f8-4a37-9534-80364b694fbd` (Merkle partition hashing, PostgreSQL physical storage layout, varlena headers, prefix-trie ordering, and key polymorphism)
> - **Chat 2**: `4ca35e82-f637-406f-8532-3d8f81996b62` (Engineering campaign timeline Sep 23–28, committed features, online recovery ProtectDB Alg. 2, uncommitted Changes A & B, TPC-C audit, det conflict tags, warehouse routing, and benchmarking methodology)
>
> **Repository**: [AriaBC (PostgreSQL-based Deterministic Concurrency Control & Merkle Tree Verification)](file:///work/ARIABC/AriaBC)
> **Date**: September 28, 2026

---

## Table of Contents

1. [Executive Summary & Synthesis of Both Sessions](#1-executive-summary--synthesis-of-both-sessions)
2. [Part 1: Low-Level Storage, Hashing & Merkle Function Semantics (Chat 04c3e522)](#2-part-1-low-level-storage-hashing--merkle-function-semantics-chat-04c3e522)
   - [Q1: Function Signature & Invocation Error](#q1-what-is-the-purpose-and-signature-of-merkle_partition_for_hash-and-why-did-select-merkle_partition_for_hasha-10-from-test28-fail)
   - [Q2: Historical Evolution of Function Signature](#q2-what-was-the-historical-evolution-of-merkle_partition_for_hashs-signature-and-why-was-the-return-type-changed-from-int4-to-int2)
   - [Q3: Varlena Header Overhead: In-Memory vs. On-Disk](#q3-does-storing-hashes-as-bytea-incur-4-bytes-of-extra-header-overhead-on-disk-compared-to-integers)
   - [Q4: Physical Tuple Alignment & Padding: bytea vs. int8 vs. int4](#q4-what-is-the-exact-physical-byte-alignment-tuple-layout-and-padding-for-bytea-vs-int8-vs-int4-in-the-index)
   - [Q5: Correctness Breakdown: Signed Ordering & Prefix Space](#q5-why-does-switching-to-an-integer-type-break-the-correctness-sorting-order-and-range-scans-of-the-merkle-tree)
   - [Q6: Why Raw Integer Keys Destroy the Merkle Trie](#q6-why-cant-we-eliminate-hashing-altogether-and-build-the-merkle-tree-directly-over-raw-integer-keys)
   - [Q7: Key Polymorphism & Composite Key Support](#q7-how-does-merkle_key_hashanyelement-provide-uniform-polymorphic-support-across-arbitrary-data-types)
   - [Q8: CPU Cost Analysis: BLAKE3 Hashing vs. Index Lookups](#q8-what-is-the-actual-cpu-cost-of-blake3-hashing-on-inserts-vs-index-lookups-do-lookups-recompute-the-hash)
   - [Q9: Developer Ergonomics & Recommended Action](#q9-what-is-the-recommended-action-and-ergonomic-solution-for-sql-callers)
3. [Part 2: Repository Features, Online Recovery & Engineering Timeline (Chat 4ca35e82)](#3-part-2-repository-features-online-recovery--engineering-timeline-chat-4ca35e82)
   - [Q10: Engineering Campaign Timeline (Sep 23–28, 2026)](#q10-what-is-the-comprehensive-engineering-timeline-across-the-campaigns-from-september-23-to-september-28-2026)
   - [Q11: Committed Features & Critical Bug Fixes](#q11-what-core-features-and-major-bug-fixes-were-committed-leading-up-to-this-point)
   - [Q12: Distributed Online Replica Recovery (ProtectDB Algorithm 2)](#q12-how-does-ariabcs-distributed-online-replica-recovery-protectdb-algorithm-2-operate-end-to-end)
   - [Q13: Merkle Composite-Key Split Bug (b46cfd7)](#q13-what-was-the-root-cause-and-fix-for-the-merkle-composite-key-split-bug-commit-b46cfd7)
   - [Q14: Merkle Stale-Route Race Under Lever D Overlap (89ee671)](#q14-what-was-the-root-cause-and-fix-for-the-merkle-stale-route-race-commit-89ee671)
4. [Part 3: Current Uncommitted Codebase Changes (Changes A & B)](#4-part-3-current-uncommitted-codebase-changes-changes-a--b)
   - [Q15: Det Conflict Tracking Column-1 Bottleneck](#q15-what-was-the-det-mode-conflict-tag-column-1-bottleneck-identified-during-the-tpc-c-audit)
   - [Q16: Change A Architecture: Key Resolution & Tag Namespacing](#q16-how-does-change-a-in-shm_transactionc-redesign-conflict-detection-and-prevent-phantom-conflicts)
   - [Q17: Change A Performance Impact on TPC-C](#q17-what-was-the-verified-performance-impact-of-change-a-on-tpc-c-w20)
   - [Q18: Change B Architecture: Merkle Warehouse / Leading-Key Routing](#q18-why-did-fixed-merkle-partitioning-hash-200-fail-to-scale-with-warehouses-and-how-does-change-b-fix-it)
   - [Q19: Synchronous vs. Asynchronous Merkle Updates](#q19-why-did-the-architecture-reject-asynchronous-per-block-ancestor-folding-and-preserve-synchronous-merkle-tree-updates)
   - [Q20: Change B Implementation Details: Reloptions, Metapage & SQL Functions](#q20-what-are-the-new-index-options-metapage-changes-and-the-new-merkle_key_hash_routed-function)
   - [Q21: Integration with Autonomous Replica Repair](#q21-how-does-replica_repaircxx-integrate-with-leading-key-routed-merkle-indexes)
5. [Part 4: Benchmark Findings, Experimental Pitfalls & Methodology](#5-part-4-benchmark-findings-experimental-pitfalls--methodology)
   - [Q22: TPC-C Ranking Sweep Benchmark Results](#q22-what-are-the-definitive-benchmark-findings-from-the-tpc-c-ranking-sweep-on-ranking-10129757)
   - [Q23: The YCSB PostgreSQL Baseline SSI Backoff Artifact](#q23-why-is-the-claim-deterministic-execution-beats-postgresql-under-high-contention-an-invalid-artifact-in-ycsb)
   - [Q24: The TPC-C Worker Plateau & Head-of-Line Blocking](#q24-what-causes-the-worker-scaling-plateau-at-32-workers-in-det-and-merkle-modes)
   - [Q25: Experimental Hygiene & Methodology Rules](#q25-what-rigorous-benchmarking-methodology-rules-were-established-across-the-campaigns)
   - [Q26: Current Uncommitted State & Next Action Items](#q26-what-is-the-status-of-the-15-uncommitted-files-and-what-are-the-next-recommended-steps)

---

## 1. Executive Summary & Synthesis of Both Sessions

Across both Claude sessions (`04c3e522` and `4ca35e82`), a complete architectural picture of AriaBC emerges.

- **Session `04c3e522` (Micro-Architecture & Storage Foundations)** tackled low-level PostgreSQL storage engine mechanics: why Merkle tree partitioning functions must take `bytea` rather than integer types (`int4`/`int8`), demonstrating that PostgreSQL's on-disk `1B varlena` packing makes `bytea` smaller than an aligned `int8` (28 bytes vs. 36 bytes per index tuple), while preserving unsigned MSB-first bitwise prefix ordering required for cryptographic trie traversals, bound calculations, and polymorphic key support.
- **Session `4ca35e82` (Macro-Architecture, Distributed Systems & Concurrency Optimization)** examined repository-level features across the September 23–28 campaign:
  1. **Committed Systems**: BLAKE3 per-transaction signing, stable Merkle node catalog naming, autonomous C++ replica repair (`replica_repair.cxx`), distributed online recovery (ProtectDB Algorithm 2, achieving 20 leaf repairs in 56 ms with zero client stalls), composite-key split fixes, and stale-route race resolution under Lever D.
  2. **Uncommitted Optimizations (15 files, +864/−152)**:
     - **Change A**: Multi-column, opclass-aware deterministic conflict tracking in [shm_transaction.c](file:///work/ARIABC/AriaBC/src/backend/bcdb/shm_transaction.c), eliminating false warehouse-level contention and lifting TPC-C W=20 TPS from 804 to 1,485 (+84.7%).
     - **Change B**: Leading-key / warehouse Merkle partition routing in [merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c) and [replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx), eliminating root lock contention and scaling Merkle TPC-C W=100 from 625 to 1,495 TPS (2.4× speedup).
  3. **Methodology Audits**: Debunking the YCSB "det beats pg" artifact (caused by SSI retry exhaustion and exponential backoff), identifying head-of-line blocking at the ordered commit gate, and codifying strict experimental rules (cold resets + `pg_prewarm`, post-restore `CHECKPOINT`, and memory isolation on Node 2 `user4`).

---

## 2. Part 1: Low-Level Storage, Hashing & Merkle Function Semantics (Chat 04c3e522)

### Q1: What is the purpose and signature of `merkle_partition_for_hash`, and why did `SELECT merkle_partition_for_hash(a, 10) FROM test28;` fail?

**Answer:**
The SQL function `merkle_partition_for_hash` maps an 8-byte cryptographic route digest of a database row's index key to a specific partition number $p \in [0, \text{partitions}-1]$.

Its catalog registration in [pg_proc.dat](file:///work/ARIABC/AriaBC/src/include/catalog/pg_proc.dat#L1056-L1060) is:
```perl
{ oid => '9940', descr => 'compute partition id for an 8-byte Merkle route hash',
  proname => 'merkle_partition_for_hash', provolatile => 'i', proparallel => 's', proisstrict => 't',
  prorettype => 'int2', proargtypes => 'bytea int4',
  proargnames => '{key_hash,partitions}',
  prosrc => 'merkle_partition_for_hash' }
```

The user query:
```sql
safe28=# select merkle_partition_for_hash(a, 10) from test28;
ERROR:  function merkle_partition_for_hash(integer, integer) does not exist
```
failed because column `a` in `test28` is of type `integer` (`int4`), but the function's first argument requires `bytea` (specifically, the 8-byte cryptographic route hash generated by `merkle_key_hash`). PostgreSQL does not provide an implicit cast from `integer` to `bytea`.

The correct invocation is:
```sql
SELECT a, b, merkle_partition_for_hash(merkle_key_hash(a), 10) AS partition FROM test28;
```
This matches the exact expression used in production expression lookup indexes (`usertable_merkle_lookup_idx`), repair scripts (`replica_repair.cxx`), and partition manifests.

---

### Q2: What was the historical evolution of `merkle_partition_for_hash`'s signature, and why was the return type changed from `int4` to `int2`?

**Answer:**
- **First Argument (`key_hash`)**: Has **always been `bytea`** since commit `8adbe6d` (2026-08-07). It was never an integer type.
- **Return Type (`partition_id`)**: Was originally `int4` (4-byte integer) and was deliberately changed to `int2` (2-byte `smallint`).

**Rationale for Changing Return Type to `int2`**:
In the Merkle lookup index `(partition_id, key_hash, key)`, the partition ID is stored in every single index tuple. In PostgreSQL, AriaBC caps the maximum partition count to 32,767 (`MERKLE_MAX_PARTITIONS`), which fits comfortably in a signed 16-bit integer (`int2`). Changing the return type from `int4` to `int2` cut **2 bytes of storage from every index leaf entry across 100M rows**, saving hundreds of megabytes without loss of functionality.

---

### Q3: Does storing hashes as `bytea` incur 4 bytes of extra header overhead on disk compared to integers?

**Answer:**
**No.** This is a common misconception about PostgreSQL storage.

While uncompressed `varlena` types in memory use a 4-byte header (`struct varlena { char vl_len_[4]; ... }`), PostgreSQL's on-disk tuple serialization engine uses **short varlena headers** (`1B varlena` / `SET_VARSIZE_SHORT`) for data up to 126 bytes:
- A 1-byte header has its lowest bit set to 1 (`VARATT_IS_SHORT`), leaving 7 bits to encode lengths up to 127 bytes.
- Because an 8-byte hash is fixed at 8 bytes, its on-disk storage is:
  $$\text{Disk Footprint} = 1\text{ byte header} + 8\text{ bytes hash data} = 9\text{ bytes}$$
- It does **not** take 12 bytes on disk.

Furthermore, a short varlena header has **no alignment requirement** (`attalign = 'c'`), meaning it can start immediately at any byte offset without padding bytes preceding it.

---

### Q4: What is the exact physical byte alignment, tuple layout, and padding for `bytea` vs. `int8` vs. `int4` in the index?

**Answer:**
Consider the standard lookup index entry `(partition_id, key_hash, ycsb_key)` where `partition_id` is `int2` (2 bytes) and `ycsb_key` is `int4` (4 bytes), following an 8-byte index tuple header (`IndexTupleData`).

Here is the exact byte-by-byte alignment comparison:

| Column Field | Offset with `bytea` (Current) | Offset with `int8` (Hypothetical) | Offset with `int4` (32-bit Hash) |
| :--- | :--- | :--- | :--- |
| **Index Header** | Bytes 0–7 (8 bytes) | Bytes 0–7 (8 bytes) | Bytes 0–7 (8 bytes) |
| **`partition_id` (`int2`)** | Bytes 8–9 (2 bytes) | Bytes 8–9 (2 bytes) | Bytes 8–9 (2 bytes) |
| **Padding before hash** | **0 bytes** (short `bytea` has 1-byte alignment) | **6 bytes padding** (to reach 8-byte alignment) | **2 bytes padding** (to reach 4-byte alignment) |
| **Hash Column** | Bytes 10–18 (1B header + 8B data = 9B) | Bytes 16–23 (8 bytes raw `int8`) | Bytes 12–15 (4 bytes raw `int4`) |
| **Padding before key** | **1 byte padding** (to reach 4-byte alignment for `int4`) | **0 bytes** (already at byte offset 24, divisible by 4) | **0 bytes** (already at byte offset 16, divisible by 4) |
| **`ycsb_key` (`int4`)** | Bytes 20–23 (4 bytes) | Bytes 24–27 (4 bytes) | Bytes 16–19 (4 bytes) |
| **Raw Tuple Payload** | **24 bytes** | **28 bytes** | **20 bytes** |
| **MAXALIGN (8-byte round)** | **24 bytes** | **32 bytes** (28 rounded to next multiple of 8) | **24 bytes** (20 rounded to next multiple of 8) |
| **Line Pointer (`ItemIdData`)**| + 4 bytes | + 4 bytes | + 4 bytes |
| **Total Leaf Entry Size** | **28 bytes** | **36 bytes** (+28.6% larger!) | **28 bytes** (Zero space saved!) |

#### Key Takeaways:
1. **`int8` is 28.6% larger**: Because `int8` mandates 8-byte alignment, placing it after a 2-byte `int2` forces 6 wasted padding bytes. The resulting 28-byte tuple then rounds up to 32 bytes under `MAXALIGN`. On a 100M-row dataset, `int8` adds **~800 MB of index bloat and ~110,000 extra leaf pages**.
2. **`int4` saves nothing**: Even though a 32-bit integer takes 4 bytes instead of 9, alignment padding rounds the final tuple up to 24 bytes (identical to `bytea`), while destroying half the hash entropy (32 bits vs 64 bits).

---

### Q5: Why does switching to an integer type break the correctness, sorting order, and range scans of the Merkle tree?

**Answer:**
Switching to an integer hash would corrupt the Merkle tree and recovery system for three fundamental reasons:

1. **Byte Ordering vs. Signed Integer Sorting**:
   - `bytea` compares bytes **unsigned, most-significant-byte (MSB) first**. This bitwise lexicographical order corresponds 1-to-1 with a Merkle prefix trie.
   - In PostgreSQL, integer types (`int2`, `int4`, `int8`) are **two's-complement signed**. Any hash with its leading bit set (`0x80` to `0xFF`) is treated as negative.
   - Consequently, negative hashes sort *before* positive hashes (`0x00` to `0x7F`). Node boundaries calculated by `merkle_node_upper_bound()` and SQL queries like:
     ```sql
     WHERE key_hash BETWEEN b.lo AND b.hi
     ```
     in [replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx#L254) would return incorrect row ranges or miss rows completely unless complex sign-flipping transformations were applied everywhere.
2. **Prefix Address Space Collapse**:
   - Merkle node IDs in the catalog table `merkle_node` are 8-byte `bytea` strings with a `prefix_len` parameter ranging from 0 to 64 bits (defined in [merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c)).
   - A 32-bit integer (`int4`) cannot represent or address 64-bit prefixes, cutting the tree's addressable capacity from $2^{64}$ to $2^{32}$.
3. **Subsystem Schema Incompatibility**:
   - The catalog table `merkle_node`, the C-engine functions `merkle_node_upper_bound` and `merkle_find_spurious_key`, and the C++ recovery pipeline (`replica_repair.cxx`) all consume and pass 8-byte `bytea` values. Using an integer would necessitate type casts on every join and compare.

---

### Q6: Why can't we eliminate hashing altogether and build the Merkle tree directly over raw integer keys?

**Answer:**
A Merkle index in AriaBC is an in-database bitwise prefix trie. Each trie node covers a binary range $[0, 2^{\text{prefix\_len}}-1]$ and splits into child nodes when the number of rows exceeds `split_threshold` (typically 32 rows).

This trie structure relies entirely on keys being **uniformly distributed across the 64-bit space**:
1. **Trie Degeneration & Depth Cliffs**:
   - For sequential or dense integer keys (e.g., $1 \dots 100,000,000$), the binary representation is `0x0000000000000001` through `0x0000000005F5E100`.
   - The top 37 bits are identical (all zeros) for every single row in the database!
   - Without cryptographic hashing, the trie would construct a pathological, deep, single-child linked list of 37 empty intermediate nodes before reaching a single branching split. Every search, insert, and update would suffer severe traversal penalties, and the `merkle_node` catalog table would balloon with millions of redundant single-child rows.
2. **Extreme Lock Contention on Sequential Inserts**:
   - In workloads with auto-incrementing or monotonic keys, every concurrent transaction would route to the exact same rightmost leaf node in the trie.
   - Node-level locking during splits would serialize the entire ingestion pipeline.
3. **Partition Skew**:
   - Merkle partition routing relies on uniform hashing. Without hashing, composite keys or non-numeric keys could not be partitioned evenly.

---

### Q7: How does `merkle_key_hash(anyelement)` provide uniform polymorphic support across arbitrary data types?

**Answer:**
`merkle_key_hash(anyelement)` is a polymorphic PostgreSQL internal C function registered in `pg_proc.dat`:
- It accepts **any PostgreSQL data type** (`integer`, `bigint`, `text`, `varchar`, `uuid`, `timestamp`, or composite row structures `ROW(col1, col2, ...)`).
- Internally, it serializes the input values into a canonical binary representation and executes a BLAKE3 cryptographic hash, returning a standardized 8-byte `bytea` digest.
- This decoupling allows the entire Merkle index engine, recovery engine, catalog schema, and verification scripts to remain completely agnostic of the underlying table's data schema.

---

### Q8: What is the actual CPU cost of BLAKE3 hashing on inserts vs. index lookups? Do lookups recompute the hash?

**Answer:**
The CPU cost is negligible on inserts and **zero on lookups**:

1. **On Inserts (The Hash is Already Computed)**:
   - When a row is inserted, the Merkle Access Method (`merkleinsert.c:68`) already computes the route hash via `merkle_compute_route()`, hashing the key with BLAKE3 to traverse the trie.
   - BLAKE3 processes the ~12-byte payload (`ARIAROUT` magic byte header, null bitmap, 4-byte key) in **tens of nanoseconds** (~15–30 ns on modern x86_64). This is dwarfed by the microseconds spent on PostgreSQL WAL logging, buffer pinning, and page latching.
2. **On Lookups (Zero Recomputation / Index-Only Scans)**:
   - Queries look up keys using the expression index `usertable_merkle_lookup_idx`:
     ```sql
     CREATE INDEX usertable_merkle_lookup_idx ON usertable (
         merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200),
         merkle_key_hash(ycsb_key),
         ycsb_key
     );
     ```
   - When executing `WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200) = $p AND merkle_key_hash(ycsb_key) BETWEEN $1 AND $2`, PostgreSQL matches the expressions directly against the pre-stored index columns.
   - **PostgreSQL executes an `Index Only Scan` with `Heap Fetches: 0`**. The hash function is **never evaluated during queries**; stored values are read directly from index pages in **~1 ms**.

---

### Q9: What is the recommended action and ergonomic solution for SQL callers?

**Answer:**
1. **Core Recommendation**: **Change nothing in the low-level engine.** The current `(int2, 1B-varlena bytea)` design is mathematically and physically optimal (smallest leaf footprint of 28 bytes, uniform trie balance, and correct unsigned sort order).
2. **Developer Ergonomics**: If writing `merkle_partition_for_hash(merkle_key_hash(a), 10)` is considered cumbersome, the clean solution is to add a SQL convenience wrapper:
   ```sql
   CREATE OR REPLACE FUNCTION merkle_partition_for_key(key anyelement, partitions integer)
   RETURNS smallint LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
       SELECT merkle_partition_for_hash(merkle_key_hash(key), partitions);
   $$;
   ```
   This improves user ergonomics without altering storage layout, index performance, or correctness.

---

## 3. Part 2: Repository Features, Online Recovery & Engineering Timeline (Chat 4ca35e82)

### Q10: What is the comprehensive engineering timeline across the campaigns from September 23 to September 28, 2026?

**Answer:**
The engineering campaign progressed through four distinct phases:

```mermaid
timeline
    title AriaBC Engineering Campaign Timeline (Sep 23–28, 2026)
    Sep 23–24 : Final-results rerun campaign
              : Gateway 35s wait timeout fix
              : TPC-C prewarm methodology (pg_prewarm)
              : BCDB config drift alignment
    Sep 24–25 : Merkle composite-key split bug (b46cfd7)
              : Merkle stale-route race fix under Lever D (89ee671)
              : TPC-C host stale build sync on .7.57
              : Worker scaling plateau analysis
    Sep 25–27 : Autonomous C++ replica repair (bd40a23)
              : Distributed online recovery & fault injector (ec02e33)
              : Per-transaction BLAKE3 signing (4974153)
              : Stable Merkle node table naming (00fae31)
              : Latency CSV capture & Final Results (379121e)
    Sep 27–28 : TPC-C audit: det conflict tag column-1 bug
              : TPC-C audit: Merkle hash%200 root collision floor
              : Implementation of Change A (shm_transaction.c)
              : Implementation of Change B (merkleutil.c routing)
              : Definitive 84-run TPC-C ranking sweep
```

- **Sep 23–24 (Harness Hardening & TPC-C Setup)**: Fixed gateway socket receive timeout (`SO_RCVTIMEO`) that failed runs over 35s; converted TPC-C from cold disk I/O to cold reset + `pg_prewarm` (99.8% buffer hit rate); aligned BCDB config drift on ranking host `10.129.7.57`.
- **Sep 24–25 (Correctness & Concurrency Races)**: Discovered and fixed the composite-key split bug (`b46cfd7`); resolved the Merkle stale-route race caused by Lever D commit-gate early release (`89ee671`); identified worker scaling plateau as ordered-gate head-of-line blocking.
- **Sep 25–27 (Recovery & Integrity Systems)**: Implemented autonomous C++ replica repair (`replica_repair.cxx`) via precommit ring; implemented distributed online recovery framework (ProtectDB Algorithm 2); added BLAKE3 transaction signing; captured per-transaction latency.
- **Sep 27–28 (TPC-C Scalability Breakthrough)**: Audited why det-mode and Merkle-mode plateaued on TPC-C; developed Change A (opclass-aware multi-column conflict tags) and Change B (leading-key warehouse Merkle routing); conducted 84-run ranking sweep confirming 2.4× speedup.

---

### Q11: What core features and major bug fixes were committed leading up to this point?

**Answer:**

| Commit | Subsystem | Description & Operational Impact |
| :--- | :--- | :--- |
| [`4974153`](file:///work/ARIABC/AriaBC) | Transaction Signing | **Per-transaction BLAKE3 cryptographic signing**. Gateway flags `--tx-sign/--no-tx-sign`. Verified across YCSB A/B/C/D/F with 0 divergences and 100% Merkle verification pass. |
| [`00fae31`](file:///work/ARIABC/AriaBC) | Merkle Catalog | **Stable Merkle node table naming**. Tables named `merkle_node_<table>` rather than using index OIDs (which differ across PostgreSQL replica instances), allowing recovery engines to match tables deterministically. |
| [`ec02e33`](file:///work/ARIABC/AriaBC) | Fault Injection & Recovery | **Distributed Online Recovery Engine & Fault Injector**. Added `gateway_recovery_manager.hxx` supporting `--recovery-mode off/active/passive/both`. Demoted `merkleverify` lock to `AccessShareLock` so audits run concurrently with active workloads. |
| [`379121e`](file:///work/ARIABC/AriaBC) | Telemetry & Results | **Comprehensive Latency Telemetry & Final Results Artifacts**. Gateway flag `--txLatencyCsv` records per-transaction latencies for CDFs and queue analysis. |
| [`bd40a23`](file:///work/ARIABC/AriaBC) | Autonomous Repair | **Autonomous C++ Replica Repair Engine ([replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx))**. Uses a "precommit ring" snapshot cut, compares sparse Merkle leaves with healthy nodes, and repairs only mismatched ranges via PostgreSQL `COPY`. |
| [`b46cfd7`](file:///work/ARIABC/AriaBC) | Merkle Core | **Composite-Key Split Bug Fix**. Resolved bug where `do_split()` used `pg_get_indexdef(oid, 1, true)`, hashing only column 1 and silently skipping splits on multi-column indexes. |
| [`89ee671`](file:///work/ARIABC/AriaBC) | Merkle Concurrency | **Merkle Stale-Route Race Fix**. Fixed race under Lever D where concurrent transactions modified Merkle nodes using stale snapshots, causing `Merkle node update failed` crashes. |

---

### Q12: How does AriaBC's Distributed Online Replica Recovery (ProtectDB Algorithm 2) operate end-to-end?

**Answer:**
Distributed Online Replica Recovery allows a corrupted or diverged replica to repair its state **while client transactions continue executing uninterrupted on healthy replicas**.

```mermaid
sequenceDiagram
    autonumber
    participant C as Clients
    participant GW as Gateway / Coordinator
    participant N1 as Node 1 (Healthy Leader)
    participant N2 as Node 2 (Healthy Follower)
    participant N4 as Node 4 (Damaged Follower)

    C->>GW: Submit Transactions
    GW->>N1: Replicate & Execute
    GW->>N2: Replicate & Execute
    GW->>N4: Replicate & Execute
    Note over N4: Tuple corruption injected at t=5.0s
    N4-->>GW: Divergent result / Merkle mismatch
    Note over GW: QUARANTINE Node 4 (stop sending new tx)
    GW->>N1: Request MVCC Snapshot Cut at Raft Log Index L
    N1-->>GW: Snapshot Exported (Target Commit = L)
    GW->>N4: Initiate Sparse Merkle Repair from Node 1
    Note over N4: Sparse Merkle tree descent (200 partitions)<br/>Localized 20-185 differing leaves (5.9 ms)
    N1->>N4: Stream differing row ranges via COPY (54-115 ms)
    Note over N4: Rebase local sequences & state
    Note over N4: Catch up & Replay missed Raft log entries from L
    Note over GW: UNQUARANTINE Node 4 (Rejoin Quorum)
    GW->>C: Zero client stalls, 100% throughput sustained
```

#### Detailed Recovery Steps:
1. **Fault Detection**: Triggered either in **Active Mode** (the gateway detects a transaction return value divergence) or **Passive Mode** (background periodic Merkle root audit reveals a hash mismatch).
2. **Quarantine & Cut Alignment**: The gateway stops routing new requests to the damaged node. It selects a healthy reference node (via *Dynamic Prioritized Reference Selection*) and requests an MVCC snapshot at exact Raft commit boundary $L$.
3. **Sparse Merkle Tree Descent**: Rather than dumping gigabytes of table data, the repair engine compares Merkle partition roots across all 200 partitions (taking **5.9 ms**). It recursively descends only into mismatched subtrees to identify the precise differing leaf ranges.
4. **Targeted Range Streaming**: The damaged node deletes invalid rows in the differing ranges and streams the correct rows from the reference node via PostgreSQL `COPY ... TO STDOUT` into a temporary staging table.
5. **Rebase & Local Replay**: The damaged node updates its local Merkle nodes, rebases sequences, replays missed transactions from its local Raft log from index $L$ to the head, and rejoins the cluster quorum.

#### Verified Experimental Metrics ([Report.md](file:///work/ARIABC/AriaBC/Final_Results/ONLINE_RECOVERY/Report.md)):
- **Throughput Preservation**: Client throughput during follower repair stayed at **8,692.82 TPS** (only **1.78% overhead** vs. the 8,850.05 TPS fault-free baseline).
- **Zero Client Stalls**: Longest transaction completion gap was **91.89 ms**; zero 100ms buckets dropped to 0 TPS.
- **Repair Speed**: 20 corrupted leaves were repaired in **54 ms to 115 ms**. Zero full-table copies occurred.
- **Vote Drain Fix**: Quorum continuity ensures in-flight pre-corruption votes from the damaged replica still count toward quorum if they match healthy nodes, preventing cluster stalls.
- **Dynamic Prioritized Reference Selection**: When the leader was corrupted, querying node status avoided selecting the memory-constrained Node 2 (`user4`), cutting snapshot export latency from 14.56s to **2.78s** and boosting leader recovery throughput from 4,440 to **8,674 TPS (+95.3%)**.

---

### Q13: What was the root cause and fix for the Merkle Composite-Key Split Bug (commit `b46cfd7`)?

**Answer:**
- **Root Cause**: In [merkleapply.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleapply.c), `do_split()` redistributes tuples when an oversized leaf exceeds `split_threshold`. To query the leaf's rows from the heap, it constructed SQL using `get_index_key_expr_str()`. That function called `pg_get_indexdef(oid, 1, true)`, which retrieved **only the first column of the index**.
- **The Failure**: In TPC-C, 7 out of 9 indexes have composite keys (e.g., `oorder` is keyed on `(o_w_id, o_d_id, o_id)`). When `do_split()` queried the heap using only `o_w_id`, the split query returned 0 rows matching the specific leaf. The split was silently aborted with a `DEBUG1` log message. After 20,000 transactions, `oorder` had 14 oversized leaves (33+ rows) and `history` had 81 oversized leaves. Because `merkle_verify_index` checks the XOR sum of partition roots against heap row XORs, it was blind to skipped splits and misplaced rows.
- **The Fix**: Committed in `b46cfd7`. Modified `get_index_key_expr_str()` to use `string_agg` across all index columns, wrapping them in `merkle_key_hash(ROW(...))` to match the lookup index expressions.

---

### Q14: What was the root cause and fix for the Merkle Stale-Route Race under Lever D overlap (commit `89ee671`)?

**Answer:**
- **Root Cause**: Under optimization "Lever D" ([worker.c](file:///work/ARIABC/AriaBC/src/backend/bcdb/worker.c)), PostgreSQL releases the deterministic serial gate *before* applying Merkle index maintenance to maximize pipelining. As a result, Merkle maintenance of transaction $N$ and $N+1$ executes concurrently.
- **The Race Conditions**:
  1. `merkle_direct_update_node()` resolved tree routes using `GetActiveSnapshot()`. If transaction $N$ had just split a node, transaction $N+1$ could not see the newly created child nodes, throwing `Merkle node update failed for index N partition P prefix 10` and crashing the gateway.
  2. Transactions followed `ctid` chains without checking `is_leaf`, silently applying deltas to nodes that had just been split into internal nodes.
  3. `do_merge_check()` suffered deadlocks by attempting to delete sibling nodes while another transaction held them locked.
- **The Fix**: Committed in `89ee671`. Re-architected Merkle node maintenance into a two-phase protocol:
  - **Leaf-First Phase**: Performs scans using the latest snapshot, verifies leaf freshness, and restarts traversal if a node split is detected.
  - **Ancestor Phase**: Applies updates bottom-up.
  - **Skip-Locked Merging**: Uses `FOR UPDATE SKIP LOCKED` during sibling merges so concurrent transactions never deadlock on node deletion.

---

## 4. Part 3: Current Uncommitted Codebase Changes (Changes A & B)

### Q15: What was the "det-mode conflict tag column 1" bottleneck identified during the TPC-C audit?

**Answer:**
Deterministic concurrency control (`bcdb_dt_conflict_tracking`) serializes transactions according to a pre-ordered schedule. If a transaction detects a conflict with an in-flight predecessor, it aborts and restarts.

In the original implementation:
- `bcdb_compute_key_tag()` in `nodeModifyTable.c`: called `slot_getattr(slot, 1)` (column 1 only).
- `PredicateLockTuple()` in `predicate.c`: called `heap_getattr(tuple, 1)` (column 1 only).
- `_bt_first()` in `nbtsearch.c`: read `startKeys[0]` only (column 1 only).

**The Disaster on TPC-C**:
In the TPC-C schema, **column 1 is `w_id` (Warehouse ID)** across almost all tables (`warehouse`, `district`, `customer`, `new_order`, `oorder`, `order_line`, `stock`).
- Because only column 1 was hashed, conflicts were tracked per **(table, warehouse)** rather than per **logical row**!
- Any two concurrent transactions accessing the same warehouse—even if modifying completely different customers, items, or districts—were flagged as conflicting!
- At W=100 with 32 workers, true row-level contention was ~10%, but det-mode behaved as if contention were **28%**, suffering 3,261 transaction restarts and capping throughput at ~800 TPS.

---

### Q16: How does Change A in `shm_transaction.c` redesign conflict detection and prevent phantom conflicts?

**Answer:**
Change A ([shm_transaction.c](file:///work/ARIABC/AriaBC/src/backend/bcdb/shm_transaction.c), +437 lines) implements a general, table-agnostic, multi-column conflict tracking engine:

1. **Intelligent Key Resolution Hierarchy**:
   To find the true logical key of a table, `bcdb_get_keytag_info()` inspects the table metadata in order:
   $$\text{Primary Key} \longrightarrow \text{Replica Identity Index} \longrightarrow \text{Narrowest Plain Unique Index} \longrightarrow \text{Column 1 Fallback}$$
   *Gotcha Resolved*: The standard TPC-C benchmark schema defines `pk_*` as plain `UNIQUE INDEX`es without formal `PRIMARY KEY` constraints. Checking only `rel->rd_pkindex` previously caused silent fallback to column 1. The new logic inspects `RelationGetIndexList()` to select the narrowest unique index.
2. **Opclass-Aware Hashing**:
   Rather than treating values as `int4`, it looks up each column's type in the system cache (`get_opcode()`, `typcache->hash_proc`) and invokes its native type-specific hash function (identical to hash joins).
3. **Checked Write Tags vs. Publish-Only Prefix Tags**:
   - **Full-Key Tag**: An INSERT/UPDATE/DELETE reserves its full-key tag in `ws_table_record`. This is actively checked for Write-After-Write (WAW) conflicts.
   - **Publish-Only Prefix Tags**: A writer also generates tags for every proper prefix (e.g., warehouse + district) and places them in a new structure: `ws_table_publish_record`. These tags are **published into shared memory but never checked against other writers**. This guarantees that two writers inserting rows with the same prefix do *not* falsely conflict with each other!
4. **Phantom Protection for Range/Index Scans**:
   - When a transaction performs an index scan with $m$ equality conditions (e.g., `WHERE d_w_id = 1 AND d_id = 5`), `bcdb_reserve_read_key_tag_scan()` reserves an $m$-column prefix tag.
   - If a concurrent transaction inserts a new row into that prefix, the scan detects the writer's published prefix tag, correctly detecting phantoms.
5. **Namespacing via Tag DB Field**:
   The number of key columns covered is stored in the tag's `db` field:
   ```c
   tag.db = BCDB_KEYTAG_DB_BASE | prefix_cols;
   ```
   *Gotcha Resolved*: In AriaBC, `SET_PREDICATELOCKTARGETTAG_TUPLE` zeroes `locktag_field1`. The tag's `db` field is assigned *after* the macro invocation, ensuring prefix tags and full-key tags never collide.

---

### Q17: What was the verified performance impact of Change A on TPC-C (W=20)?

**Answer:**
On ranking host `10.129.7.57`, testing 4,000 transactions at W=20 with 32 workers:

| Metric | Before Change A (Column-1 Tag) | After Change A (True Multi-Column Key Tag) | Relative Improvement |
| :--- | :--- | :--- | :--- |
| **Transaction Restarts** | 3,261 | **1,331** | **-59.2% restarts** |
| **Throughput (TPS)** | 804 TPS | **1,485 TPS** | **+84.7% throughput** |
| **Correctness** | Verified | Verified (Det state matched serial in 100% of runs) | Zero divergence |

---

### Q18: Why did fixed Merkle partitioning (`hash % 200`) fail to scale with warehouses, and how does Change B fix it?

**Answer:**
- **The Bottleneck**: Originally, all Merkle indexes routed rows using `hash(full_key) % 200`, creating a static set of 200 partitions regardless of the database size or warehouse count $W$.
  - In TPC-C, a `NewOrder` transaction modifies ~25 partition roots, and a `Delivery` transaction modifies ~105 partition roots.
  - With only 200 partitions, the mathematical probability of two concurrent transactions touching the same partition root was ~72% at W=100.
  - Adding more warehouses did nothing to reduce partition lock contention. Merkle mode hit a rigid throughput ceiling of ~600 TPS across all warehouse counts.
- **Change B: Leading-Key Warehouse Routing**:
  Instead of scattering a warehouse's rows randomly across all 200 partitions, Change B groups rows belonging to the same leading key column (`w_id`) into dedicated contiguous clusters of partitions.
  - For warehouse $w$, its data is confined to a small subset of subpartitions (e.g., 16 subpartitions).
  - Transactions operating on different warehouses now touch completely disjoint partition roots! Root lock collisions drop to zero for cross-warehouse transactions.

---

### Q19: Why did the architecture reject asynchronous/per-block ancestor folding and preserve synchronous Merkle tree updates?

**Answer:**
An alternative proposal was to make Merkle tree maintenance asynchronous, buffering row modifications and folding ancestor node hashes in background batches (per block).

**Why Asynchronous Folding Was Rejected**:
1. **Recovery Correctness (ProtectDB Invariant)**: In distributed online recovery, a healthy replica must export an exact snapshot at Raft commit boundary $L$. The recovery engine performs sparse Merkle descent comparing root hashes between replicas. If ancestor folding were asynchronous or delayed, the Merkle root would reflect an uncommitted or lagging state, invalidating cryptographic diff comparisons.
2. **Determinism Across Replicas**: Replicas execute at varying speeds. Asynchronous batching introduces timing nondeterminism, meaning two replicas with identical committed row states could have different transient Merkle roots.
3. **Simplicity**: Synchronous updates ensure that the instant a transaction commits, its Merkle root is 100% authoritative and cryptographically verifiable.

---

### Q20: What are the new index options, metapage changes, and the new `merkle_key_hash_routed` function?

**Answer:**
Change B introduces two new index reloptions in [merkle.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkle.c#L182-L192):
- `partition_key_columns=n`: The number of leading index key columns that determine the partition group ($n=1$ for warehouse routing).
- `subpartitions=m`: The number of contiguous partitions assigned to each leading-key group ($m=16$).

#### Metapage & Engine Updates:
1. **Metapage Persistence**: Stored in `MerkleMetaPageData` on disk and cached in `MerkleMetaCacheEntry` ([merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L60)). Legacy indexes read 0, preserving full backward compatibility.
2. **Partition Limit Adjustment**: Capped at `MERKLE_MAX_PARTITIONS = 32767` (fitting signed `int2`).
3. **Routing Function ([merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L495))**:
   `merkle_route_digest_apply_leading()` digests the leading $n$ columns to pick a partition group, using the full-key digest to select the subpartition within the group. It modifies only the first 8 bytes of the route hash, ensuring `route_value % num_partitions` remains consistent across builds, splits, lookups, and repairs.
4. **New Catalog Function**:
   Registered in [pg_proc.dat](file:///work/ARIABC/AriaBC/src/include/catalog/pg_proc.dat#L1061):
   ```perl
   { oid => '9942', proname => 'merkle_key_hash_routed', provolatile => 'i', proparallel => 's', proisstrict => 't',
     prorettype => 'bytea', proargtypes => 'any any int4 int4',
     proargnames => '{full_key,leading_key,partitions,subpartitions}',
     prosrc => 'merkle_key_hash_routed_sql' }
   ```

---

### Q21: How does `replica_repair.cxx` integrate with leading-key routed Merkle indexes?

**Answer:**
In [replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx#L156-L256), table discovery was enhanced to parse index options:
```cpp
if (opt.rfind("partition_key_columns=", 0) == 0) {
    t.partition_key_columns = std::max(0, std::atoi(opt.c_str() + 22));
} else if (opt.rfind("subpartitions=", 0) == 0) {
    t.subpartitions = std::max(1, std::atoi(opt.c_str() + 14));
}
```

When building SQL predicates for sparse leaf repairs:
```cpp
std::string route_hash_expr(const table_info& t, const std::string& key_expr) {
    if (t.partition_key_columns > 0)
        return "merkle_key_hash_routed(ROW(" + key_expr + "), ROW(" + key_expr + "), " +
               std::to_string(t.partitions) + ", " + std::to_string(t.subpartitions) + ")";
    return "merkle_key_hash(" + key_expr + ")";
}
```
This guarantees that autonomous C++ replica repair streams the exact matching row ranges, whether the table uses legacy hash partitioning or leading-key warehouse routing.

---

## 5. Part 4: Benchmark Findings, Experimental Pitfalls & Methodology

### Q22: What are the definitive benchmark findings from the TPC-C Ranking Sweep on ranking (`10.129.7.57`)?

**Answer:**
The definitive sweep ([Final_Results/TPCC/](file:///work/ARIABC/AriaBC/Final_Results/TPCC/warehouses_w32/summary_median.csv)) evaluated 84 total runs across 3 trials each from W=5 to W=100 with 32 worker threads.

#### Median Throughput (TPS) Across Configurations:

| Warehouses (W) | PostgreSQL Baseline | BCDB Det Mode (Change A) | Merkle Routed (Change B) | Merkle Current (hash % 200) | Routed Speedup vs. Current |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **W = 5** | 1,791.7 | 910.4 | **583.1** | 511.2 | 1.14× |
| **W = 10** | 2,750.3 | 1,387.6 | **738.4** | 594.1 | 1.24× |
| **W = 20** | 2,550.4 | 1,845.2 | **960.5** | 532.0 | 1.80× |
| **W = 30** | 3,883.4 | 2,112.5 | **1,063.2** | 631.4 | 1.68× |
| **W = 50** | 4,304.0 | 2,450.1 | **1,195.4** | 277.3 | **4.31×** |
| **W = 75** | 4,223.2 | 2,580.4 | **1,522.0** | 524.1 | **2.90×** |
| **W = 100** | **4,304.3** | **2,601.2** | **1,495.0** | **625.0** | **2.39×** |

```
           TPC-C Scalability Curve (W=5 to W=100, 32 Workers)
  TPS
  5000 |                                    --- PG Baseline (Saturates at ~4.3k)
  4000 |                          ---
  3000 |                 ---                ... BCDB Det (Change A: scales to 2.6k)
  2000 |        ...                         === Merkle Routed (Change B: scales to 1.5k)
  1000 |  ===          ===
     0 |  ___          ___                  ___ Merkle Current (Flat at ~600 TPS)
       +--------+--------+--------+--------+
       W=5     W=20     W=50     W=75    W=100
```

#### Correctness & Tree Depth Observations:
- **100% Correctness**: All 84 runs passed `merkle_verify` (9/9 tables `true`), 0 divergences, and deterministic state matched serial execution in 42 out of 42 comparison checks.
- **Tree Depth Stability**:
  - Legacy layout suffered depth cliffs: `stock` tree depth jumped from 3.0 to 4.0 at W=50, explaining the severe 277 TPS dip.
  - Routed layout kept tree depth flat (`stock` 3.0, `order_line` 3.5–3.7) across all warehouse counts.

---

### Q23: Why is the claim "Deterministic execution beats PostgreSQL under high contention" an invalid artifact in YCSB?

**Answer:**
In the YCSB Workload A skew sweeps, historical reports claimed that AriaBC det-mode outperformed vanilla PostgreSQL at extreme skews ($\theta = 0.99$ and $\theta = 1.2$).

**The Audit Finding**:
- The PostgreSQL harness executed under strict `SERIALIZABLE` isolation with **exponential backoff retry logic** (capping at 40,000 retries).
- At $\theta \ge 0.99$, vanilla PostgreSQL suffered a catastrophic retry storm (over 21,000 serialization failure aborts `SQLSTATE 40001` per run). Client threads spent >95% of their time sleeping in exponential backoff delay loops.
- AriaBC's deterministic execution does not use SSI backoff; it re-sequences transactions via its shared-memory commit ring.
- **Conclusion**: The collapse of PostgreSQL was an artifact of client-side backoff configuration, not database engine capacity. Comparing backoff-stalled PostgreSQL against deterministic execution is invalid and must not be claimed as an inherent architectural victory.

---

### Q24: What causes the worker scaling plateau at 32 workers in Det and Merkle modes?

**Answer:**
When scaling worker threads from 1 to 32 on TPC-C, throughput in det and Merkle modes plateaus earlier than PostgreSQL:
- **PostgreSQL**: Bound by WAL flush latency and group commit batching (`walwriter`).
- **AriaBC Det & Merkle Modes**: Bound by **ordered commit gate head-of-line (HOL) blocking** (`BlockCommit` in [worker.c](file:///work/ARIABC/AriaBC/src/backend/bcdb/worker.c)).
  - Transactions must commit strictly in the pre-assigned deterministic sequence order $T_1, T_2, T_3 \dots$
  - If transaction $T_5$ (e.g., an expensive Delivery transaction) takes 15 ms to execute while transactions $T_6 \dots T_{32}$ finish in 1 ms, all 26 worker threads are blocked at the commit gate waiting for $T_5$ to commit.
  - Profile telemetry confirmed that gate wait time accounts for >80% of execution time at 32 workers. This is an intrinsic property of deterministic commit sequencing, not a configuration bug.

---

### Q25: What rigorous benchmarking methodology rules were established across the campaigns?

**Answer:**

1. **Cold Reset + `pg_prewarm`**:
   Un-prewarmed runs are bottlenecked by disk I/O, obscuring CPU and concurrency differences. The benchmark harness enforces `--tpcc-prewarm`, achieving a verified 99.8% buffer cache hit rate before measurement starts.
2. **Mandatory Post-Restore `CHECKPOINT`**:
   Restoring a database leaves dirty WAL buffers. If a checkpoint fires during the measurement window, TPS drops by ~30% (e.g., at W=100, det measured 1,060 TPS without checkpoint and 1,528 TPS with checkpoint). A synchronous `CHECKPOINT` is now executed immediately following restoration.
3. **Execution Exclusively on Remote Lab Hosts**:
   All builds and benchmarks must run on `10.129.148.247` (YCSB/Cluster) or `10.129.7.57` (TPC-C), never locally.
4. **Memory Guardrails for Test Instances**:
   Node 2 (`user4`, `10.129.148.246`) has severe memory constraints (0 GB free, heavy swap). A previous unconstrained test database on `.247` triggered an OOM killer that terminated the canonical PostgreSQL instance. Test instances must be assigned restricted `shared_buffers` (e.g., 32 MB to 1 GB) and run on isolated ports.
5. **Multi-Trial Averaging on Shared Machines**:
   Ranking host `10.129.7.57` experiences load swings (load average 13–22 from other researchers). Single-trial benchmarks can vary by up to 2×. All published findings must report medians across $\ge 3$ (preferably 5) trials.
6. **NUMA Interleaving**:
   Worker thread scaling on dual-socket machines shows bi-modal throughput swings (e.g., 2,562 TPS vs. 769 TPS at 32 workers). Harnesses should be invoked under `numactl --interleave=all`.

---

### Q26: What is the status of the 15 uncommitted files and what are the next recommended steps?

**Answer:**
The working tree currently contains 15 modified files (+864/−152 lines):

```
ariabc_pg/src/replica_repair.cxx                   |  27 +-
scripts/distributed/run_all_modes_gateway_sweep.py |  16 +-
src/backend/access/merkle/merkle.c                 |  43 +-
src/backend/access/merkle/merkleapply.c            |  56 ++-
src/backend/access/merkle/merklebuild.c            |  92 ++++-
src/backend/access/merkle/merkleutil.c             | 218 +++++++++-
src/backend/access/merkle/merkleverify.c           |   4 +
src/backend/access/nbtree/nbtsearch.c              |  14 +-
src/backend/bcdb/shm_transaction.c                 | 437 ++++++++++++++++++++-
src/backend/bcdb/worker.c                          |  12 +-
src/backend/executor/nodeModifyTable.c             |  46 +--
src/backend/storage/lmgr/predicate.c               |  18 +-
src/include/access/merkle.h                        |  19 +
src/include/bcdb/shm_transaction.h                 |   9 +-
src/include/catalog/pg_proc.dat                    |   5 +
```

#### Action Items:
1. **Commit Change A (Det Conflict Tag Engine)**:
   Commit [shm_transaction.c](file:///work/ARIABC/AriaBC/src/backend/bcdb/shm_transaction.c), [shm_transaction.h](file:///work/ARIABC/AriaBC/src/include/bcdb/shm_transaction.h), [nodeModifyTable.c](file:///work/ARIABC/AriaBC/src/backend/executor/nodeModifyTable.c), [predicate.c](file:///work/ARIABC/AriaBC/src/backend/storage/lmgr/predicate.c), and [nbtsearch.c](file:///work/ARIABC/AriaBC/src/backend/access/nbtree/nbtsearch.c) with description: `bcdb: opclass-aware multi-column conflict tracking for deterministic execution`.
2. **Commit Change B (Merkle Leading-Key Routing & Repair)**:
   Commit [merkle.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkle.c), [merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c), [merklebuild.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merklebuild.c), [merkleapply.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleapply.c), [pg_proc.dat](file:///work/ARIABC/AriaBC/src/include/catalog/pg_proc.dat), and [replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx) with description: `merkle: leading-key warehouse partition routing and replica repair integration`.
3. **Commit Benchmark Results & Scripts**:
   Stage `Final_Results/TPCC/` and `Final_Results/ONLINE_RECOVERY/`.
4. **Update Graphify Knowledge Graph**:
   Run `graphify update .` locally after committing to keep code symbols and AST relations synchronized.
