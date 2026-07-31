# ARIABC Merkle Tree Access Method & Recovery Engine

This document provides a comprehensive technical reference for the Merkle tree engine integrated directly into PostgreSQL/AriaBC (`src/backend/access/merkle/`). It covers the native dynamic Merkle access method, synchronous Copy-on-Write (COW) page layouts (v8 format), BLAKE3 cryptographic hashing, SQL query interfaces, distributed recovery verification, and the live dynamic Merkle visualizer.

---

## 🌟 Architectural Overview

The Merkle tree engine provides cryptographic verification of database integrity and high-speed state synchronization across distributed replicas through a hierarchical hash tree structure.

```text
                        Global Root Hash (BLAKE3 256-bit)
                       /               |               \
                Partition 0       Partition 1       Partition 2 ...
                /        \         /        \         /        \
            Node 0.1   Node 0.2  Node 1.1  Node 1.2  Node 2.1   Node 2.2
             /   \      /   \     /   \     /   \     /   \      /   \
           L0     L1   L2   L3   L4    L5  L6    L7  L8    L9   L10  L11
           [Tuples: Bounded Leaf Buckets (Dynamic Splitting/Merging)]
```

### Key Technical Innovations

1. **Native Access Method (`USING merkle`)**: Fully integrated into PostgreSQL's Table and Index Access Method interfaces. Supports both single-key and multi-key index targets.
2. **Dynamic Bounded Leaf Geometry**: Automatically splits leaves when tuple occupancy exceeds `split_threshold` and merges leaves when tuple count drops below `merge_threshold`, maintaining optimal tree depth and row density.
3. **Synchronous Copy-on-Write (COW)**: Page updates retain immutable historic node records and write new node versions atomically without blocking concurrent reader queries.
4. **BLAKE3 Cryptographic Hashing**: Uses 256-bit BLAKE3 hashing, enabling SIMD-accelerated, highly parallelized hash computation.
5. **Dynamic Depth Scaling**: Trees dynamically scale in depth (e.g. from Level 4 at 1M rows to Level 7 at 50M rows) with strictly bounded $O(\log_F N)$ descent latency.
6. **Optimized Recovery Pipeline**: Uses array-based batching and frontier pruning to accelerate tree localization and candidate row repair across corrupted replicas.

---

## 📁 Source Code Organization

The core engine lives inside PostgreSQL kernel sources:

```text
src/backend/access/merkle/
├── merkle.c        # Main access method interface routines and AM handlers
├── merklebuild.c   # Bulk index build, tree initialisation, and tuple insertion
├── merkleinsert.c  # Transactional single-row insertion and tuple routing
├── merkleutil.c    # Cryptographic hashing (BLAKE3), memory management, tree navigation
├── merkleverify.c  # In-kernel verification procedures, proof generation, catalog audits
├── merkleapply.c   # Dynamic leaf split/merge execution and page-level COW mutators
├── merkledelta.c   # In-memory transaction delta staging and PRE_COMMIT apply routines
└── Makefile        # Module build rules
```

### Associated Headers & Catalogs

- `src/include/access/merkle.h`: Main header declaring C data structures (`MerkleBuildState`, `MerkleNodeRecord`, `MerkleLeafBucket`), version macros (layout v8), and exported functions.
- `src/common/blake3.c` & `src/include/common/blake3.h`: BLAKE3 SIMD/C hash implementation.
- Catalog files (`src/include/catalog/pg_am.dat`, `pg_proc.dat`, `pg_opclass.dat`): System catalog registrations for the `merkle` access method.

---

## 🛠️ DDL & Index Creation Options

### Creating Dynamic Merkle Indexes

Dynamic Merkle indexes adapt their tree geometry based on workload volume.

```sql
-- Create a Dynamic Merkle index with standard F=32 fanout and 1024-tuple leaf threshold
CREATE INDEX usertable_dynamic_merkle_idx ON usertable
USING merkle (ycsb_key)
WITH (
    fanout = 32,
    split_threshold = 1024,
    merge_threshold = 256,
    dynamic = true
);

-- Create a Multi-Key Dynamic Merkle Index
CREATE INDEX usertable_multikey_merkle ON usertable
USING merkle (ycsb_key, field1)
WITH (
    fanout = 32,
    split_threshold = 512,
    merge_threshold = 128,
    dynamic = true
);
```

### Index Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fanout` | Integer | `32` | Logical child capacity per internal tree node ($F$). |
| `split_threshold` | Integer | `1024` | Maximum tuple count per leaf before a dynamic split occurs. |
| `merge_threshold` | Integer | `256` | Minimum tuple count per leaf before a dynamic merge occurs. |
| `dynamic` | Boolean | `true` | Enables dynamic leaf splitting/merging and COW node updates. |
| `partitions` | Integer | `200` | Static partition count (for fixed static Merkle index configurations). |

---

## 🔍 SQL Diagnostics & Verification Functions

PostgreSQL provides built-in SQL functions to inspect Merkle tree structures, verify data integrity, and fetch proofs:

```sql
-- 1. Full Tree Integrity Verification
-- Computes and re-verifies all hashes across the tree. Returns TRUE if valid.
SELECT merkle_verify('usertable');

-- 2. Fetch Root Hash
-- Returns the 256-bit BLAKE3 global root hash string (in hex).
SELECT merkle_root_hash('usertable');

-- 3. Detailed Tree Statistics
-- Displays total nodes, leaf count, dynamic depth, and occupancy metrics.
SELECT merkle_tree_stats('usertable');

-- 4. View Node Hashes
-- Lists nodeid, partition, node_in_partition, is_leaf, leaf_id, and BLAKE3 hash.
SELECT * FROM merkle_node_hash('usertable') LIMIT 10;

-- 5. View Tuple-to-Leaf Bucketing
-- Maps tuples to their target leaf buckets.
SELECT * FROM merkle_leaf_tuples('usertable') LIMIT 10;

-- 6. Locate Leaf ID for Key(s)
-- Returns target leaf_id, partition, and node_in_partition for a key value.
SELECT * FROM merkle_leaf_id('usertable', 1199);
```

---

## ⚡ Synchronous Copy-on-Write (COW) & Dynamic Geometry

### Dynamic Leaf Splitting & Merging

When transaction updates or bulk loads insert tuples into a leaf node:
1. The target leaf bucket receives the new key hash.
2. If total tuples in the leaf exceed `split_threshold`, `merkle_do_split()` executes:
   - Allocates two new sibling leaf bucket records.
   - Redistributes existing tuples across sibling leaves according to key space ranges.
   - Propagates new leaf node hashes up the tree hierarchy.
3. If tuple deletions drop leaf occupancy below `merge_threshold`, sibling leaves are merged into a unified node.

### Layout v8 Physical Records (`pageinspect`)

Physical pages written by `merkleapply.c` use layout **v8** binary record layout:
- **Header Magic & Version**: Guarantees layout integrity (`v8`).
- **CRC32C Checksum**: Validates record byte integrity on disk/buffer read.
- **Physical Locators**: Decoded using `(block, offset, page_generation)` locators.
- **Root Journaling**: Retains previous-version links for historical snapshot reads and MVCC consistency.

---

## 🔄 State Recovery Engine (`scripts/benchmark/recovery/`)

The Merkle recovery engine synchronizes damaged replicas with healthy reference databases in five distinct phases:

```text
[Phase 1: Localisation] ➔ [Phase 2: Candidate Fetch] ➔ [Phase 3: Row Comparison] ➔ [Phase 4: Repair DML] ➔ [Phase 5: Verification Audit]
```

1. **Phase 1: Localisation**: Performs top-down Merkle tree comparison between healthy and damaged replicas. Divergent parent nodes are expanded down to dynamic leaf buckets using frontier pruning.
2. **Phase 2: Candidate Fetch**: Selects all row keys falling inside the localized corrupt leaf ranges from both healthy and damaged tables.
3. **Phase 3: Row/Tuple Comparison**: Executes in-memory key alignment identifying missing, extra, or modified tuple columns.
4. **Phase 4: Repair DML Execution**: Issues targeted `INSERT`, `UPDATE`, or `DELETE` SQL DML statements to correct corrupt tuples.
5. **Phase 5: Post-Repair Audit**: Re-evaluates `merkle_verify()` and asserts `divergence_count = 0`.

### Synthetic Corruption Modes

The benchmark suite (`run_merkle_recovery_benchmark.py`) supports 5 synthetic corruption injection modes:

| Mode | Injection Description | Use Case |
|---|---|---|
| `paper-update-only` | Mutates existing row columns (`field9`) | Standard paper recovery benchmark profile |
| `update-only` | Modifies tuple values across bad leaves | Value corruption validation |
| `delete-only` | Drops targeted rows from damaged replica | Missing-data recovery validation |
| `insert-only` | Injects spurious rows into damaged replica | Extra-data purging validation |
| `mixed` | Equal split of updates, deletes, and inserts | Real-world complex failure recovery |

For complete benchmark instructions, detailed timing contracts, and plotting scripts, see [`scripts/benchmark/recovery/README.md`](scripts/benchmark/recovery/README.md).

---

## 🖥️ Live Dynamic Merkle Inspector (`dynamic_merkle_visualizer/`)

The repository includes a web-based inspector (`dynamic_merkle_visualizer/`) for interactive analysis of live PostgreSQL dynamic Merkle indexes.

```bash
# Launch inspector web app
MERKLE_VIZ_CONNINFO='host=127.0.0.1 port=5432 dbname=postgres user=postgres' \
  ./.venv/bin/python3 dynamic_merkle_visualizer/app.py
```

### Features

- **Live Buffer Inspection**: Reads direct PostgreSQL shared buffers using `pageinspect.get_raw_page()`.
- **Reachable Physical Tree View**: Displays active dynamic MVCC root nodes, internal slots, leaf buckets, item heads, and BLAKE3 hashes.
- **Record Journaling**: Inspects retained immutable physical records, identifying retired vs reachable node versions and COW link lineages.
- **Direct Mutator**: Provides controls to insert, update, or delete live tuples and observe real-time dynamic tree splitting, merging, and hash propagation.

---

## 📊 Performance & Scalability Summary

Empirical benchmarking across 1M to 50M tuple scaling runs (documented in [`Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md`](Dynamic_merkle_docs/RECOVERY_ARCHITECTURE_ANALYSIS.md)) demonstrates:

- **Bounded Recovery Time**: Total recovery latency scales predictably with tree depth $O(\log_F N)$.
- **Sub-Second Repair**: Localized 300-tuple corruption across 50,000,000 rows is repaired in under 1 second.
- **BLAKE3 Efficiency**: In-kernel BLAKE3 hashing introduces < 5% overhead during high-volume transactional ingestion.

---

## 🔐 Security & Immutability

- **Cryptographic Tamper-Evidence**: Any unauthorized modification to row values or index pointers invalidates the BLAKE3 root hash.
- **Proof Generation**: Independent Merkle inclusion proofs can be generated and verified without full table scans.
- **Transactional Consistency**: Merkle deltas stage in memory and commit atomically during `PRE_COMMIT` execution in `merkledelta.c`.
