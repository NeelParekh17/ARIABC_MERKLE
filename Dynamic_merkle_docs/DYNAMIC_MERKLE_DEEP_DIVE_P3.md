# Dynamic Merkle Tree — Deep Dive (Part 3 of 3)
# Merge, Ancestor Propagation, Crash-Safe Delta Pipeline, and Verification

> **Files covered in this part**
> - `src/backend/access/merkle/merkleapply.c` (lines 632–710, 1089–1228, 2369–2425, 2427–2650)
> - `src/backend/access/merkle/merkledelta.c`
> - `src/backend/access/merkle/merkleverify.c`

---

## 1. Ancestor Hash Propagation

Every time a leaf is updated (INSERT/DELETE/UPDATE), the XOR delta must be propagated
upward through all ancestor nodes so every internal node's `hash` stays as the XOR of
all descendant leaf hashes.

### 1.1 Two propagation functions

There are two propagation functions for different call sites:

| Function | Used by | Style |
|---|---|---|
| `propagate_hash_to_ancestors()` | Raft ordered applier path | Classic SELECT + UPDATE per ancestor |
| `propagate_hash_to_ancestors_atomic()` | Synchronous direct path | Prepared plan, single UPDATE per ancestor |

### 1.2 `propagate_hash_to_ancestors()` — Raft applier path

```c
// merkleapply.c  lines 632–710
static void
propagate_hash_to_ancestors(Oid index_oid, const uint8 *leaf_node_id,
                             int leaf_prefix_len,
                             const MerkleHash *tuple_hash_delta,
                             int64 count_delta)
{
    uint8 curr_node_id[8];
    int curr_prefix_len = leaf_prefix_len;
    memcpy(curr_node_id, leaf_node_id, 8);

    while (curr_prefix_len > 0)
    {
        uint8 parent_node_id[8];
        int parent_prefix_len = merkle_parent_of(parent_node_id, curr_node_id,
                                                  curr_prefix_len, bits_per_split);

        // 1. SELECT current parent hash + count
        SELECT hash, tuple_count FROM ariabc_internal.merkle_node
         WHERE index_oid=$1 AND node_id=$2 AND prefix_len=$3;

        if (FOUND) {
            // 2. Apply XOR delta
            new_parent_hash = parent_hash XOR tuple_hash_delta;
            new_p_count     = p_count + count_delta;

            // 3. UPDATE parent
            UPDATE ariabc_internal.merkle_node
               SET hash=$new_hash, tuple_count=$new_count
             WHERE index_oid=$1 AND node_id=$2 AND prefix_len=$3;
        }

        // Move up one level
        memcpy(curr_node_id, parent_node_id, 8);
        curr_prefix_len = parent_prefix_len;
    }
    // Terminates when curr_prefix_len == 0 (reached root, root has no parent)
}
```

**Walk through example** for a 3-level tree, leaf at `prefix_len=4`:
```
Iteration 1: curr=leaf(plen=4)
  parent_of(plen=4, w=2) → parent(plen=2)
  UPDATE merkle_node WHERE node_id=parent2 AND prefix_len=2

Iteration 2: curr=internal(plen=2)
  parent_of(plen=2, w=2) → parent(plen=0)
  UPDATE merkle_node WHERE node_id=root AND prefix_len=0

Iteration 3: curr_prefix_len=0 → STOP
```

Cost: `O(depth)` SPI round trips. For `MAX_PREFIX_LEN=60` with `BITS_PER_SPLIT=2`,
at most 30 ancestors. In practice, most trees are 2–5 levels deep.

### 1.3 `propagate_hash_to_ancestors_atomic()` — Synchronous path

```c
// merkleapply.c  lines 2369–2425
static void
propagate_hash_to_ancestors_atomic(Oid index_oid, const uint8 *leaf_node_id,
                                    int leaf_prefix_len,
                                    const MerkleHash *tuple_hash_delta,
                                    int64 count_delta, int bits_per_split)
{
    uint8 curr_node_id[8] = leaf_node_id;
    int curr_prefix_len = leaf_prefix_len;

    merkle_sync_prepare_plans(); // prepare SPI plans once per session

    while (curr_prefix_len > 0)
    {
        uint8 parent_node_id[8];
        int parent_prefix_len = merkle_parent_of(parent_node_id, curr_node_id,
                                                  curr_prefix_len, bits_per_split);

        // Single-statement atomic UPDATE using a pre-compiled SPI plan:
        // "UPDATE merkle_node
        //    SET hash = CASE WHEN GREATEST(tuple_count+$2,0)=0
        //                    THEN zero_hash
        //                    ELSE merkle_hash_xor_sql(hash, $1) END,
        //        tuple_count = GREATEST(tuple_count+$2, 0)
        //  WHERE index_oid=$3 AND node_id=$4 AND prefix_len=$5"
        SPI_execute_plan(merkle_sync_ancestor_update_plan, upd_values, ...);

        if (SPI_processed == 0)
            ereport(ERROR, "Merkle parent node disappeared");

        curr_prefix_len = parent_prefix_len;
        memcpy(curr_node_id, parent_node_id, 8);
    }
}
```

**Key difference from the Raft path**: This version does NOT SELECT the parent first.
It does a single UPDATE that atomically applies `XOR(old_hash, delta)`. The CASE handles
the special case where `tuple_count` drops to 0 — in that case, force hash to all-zeros
(an empty leaf must have a zero hash, not a nonzero XOR artifact from previous deletions).

The prepared SPI plans are created once per session:

```c
// merkleapply.c  lines 2317–2367
static void
merkle_sync_prepare_plans(void)
{
    // Route plan: SELECT is_leaf FROM merkle_node WHERE (index_oid, node_id, prefix_len)
    merkle_sync_route_plan = SPI_prepare(...);
    SPI_keepplan(plan);

    // Leaf update plan: UPDATE + RETURNING tuple_count, guarded by is_leaf=true
    merkle_sync_leaf_update_plan = SPI_prepare(
        "UPDATE ariabc_internal.merkle_node"
        "   SET hash = CASE WHEN tuple_count+$2=0 THEN zero ELSE xor(hash,$1) END,"
        "       tuple_count = tuple_count + $2"
        " WHERE index_oid=$3 AND node_id=$4 AND prefix_len=$5"
        "   AND is_leaf = true"          // ← guard: fails if node was split
        "   AND tuple_count + $2 >= 0"  // ← guard: no negative counts
        " RETURNING tuple_count", ...);

    // Ancestor update plan: UPDATE without is_leaf guard
    merkle_sync_ancestor_update_plan = SPI_prepare(
        "UPDATE ariabc_internal.merkle_node"
        "   SET hash = CASE WHEN GREATEST(tuple_count+$2,0)=0 THEN zero
                             ELSE xor(hash,$1) END,"
        "       tuple_count = GREATEST(tuple_count+$2, 0)"
        " WHERE index_oid=$3 AND node_id=$4 AND prefix_len=$5", ...);
}
```

The `is_leaf=true` guard on the **leaf update plan** is crucial: if another concurrent
transaction split this node between our route resolution and our update, the UPDATE
matches 0 rows (`SPI_processed=0`), which the caller detects and retries:

```c
// merkleapply.c  lines 2697–2718
rows_updated = merkle_atomic_update_leaf(...);
if (rows_updated == 1) {
    propagate_hash_to_ancestors_atomic(...);
    ...
    break;
}
// 0 rows updated: node may have been split
merkle_route_cache_invalidate(index_oid, routing_key);
if (!merkle_node_is_leaf(index_oid, leaf_node_id, leaf_prefix_len))
    continue;  // split happened — re-resolve route
else
    ereport(ERROR, "count delta would make tuple_count negative");
```

---

## 2. Merge — `do_merge_check()` Deep Dive

Node merging is the inverse of node splitting. When deletions reduce a leaf node's tuple count below `merge_threshold` (e.g. `merge_threshold = 8`), `do_merge_check()` evaluates whether sibling leaf nodes can be **collapsed** back into their parent node, converting the internal parent back into a single leaf node (`is_leaf = true`).

---

### 2.1 Complete C Implementation

```c
// merkleapply.c  lines 1117–1269
static void
do_merge_check(Oid index_oid, const uint8 *node_id, int prefix_len, int merge_thresh)
{
    uint8 parent_node_id[8];
    int parent_prefix_len;
    Relation index_rel;
    int fanout, bits_per_split;

    // Guard 1: Root node (prefix_len = 0) has no parent and can NEVER be merged
    if (prefix_len <= 0)
        return;

    index_rel = index_open(index_oid, AccessShareLock);
    fanout = DYNAMIC_MERKLE_FANOUT; // default = 4
    merkle_read_meta(index_rel, &fanout, NULL, NULL);
    bits_per_split = merkle_bits_per_split_for_fanout(fanout); // = 2
    index_close(index_rel, AccessShareLock);

    // Step 1: Find parent node ID and parent prefix length
    parent_prefix_len = merkle_parent_of(parent_node_id, node_id, prefix_len, bits_per_split);

    // Step 2: Compute lower and upper bytea bounds for all siblings under parent
    uint8 lower[8], upper[8];
    memcpy(lower, parent_node_id, 8);
    merkle_bytea_upper_bound(upper, parent_node_id, parent_prefix_len);

    // Step 3: Query all siblings at current prefix_len in the parent's range
    SELECT count(*), bool_and(is_leaf), sum(tuple_count)::bigint
      FROM ariabc_internal.merkle_node
     WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4;
    // $2 = prefix_len (e.g. 4)
    // $3 = lower (parent_node_id)
    // $4 = upper (upper bound of parent prefix)

    // Step 4: Evaluate merge conditions
    if (all_leaves && total_count < merge_thresh)
    {
        // Step 5A: XOR all sibling hashes together
        SELECT hash FROM ariabc_internal.merkle_node
         WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4;
        merged_hash = hash_1 XOR hash_2 XOR hash_3 XOR hash_4;

        if (total_count == 0)
            merged_hash = 0x000...000; // force all-zeros for empty subtree

        CommandCounterIncrement();

        // Step 5B: Delete all sibling nodes from the catalog
        DELETE FROM ariabc_internal.merkle_node
         WHERE index_oid = $1 AND prefix_len = $2 AND node_id BETWEEN $3 AND $4;

        // Step 5C: Convert parent node back into a LEAF node
        UPDATE ariabc_internal.merkle_node
           SET is_leaf = true, tuple_count = $total_count, hash = $merged_hash
         WHERE index_oid = $1 AND node_id = $parent_node_id AND prefix_len = $parent_prefix_len;

        merkle_route_cache_clear_index(index_oid);
        CommandCounterIncrement();

        // Step 6: Recurse upward — parent is now a leaf and may qualify for merge into grandparent!
        if (parent_prefix_len > 0)
            do_merge_check(index_oid, parent_node_id, parent_prefix_len, merge_thresh);
    }
}
```

---

### 2.2 Detailed Step-by-Step Mechanism

#### **Step 1: Parent Identification & Sibling Range Derivation**
For a node at `prefix_len = 4` (e.g. `node_id = 0x8000...`):
1. `parent_prefix_len = prefix_len - bits_per_split` $= 4 - 2 = 2$.
2. `merkle_parent_of()` derives `parent_node_id = 0x8000...` (keeping first 2 bits `10`, clearing bits 2..63 to zero).
3. `merkle_bytea_upper_bound()` calculates the upper bound for the parent range:
   - `lower = 0x8000000000000000` (`1000 0000 ...`)
   - `upper = 0xBFFFFFFFFFFFFFFF` (`1011 1111 ...`)
4. Any node with `prefix_len = 4` whose `node_id` lies between `lower` and `upper` is a **direct sibling** under this parent.

#### **Step 2: Checking the Three Mandatory Merge Rules**
The SPI query `SELECT count(*), bool_and(is_leaf), sum(tuple_count)` inspects all siblings. Merge occurs **if and only if** ALL THREE of the following rules are satisfied:

| Rule | Condition | Rationale |
|---|---|---|
| **1. Non-Root Guard** | `prefix_len > 0` | The Root node (`prefix_len = 0`) has no parent and cannot be deleted or collapsed. |
| **2. All Sibling Leaves** | `bool_and(is_leaf) == true` | All siblings under the parent must be leaf nodes. If any sibling is an internal node (`is_leaf = false`), grandchildren still exist deeper in that sub-branch, so the parent cannot collapse into a single leaf yet. |
| **3. Under Threshold** | `sum(tuple_count) < merge_threshold` | The combined row count of **all** siblings must fit under `merge_threshold` (e.g., total count of 5 tuples $< 8$). |

#### **Step 3: XOR Hash Re-aggregation & Catalog Collapse**
When a merge triggers:
1. **Combine Hashes**: All sibling hashes are XORed together ($\text{merged\_hash} = H_0 \oplus H_1 \oplus H_2 \oplus H_3$). Because XOR is commutative and associative, the parent's hash becomes the exact XOR aggregate of all remaining tuples in that subtree.
2. **Remove Sibling Rows**: All sibling leaf rows at `prefix_len = 4` are deleted from `ariabc_internal.merkle_node`.
3. **Promote Parent**: The parent node at `parent_prefix_len = 2` is updated with `is_leaf = true`, `tuple_count = total_count`, and `hash = merged_hash`.

#### **Step 4: Recursive Upward Cascade**
After the parent node becomes a leaf node with `tuple_count = total_count < merge_threshold`, `do_merge_check` calls itself recursively on the parent node (`parent_prefix_len = 2`). If the parent's own siblings under the Root also satisfy `total_count < merge_threshold`, the tree collapses further up to `prefix_len = 0` (single Root leaf)!

---

### 2.3 Concrete Visual Walkthrough with Example

Suppose `fanout = 4`, `split_threshold = 16`, `merge_threshold = 8`.

#### **Initial State: 2-Level Tree with 10 Total Rows**
* Root (`prefix_len = 0`, `is_leaf = false`, `tuple_count = 10`)
* Parent 1 at `prefix_len = 2` (`0x00...`, `is_leaf = false`, `tuple_count = 7`)
* Level-4 Leaves under Parent 1:
  - Child `0x00...` (`prefix_len = 4`, `is_leaf = true`, `tuple_count = 2`, `hash = H0`)
  - Child `0x10...` (`prefix_len = 4`, `is_leaf = true`, `tuple_count = 1`, `hash = H1`)
  - Child `0x20...` (`prefix_len = 4`, `is_leaf = true`, `tuple_count = 2`, `hash = H2`)
  - Child `0x30...` (`prefix_len = 4`, `is_leaf = true`, `tuple_count = 2`, `hash = H3`)

```
               [ Root (prefix_len=0, is_leaf=f, count=10) ]
                                    │
               ┌────────────────────┴────────────────────┐
               ▼                                         ▼
   [ Parent 0x00 (plen=2, is_leaf=f, count=7) ]     [ Parent 0x80 (plen=2, is_leaf=t, count=3) ]
     │          │          │          │
     ▼          ▼          ▼          ▼
  [0x00]     [0x10]     [0x20]     [0x30]
 (plen=4)   (plen=4)   (plen=4)   (plen=4)
 count=2    count=1    count=2    count=2
```

---

#### **Step A: Deletion Triggers `do_merge_check`**
A `DELETE` statement deletes 3 rows from Child `0x20...` and Child `0x30...`.
Child `0x20...` tuple count drops to 0. `do_merge_check` is called for Child `0x20...` (`prefix_len = 4`).

1. **Calculate Parent**: `merkle_parent_of(0x20..., 4)` $\to$ `parent_node_id = 0x00...`, `parent_prefix_len = 2`.
2. **Query Siblings**:
   ```sql
   SELECT count(*), bool_and(is_leaf), sum(tuple_count)::bigint
     FROM ariabc_internal.merkle_node
    WHERE index_oid = ... AND prefix_len = 4 AND node_id BETWEEN '\x00...' AND '\x3F...';
   ```
   - `count(*)` = 4
   - `bool_and(is_leaf)` = `true` (all 4 siblings `0x00`, `0x10`, `0x20`, `0x30` are leaves)
   - `sum(tuple_count)` = $2 + 1 + 0 + 1 = 4$ tuples.

3. **Check Threshold**: `total_count` (4) $< \text{merge\_threshold}$ (8) $\to$ **MERGE ELIGIBLE!**

4. **XOR Sibling Hashes**:
   $$\text{merged\_hash} = H_0 \oplus H_1 \oplus H_2 \oplus H_3$$

5. **Execute Collapse**:
   - `DELETE FROM merkle_node WHERE prefix_len = 4 AND node_id BETWEEN '\x00...' AND '\x3F...'` (removes all 4 level-4 leaves).
   - `UPDATE merkle_node SET is_leaf = true, tuple_count = 4, hash = merged_hash WHERE node_id = '\x00...' AND prefix_len = 2`.

```
After Level-4 Collapse:
               [ Root (prefix_len=0, is_leaf=f, count=7) ]
                                    │
               ┌────────────────────┴────────────────────┐
               ▼                                         ▼
   [ Parent 0x00 (plen=2, is_leaf=t, count=4) ]     [ Parent 0x80 (plen=2, is_leaf=t, count=3) ]
```

---

#### **Step B: Upward Recursive Cascade**
Now `do_merge_check` recurses to `parent_node_id = 0x00...` at `parent_prefix_len = 2`:

1. **Calculate Grandparent**: `merkle_parent_of(0x00..., 2)` $\to$ `grandparent_node_id = 0x00...` (Root), `grandparent_prefix_len = 0`.
2. **Query Level-2 Siblings**:
   - Level-2 children under Root are `0x00` (`is_leaf = true`, count = 4) and `0x80` (`is_leaf = true`, count = 3).
   - `bool_and(is_leaf)` = `true`.
   - `sum(tuple_count)` = $4 + 3 = 7$ tuples.
3. **Check Threshold**: `total_count` (7) $< \text{merge\_threshold}$ (8) $\to$ **ROOT LEVEL MERGE ELIGIBLE!**
4. **Execute Root Collapse**:
   - `DELETE FROM merkle_node WHERE prefix_len = 2` (removes Level-2 children `0x00` and `0x80`).
   - `UPDATE merkle_node SET is_leaf = true, tuple_count = 7, hash = (H_Parent00 XOR H_Parent80) WHERE node_id = '\x00...' AND prefix_len = 0`.

```
Final Collapsed State:
               [ Root (prefix_len=0, is_leaf=true, count=7, hash=H_ROOT) ]
```

The entire tree has cleanly collapsed back to a single Root leaf node!

---

## 3. Transaction-Local Delta Staging Pipeline

### 3.1 In-memory subxact frame stack

```c
// merkledelta.c  lines 34–43
typedef struct MerkleSubxactFrame {
    SubTransactionId  subxid;
    HTAB             *entries;   // hash map: MerkleDeltaKey → MerkleDeltaEntry
    struct MerkleSubxactFrame *next;
} MerkleSubxactFrame;

static MerkleSubxactFrame *merkle_delta_frames = NULL;
```

The hash map key (`MerkleDeltaKey`) is:

```c
// src/include/access/merkle.h  lines 102–109
typedef struct MerkleDeltaKey {
    Oid         index_oid;
    RelFileNode index_rnode;
    uint8       event_type;      // INSERT=0, DELETE=1, UPDATE_SAME_LEAF=2
    uint8       old_key_hash[8]; // valid for DELETE and UPDATE
    uint8       new_key_hash[8]; // valid for INSERT and UPDATE
} MerkleDeltaKey;
```

The hash map value adds the accumulated XOR delta:

```c
// src/include/access/merkle.h  lines 125–129
typedef struct MerkleDeltaEntry {
    MerkleDeltaKey key;
    MerkleHash     xor_delta;   // XOR of all tuple hashes for this (index, key) pair
} MerkleDeltaEntry;
```

When the XOR delta cancels to zero (e.g., insert then delete same row), the entry is
removed from the map entirely:

```c
// merkledelta.c  line 211–213
merkle_hash_xor(&entry->xor_delta, hash);
if (merkle_hash_is_zero(&entry->xor_delta))
    hash_search(frame->entries, &key, HASH_REMOVE, NULL);
```

### 3.2 Serialization for Raft — `merkle_serialize_staged_delta()`

For Raft-ledger transactions, the delta is serialized into a binary blob that travels
inside the Raft log entry:

```c
// merkledelta.c  lines 279–375
bytea *
merkle_serialize_staged_delta(uint64 raft_log_index, uint32 item_ordinal)
{
    // 1. Merge all subxact frames into one combined map
    combined = merkle_delta_create_map(...);
    for (frame = merkle_delta_frames; frame != NULL; frame = frame->next)
        merge all entries from frame into combined;

    // 2. Sort entries deterministically
    count  = hash_get_num_entries(combined);
    sorted = palloc(count * sizeof(MerkleDeltaEntry));
    // ... fill and qsort by (index_oid, rnode, event_type, old_key, new_key) ...

    // 3. Write binary blob
    // Header (40 bytes):
    //   magic(4) = 0x4D444C54 "MDLT"
    //   version(4) = 1
    //   flags(4)   = 1 if raft-bound
    //   entry_count(4)
    //   payload_len(4)
    //   crc32c(4)
    //   raft_log_index(8)
    //   item_ordinal(4)
    //   reserved(4)
    //
    // Payload: entry_count × 72 bytes each:
    //   index_oid(4), spcNode(4), dbNode(4), relNode(4)
    //   event_type(1), old_key_hash(8), new_key_hash(8)
    //   format_version(4), padding(3), xor_delta(32)

    merkle_delta_put_u32(header+0,  MERKLE_DELTA_MAGIC);
    merkle_delta_put_u32(header+4,  MERKLE_DELTA_VERSION); // = 1
    merkle_delta_put_u32(header+8,  flags);
    merkle_delta_put_u32(header+12, count);
    merkle_delta_put_u32(header+16, payload_len);
    // ... CRC covers header (with CRC field zeroed) + payload ...
    merkle_delta_put_u32(header+20, crc);
    merkle_delta_put_u64(header+24, raft_log_index);
    merkle_delta_put_u32(header+32, item_ordinal);

    for each entry:
        write 72 bytes to payload ...

    return result; // palloc'd bytea
}
```

The CRC32C covers both header (with the CRC field zeroed) and payload, verified on replay.

### 3.3 Ordered Raft Applier — `merkle_apply_until_impl()`

On the receiving (replica) side, an ordered applier replays all committed deltas in
strict sequence order:

```c
// merkleapply.c  lines 1574–1780
static uint64
merkle_apply_until_impl(uint64 required_seq)
{
    // 1. Lock apply-state singleton row (serialize concurrent appliers)
    SELECT applied_seq, state FROM merkle_apply_state WHERE singleton FOR UPDATE;

    for (;;)
    {
        // 2. Fetch a batch of committed raft_apply_item rows
        SELECT apply_seq, source_state, delta_version, delta_blob,
               raft_log_index, item_ordinal
          FROM ariabc_internal.raft_apply_item
         WHERE merkle_apply_seq > $applied_seq
           AND merkle_apply_seq <= $required_seq
         ORDER BY apply_seq LIMIT $batch_items;

        // 3. For each row in sequence order:
        for (row = 0; row < SPI_processed; row++) {
            if (source_seq != expected_seq) break; // gap — stop here
            if (source_state != 2,3,4) break;      // not terminal — stop

            if (delta_version == 1)
                merkle_parse_delta_blob(blob, ...);  // parse entries into events[]
            // delta_version==0 means no-op (tombstone/error item)
            batch_end = source_seq;
            expected_seq++;
        }

        if (batch_end == applied_seq) break; // no progress

        // 4. Apply all leaf events from this batch
        merkle_apply_leaf_events(&events, batch_end);
        applied_seq = batch_end;
    }

    // 5. Persist new applied_seq
    UPDATE merkle_apply_state SET applied_seq=$applied_seq, state=0 WHERE singleton;

    // 6. Advance terminal_prefix_seq (P0.2 invariant)
    merkle_advance_terminal_prefix_spi();

    // 7. GC: clear blob payloads for applied rows (keep metadata)
    UPDATE raft_apply_item
       SET merkle_delta_version=0, merkle_delta_blob=NULL
     WHERE merkle_apply_seq <= $applied_seq AND merkle_delta_blob IS NOT NULL;
}
```

The applier stops at the first **gap** in sequence numbers. If sequence 5 is committed
but sequence 4 is not yet visible, the applier stops at 3 and waits. This ensures
strictly ordered, idempotent application.

### 3.4 Batch limits

Three configurable limits prevent unbounded work in a single apply transaction:

```c
// src/include/access/merkle.h  lines 38–45
#define MERKLE_APPLY_DEFAULT_BATCH_ITEMS  256    // max delta rows per batch
#define MERKLE_APPLY_DEFAULT_BATCH_BYTES  (1MB)  // max blob bytes per batch
#define MERKLE_APPLY_DEFAULT_BATCH_PAGES  128    // max estimated page touches
#define MERKLE_APPLY_DEFAULT_BATCH_TIME_MS 1     // max wall time per batch
```

---

## 4. Verification — `merkle_verify_index()`

```c
// merkleverify.c  lines 254–326
Datum
merkle_verify_index(PG_FUNCTION_ARGS)
{
    Oid indexOid = PG_GETARG_OID(0);

    // 1. Require freshness gate (applied_seq == target_seq)
    merkle_open_consistent_index(indexOid);  // calls merkle_require_fresh()

    // 2. Fetch stored root hash from catalog
    SELECT hash FROM ariabc_internal.merkle_node
     WHERE index_oid=$1 AND prefix_len=0;  // root has prefix_len=0
    → stored_root_hash

    // 3. Recompute XOR of all heap tuples
    scan = table_beginscan(heapRel, GetActiveSnapshot(), 0, NULL);
    heap_tuple_xor_hash = 0x0000...;
    while (table_scan_getnextslot(scan, ...))
    {
        merkle_compute_slot_hash(heapRel, slot, &th);  // BLAKE3 of all cols
        merkle_hash_xor(&heap_tuple_xor_hash, &th);
    }

    // 4. Compare
    match = (stored_root_hash == heap_tuple_xor_hash);
    if (!match)
        elog(WARNING, "merkle_verify_index mismatch: stored=%s heap_xor=%s", ...);

    PG_RETURN_BOOL(match);
}
```

**Why is the root hash equal to the XOR of all tuples?**

By induction:
- Leaf `hash = XOR(tuple_hash(r) for all r in leaf)`
- Internal node `hash = XOR(child.hash for all children)`
  = `XOR(XOR(tuple_hash(r) for r in child) for all children)`
  = `XOR(tuple_hash(r) for all r in subtree)`
- Root = `XOR(all tuple hashes)` ✓

So `merkle_verify_index` simply checks that the global XOR aggregate stored in the root
matches a fresh computation from the heap.

---

## 5. Live Proof via `bcpsql`

Connect to the running server and run this complete test:

```sql
-- Setup schema first
\i scripts/distributed/sql/raft_apply_ledger_schema.sql
SET merkle_apply_synchronous_direct = on;
SET merkle_read_lag_policy = apply;

-- Create table with small thresholds to trigger split/merge quickly
CREATE TABLE proof_test (id int, val text);
CREATE INDEX proof_idx ON proof_test USING merkle (id)
  WITH (split_threshold=10, merge_threshold=3);

-- Initial state: single root leaf, 0 rows
SELECT prefix_len, is_leaf, tuple_count
  FROM ariabc_internal.merkle_node
 WHERE index_oid = 'proof_idx'::regclass
 ORDER BY prefix_len, node_id;
-- Expected: 1 row: plen=0, is_leaf=t, count=0

-- Insert 5 rows (below split_threshold=10): still a single leaf
INSERT INTO proof_test SELECT g, 'v'||g FROM generate_series(1,5) g;
SELECT prefix_len, is_leaf, tuple_count
  FROM ariabc_internal.merkle_node
 WHERE index_oid = 'proof_idx'::regclass ORDER BY prefix_len, node_id;
-- Expected: plen=0, is_leaf=t, count=5

-- Verify integrity
SELECT merkle_verify('proof_test');  -- should return true

-- Insert 8 more (total 13 > threshold=10): SPLIT happens
INSERT INTO proof_test SELECT g, 'v'||g FROM generate_series(6,13) g;
SELECT prefix_len, is_leaf, tuple_count
  FROM ariabc_internal.merkle_node
 WHERE index_oid = 'proof_idx'::regclass ORDER BY prefix_len, node_id;
-- Expected:
--   plen=0, is_leaf=f (internal now)
--   plen=2, is_leaf=t × up to 4 children with varying counts

-- Root is now internal; verify still passes
SELECT merkle_verify('proof_test');

-- Delete most rows (leave 2 total < merge_threshold=3): MERGE happens
DELETE FROM proof_test WHERE id > 2;
SELECT prefix_len, is_leaf, tuple_count
  FROM ariabc_internal.merkle_node
 WHERE index_oid = 'proof_idx'::regclass ORDER BY prefix_len, node_id;
-- Expected: back to 1 row: plen=0, is_leaf=t, count=2

-- Verify after merge
SELECT merkle_verify('proof_test');  -- must still return true

-- Inspect root hash
SELECT merkle_root_hash('proof_test');

-- Cleanup
DROP TABLE proof_test;
```

---

## 6. Complete Flow Diagram

```
User DML (INSERT row R with key K):
│
├─ heap_insert(R)  [PostgreSQL core]
│
├─ merkleInsert(K, tid)                          [merkleinsert.c]
│   ├─ merkle_compute_route(K)  → route_digest   [merkleutil.c]
│   │     BLAKE3(magic=ARIAROOT, key_cols_binary_send)
│   │     → route_digest[0..7] = node_id bytes
│   ├─ merkle_compute_row_hash(tid)  → hash      [merkleutil.c]
│   │     BLAKE3(magic=ARIAMRKL, all_cols_binary_send)
│   └─ merkle_stage_delta_event(INSERT, new_key=route_digest[0..7], hash)
│         → XOR into per-tx subxact frame hash map
│
└─ XACT_EVENT_PRE_COMMIT
    └─ merkle_apply_staged_deltas_synchronously()  [merkledelta.c]
        └─ merkle_apply_staged_synchronous_safe(combined_map)
            ├─ Sort entries by (index_oid, key_hash)
            └─ for each entry:
                └─ merkle_apply_single_coalesced_entry()
                    ├─ merkle_resolve_route_leaf()   [find leaf node_id]
                    │   ├─ check route cache (1024-slot flat hash map)
                    │   └─ if miss: walk tree top-down via SELECT is_leaf
                    │         merkle_next_bits() → which child to follow
                    │         merkle_bytea_extend() → compute child node_id
                    ├─ merkle_atomic_update_leaf()   [UPDATE + RETURNING]
                    │   leaf.hash XOR= delta,  leaf.count += delta_count
                    │   GUARDED BY: is_leaf=true (fails if node was split)
                    ├─ propagate_hash_to_ancestors_atomic()
                    │   for each ancestor (leaf→root):
                    │       merkle_parent_of() → parent node_id
                    │       UPDATE ancestor: hash XOR= delta, count += delta_count
                    └─ merkle_check_split_merge_guarded()
                        ├─ if count > split_threshold:
                        │    advisory_lock(index, node)
                        │    if still_leaf: do_split()
                        │       fetch rows from heap in [lower,upper] range
                        │       merkle_do_split_in_memory():
                        │           classify into 4 buckets by next 2 routing bits
                        │           INSERT 4 child leaves
                        │           UPDATE parent: is_leaf=false
                        │           recurse if any child > threshold
                        └─ if count < merge_threshold:
                             advisory_lock(index, node)
                             if still_leaf: do_merge_check()
                                 if all_siblings_are_leaves AND combined < threshold:
                                     DELETE all siblings
                                     UPDATE parent: is_leaf=true, hash=XOR(siblings)
                                     recurse upward
```

---

## Summary — Three-Part Reference

| Part | Topics |
|---|---|
| **Part 1** | Data model (`merkle_node` schema), two-layer BLAKE3 hashing, XOR aggregate, prefix/node_id bit encoding (`merkle_next_bits`, `merkle_bytea_extend`, `merkle_bytea_upper_bound`, `merkle_parent_of`), metapage cache |
| **Part 2** | Index build (`merkleBuild`), insert callback, delta staging, PRE_COMMIT apply, `apply_leaf_event` tree walk, split deferred queue, `do_split` heap range query, `merkle_do_split_in_memory` bucket classification, split range guard, route cache |
| **Part 3** | Ancestor hash propagation (both paths), merge algorithm (`do_merge_check`), transaction delta serialization format, ordered Raft applier (`merkle_apply_until_impl`), batch limits, `merkle_verify_index` correctness argument, live SQL proof |

---

**Files quick-reference**:
```
src/include/access/merkle.h           → constants, structs, inline bit helpers
src/backend/access/merkle/
  merkle.c        → AM handler, reloptions, cost estimate
  merkleutil.c    → BLAKE3 hashing, metapage cache, route computation
  merklebuild.c   → CREATE INDEX build
  merkleinsert.c  → per-row INSERT callback
  merkledelta.c   → tx-local staging, serialization, PRE_COMMIT hook
  merkleapply.c   → leaf event application, split, merge, propagation,
                     route cache, Raft ordered applier, synchronous path
  merkleverify.c  → merkle_verify_index, merkle_root_hash_index, tree stats
scripts/distributed/sql/
  raft_apply_ledger_schema.sql → merkle_node table + all support tables
```
