# Dynamic Merkle Tree — Deep Dive (Part 2 of 3)
# Index Build, Insert Path, and Node Splitting

> **Files covered in this part**
> - `src/backend/access/merkle/merklebuild.c`
> - `src/backend/access/merkle/merkleinsert.c`
> - `src/backend/access/merkle/merkleapply.c` (lines 716–932, 978–1087, 1242–1466)

---

## 1. Index Build — `merkleBuild()`

Called by `CREATE INDEX ... USING merkle`.

### Phase 1 — Scan heap, collect entries in memory

```c
// merklebuild.c  lines 146–282
IndexBuildResult *
merkleBuild(Relation heapRel, Relation indexRel, struct IndexInfo *indexInfo)
{
    MerkleBuildState buildstate;
    buildstate.max_entries  = 1000000;      // start with 1M slot buffer
    buildstate.entries      = malloc(max_entries * sizeof(MerkleTupleHashEntry));
    buildstate.num_entries  = 0;
    buildstate.bits_per_split = merkle_bits_per_split_for_fanout(fanout);
    ...
    // heap scan — one callback per live tuple
    table_index_build_scan(heapRel, indexRel, indexInfo,
                           true, false,
                           merkle_build_callback, &buildstate, NULL);
```

Each tuple callback does:

```c
// merklebuild.c  lines 75–138  (merkle_build_callback)
static void
merkle_build_callback(Relation indexRel, ItemPointer tid,
                      Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    if (!tupleIsAlive) return;

    // 1. Compute routing key (index key columns → BLAKE3 → 8 bytes)
    merkle_compute_route(indexRel, values, isnull, nkeys, &route);

    // 2. Fetch the live HOT-chain successor and hash ALL columns
    table_index_fetch_tuple(heapFetch, tid, SnapshotSelf, heapSlot, ...);
    merkle_compute_slot_hash(heapRel, heapSlot, &hash);

    // 3. Store in flat array for bulk split
    memcpy(entries[num_entries].key_hash,  route.route_digest, 8);
    memcpy(&entries[num_entries].tuple_hash, &hash, sizeof(MerkleHash));
    num_entries++;
}
```

**Why HOT-chain fetch?** The heap scan gives the root TID of a HOT chain, not the live
successor. `table_index_fetch_tuple` follows the chain to the visible version, so the
hash matches the actual live row data, not a dead HOT root.

### Phase 2 — Build tree into catalog via SPI

```c
// merklebuild.c  lines 296–377
if (SPI_connect() == SPI_OK_CONNECT)
{
    // Sort all entries by key_hash for deterministic splits
    if (num_entries > 1)
        qsort(entries, num_entries, sizeof(MerkleTupleHashEntry),
              merkle_entry_key_cmp); // memcmp on key_hash[8]

    if (indtuples <= split_threshold)
    {
        // Small table: single root leaf node
        INSERT INTO ariabc_internal.merkle_node
            (index_oid, node_id=0x0000..., prefix_len=0, is_leaf=true,
             tuple_count=N, hash=XOR_of_all)
        ON CONFLICT DO UPDATE ...
    }
    else
    {
        // Large table: insert root then recursively split
        INSERT root node (is_leaf=true initially, will flip in split)...
        merkle_do_split_in_memory(index_oid, zero_node_id, 0,
                                  entries, num_entries, fanout,
                                  bits_per_split, split_threshold);
    }
}
```

The initial root insert sets `is_leaf=true`; `merkle_do_split_in_memory` then
immediately flips it to `is_leaf=false` if a split occurs.

---

## 2. Insert Path — From DML to Leaf Update

### 2.1 `merkleInsert()` — The AM callback hook

```c
// merkleinsert.c (simplified)
bool
merkleInsert(Relation indexRel, Datum *values, bool *isnull,
             ItemPointer ht_ctid, Relation heapRel, ...)
{
    MerkleHash hash;
    MerkleRoute route;
    Relation actualHeapRel;

    if (!enable_merkle_index) return false;

    // Compute route (key cols only)
    merkle_compute_route(indexRel, values, isnull, nkeys, &route);

    // Fetch live row and hash ALL columns
    actualHeapRel = table_open(index->indrelid, NoLock);
    merkle_compute_row_hash(actualHeapRel, ht_ctid, &hash);
    table_close(actualHeapRel, NoLock);

    // Stage the delta for this transaction (not applied yet)
    merkle_stage_delta_event(indexRel,
                             MERKLE_DELTA_INSERT,
                             NULL,           // no old key hash
                             route.route_digest,  // new key hash (8 bytes used)
                             &hash);
    return false; // not a unique index
}
```

The insert does NOT immediately update `merkle_node`. It stages a `MerkleDeltaEntry`
in a per-transaction hash map in `TopTransactionContext`.

### 2.2 Delta Staging — `merkle_stage_delta_event()`

```c
// merkledelta.c  lines 174–216
void
merkle_stage_delta_event(Relation indexRel, MerkleDeltaEventType event_type,
                         const uint8 *old_key_hash, const uint8 *new_key_hash,
                         const MerkleHash *hash)
{
    MerkleDeltaKey key;
    key.index_oid   = RelationGetRelid(indexRel);
    key.index_rnode = indexRel->rd_node;   // physical identity
    key.event_type  = event_type;
    memcpy(key.old_key_hash, old_key_hash, 8); // first 8 bytes of route_digest
    memcpy(key.new_key_hash, new_key_hash, 8);

    entry = hash_search(frame->entries, &key, HASH_ENTER, &found);
    if (!found) {
        entry->key = key;
        merkle_hash_zero(&entry->xor_delta);
    }
    merkle_hash_xor(&entry->xor_delta, hash);  // accumulate XOR

    // If delta cancelled out (e.g. insert then delete same row):
    if (merkle_hash_is_zero(&entry->xor_delta))
        hash_search(frame->entries, &key, HASH_REMOVE, NULL);
}
```

**Key insight**: Within one transaction, repeated INSERT+DELETE of the same row
produces a zero delta → the entry is removed → no Merkle update needed. This is the
core self-inverse property of XOR.

### 2.3 Subxact rollback support

```c
// merkledelta.c  lines 613–639
static void
merkle_delta_subxact_callback(SubXactEvent event, ...)
{
    child = merkle_delta_find_frame(mySubid);

    if (event == SUBXACT_EVENT_COMMIT_SUB) {
        // merge child frame into parent frame
        hash_seq_init(&seq, child->entries);
        while ((entry = hash_seq_search(&seq)) != NULL)
            merkle_delta_merge_one(parent->entries, entry);
        merkle_delta_unlink_frame(child);
    }
    else if (event == SUBXACT_EVENT_ABORT_SUB)
        merkle_delta_unlink_frame(child); // discard — rolled back
}
```

Each savepoint has its own `MerkleSubxactFrame`. Commit merges up; abort discards.

### 2.4 `PRE_COMMIT` — Synchronous Apply

```c
// merkledelta.c  lines 574–596
if (event == XACT_EVENT_PRE_COMMIT) {
    if (merkle_has_staged_delta()) {
        if (!merkle_staged_delta_persisted &&
            merkle_apply_synchronous_direct &&
            (activeTx == NULL || !activeTx->raft_ledger_enabled))
        {
            merkle_apply_staged_deltas_synchronously();
        }
        // For raft_ledger path: delta was already serialized into raft_apply_item
        // and applied synchronously by the middleware before returning.
    }
}
```

For **direct mode** (non-Raft): applies staged deltas synchronously before commit.
For **Raft ledger mode**: delta is serialized into the blob, committed to the ledger,
then replayed via the ordered applier.

---

## 3. Applying a Leaf Event — `apply_leaf_event()`

This is the hot path that actually updates `merkle_node`:

```c
// merkleapply.c  lines 1243–1466
static void
apply_leaf_event(Oid index_oid, const uint8 key_hash[8],
                 const MerkleHash *tuple_hash_delta, int64 count_delta)
{
    uint8 node_id[8] = {0};
    int prefix_len = 0;
    int bits_per_split = merkle_bits_per_split_for_fanout(fanout);

    // Tree walk: start at root, descend until leaf found
    for (;;)
    {
        SELECT is_leaf, tuple_count, hash
          FROM ariabc_internal.merkle_node
         WHERE index_oid=$1 AND node_id=$2 AND prefix_len=$3;

        if (NOT FOUND && prefix_len == 0) {
            // Bootstrap: create root if missing
            INSERT INTO merkle_node (... is_leaf=true, tuple_count=0, hash=zero ...)
            ON CONFLICT DO NOTHING;
            // re-query...
        }

        if (is_leaf) {
            // FOUND THE LEAF — apply XOR delta
            new_hash  = current_hash XOR tuple_hash_delta;
            new_count = current_count + count_delta;

            UPDATE merkle_node SET hash=$new_hash, tuple_count=$new_count
             WHERE index_oid=$1 AND node_id=$2 AND prefix_len=$3;

            // Propagate XOR delta up to all ancestors
            propagate_hash_to_ancestors(index_oid, node_id, prefix_len,
                                        tuple_hash_delta, count_delta);

            // Check if split or merge needed
            if (new_count > split_thresh && prefix_len < MAX_PREFIX_LEN)
                enqueue pending split;
            else if (new_count < merge_thresh && prefix_len > 0)
                enqueue pending merge;

            return;
        }
        else {
            // Internal node: descend to the correct child
            uint8 bits = merkle_next_bits(key_hash, prefix_len, bits_per_split);
            merkle_bytea_extend(next_node_id, node_id, prefix_len, bits, bits_per_split);
            node_id = next_node_id;
            prefix_len += bits_per_split;
        }
    }
}
```

**Tree walk example** for a 3-level tree with key starting `0b10_11_00...`:
```
Level 0: node_id=0x00..., prefix_len=0, is_leaf=false
         → bits = 0b10 = 2 → next_node_id sets bits[0..1]=10
Level 1: node_id=0x80..., prefix_len=2, is_leaf=false
         → bits = 0b11 = 3 → extend bits[2..3]=11
Level 2: node_id=0xB0..., prefix_len=4, is_leaf=true ← UPDATE HERE
```

---

## 4. Split — Full Deep Dive

Split is triggered when `tuple_count > split_threshold` after a leaf update.

### 4.1 Deferred execution

Splits are NOT done immediately inside `apply_leaf_event`. They are enqueued:

```c
// merkleapply.c  lines 1402–1424
if (new_count > split_thresh && prefix_len < MAX_PREFIX_LEN)
{
    // Deduplicate: don't enqueue same node twice
    bool found = false;
    for (k = 0; k < num_pending_sm; k++)
        if (pending_sm[k].index_oid == index_oid &&
            pending_sm[k].prefix_len == prefix_len &&
            memcmp(pending_sm[k].node_id, node_id, 8) == 0 &&
            pending_sm[k].is_split == true)
        { found = true; break; }

    if (!found && num_pending_sm < MAX_PENDING_SPLIT_MERGE)
    {
        pending_sm[num_pending_sm].index_oid = index_oid;
        memcpy(pending_sm[num_pending_sm].node_id, node_id, 8);
        pending_sm[num_pending_sm].prefix_len = prefix_len;
        pending_sm[num_pending_sm].is_split   = true;
        num_pending_sm++;
    }
}
```

After all leaf events in the batch are processed, splits execute:

```c
// merkleapply.c  lines 1499–1506
for (i = 0; i < num_pending_sm; i++) {
    if (pending_sm[i].is_split)
        do_split(pending_sm[i].index_oid,
                 pending_sm[i].node_id,
                 pending_sm[i].prefix_len, 0);
    else
        do_merge_check(...);
}
num_pending_sm = 0;
```

For the **synchronous path** (direct DML), split/merge is guarded by an advisory lock
and checks whether the node is still a leaf (avoids double-split race):

```c
// merkleapply.c  lines 2620–2649
static void
merkle_check_split_merge_guarded(Oid index_oid, const uint8 *node_id,
                                 int prefix_len, int64 current_count,
                                 int split_thresh, int merge_thresh)
{
    if (current_count > split_thresh && prefix_len < MAX_PREFIX_LEN)
    {
        int64 lock_key = merkle_compute_advisory_lock_key(
                             index_oid, node_id, prefix_len);
        // Block until we hold the advisory lock for this (index, node) pair
        DirectFunctionCall1(pg_advisory_xact_lock_int8,
                            Int64GetDatum(lock_key));

        // Re-check: another concurrent transaction may have already split it
        if (merkle_node_is_leaf(index_oid, node_id, prefix_len))
            do_split(index_oid, node_id, prefix_len, current_count);
    }
    ...
}
```

### 4.2 `do_split()` — Fetching rows from the heap

```c
// merkleapply.c  lines 978–1087
void
do_split(Oid index_oid, const uint8 *node_id, int prefix_len, int64 target_count)
{
    // Build prefix range: [node_id, upper_bound(node_id, prefix_len)]
    memcpy(lower, node_id, 8);
    merkle_bytea_upper_bound(upper, node_id, prefix_len);

    // Read index metadata
    merkle_read_meta(index_rel, &fanout, &split_threshold, NULL);
    bits_per_split = merkle_bits_per_split_for_fanout(fanout);

    // Get heap table name and key expression (e.g. "merkle_key_hash(id)")
    heap_name = quote_qualified_identifier(...);
    key_expr  = get_index_key_expr_str(index_oid);

    // Query heap rows in this leaf's prefix range
    appendStringInfo(&buf,
        "SELECT %s AS kh, merkle_tuple_hash(%s.*) AS th"
        "  FROM %s"
        " WHERE %s BETWEEN $1 AND $2"
        " ORDER BY ctid",
        key_expr, heap_name, heap_name, key_expr);

    // Execute with lower/upper as bytea range bounds
    spi_rc = SPI_execute_with_args(buf.data, 2, argtypes, values, ...);

    if (SPI_processed > 0) {
        // Collect (key_hash[8], tuple_hash[32]) for every row
        entries = malloc(SPI_processed * sizeof(MerkleTupleHashEntry));
        for (i = 0; i < SPI_processed; i++) {
            memcpy(entries[i].key_hash,       VARDATA(kh_b), 8);
            memcpy(entries[i].tuple_hash.data, VARDATA(th_b), 32);
        }

        // Do the actual split entirely in memory + catalog writes
        merkle_do_split_in_memory(index_oid, node_id, prefix_len,
                                  entries, SPI_processed,
                                  fanout, bits_per_split, split_threshold);

        // Register this range as "in split" so concurrent updates skip it
        merkle_register_split_range(index_oid, lower, upper);
        merkle_route_cache_clear_index(index_oid);
        CommandCounterIncrement();
    }
}
```

**The range query `WHERE key_hash BETWEEN lower AND upper`** is what makes the prefix
work as a trie. Every row whose BLAKE3 route key starts with the prefix `node_id[0..prefix_len-1]`
will have a `key_hash` in the range `[node_id, upper_bound]`. The `merkle_bytea_upper_bound()`
function fills free bits with 1s to define this range correctly.

### 4.3 `merkle_do_split_in_memory()` — The Core Split Logic

```c
// merkleapply.c  lines 716–932
void
merkle_do_split_in_memory(Oid index_oid, const uint8 *node_id, int prefix_len,
                           MerkleTupleHashEntry *entries, int num_entries,
                           int fanout, int bits_per_split, int split_threshold)
{
    int   *bucket_counts = palloc0(fanout * sizeof(int));
    MerkleHash *bucket_hashes = palloc0(fanout * sizeof(MerkleHash));

    // Step 1: Classify each entry into a bucket (0..fanout-1)
    for (i = 0; i < num_entries; i++) {
        uint8 b = merkle_next_bits(entries[i].key_hash, prefix_len, bits_per_split);
        // b = the next 2 bits of this row's routing key at the current depth
        bucket_counts[b]++;
        merkle_hash_xor(&bucket_hashes[b], &entries[i].tuple_hash);
    }

    // Step 2: Partition entries array by bucket (for recursive splits)
    partitioned_entries = malloc(num_entries * sizeof(...));
    // ... build bucket_offsets and scatter entries into partitioned_entries ...

    // Step 3: Create child nodes for each bucket
    for (i = 0; i < fanout; i++) {
        uint8 child_node_id[8];
        int   child_prefix_len = prefix_len + bits_per_split;

        // Build child node_id by appending bucket index i at prefix_len
        merkle_bytea_extend(child_node_id, node_id, prefix_len, (uint8)i,
                            bits_per_split);

        INSERT INTO ariabc_internal.merkle_node
            (index_oid, child_node_id, child_prefix_len,
             is_leaf=true, tuple_count=bucket_counts[i], hash=bucket_hashes[i])
        ON CONFLICT DO UPDATE SET is_leaf=true, tuple_count=..., hash=...;

        // Recursively split child if it's also over threshold
        if (bucket_counts[i] > split_threshold && child_prefix_len < MAX_PREFIX_LEN)
            merkle_do_split_in_memory(index_oid, child_node_id, child_prefix_len,
                                      &partitioned_entries[bucket_offsets[i]],
                                      bucket_counts[i],
                                      fanout, bits_per_split, split_threshold);
    }

    // Step 4: Flip the split node from leaf to internal
    UPDATE ariabc_internal.merkle_node
       SET is_leaf=false,
           tuple_count=total_split_count,
           hash=XOR(all bucket hashes)
     WHERE index_oid=$1 AND node_id=$2 AND prefix_len=$3;

    // Step 5: Propagate hash delta to ancestors
    if (prefix_len > 0) {
        hash_delta = new_total_hash XOR old_hash;  // what changed
        count_delta = total_split_count - old_count;
        propagate_hash_to_ancestors_atomic(index_oid, node_id, prefix_len,
                                           &hash_delta, count_delta,
                                           bits_per_split);
    }
}
```

### 4.4 Concrete Split Example

Starting state: root leaf with `prefix_len=0`, 40 tuples, `split_threshold=32`.

Suppose 40 rows distribute as: bucket0=12, bucket1=8, bucket2=15, bucket3=5.

```
Before split:
  merkle_node: (oid, 0x0000..., 0, is_leaf=true, 40, HASH_ROOT)

After split:
  merkle_node: (oid, 0x0000..., 0,  is_leaf=false, 40, XOR(4 buckets))
  merkle_node: (oid, 0x0000..., 2,  is_leaf=true,  12, HASH_B0)  ← bucket 0 (bits=00)
  merkle_node: (oid, 0x4000..., 2,  is_leaf=true,   8, HASH_B1)  ← bucket 1 (bits=01)
  merkle_node: (oid, 0x8000..., 2,  is_leaf=true,  15, HASH_B2)  ← bucket 2 (bits=10)
  merkle_node: (oid, 0xC000..., 2,  is_leaf=true,   5, HASH_B3)  ← bucket 3 (bits=11)
```

Child `node_id` derivation for bucket 2 (bits=`0b10`):
- Start: `node_id=0x0000...`, prefix_len=0
- `merkle_bytea_extend(child, node_id, 0, 0b10, 2)`
- bit 0 (MSB of byte 0) = 1 → `0x80...`
- bit 1 (bit 6 of byte 0) = 0 → stays `0x80...`
- Result: `child_node_id = 0x8000000000000000`

Bucket 2 has 15 rows, still below threshold=32, so no further split.

### 4.5 Split Range Guard

While a split is in progress, newly arriving deltas for the same range are skipped:

```c
// merkleapply.c  lines 962–976
static bool
merkle_is_in_split_range(Oid index_oid, const uint8 *routing_key)
{
    for (i = 0; i < num_active_split_ranges; i++)
        if (active_split_ranges[i].index_oid == index_oid &&
            memcmp(routing_key, active_split_ranges[i].lower, 8) >= 0 &&
            memcmp(routing_key, active_split_ranges[i].upper, 8) <= 0)
            return true;
    return false;
}

// Used in synchronous path:
// merkleapply.c  line 2681
if (merkle_is_in_split_range(index_oid, routing_key))
    return;  // skip this delta — split will recompute the leaf
```

This prevents a race where a delta arrives for a key that's in the middle of being
redistributed across new children. The `do_split` path fetches a fresh snapshot
from the heap, so those rows will be correctly included in the child buckets.

---

## 5. Route Cache — `merkle_route_cache`

The synchronous path maintains a 1024-slot flat hash cache to avoid re-traversing the
tree for hot keys:

```c
// merkleapply.c  lines 2220–2231
#define MERKLE_ROUTE_CACHE_SLOTS 1024
typedef struct MerkleRouteCacheEntry {
    bool        valid;
    Oid         index_oid;
    RelFileNode index_rnode;     // physical identity: survives DROP+CREATE with same OID
    uint8       routing_key[8];
    uint8       leaf_node_id[8];
    int         leaf_prefix_len;
} MerkleRouteCacheEntry;

static MerkleRouteCacheEntry merkle_route_cache[MERKLE_ROUTE_CACHE_SLOTS];
```

Cache lookup uses a simple multiplicative hash:

```c
// merkleapply.c  lines 2233–2246
static uint32
merkle_route_cache_hash(Oid index_oid, const RelFileNode *rnode,
                        const uint8 *routing_key)
{
    uint32 hash = index_oid;
    hash = hash * 33U + rnode->spcNode;
    hash = hash * 33U + rnode->dbNode;
    hash = hash * 33U + rnode->relNode;
    for (i = 0; i < 8; i++)
        hash = hash * 33U + routing_key[i];
    return hash;
}
```

A cache entry is **invalidated** when the leaf update returns 0 rows (meaning the node
was split since the cache was populated):

```c
// merkleapply.c  lines 2713–2718
merkle_route_cache_invalidate(index_oid, routing_key);
if (!merkle_node_is_leaf(index_oid, leaf_node_id, leaf_prefix_len))
    continue;  // node was split — retry route resolution
```

The entire index's cache slots are cleared after any split:
```c
// line 1065
merkle_route_cache_clear_index(index_oid);
```

---

## Summary of Part 2

```
INSERT path:
  merkleInsert()
    → merkle_compute_route()     (route hash = first 8 bytes of BLAKE3 of key cols)
    → merkle_compute_row_hash()  (tuple hash = BLAKE3 of all cols)
    → merkle_stage_delta_event() (XOR-accumulate per-tx delta map)
  PRE_COMMIT:
    → merkle_apply_staged_deltas_synchronously()
       → merkle_apply_staged_synchronous_safe()
          → apply_leaf_event() for each delta

SPLIT path (deferred, post all-leaf-events):
  do_split()
    → fetch all rows in [lower, upper] range from heap via SPI
    → merkle_do_split_in_memory()
       → classify rows into fanout=4 buckets by next 2 routing bits
       → INSERT fanout child nodes (is_leaf=true)
       → UPDATE parent node (is_leaf=false)
       → recurse if any child > split_threshold
       → propagate_hash_to_ancestors_atomic() for hash consistency
    → register split range guard
    → clear route cache for this index
```

**→ Part 3** covers: Merge algorithm, ancestor propagation (the core XOR walk up the
tree), crash-safe delta serialization, the ordered Raft applier, and verification.
