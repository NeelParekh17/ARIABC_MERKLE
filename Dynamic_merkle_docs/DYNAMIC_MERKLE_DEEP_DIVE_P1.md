# Dynamic Merkle Tree — Deep Dive (Part 1 of 3)
# Core Concepts, Catalog Schema, Hashing, and Prefix Routing

> **Files covered in this part**
> - `src/include/access/merkle.h`
> - `src/backend/access/merkle/merkleutil.c`
> - `scripts/distributed/sql/raft_apply_ledger_schema.sql` (lines 142–151)

---

## 1. What Is the Dynamic Merkle Tree?

The AriaBC Merkle index is a **custom PostgreSQL access method** (`amname = 'merkle'`) that
maintains a **4-ary radix tree** whose leaves store XOR-aggregated BLAKE3-256 hashes of every
heap row. It is *not* a search index — the planner cost estimate is set astronomically high
(`1.0e10`) so the optimizer never chooses it for queries. Its sole purpose is **data integrity
verification**: two replicas are consistent if and only if their root hashes match.

The tree is **dynamic**: it splits when a leaf gets too full and merges when sibling leaves
become too sparse together, keeping the tree balanced without any offline rebuild.

---

## 2. Catalog Schema — `ariabc_internal.merkle_node`

Every node (both internal and leaf) lives in one PostgreSQL table:

```sql
-- raft_apply_ledger_schema.sql  lines 142–151
CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_node (
    index_oid    oid      NOT NULL,   -- which Merkle index this belongs to
    node_id      bytea    NOT NULL,   -- 8-byte prefix value (see §4)
    prefix_len   smallint NOT NULL,   -- how many bits of node_id are significant
    is_leaf      boolean  NOT NULL,   -- true = leaf, false = internal node
    tuple_count  bigint   NOT NULL DEFAULT 0,
    hash         bytea    NOT NULL,   -- 32-byte BLAKE3 XOR aggregate
    PRIMARY KEY (index_oid, node_id, prefix_len)
);
CREATE INDEX IF NOT EXISTS merkle_node_prefix_idx
    ON ariabc_internal.merkle_node (index_oid, prefix_len);
```

### Key design decisions

| Column | Why |
|---|---|
| `node_id` (bytea, 8 bytes) | First 8 bytes of the 256-bit BLAKE3 routing digest; 64-bit prefix space |
| `prefix_len` (smallint) | Number of *significant* bits in `node_id`; root always has `prefix_len = 0` |
| `is_leaf` | Determines whether DML updates this node directly or descends to a child |
| `tuple_count` | Snapshot of row count at last split/merge; used for threshold decisions |
| `hash` (bytea, 32 bytes) | XOR of all `BLAKE3(row)` hashes for every row under this node |

### Index metadata page — `MerkleMetaPageData`

Stored at block 0 of the index file (NOT in the catalog):

```c
// src/include/access/merkle.h  lines 134–144
typedef struct MerkleMetaPageData
{
    uint32  version;              /* MERKLE_VERSION = 9 (current) */
    Oid     heapRelid;
    int32   fanout;               /* default 4 */
    int32   split_threshold;      /* default 32 */
    int32   merge_threshold;      /* default  8 */
    uint32  routeFormatVersion;   /* = 4 */
    uint32  rowHashFormatVersion; /* = 1 */
    uint64  baselineApplySeq;
} MerkleMetaPageData;
```

Configured via `WITH (fanout=4, split_threshold=32, merge_threshold=8)` on CREATE INDEX.

---

## 3. Constants That Govern Tree Geometry

```c
// src/include/access/merkle.h  lines 88–93
#define DYNAMIC_MERKLE_FANOUT   4   /* children per internal node */
#define BITS_PER_SPLIT          2   /* log2(4) = 2 bits consumed per level */
#define SPLIT_THRESHOLD        32   /* split leaf when tuple_count > 32 */
#define MERKLE_MERGE_THRESHOLD  8   /* merge siblings when combined < 8 */
#define MAX_PREFIX_LEN         60   /* tree depth cap (60 bits = 30 levels) */
#define MERKLE_HASH_BYTES      32   /* BLAKE3-256 output = 32 bytes */
```

`BITS_PER_SPLIT = 2` means each tree level consumes 2 bits of the routing key.
With `MAX_PREFIX_LEN = 60`, the tree can be at most **30 levels deep**.

```c
// src/include/access/merkle.h  lines 288–295
static inline int
merkle_bits_per_split_for_fanout(int fanout)
{
    int bits = 0;
    while ((1 << bits) < fanout && bits < 8)
        bits++;
    return bits > 0 ? bits : 2;
}
// For fanout=4: 1<<0=1<4, 1<<1=2<4, 1<<2=4 not <4 => bits=2
```

---

## 4. Two-Layer Hash System

Two independent BLAKE3 hashes exist for every row:

| Hash | Purpose | Columns covered |
|---|---|---|
| **Route hash** (`route_digest`) | Determines *which leaf* a row belongs to | Index key columns only |
| **Tuple hash** | Integrity fingerprint of the row | ALL heap columns |

### 4.1 Route Hash — `merkle_compute_canonical_route_digest()`

```c
// src/backend/access/merkle/merkleutil.c  lines 380–423
static void
merkle_compute_canonical_route_digest(Datum *values, bool *isnull, int nkeys,
                                      TupleDesc tupdesc,
                                      uint8 digest[MERKLE_HASH_BYTES])
{
    blake3_hasher hasher;
    static const uint8 magic[] = {'A','R','I','A','R','O','U','T'}; // "ARIAROOT"

    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, magic, sizeof(magic));
    merkle_hash_uint32(&hasher, MERKLE_ROUTE_FORMAT_VERSION); // = 4
    merkle_hash_uint32(&hasher, (uint32) nkeys);

    for (i = 0; i < nkeys; i++) {
        // Schema descriptor: position, type OID, typmod, null flag
        merkle_hash_uint32(&hasher, (uint32)(i + 1));
        merkle_hash_uint32(&hasher, (uint32) attr->atttypid);
        merkle_hash_uint32(&hasher, (uint32) attr->atttypmod);
        blake3_hasher_update(&hasher, &null_flag, 1);
        if (!isnull[i]) {
            // type's binary send() output — GUC-independent wire format
            encoded = OidSendFunctionCall(typsend, values[i]);
            length  = VARSIZE_ANY_EXHDR(encoded);
            merkle_hash_uint32(&hasher, length);
            blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
        }
    }
    blake3_hasher_finalize(&hasher, digest, MERKLE_HASH_BYTES);
}
```

**Why `OidSendFunctionCall` / binary send?**
The binary send function (e.g., `int4send`, `textsend`) produces PostgreSQL's canonical
wire representation, independent of `DateStyle`, `TimeZone`, `lc_numeric`, or any GUC.
This makes the route hash deterministic across all cluster nodes regardless of locale.

The result:

```c
// src/include/access/merkle.h  lines 164–168
typedef struct MerkleRoute {
    uint8  route_digest[MERKLE_HASH_BYTES]; // full 256-bit BLAKE3 digest
    uint64 static_route_value;              // first 8 bytes as uint64
} MerkleRoute;
```

Only the **first 8 bytes** (`route_digest[0..7]`) are stored as `node_id` in the catalog.
These 64 bits address `60 / 2 = 30` addressable levels.

### 4.2 Tuple Hash — `merkle_hash_slot_canonical_desc()`

```c
// src/backend/access/merkle/merkleutil.c  lines 186–247
void
merkle_hash_slot_canonical_desc(TupleDesc tupdesc, TupleTableSlot *slot,
                                 MerkleHash *result)
{
    blake3_hasher hasher;
    static const uint8 magic[] = {'A','R','I','A','M','R','K','L'}; // "ARIAMRKL"

    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, magic, sizeof(magic));
    merkle_hash_uint32(&hasher, MERKLE_ROW_HASH_FORMAT_VERSION); // = 1
    merkle_hash_uint32(&hasher, live_attributes); // count of non-dropped cols

    for each non-dropped attribute {
        merkle_hash_uint32(&hasher, attnum);
        merkle_hash_uint32(&hasher, atttypid);
        merkle_hash_uint32(&hasher, atttypmod);
        null_flag = isnull ? 1 : 0;
        blake3_hasher_update(&hasher, &null_flag, 1);
        if (!isnull) {
            encoded = OidSendFunctionCall(typsend, val);
            length  = VARSIZE_ANY_EXHDR(encoded);
            merkle_hash_uint32(&hasher, length);
            blake3_hasher_update(&hasher, VARDATA_ANY(encoded), length);
        }
    }
    blake3_hasher_finalize(&hasher, result->data, MERKLE_HASH_BYTES);
}
```

Differences from route hash:
- Magic = `ARIAMRKL` (not `ARIAROOT`)
- Covers **all** non-dropped columns (not just index key columns)
- Separate format version (`MERKLE_ROW_HASH_FORMAT_VERSION = 1`)

---

## 5. The XOR Aggregate — How Hashes Combine

Each leaf stores: `hash = XOR( tuple_hash(r)  for all r in leaf )`

**Why XOR?**
- Commutative + associative: insertion order does not matter
- Self-inverse: inserting then deleting the same row returns to original hash
- O(1) update: `new_hash = old_hash XOR tuple_hash(row)`

```c
// src/backend/access/merkle/merkleutil.c  lines 117–124
void
merkle_hash_xor(MerkleHash *dest, const MerkleHash *src)
{
    int i;
    for (i = 0; i < MERKLE_HASH_BYTES; i++)
        dest->data[i] ^= src->data[i];
}
```

Internal nodes also aggregate via XOR, so the root hash equals the XOR of every
single tuple hash in the table. `merkle_verify_index()` confirms this by scanning
the heap and comparing XOR results to the stored root.

---

## 6. Prefix / node_id Encoding — The Bit-Addressed Trie

Every node is identified by `(node_id: bytea[8], prefix_len: int)`.

### 6.1 Reading bits — `merkle_next_bits()`

```c
// src/include/access/merkle.h  lines 301–315
static inline uint8
merkle_next_bits(const uint8 *key_hash, int prefix_len, int w)
{
    uint32 res = 0;
    for (int i = 0; i < w; i++)
    {
        int bit_idx  = prefix_len + i;      // absolute bit position in key_hash
        int byte_pos = bit_idx / 8;
        int bit_pos  = 7 - (bit_idx % 8);  // MSB-first within each byte
        uint8 bit    = (key_hash[byte_pos] >> bit_pos) & 1;
        res = (res << 1) | bit;
    }
    return (uint8) res;
}
```

**Example**: route key starts with `0b10110100...`, `prefix_len=0, w=2`:
- bit 0 = byte[0] bit7 (MSB) = `1`
- bit 1 = byte[0] bit6 = `0`
- Result = `0b10` = 2 → child bucket index **2** out of {0,1,2,3}

### 6.2 Writing bits — `merkle_bytea_extend()`

```c
// src/include/access/merkle.h  lines 317–333
static inline void
merkle_bytea_extend(uint8 *result_node_id, const uint8 *node_id,
                    int prefix_len, uint8 bits, int w)
{
    memcpy(result_node_id, node_id, 8);
    for (int i = 0; i < w; i++)
    {
        int bit_idx  = prefix_len + i;
        int byte_pos = bit_idx / 8;
        int bit_pos  = 7 - (bit_idx % 8);
        uint8 bit    = (bits >> (w - 1 - i)) & 1;
        if (bit)
            result_node_id[byte_pos] |=  (1 << bit_pos);
        else
            result_node_id[byte_pos] &= ~(1 << bit_pos);
    }
}
```

Copies parent `node_id` then sets bits `[prefix_len .. prefix_len+w-1]` to the
child bucket index. Used to build child `node_id` values during split and routing.

### 6.3 Upper bound of a prefix range — `merkle_bytea_upper_bound()`

```c
// src/include/access/merkle.h  lines 335–356
static inline void
merkle_bytea_upper_bound(uint8 *result_upper, const uint8 *node_id, int prefix_len)
{
    int full_bytes = prefix_len / 8;
    int rem        = prefix_len % 8;

    memcpy(result_upper, node_id, 8);
    if (rem > 0) {
        uint8 mask = 0xFF >> rem;       // fill "free" bits with 1s
        result_upper[full_bytes] |= mask;
        first_free = full_bytes + 1;
    } else
        first_free = full_bytes;

    for (i = first_free; i < 8; i++)
        result_upper[i] = 0xFF;
}
```

For `prefix_len=4` and `node_id[0] = 0b10110000`:
- `mask = 0xFF >> 4 = 0x0F`
- `node_id[0] | 0x0F = 0b10111111`
- bytes 1–7 = `0xFF`

This gives the upper bound for range queries:
`WHERE node_id BETWEEN lower AND upper` — used in both split and merge.

### 6.4 Walking to parent — `merkle_parent_of()`

```c
// src/include/access/merkle.h  lines 358–375
static inline int
merkle_parent_of(uint8 *parent_node_id, const uint8 *node_id,
                 int prefix_len, int w)
{
    int parent_prefix_len = prefix_len - w;
    memcpy(parent_node_id, node_id, 8);
    if (parent_prefix_len < 0) parent_prefix_len = 0;

    // zero all bits from parent_prefix_len onward
    for (int i = parent_prefix_len; i < 64; i++) {
        int byte_pos = i / 8;
        int bit_pos  = 7 - (i % 8);
        parent_node_id[byte_pos] &= ~(1 << bit_pos);
    }
    return parent_prefix_len;
}
```

Strips the last `w=2` bits, giving parent `node_id` with `prefix_len - 2`.

---

## 7. Metapage Cache — Avoiding Repeated Buffer Pins

Reading the metapage (block 0) on every delta is expensive. A 4-slot
transaction-local cache avoids redundant buffer pins:

```c
// src/backend/access/merkle/merkleutil.c  lines 49–109
#define MERKLE_META_CACHE_SLOTS 4
typedef struct MerkleMetaCacheEntry {
    Oid relid;
    int fanout;
    int split_threshold;
    int merge_threshold;
} MerkleMetaCacheEntry;
static MerkleMetaCacheEntry merkle_meta_cache[MERKLE_META_CACHE_SLOTS];
```

Cache is cleared at transaction end (protects against REINDEX geometry changes):

```c
// lines 67–77
static void
merkle_meta_cache_xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT || ...)
        merkle_meta_cache_clear();
}
```

---

## Summary of Part 1

```
Two independent BLAKE3 hashes per row:
  route_digest (magic=ARIAROOT, key cols)
    ├── first 8 bytes → node_id  (trie address for this row's leaf)
    └── 64 bits → up to 30 levels at 2 bits per level

  tuple_hash (magic=ARIAMRKL, all cols)
    └── XOR'd into leaf.hash, propagated up via XOR delta

merkle_node table PK: (index_oid, node_id, prefix_len)
  ├── is_leaf      → routing decision
  ├── tuple_count  → split/merge threshold check
  └── hash         → 32-byte XOR aggregate of all rows in subtree
```

---

**→ Part 2** covers: Index Build (`merkleBuild`), Insert path (`merkleInsert` →
`apply_leaf_event`), and the full **split** algorithm (`do_split` /
`merkle_do_split_in_memory`).

**→ Part 3** covers: **Merge** algorithm (`do_merge_check`), ancestor hash
propagation, route cache, delta staging pipeline, and crash-safe recovery.
