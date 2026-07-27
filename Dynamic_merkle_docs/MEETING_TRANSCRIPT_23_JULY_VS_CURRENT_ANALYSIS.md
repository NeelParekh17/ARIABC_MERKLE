# July 23 transcript versus the current dynamic Merkle implementation

Date: 2026-07-27

This report explains the July 23 meeting transcript in plain language, compares
it with the live native v8 code, and gives a recommendation for the guide
meeting. The transcript is [Meeting_transcript_23rd_July.txt](./Meeting_transcript_23rd_July.txt).
The current implementation description is
[CURRENT_DYNAMIC_MERKLE_ARCHITECTURE.md](./CURRENT_DYNAMIC_MERKLE_ARCHITECTURE.md).

## Decision first

Continue with the current native v8 implementation as the correctness and
distributed-recovery baseline. Do not replace it with the transcript proposal
before a focused experiment proves that PostgreSQL can provide the required
full-tuple-hash index-only scans and that the new lookup model preserves the
existing transaction, recovery, and replica-equality contracts.

Adopt three ideas from the transcript immediately at the design/documentation
level:

1. explain the Merkle tree separately from the tuple-location/index structure;
2. distinguish the tuple-content hash from the key/route used to locate an
   item; and
3. treat logical-range equality as the semantic goal, while measuring physical
   topology separately rather than confusing the two.

Consider the transcript's full-hash B-tree/index-only lookup as an optional
performance variant. It should be prototyped beside the current implementation,
not substituted into the correctness path yet.

## What the transcript is actually proposing

The transcript contains several ideas that are discussed back and forth. The
final proposal near the end is much simpler than the early “store keys inside
the dynamic leaf” design.

### Transcript's final model

```text
                    user table row
                          |
                          v
              full tuple-content hash H(row)
                          |
                          v
          B-tree / functional index / generated column
          ordered by the COMPLETE hash value
                          |
                          v
         prefix lookup is a range scan over that B-tree

       separate native Merkle relation/tree
       ------------------------------------
       (prefix, is_leaf, tuple_count, hash/commitment)
```

The transcript says that a dynamic leaf should not need to store every key.
Instead, the Merkle tree records the prefix and commitment, while the separate
ordered index lets the system find all rows whose full tuple hash begins with a
given prefix. A prefix of `b7` becomes the numeric range `[b700..., b7ff...]`
over the full 256-bit hash index.

### What problem this solves

The transcript is worried about this operation:

```text
leaf prefix P overflows
        |
        +--> P becomes an internal node
        +--> P0 and P1 become new children
        +--> every tuple formerly mapped to P may need a longer mapping
```

If tuple-to-leaf membership were represented by a computed prefix column, the
split could require updating many table/index entries. The transcript proposes
leaving each row's full tuple hash unchanged and changing only the range used
to retrieve it:

```text
before split:  scan full-hash index for prefix P
after split:   scan full-hash index for prefixes P0 and P1

the user rows and the full-hash B-tree entries do not move because of the
logical split
```

The relevant discussion is in transcript lines 117-131 and 191-207, with the
final “use a generated column or functional index and index-only scan” proposal
at lines 207-246.

## The current implementation's model

The live native v8 path uses a different separation:

```text
indexed key columns
        |
        v
canonical key bytes --BLAKE3--> route_digest
        |                              |
        |                              +--> partition_id = first route value % P
        |                              +--> prefix navigation inside partition
        |
        +--> stored in native leaf item record

all heap columns
        |
        v
canonical row bytes --BLAKE3--> tuple_hash
                                   |
                                   +--> stored in the same native leaf item
                                   +--> contributes to data/content commitments
```

The native leaf item contains the canonical key, route digest, and tuple hash.
The leaf does not need to consult a user-table functional index to find its
items. On a mutation, `native_apply_batch_node()` loads the affected leaf's
bounded item vector, applies old/new transitions, and rebuilds only the affected
path. The records are append-only and the new root is published at PRE_COMMIT.

The key live definitions are:

- `MerkleNativeItemRecord` / packed v8 items: [merkle.h:319](/work/ARIABC/AriaBC/src/include/access/merkle.h:319)
- canonical key and route selection: [merkle_compute_dynamic_item_identity()](/work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c:594)
- tuple hashing from the heap row: [merkle_compute_row_hash()](/work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c:285)
- leaf item application: [native_apply_items()](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:2541)
- affected-path rebuild: [native_apply_batch_node()](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:2703)

## Side-by-side comparison

| Question | July 23 transcript proposal | Current native v8 implementation |
| --- | --- | --- |
| What is the Merkle tree storing? | Prefix/node commitment, leaf flag, count; preferably no row keys in leaves. | Native root, internal/leaf records, and packed canonical-key/route/tuple-hash items. |
| What identifies row content? | Full tuple hash, indexed separately in a B-tree-like structure. | Full row hash stored in each native leaf item and included in commitments. |
| What identifies location? | Prefix range over the full tuple-hash index. | Canonical indexed key -> route digest -> partition and prefix path. |
| What happens on a logical split? | Change the prefix range queried; avoid rewriting row/index mappings. | Read bounded items from the old leaf, partition them by route bits, append new leaf/internal records, publish a new root. |
| Does the user table need a new generated hash column? | Probably yes, unless a functional index can support index-only scans. | No schema change; row hash is computed inside the Merkle executor/AM path. |
| Does an update change the Merkle membership? | Yes: tuple hash changes, so old hash is removed and new hash inserted in the hash index/range. | Yes: old item transition plus new item transition; route may stay or move depending on key changes. |
| Does a non-key update change the route? | No if route is based on a stable key; tuple hash changes. | No if indexed key is unchanged; tuple hash changes. |
| Does a key update change the route? | The transcript eventually emphasizes tuple hash, so this is underspecified; a stable row-key locator would need a separate policy. | Yes, because canonical indexed key is the route identity. |
| How are items found for recovery? | Prefix range query over the full-hash B-tree. | Native range/item traversal over immutable native records. |
| Physical tree equality required? | Transcript says logical structure is the semantic comparison. | Current distributed runner checks logical roots, physical topology digest, leaf-item digest, layout, fanout, and native verification. |
| Main cost | Very cheap split metadata changes, but potentially expensive range scans and schema/index coupling. | Bounded local path rebuild and native item storage, but extra Merkle index bytes and item-record maintenance. |

## What is already the same

The transcript is correcting a real conceptual confusion, and the current code
already agrees with the correction.

### Tuple hash is not the routing key

The transcript spends significant time resolving whether the tree is indexed by
the key hash or the tuple hash. The correct model is two values:

```text
route identity:  canonical indexed key -> route digest
content proof:   complete heap row   -> tuple hash
```

This is exactly what the current code implements. A non-key column update keeps
the route but changes the tuple hash; the controlled live probe showed the item
count staying at 195 while the combined root changed. A key update can change
both route and tuple hash.

### A split must preserve the row-to-content relationship

The transcript is also right that the Merkle tree alone does not magically
return a user tuple. A comparison that identifies a changed commitment must
eventually obtain the corresponding row/key/hash records. The current native
leaf item deliberately keeps enough canonical key and tuple-hash information to
perform that bounded lookup without a second user-table index.

### Splits should be local and bounded

The transcript worries about a 1,000-item leaf. The current configuration does
not allow that: the accepted v8 contract uses bounded leaves, normally
`leaf_capacity=32` and in the executed reproduction `leaf_capacity=6`.
The current split code reads the affected leaf's items, not the whole table.
Therefore, the transcript's worst-case “update 1,000 mappings” problem is not
the current implementation's behavior. It remains a valid concern if someone
raises leaf capacity or chooses a separate mapping index with unbounded buckets.

## What is genuinely different

### Difference 1: where the row locator lives

The transcript wants this:

```text
Merkle node:       prefix -> commitment
User/B-tree index: full_tuple_hash -> row/key/TID
```

The current v8 path wants this:

```text
Native leaf item:  route + canonical key + tuple_hash
Merkle node:       summary over those items
```

This is the largest architectural difference. The transcript's design moves
the item lookup responsibility into PostgreSQL's ordinary indexing machinery;
the current design keeps it inside the native Merkle index format.

### Difference 2: eager item rewrite versus lazy prefix lookup

Current v8 is eager with respect to the affected leaf: after a split, it writes
new item chunks for the bounded set of items in the changed subtree. It is lazy
with respect to the rest of the tree: unaffected records are reused by locator.

The transcript proposal is lazy with respect to item-to-prefix mapping: it
would not rewrite item mappings after a split. The lookup would use the current
prefix length and a range scan over a full-hash B-tree.

### Difference 3: logical equality versus topology equality

The transcript says the replicas need not have identical physical shape as
long as their logical content/range commitments agree. The current code has
both concepts:

```text
data_root/content_xor       topology-independent item commitment
structure_root/structure_hash topology-sensitive tree commitment
combined_root                commits to both
```

The current recovery and distributed runners currently require more than the
transcript's minimum semantic idea. They check physical topology and leaf-item
digests as additional acceptance gates. That is a policy choice, not proof that
logical equality is impossible.

If we adopt the transcript's semantic rule, the safe change is to add a clearly
named logical-equality gate and retain physical topology as a diagnostic or
strict mode. Removing the current physical gate without defining recovery
alignment rules would reduce evidence, not improve the implementation.

## What is better in the transcript proposal

### 1. It gives a cleaner paper-level abstraction

The transcript's best conceptual contribution is:

```text
Merkle tree = range/prefix -> commitment
row lookup  = separate ordered full-hash index
```

That makes the mathematical Merkle structure easier to explain and separates
cryptographic aggregation from PostgreSQL row lookup. This should be adopted in
the guide/paper explanation even if the implementation continues storing items
inside native leaves.

### 2. It may reduce split write amplification

With a valid covering index-only scan, a split could avoid rewriting a mapping
column for every affected row. The Merkle metadata would change while the user
table and its full-hash index entries remained unchanged.

This could matter for very large leaves or workloads where split frequency is
high. It is not yet demonstrated in this repository.

### 3. It could simplify the Merkle leaf record

If the external index is authoritative and reliable, native leaves might only
need prefix, count, item commitment, and child/range metadata. That could reduce
native item storage and some leaf rebuild work.

This benefit must be measured against the cost of scanning and grouping the
external index entries during split/recovery.

### 4. It encourages index-only lookup experiments

The transcript correctly identifies the critical performance question: can the
full-hash ordered index return the required hash/key information without heap
fetches? If not, the proposal loses its main advantage because split handling
falls back to random heap reads.

## What is better in the current implementation

### 1. It has a complete transactional authority

The current path already has a native v8 record format, page-generation-safe
locators, checksums, XID-visible roots, deterministic transition ordering, and
PRE_COMMIT publication. A generated column plus B-tree would introduce another
stateful structure that must be updated atomically with heap DML and Merkle
roots.

### 2. It handles arbitrary indexed keys and row layouts internally

The current canonical-key serializer and row-hash serializer run inside the
backend and already enforce format versions and key-size limits. The transcript
proposal would need a SQL-visible immutable full-row hash function or a stored
column maintenance path. That raises questions about dropped columns, nulls,
types, typmods, schema changes, generated-column restrictions, and versioned
hash semantics.

### 3. It bounds the split work by leaf capacity

The current leaf is deliberately bounded by both item count and encoded bytes.
With the accepted capacity 32, a split does not touch a million-row table or a
1,000-row mapping set. It rewrites only the affected path and bounded leaf item
chunks.

### 4. It preserves self-contained recovery evidence

Native range traversal, leaf items, root commitments, and verification all use
one durable index generation. The current runners can compare logical roots,
physical topology, leaf-item assignments, sequence provenance, and full native
verification. The transcript proposal would need an equivalent consistency
protocol between the Merkle relation and the user-table hash index.

## The transcript's unresolved issues

### A. “Full tuple hash” is not enough to identify a row

Two different rows can theoretically have the same 256-bit hash. Even ignoring
cryptographic collision probability, duplicate equal rows can exist. A lookup
by tuple hash therefore returns a set, not necessarily one tuple. The external
index must retain a stable discriminator such as the primary key, canonical key,
or TID, and the comparison protocol must define how duplicates are ordered.

The transcript sometimes says “full hash maps to key” and sometimes says “the
key is not part of the Merkle structure.” Those are different layers and must be
written separately in a final design.

### B. A generated column would create a new DML contract

The current row hash is computed by backend code over the complete tuple. To use
the transcript design, we need to establish all of the following:

```text
SQL-visible immutable hash function exists
        |
        +--> generated/stored column recomputes on every row update
        +--> B-tree index is covering for split/recovery lookup
        +--> hash format version survives schema/index upgrades
        +--> NULL/type/typmod/dropped-column semantics match backend hashing
        +--> replica apply path preserves the same value
```

The transcript says “generated column or functional index,” but does not specify
which one, how it is maintained under the AriaBC executor, or how it is tied to
the Merkle format version. This is an implementation project, not a free reuse
of the current `merkle_compute_row_hash()` function.

### C. Index-only scan is an acceptance requirement, not an assumption

The proposal's performance depends on this property:

```text
prefix range -> B-tree leaf pages -> required hash/key data
                              no heap fetches
```

If the scan must visit heap pages to obtain the row/key/TID, the main argument
against native leaf items disappears. The prototype must measure heap fetches,
index pages, split latency, and bytes written for the same leaf capacity and
workload.

### D. Logical-only equality needs a complete recovery algorithm

If two replicas have different physical shapes, the recovery code cannot pair
nodes by page, prefix length, or allocation order. It must align logical ranges:

```text
compare range R
    |
    +--> if commitments equal: stop
    +--> if one side is a leaf and other is internal:
    |        expand the leaf's range against the internal children
    +--> recurse on overlapping logical child ranges
    +--> fetch candidate rows only for irreducible mismatches
```

The current recovery analysis already describes this kind of logical-range
alignment for prefix-compressed native trees. The transcript identifies the
principle but does not define the complete algorithm or the repair publication
contract.

### E. Split and merge decisions must remain deterministic

The current native implementation derives split/merge changes from ordered
transitions and the visible root. An external B-tree design still needs exact
rules for:

- which prefix is split;
- how many additional bits are consumed;
- byte and tuple thresholds;
- merge hysteresis;
- duplicate full-hash/key ordering;
- concurrent split/update conflict handling; and
- crash recovery between B-tree maintenance and Merkle root publication.

The transcript's “just query a narrower range” is logically attractive, but a
production implementation still needs a durable state machine for the tree's
`is_leaf`, prefix, count, and child commitments.

## Recommended implementation strategy

### Keep as-is now: current native v8 correctness path

Keep these as the authoritative path:

```text
synchronous_cow
native v8 index pages
canonical key + route digest in native items
tuple hash in native items
copy-on-write affected-path updates
native roots and verification
current distributed sequence/provenance gates
```

This is already implemented and tested. Replacing it immediately would trade a
working, crash-safe path for an unvalidated schema/index coupling.

### Adopt now: conceptual and documentation changes

For the guide and paper, explain the layers in this order:

```text
1. Merkle commitment: prefix/range -> aggregate hash
2. Content identity: full tuple -> tuple hash
3. Location identity: indexed key -> route/prefix
4. Row lookup: native leaf item in current v8
   or external full-hash index in the transcript variant
```

Also state explicitly that logical equality is the semantic target, while
physical topology equality is currently an additional strict acceptance gate.

### Prototype later: transcript's external full-hash lookup

Build a non-authoritative prototype with a separate table/index or test relation.
Do not change the production native path first. The prototype should answer:

```sql
-- Illustrative only; not an approved production command.
CREATE INDEX ... ON usertable_small (full_tuple_hash, primary_key)
     INCLUDE (route/key data needed by the lookup);
```

Measure, for a leaf split:

| Measurement | Required result |
| --- | --- |
| index-only scan heap fetches | zero or acceptably near zero |
| rows scanned | bounded by the old prefix range |
| split bytes written | lower than current native leaf rewrite |
| split latency | lower at equal capacity and workload |
| update latency | no regression from maintaining the hash column/index |
| crash/restart correctness | roots and lookup state recover consistently |
| replica equality | logical roots match; strict topology behavior understood |

Only if this experiment wins should we decide whether to add an optional
external lookup mode to the native v8 design.

## What to tell your guide today

Use this concise explanation:

> The July 23 transcript proposes separating the Merkle commitment from row
> lookup. The Merkle tree would store prefix commitments, while a B-tree on the
> complete tuple hash would answer prefix-range lookups without rewriting row
> mappings after a split. Our current native v8 implementation solves the same
> bounded-split problem differently: it stores canonical keys, route digests,
> and tuple hashes in immutable native leaf chunks and rebuilds only the affected
> copy-on-write path. The transcript's separation is a good conceptual and
> possible performance optimization, but it is not yet a complete transactional
> design. We should continue with native v8 for correctness and distributed
> recovery, adopt the transcript's logical-vs-physical and tuple-hash-vs-route
> distinctions in the documentation, and prototype the full-hash index-only
> lookup before changing the authority model.

## Final recommendation

```text
                         July 23 transcript
                                  |
                  +---------------+----------------+
                  |                                |
          adopt as explanation              prototype as optimization
          and design principle              only after measurement
                  |                                |
                  +---------------+----------------+
                                  v
                       current native v8 authority
                       remains the production path
```

Do not present the transcript as “the current implementation.” Present it as a
promising alternative lookup/storage formulation that explains a real tradeoff:

```text
current:  more self-contained native item storage,
          bounded local rewrite, stronger existing proof path

transcript: simpler Merkle abstraction,
            potentially less split rewrite,
            but new B-tree/generated-column and recovery obligations
```

That is the honest technical position supported by the transcript and the live
repository.
