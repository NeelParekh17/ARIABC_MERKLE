# Updated decisions for the dynamic Merkle architecture

Your corrections improve the design substantially. After checking them against the meeting discussion and the live repository, this is the revised conclusion:

```text
Issue 1: resolved using sparse child-range descriptors
Issue 2: root represents data only
Issue 3: use 256-bit authoritative tuple hashes
Issue 4: current static Merkle sequencing is deterministic,
         but the dynamic delta format must be extended
Issue 5: split=32, merge=8 gives excellent hysteresis
Issue 6: recursively split until every leaf <=32
Issue 7: use optimized binary physical splitting
Issue 8: use a materialized leaf-item relation
Issue 9: compare logical ranges, not physical shapes
Issue 10: dynamic structural changes must be transactional and replayable
```

---

# 1. Your two-pointer idea is correct, with one necessary adjustment

You are proposing this after splitting a leaf:

```text
Logical capacity of node L = 32 slots

Only two physical children currently exist:

slot range  0..15  -> A
slot range 16..31  -> B

Remaining physical child entries are null
```

Conceptually:

```text
                  Internal node L
                32 logical positions
                         |
              +----------+----------+
              |                     |
         range 0..15            range 16..31
              |                     |
              v                     v
              A                     B
```

This avoids repeated pointers completely.

The meeting proposed representing this as:

```text
child[0..15]  = A
child[16..31] = B
```

which duplicates `A` and `B` inside the routing array. 

Your representation is cleaner:

```text
child[0]  = A
child[16] = B
all other entries = NULL
```

But there is one important routing issue.

## Why two plain pointers are insufficient

Suppose the next five bits of a key are:

```text
00101 = logical slot 5
```

If routing does this:

```c
child = children[5];
```

it receives `NULL`, even though slot 5 is represented by child `A`.

Therefore, each non-null child must also store its represented range.

## Correct representation: sparse child ranges

Instead of a plain 32-element child ID array, use up to 32 range descriptors:

```text
ChildRange
+----------------+
| slot_start     |
| slot_span      |
| child_node_id  |
+----------------+
```

After the first split:

```text
active_child_count = 2

child_ranges[0] = {
    slot_start = 0,
    slot_span  = 16,
    child      = A
}

child_ranges[1] = {
    slot_start = 16,
    slot_span  = 16,
    child      = B
}
```

Routing for slot 5 asks:

```text
Which range contains slot 5?

0 <= 5 < 16
```

Therefore it selects `A`.

Routing for slot 27 asks:

```text
16 <= 27 < 32
```

Therefore it selects `B`.

---

## After another binary split

Suppose `A` overflows.

```text
Before:

0...............15  16...............31
        A                    B
```

After splitting `A`:

```text
0.......7  8.......15  16...............31
    A0         A1               B
```

The descriptors become:

```text
(0,  8, A0)
(8,  8, A1)
(16,16, B)
```

After splitting `A0`:

```text
(0,  4, A00)
(4,  4, A01)
(8,  8, A1)
(16,16, B)
```

So the physical occupancy progresses as:

```text
1 physical child
2 physical children
3 physical children
4 physical children
...
maximum 32 physical children
```

while the node always represents a logical 32-way routing level.

## Parent hash

The parent should include each physical child exactly once:

```text
H(L) = H(A) XOR H(B)
```

Null child entries contribute nothing:

```text
H(NULL) = 0
```

After splitting `A`:

```text
H(L) = H(A0) XOR H(A1) XOR H(B)
```

Because:

```text
H(A) = H(A0) XOR H(A1)
```

the parent hash remains unchanged by the structural split:

```text
H(L) before split = H(L) after split
```

That matches your requirement that the root represent data rather than physical structure.

### Final decision

Use:

```text
maximum logical fanout = 32
sparse active child ranges
no repeated child pointers
unused child descriptors are absent or null
```

This fully resolves issue 1.

---

# 2. Root hash should represent only data

Agreed.

The root invariant should be:

```text
root_hash =
    XOR of the authoritative 256-bit hashes
    of every live tuple in the indexed relation
```

Therefore:

```text
insert tuple T:
    root ^= H(T)

delete tuple T:
    root ^= H(T)

update T_old -> T_new:
    root ^= H(T_old)
    root ^= H(T_new)
```

But:

```text
split a leaf:
    root does not change

merge leaves:
    root does not change

move a tuple between physical leaves:
    root does not change
```

The physical structure is checked through separate structural invariants:

```text
every live leaf item is reachable exactly once
every key belongs to the correct prefix range
every child range is valid and non-overlapping
tuple counts match descendants
all leaves contain at most 32 items
node hashes equal the XOR of descendant tuple hashes
```

This is compatible with the current repository, where `MerkleNode.hash` is explicitly an XOR-aggregated 256-bit value. ([GitHub][1])

---

# 3. Collision probability for 50 million rows

Let:

```text
n = 50,000,000 hashes
b = number of hash bits
M = 2^b possible hash values
```

For uniformly random hashes, the birthday approximation for at least one collision is:

[
P(\text{at least one collision})
\approx
1-\exp\left(-\frac{n(n-1)}{2\cdot 2^b}\right)
]

Define:

[
\lambda=\frac{n(n-1)}{2\cdot 2^b}
]

`λ` is also approximately the expected number of colliding pairs.

## Results for 50 million rows

| Hash width | Expected colliding pairs, λ | Probability of at least one collision | Approximate interpretation                        |
| ---------: | --------------------------: | ------------------------------------: | ------------------------------------------------- |
|    32 bits |                  291,038.30 |                      Effectively 100% | Hundreds of thousands of colliding pairs expected |
|    64 bits |                0.0000677626 |                             0.006776% | About 1 in 14,758 such datasets                   |
|   256 bits |      (1.0795\times10^{-62}) |                (1.0795\times10^{-62}) | About 1 in (9.26\times10^{61}) datasets           |

---

## 32-bit case

The hash space contains:

```text
2^32 = 4,294,967,296 values
```

With 50 million rows:

[
\lambda \approx 291,038
]

The probability of no collision is approximately:

[
e^{-291038}
]

which is around:

[
10^{-126396}
]

Therefore:

```text
P(at least one collision)
≈ 1 - 10^-126396
≈ 100%
```

This does not merely mean that a collision is possible.

It means:

```text
around 291,000 colliding row pairs are expected
```

Therefore 32-bit hashes are completely unsuitable as authoritative row-integrity hashes for 50 million rows.

---

## 64-bit case

The hash space contains:

```text
2^64 = 18,446,744,073,709,551,616 values
```

For 50 million rows:

[
\lambda \approx 0.0000677626
]

Because this value is small:

[
P(\text{collision})\approx\lambda
]

Therefore:

```text
probability = 0.0000677603
percentage  = 0.00677603%
```

That is approximately:

```text
1 collision-bearing dataset
per 14,758 independent 50M-row datasets
```

This is not enormous, but it is also not suitable for a correctness-critical database that is expected to run repeatedly, across many replicas, tables, rebuilds, and years.

---

## 256-bit case

The hash space contains:

```text
2^256 ≈ 1.1579 × 10^77 values
```

For 50 million rows:

[
P(\text{collision})
\approx 1.0795\times10^{-62}
]

That is approximately:

```text
1 in 92,633,673,242,526,420,000,000,000,000,
000,000,000,000,000,000,000,000,000,000 datasets
```

It is effectively impossible for accidental corruption.

The current repository already uses BLAKE3-256 for both row integrity and routing, and explicitly stores 32-byte hashes. ([GitHub][1])

---

# Birthday collision versus missed recovery corruption

There is an important distinction.

## Case A: two different keys have the same hash

Suppose:

```text
key 100 -> hash X
key 900 -> hash X
```

Because your leaf summary stores:

```text
(key, tuple_hash)
```

these entries are still distinct:

```text
(100, X)
(900, X)
```

Recovery does not confuse the keys.

So an arbitrary collision between two different keys does not automatically cause a missed repair.

---

## Case B: the same key has different contents but the same truncated hash

This is the dangerous recovery case:

```text
Healthy:
(key=100, row="correct", hash=X)

Damaged:
(key=100, row="corrupt", hash=X)
```

Recovery compares:

```text
same key
same hash
```

and incorrectly concludes that the row is healthy.

For random corruption, the probability that one changed row keeps the same truncated hash is:

[
\frac{1}{2^b}
]

For 300 corrupted rows, the approximate probability that at least one is hidden is:

[
\frac{300}{2^b}
]

|    Width | Probability that one of 300 random corruptions is missed |
| -------: | -------------------------------------------------------: |
|  32 bits |        (6.98\times10^{-8}), about 1 in 14.3 million runs |
|  64 bits |                                     (1.63\times10^{-17}) |
| 256 bits |                                     (2.59\times10^{-75}) |

This direct missed-corruption probability is smaller than the 50M-row birthday probability because the comparison is against the previous hash of the **same key**, not against every other row.

However, your threat model also includes compromised replicas. A malicious replica may deliberately search for matching truncated values. That is another reason not to make 32-bit or 64-bit values authoritative.

---

## Storage cost at 50 million rows

Raw hash storage, excluding PostgreSQL row and index overhead:

|    Width | Bytes per item | Raw storage for 50M items |
| -------: | -------------: | ------------------------: |
|  32 bits |        4 bytes |                    200 MB |
|  64 bits |        8 bytes |                    400 MB |
| 256 bits |       32 bytes |                    1.6 GB |

For recovery network transfer, the cost difference is small because a physical leaf contains at most 32 items:

```text
32-bit summaries:
32 × 4 = 128 bytes of hashes

64-bit summaries:
32 × 8 = 256 bytes

256-bit summaries:
32 × 32 = 1024 bytes
```

An additional 768 bytes per bad leaf compared with 64-bit is tiny relative to database round trips and complete row payloads.

## Recommendation

```text
Authoritative tuple hash: BLAKE3-256

Optional optimization:
    retain a 64-bit prefix as an in-memory/cache prefilter

Never:
    use the 64-bit prefix as the final correctness decision
```

---

# 4. Verification of current Merkle determinism

You are substantially correct.

The current static Merkle implementation already has a strong deterministic sequencing architecture.

## Deterministic routing

All key types are serialized through a canonical, versioned binary format and hashed with BLAKE3-256. The same key therefore produces the same 256-bit route digest on every replica. The full digest is explicitly retained for future dynamic traversal. ([GitHub][2])

```text
key
 |
 v
canonical type-aware serialization
 |
 v
BLAKE3-256
 |
 v
same route digest on every replica
```

---

## Deterministic transaction delta serialization

Transaction-local deltas are first aggregated by:

```text
index relation
physical relation identity
leaf ID
```

Before serialization, the entries are explicitly sorted using `qsort()` by:

```text
index_oid
tablespace node
database node
relation node
leaf_id
```

This prevents PostgreSQL hash-table iteration order from affecting the durable delta blob. ([GitHub][3])

---

## Replica-agreed apply sequence

For direct deterministic BCDB workers, the code derives:

```text
apply_seq = replica-agreed transaction ID + 1
```

The code comment explicitly states that the transaction ID is already a replica-agreed total order. For Raft-ledger operations, the sequence is derived from the Raft log position and item ordinal. ([GitHub][3])

---

## Ordered application

The applier fetches committed deltas using:

```sql
ORDER BY apply_seq
```

It checks the expected next sequence and handles only proven terminal gaps. Node events are then sorted before page mutation. ([GitHub][4])

---

## Idempotent crash recovery

Every static Merkle page stores:

```text
last_applied_seq
```

Hash modifications and the new page watermark are written together using PostgreSQL Generic WAL. On replay, events whose sequence is already covered by the page watermark are skipped. ([GitHub][1])

Your cluster evidence also shows all three replicas finishing with the same row count, identical root, and `merkle_verify=t`, although a successful benchmark run is evidence rather than a formal proof. 

## Revised verdict on issue 4

```text
Static update determinism:
    already implemented

Global operation ordering:
    already implemented

Crash-safe idempotent application:
    already implemented

Dynamic structural determinism:
    must reuse this framework
```

So issue 4 should not be called a flaw in the current code.

It is an integration requirement for the dynamic implementation.

---

## What the current delta format lacks for dynamic trees

The current durable delta entry contains only:

```text
index identity
static leaf_id
XOR hash delta
```

It does not contain:

```text
primary key
route digest
operation type
old tuple hash
new tuple hash
dynamic node prefix
```

That is sufficient for a fixed static tree because the target leaf is already known.

It is insufficient for a dynamic tree because the applier must:

```text
locate the current physical leaf
insert/delete the leaf item
split overflowing leaves
move items between children
merge underfilled ranges
```

The current `MerkleDeltaKey` is explicitly `index_oid + relfilenode + leaf_id`, with an XOR delta payload. ([GitHub][3])

Therefore, the sequencing machinery can be retained, but the durable delta payload must be upgraded.

---

# 5. Split 32 and merge 8

Agreed.

```text
split_threshold = 32
merge_threshold = 8
```

This gives strong hysteresis:

```text
0........8......................32........
         ^                       ^
       merge                   split
```

After a split, deleting a few rows does not immediately merge the structure.

Example:

```text
33 rows -> split

31 rows -> remain split
20 rows -> remain split
9 rows  -> remain split
8 rows  -> eligible for merge
```

This avoids structural ping-pong.

## Precise merge rule

With binary physical splitting, merge only deterministic buddy ranges:

```text
Left child:
    start = S
    span  = W

Right child:
    start = S + W
    span  = W
```

They may merge when:

```text
both are leaves
combined tuple count <= 8
their ranges are adjacent binary buddies
they have the same parent
```

Result:

```text
merged range:
    start = S
    span  = 2W
```

For an internal subtree, it can collapse into a leaf when:

```text
total descendant items <= 8
```

The merge should proceed bottom-up in deterministic prefix order.

---

# 6. Recursive splitting

Agreed.

After every split:

```text
for every resulting leaf:
    while leaf.count > 32:
        split it again
```

Example:

```text
Original leaf count = 65

First split:
    left  = 65
    right = 0

Left remains oversized, so recurse:

Second split:
    left-left  = 40
    left-right = 25

left-left remains oversized, so recurse:

Third split:
    19 and 21
```

Final state:

```text
19
21
25
0
```

All non-empty physical leaves now satisfy:

```text
count <= 32
```

An empty range should remain unallocated:

```text
no leaf row
no child descriptor
no storage
```

When the first key enters that range, its leaf can be created deterministically.

At the theoretical 256-bit route-depth limit, the implementation must use an explicit collision bucket or fail with a structural error rather than recurse forever. In practical BLAKE3-256 routing, reaching this case accidentally is fantastically unlikely.

---

# 7. Final node identity for binary physical splitting

The authoritative logical identity should be:

```text
(partition_id, prefix_length, prefix_value)
```

For example:

```text
partition = 7
prefix_length = 3
prefix_value = 101
```

This represents:

```text
all route digests in partition 7
whose relevant prefix begins with 101
```

A node with:

```text
prefix_length = 1
prefix_value = 1
```

is different from:

```text
prefix_length = 5
prefix_value = 00001
```

even though both numerical values equal one.

## Production representation

Because the route digest can theoretically consume as many as 256 bits, the production identity should not rely solely on fitting the entire prefix into one `bigint`.

Use:

```text
uint16 prefix_length
uint8  prefix_value[32]
```

where unused bits are canonicalized to zero.

An optional compact `bigint` cache or surrogate can be used for shallow benchmark trees, but the authoritative identity should remain the full prefix pair.

---

# 8. Materialized leaf-item relation

Agreed.

A suitable logical schema is:

```text
dynamic_merkle_leaf_item
+------------------------------------------------+
| index_id                                       |
| partition_id                                   |
| leaf_node_id / leaf prefix identity            |
| canonical_key                                  |
| route_digest_256                               |
| tuple_hash_256                                 |
| last_applied_seq                               |
+------------------------------------------------+
```

Important indexes:

```text
PRIMARY KEY:
    (index_id, canonical_key)

Recovery lookup:
    (index_id, partition_id, leaf_node_id)

Split redistribution:
    (index_id, partition_id, leaf_node_id, route_digest)
```

During a split:

```text
old leaf L
    |
    +-- items with next bit 0 -> child A
    |
    +-- items with next bit 1 -> child B
```

The `leaf_node_id` of moved item records is updated transactionally.

This replaces the current functional `merkle_bucket_for_key()` expression-index approach, whose result is safe only because static leaf assignment never changes. The current recovery benchmark uses a functional leaf lookup index and fetches all rows in bad static leaves. 

---

# 9. Improved recovery mechanism

Recovery must compare **logical ranges**, not require identical physical shapes.

## Core work item

Every recovery task should represent:

```text
LogicalRange
+------------------+
| partition_id     |
| prefix_length    |
| prefix_value     |
+------------------+
```

Then healthy and damaged replicas describe how they physically represent that logical range.

---

## Recovery flow

```text
Healthy partition root             Damaged partition root
          |                                  |
          +---------------+------------------+
                          |
                    compare data hash
                          |
                 equal? -> skip range
                          |
                      different
                          |
                          v
             compare canonical logical frontier
                          |
           +--------------+---------------+
           |                              |
      same shape                    different shape
           |                              |
           v                              v
 compare child ranges          virtually expand/coalesce
           |                    to matching prefix ranges
           +--------------+---------------+
                          |
                          v
                bounded leaf summaries
                          |
                          v
                compare key -> hash maps
                          |
             +------------+-------------+
             |            |             |
             v            v             v
          missing       changed        extra
          damaged       tuple hash     damaged
             |            |             |
             v            v             v
           INSERT       UPDATE         DELETE
```

---

## Shape mismatch example

Healthy:

```text
prefix P
   |
   v
internal
 /      \
P0      P1
```

Damaged:

```text
prefix P
   |
   v
single leaf
```

Recovery does not declare the structure incomparable.

Instead:

```text
1. Read all bounded summary items under healthy P.
2. Read the bounded summary items from damaged P.
3. Partition the damaged leaf items virtually using route bits.
4. Compare equivalent logical subranges.
```

Because every physical leaf is bounded to 32 items, virtual expansion is cheap.

---

## Exact repair identification

For each mismatching bounded range:

```text
healthy_map[key] = tuple_hash_256
damaged_map[key] = tuple_hash_256
```

Then:

```text
healthy key absent in damaged:
    INSERT

same key, different hash:
    UPDATE

damaged key absent in healthy:
    DELETE
```

Full rows are fetched only for:

```text
INSERT keys
UPDATE keys
```

DELETE needs only the key.

This is better than the present static flow, which localizes bad leaves and then fetches every complete row from those leaves on both sides. 

---

## Set-based repair

Avoid one SQL statement per row.

Use a temporary repair batch:

```text
repair_batch
+----------------+
| operation      |
| key            |
| healthy row    |
+----------------+
```

Then execute:

```text
one batched INSERT
one batched UPDATE
one batched DELETE
```

This becomes especially important when healthy and damaged replicas are on different machines.

---

# 10. Making the dynamic tree durable

The dynamic tree should preserve the live version-7 architecture:

```text
user transactions do not directly mutate Merkle structure
```

Instead:

```text
User transaction
      |
      v
commit user-row change
+
commit semantic dynamic-Merkle delta
      |
      v
globally ordered Merkle applier
      |
      v
apply item change
+
perform recursive splits/merges
+
update counts and hashes
+
advance applied sequence
```

The current code already follows the principle that user transactions stage durable deltas and the ordered applier is the only normal page mutator. ([GitHub][3])

---

## New semantic delta format

The new dynamic delta should contain entries such as:

```text
DynamicMerkleDelta
+--------------------------------------+
| format_version                       |
| apply_seq                            |
| index identity                       |
| operation: INSERT / DELETE / UPDATE  |
| canonical key                        |
| route_digest_256                     |
| old_tuple_hash_256                   |
| new_tuple_hash_256                   |
+--------------------------------------+
```

Possible canonical encoding:

```text
INSERT:
    key
    route digest
    new hash

DELETE:
    key
    route digest
    old hash

UPDATE, unchanged key:
    key
    route digest
    old hash
    new hash

UPDATE, changed key:
    encoded as DELETE old + INSERT new
```

Entries inside one transaction must be sorted canonically by:

```text
index identity
partition
route digest
canonical key
operation ordering
```

Recommended operation order for the same key:

```text
DELETE before INSERT
```

This makes primary-key changes deterministic.

---

## Applier transaction

For every `apply_seq`, the applier performs one PostgreSQL transaction:

```text
BEGIN

1. Lock the next durable delta.
2. Lock affected partitions and nodes in canonical prefix order.
3. Apply leaf-item inserts/deletes/updates.
4. Update leaf tuple counts and hashes.
5. Propagate tuple deltas to ancestors.
6. Recursively split every leaf with count > 32.
7. Recursively merge eligible buddy ranges with count <= 8.
8. Validate local structural invariants.
9. Set node/item last_applied_seq.
10. Advance global applied_seq.

COMMIT
```

Because the node, child-range, leaf-item, and apply-state relations are ordinary WAL-logged PostgreSQL relations, all structural changes commit atomically.

---

## Crash cases

### Crash before user transaction commits

```text
heap change: absent
semantic delta: absent
```

Nothing is applied.

### Crash after user commit but before Merkle application

```text
heap change: committed
semantic delta: committed
dynamic tree: behind
```

On restart, the ordered applier reads the durable queued delta and catches up.

### Crash halfway through split

Suppose the applier has:

```text
created child A
moved some items
not yet created child B
```

but has not committed.

PostgreSQL abort recovery removes the partial transaction:

```text
child A disappears
moved items return to their original committed state
parent remains unchanged
applied_seq remains unchanged
```

The complete operation is retried.

### Crash after commit but before acknowledgement

```text
structural update committed
applied_seq committed
```

After restart, the applier sees that the sequence is already applied and skips it.

This is exactly the idempotent pattern already used by the current page watermark and applied-sequence machinery. ([GitHub][4])

---

# Final consolidated architecture

```text
                         USER TABLE
                             |
                   INSERT / UPDATE / DELETE
                             |
                             v
              Durable semantic Merkle delta
           committed atomically with user result
                             |
                             v
                 Globally ordered applier
                             |
              +--------------+--------------+
              |                             |
              v                             v
     Materialized leaf items          Dynamic nodes
     key                               partition
     route_digest_256                  prefix length/value
     tuple_hash_256                    data hash
     physical leaf ID                  tuple count
                                      leaf/internal
                                             |
                                             v
                                Sparse child ranges
                                start slot
                                slot span
                                child node
                                             |
                                             v
                                split > 32 recursively
                                merge <= 8 bottom-up
```

Recovery becomes:

```text
compare data roots
      |
compare logical prefix ranges
      |
handle shape mismatches virtually
      |
compare at most 32 key/hash summaries per physical leaf
      |
fetch only exact damaged rows
      |
perform batched repair
      |
wait for durable dynamic Merkle catch-up
      |
reconfirm logical ranges and roots
```

## Final hash policy

```text
Routing:
    BLAKE3-256

Tuple integrity:
    BLAKE3-256

Node data hash:
    XOR of descendant BLAKE3-256 tuple hashes

Optional acceleration:
    cached 64-bit prefixes

Never authoritative:
    32-bit or 64-bit tuple summaries
```

With these decisions, the dynamic design is now coherent across routing, splitting, merging, deterministic replication, recovery, and crash durability.

[1]: https://raw.githubusercontent.com/NeelParekh17/ARIABC_MERKLE/main/src/include/access/merkle.h "raw.githubusercontent.com"
[2]: https://raw.githubusercontent.com/NeelParekh17/ARIABC_MERKLE/main/src/backend/access/merkle/merkleutil.c "raw.githubusercontent.com"
[3]: https://raw.githubusercontent.com/NeelParekh17/ARIABC_MERKLE/main/src/backend/access/merkle/merkledelta.c "raw.githubusercontent.com"
[4]: https://raw.githubusercontent.com/NeelParekh17/ARIABC_MERKLE/main/src/backend/access/merkle/merkleapply.c "raw.githubusercontent.com"
