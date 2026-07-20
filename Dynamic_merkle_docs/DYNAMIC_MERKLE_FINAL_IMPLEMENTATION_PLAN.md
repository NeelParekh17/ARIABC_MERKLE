



# Dynamic Merkle Architecture Analysis

I inspected the **current `main` branch**, the live Merkle access-method code, the recovery implementation, the July 9 meeting transcript, the July 13 follow-up transcript, the handwritten design, the paper, and the latest recovery analysis.

## Final conclusion first

The live repository **does not yet implement the dynamic Merkle tree** discussed in the meetings.

What exists today is:

```text
fixed partitions
    +
fixed number of leaves per partition
    +
fixed perfect F-ary tree geometry
    +
hash(key) modulo total_leaves routing
```

The meetings propose replacing that fixed structure with:

```text
fixed partitions
    +
fixed logical fanout K, probably 32
    +
dynamic height
    +
bounded physical leaves, roughly 32 tuples
    +
prefix-based routing
    +
logical K-way nodes compressed into binary physical splits
```

The most important conceptual sentence from the July 13 meeting is:

> **Logically it is a K-ary tree, but physically the leaf splitting is binary.**

That is the architecture around which the later discussion converged. fileciteturn3file0

However, the meetings **do not completely specify a production-ready implementation**. They settle the shape of the data structure, but several correctness questions remain unresolved, particularly internal hashing with repeated child pointers, deterministic structural updates, split/merge hysteresis, node-ID encoding, and recovery when replica tree shapes differ.

---

# Part I: Architecture before dynamic Merkle

## 1. What the live repository currently contains

The live Merkle source directory contains:

```text
merkle.c
merklebuild.c
merkleinsert.c
merkleutil.c
merkleverify.c
merkledelta.c
merkleapply.c
```

There is no `merkledynamic.c`, dynamic node representation, child-pointer directory, leaf-item structure, dynamic split function, or merge function in the current Merkle access method. The header even describes its full 256-bit routing digest as something that a **future dynamic tree** may consume. citeturn648025view0turn946078view0

The current implementation is Merkle format **version 7**, not the older version 4 or 5 described in some historical documents. Version 7 contains crash-safe committed-delta application and per-page applied sequence tracking. citeturn946078view0

---

## 2. Current static Merkle structure

Each indexed relation is divided into several independent partition trees.

For one relation:

```text
                         Relation Merkle root
                                 |
               +-----------------+-----------------+
               |                 |                 |
               v                 v                 v
          Partition 0       Partition 1       Partition P-1
          perfect tree      perfect tree      perfect tree
               |                 |                 |
          fixed leaves      fixed leaves      fixed leaves
```

Each partition is a **perfect fixed-geometry tree**:

```text
Example: fanout = 32, leaves per partition = 1024

                         Partition root
                    / / / ... 32 children ... \ \ \
                  N0  N1  N2                 N31
                  |    |                       |
               32 leaves                   32 leaves

Total leaves under partition:
    32 × 32 = 1024

Height:
    root -> internal node -> leaf
```

The metapage stores the geometry:

```text
numPartitions
leavesPerPartition
nodesPerPartition
totalNodes
nodesPerPage
numTreePages
fanout
```

Every `MerkleNode` contains only:

```text
nodeId
32-byte XOR hash
```

There are no dynamic pointers, no `is_leaf` field, no tuple count, no variable depth, and no per-leaf key list in the current native index. citeturn946078view0

---

## 3. Current key routing

The live code now computes a canonical BLAKE3-256 digest for all key types:

```text
database key
     |
     v
canonical binary serialization
     |
     v
BLAKE3-256 route digest
     |
     +-----------------------------------------+
     |                                         |
     v                                         v
first 64 bits                         complete 256 bits retained
     |                               for future dynamic routing
     v
first64 % total_leaves
     |
     v
fixed leaf_id
```

Formally:

```text
route_digest = BLAKE3(canonical_key)

static_route_value =
    first 8 bytes of route_digest

leaf_id =
    static_route_value % total_leaf_count
```

The complete route digest is already available, which is excellent groundwork for dynamic prefix routing. But the live tree still consumes only the static modulo result. citeturn946078view1

### Why modulo prevents dynamic expansion

Suppose:

```text
current total leaves = 1024
leaf = hash(key) % 1024
```

Later, if total leaves becomes 2048:

```text
new leaf = hash(key) % 2048
```

Many existing keys receive a different leaf ID. Therefore, simply increasing the static leaf count would require redistributing most of the table and rebuilding the index.

This exact concern was raised in the meeting. The response was:

```text
Do not change modulo and rebuild.
Instead, traverse a prefix tree.
```

The fanout stays fixed while the height grows only where necessary. fileciteturn3file10

---

## 4. Current tuple and node hashes

The row-integrity hash is BLAKE3-256 over a canonical, versioned binary representation of the complete row.

```text
row values
   |
   v
attribute numbers + types + typmods
   |
   v
null markers + canonical binary encodings
   |
   v
BLAKE3-256 tuple hash
```

The current Merkle aggregation operation is XOR:

```text
leaf_hash =
    tuple_hash_1
    XOR tuple_hash_2
    XOR ...
    XOR tuple_hash_n

internal_hash =
    child_hash_1
    XOR child_hash_2
    XOR ...
```

XOR makes insertion and deletion inexpensive because applying the same tuple hash twice removes it:

```text
H XOR X XOR X = H
```

The code stores 32-byte node hashes and exposes the same XOR operation throughout static path maintenance. citeturn946078view0turn946078view1

---

## 5. Current durable update path

One important correction to older documents: the live version no longer directly changes Merkle pages inside the user transaction.

The live path is:

```text
INSERT / UPDATE / DELETE
          |
          v
compute old/new tuple contribution
          |
          v
stage transaction-local leaf delta
          |
          v
commit heap change + durable delta/ledger state
          |
          v
ordered Merkle applier
          |
          v
apply leaf delta to fixed leaf and ancestors
          |
          v
Generic WAL record
  includes hash changes and last_applied_seq
```

The code explicitly states:

```text
User transactions never mutate Merkle pages.
The ordered applier is the only normal-runtime page mutator.
```

This is a strong architecture for deterministic durability. Any dynamic design should extend this ordered-applier model rather than returning to direct structural page changes inside arbitrary worker transactions. citeturn946078view0turn946078view1

---

# Part II: Current recovery architecture

## 6. Healthy and damaged copies

The benchmark currently operates over two schemas:

```text
healthy.usertable
damaged.usertable
```

Each side has:

```text
primary key index
static Merkle index
functional bucket lookup index
```

The current best-tested static geometry in the latest recovery campaign is:

```text
partitions             = 200
leaves per partition   = 1024
fanout                 = 32
total leaves           = 204,800
bad leaves             = 75
corrupted tuples       = 300
```

The latest analysis reports 33 valid runs for each large-machine campaign under this geometry. fileciteturn4file2

---

## 7. Current recovery flow

```text
                    CORRUPTED REPLICA
                           |
                           v
                  commit corrupted rows
                           |
                           v
                  merkle_apply_pending()
                           |
                           v
       +-----------------------------------------+
       | Compare all partition root hashes      |
       +--------------------+--------------------+
                            |
                      mismatching roots
                            |
                            v
       +-----------------------------------------+
       | Batched child-hash descent              |
       | root -> internal nodes -> bad leaves    |
       +--------------------+--------------------+
                            |
                         bad leaf IDs
                            |
             +--------------+--------------+
             |                             |
             v                             v
     healthy.usertable             damaged.usertable
     fetch EVERY row               fetch EVERY row
     in every bad leaf             in every bad leaf
             |                             |
             +--------------+--------------+
                            |
                            v
                  compare full row maps
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
           INSERT         UPDATE        DELETE
                            |
                            v
                  commit repaired rows
                            |
                            v
                  merkle_apply_pending()
                            |
                            v
                  targeted confirmation
```

The localisation path is already efficient:

```text
2 partition-root SQL calls
+
2 batched child-fetch calls per visited depth
```

It does not issue one SQL call per internal node. The live Python implementation performs breadth-first batched descent. citeturn648025view1

The problem begins **after localisation**.

For every bad static leaf, recovery queries:

```sql
WHERE merkle_bucket_for_key(index, ycsb_key) = leaf_id
```

and returns every complete heap row in the leaf from both replicas. citeturn648025view2

---

## 8. Why the current recovery is still linear in table size

With fixed total leaves:

```text
average rows per leaf =
    N / total_leaf_count
```

For fixed `K` bad leaves:

```text
candidate rows fetched from both replicas
    ≈ 2 × K × N / total_leaf_count
```

Therefore:

```text
N increases
total_leaf_count remains fixed
rows per bad leaf increase
candidate row fetch increases
```

The measurements show this clearly:

```text
candidate rows

1 million rows:      732
20 million rows:  14,650
50 million rows:  36,622
```

The fanout-32, 1024-leaf geometry makes the slope small, but it does not remove the linear term. On the smaller-memory machine, the 50M candidate fetch becomes an I/O/cache cliff; on the large-memory EPYC server, it remains much cheaper. fileciteturn3file15

This is the problem the dynamic design is intended to remove.

---

# Part III: What the July 9 meeting proposed

## 9. The core observation

The advisor’s reasoning was:

```text
Static leaf size grows with N.
Recovery must inspect all tuples in a bad leaf.
Therefore candidate work grows with N.

Dynamic leaves should split.
Therefore leaf size stays approximately bounded.
Therefore candidate work depends on corruption size,
not total database size.
```

A leaf containing thousands of candidates was explicitly considered unacceptable. The discussion suggested keeping a physical leaf around 32 tuples, perhaps below 50, and making the threshold configurable for experiments. fileciteturn4file0

---

## 10. Fixed partitions, fixed fanout, dynamic height

The meeting does **not** propose dynamically changing everything.

The intended dimensions are:

```text
Partitions:
    fixed

Logical fanout K:
    fixed, likely 32
    configurable for experiments

Physical leaf capacity:
    bounded, likely around 32

Height:
    dynamic and path-specific
```

```text
Database growth
     |
     v
more leaves appear only under crowded prefixes
     |
     v
height grows locally
```

The number of partitions is deliberately left fixed because partitions are primarily a contention-control mechanism. Dynamically changing them would require coordinated movement of entire partition trees across replicas. The advisor also rejected an ordinary B-tree/B+ tree because its exact structure can depend on insertion order, whereas this Merkle structure should be determined by route prefixes and the current tuple set. fileciteturn1file11

---

## 11. Two different hashes must not be confused

The discussions occasionally use “hash” for multiple purposes. They are architecturally different.

```text
                   KEY
                    |
                    v
            route hash / digest
                    |
                    v
        chooses partition and path


                COMPLETE ROW
                    |
                    v
              tuple hash
                    |
                    v
        detects changed row contents
```

### Route digest

```text
route_digest = BLAKE3(key)
```

Used for:

```text
partition selection
logical child slot selection
dynamic prefix traversal
split redistribution
```

### Tuple-integrity hash

```text
tuple_hash = BLAKE3(all row columns)
```

Used for:

```text
leaf summaries
detecting changed payloads
Merkle root computation
recovery comparison
```

The current repository already provides both foundations:

```text
MerkleRoute.route_digest[32]
MerkleHash tuple/node digest[32]
```

The dynamic implementation must keep them conceptually and structurally separate. citeturn946078view0turn946078view1

---

# Part IV: The refined July 13 architecture

The July 13 meeting corrects and refines the earlier draft.

The handwritten design initially considered several relations:

```text
A: leaf information
B: node information
C: child mapping information
```

By July 13, the discussion converged toward **one logical node relation or one common node representation**, distinguished by `is_leaf`.

---

## 12. Proposed node representation

### Common node header

Every node has:

```text
partition_id
node_id
prefix_length
is_leaf
tuple_count
subtree_hash
```

Then the payload depends on node type.

```text
DynamicNode
+--------------------------------------------------+
| partition_id                                     |
| node_id                                          |
| prefix_length                                    |
| is_leaf                                          |
| tuple_count                                      |
| subtree_hash                                     |
+--------------------------------------------------+
| if is_leaf:                                      |
|     [(key, tuple_hash), ...]                     |
|                                                  |
| if not is_leaf:                                  |
|     child_node_id[0 ... K-1]                     |
+--------------------------------------------------+
```

The meeting explicitly says that the common fields should be:

```text
partition
node ID
hash
tuple count
is leaf
```

A leaf then stores key/tuple-hash pairs, while an internal node stores an array of `K` child node IDs. Child IDs may repeat. fileciteturn4file1

### Required implementation addition

For efficient splitting, a leaf must also have either:

```text
route digest stored with every item
```

or it must be possible to recompute the route digest from the key.

A robust leaf item therefore looks like:

```text
LeafItem
+--------------------+
| primary key        |
| route_digest       |
| tuple_hash_256     |
+--------------------+
```

The meeting explicitly mentions key and tuple hash, but redistribution requires the route bits as well. Recomputing them from the key is correct but adds repeated hashing work.

---

# Part V: Logical K-ary, physically binary

## 13. The crucial idea

Assume:

```text
logical fanout K = 32
bits per logical level = log2(32) = 5
```

An internal node logically has 32 child slots:

```text
slot:
00000 00001 00010 ... 11110 11111
  0     1     2          30    31
```

But the implementation does not immediately need 32 physical leaves.

Instead, multiple logical slots can point to the same physical leaf.

---

## 14. Initial compressed logical node

```text
Logical internal node with 32 slots

slots 0........................................31
      \________________________________________/
                         |
                         v
                  Physical leaf L
                  count <= capacity
```

Pointer array:

```text
child[0]  = L
child[1]  = L
...
child[31] = L
```

One physical leaf currently represents all 32 logical child ranges.

---

## 15. First overflow: physical binary split, no height increase

Suppose `L` exceeds capacity.

Split its logical range in half using one additional routing bit:

```text
Before

slots 0........................................31
      \________________________________________/
                         |
                         v
                     Leaf L
                   45 entries


After

slots 0...............15 16...............31
      \_______________/  \_______________/
              |                  |
              v                  v
          Leaf L0            Leaf L1
         22 entries          23 entries
```

Pointer array after split:

```text
child[0..15]  = L0
child[16..31] = L1
```

The logical fanout remains 32.

The physical fanout is currently only 2.

The tree height has **not increased**.

This is exactly what “logically K-ary, physically binary” means. fileciteturn3file1turn3file2

---

## 16. Further split inside the same logical node

Suppose the left physical leaf overflows:

```text
Before

0........................15
\________________________/
             |
             v
            L0
```

Split it again:

```text
slots 0.......7   slots 8......15   slots 16......31
      |                 |                   |
      v                 v                   v
     L00               L01                  L1
```

Pointers now become:

```text
child[0..7]   = L00
child[8..15]  = L01
child[16..31] = L1
```

Again:

```text
same logical node
same 32-slot directory
no additional tree level
```

Successive splits can produce:

```text
16 pointers -> one leaf
8 pointers  -> one leaf
4 pointers  -> one leaf
2 pointers  -> one leaf
1 pointer   -> one leaf
```

The leaf’s prefix length tells how much of the logical directory it represents. fileciteturn4file1

---

## 17. When height finally increases

Eventually, a physical leaf may correspond to exactly one logical slot:

```text
parent child slot 19
        |
        v
      Leaf X
```

Its route prefix already consumes all 5 bits assigned to that logical level.

If `Leaf X` overflows again, there is no unused slot range left at that parent.

Therefore:

```text
Leaf X becomes an internal node
```

and a new logical 32-way directory is created beneath it using the next five route bits.

```text
Before

Parent
  |
slot 19
  |
  v
Leaf X, count > capacity


After

Parent
  |
slot 19
  |
  v
Internal node X
  |
  +-- 32 logical child slots using next 5 route bits
           |
           +-- physically compressed into a small number
               of leaves, initially perhaps two
```

Expanded:

```text
                               Parent
                                  |
                               slot 19
                                  |
                                  v
                        Internal node X
                 next route bits choose 0..31
                  / / / /               \ \ \ \
                 /                         \
          slots 0..15                 slots 16..31
                |                            |
                v                            v
             Leaf XA                      Leaf XB
```

This is the second kind of split described in the meeting:

```text
Case A:
physical leaf represents multiple logical slots
-> divide those slots between two physical leaves
-> height unchanged

Case B:
physical leaf represents exactly one logical slot
-> turn the leaf into an internal node
-> create a new logical K-way level
-> height increases on this path
```

The meeting states these two cases explicitly. fileciteturn3file9turn4file1

---

# Part VI: Why unused space cannot simply be borrowed

Suppose one half keeps growing while the other half remains almost empty:

```text
                Internal node
                /           \
           prefix 0       prefix 1
             crowded        nearly empty
```

It may seem attractive to move some prefix-0 tuples into the empty prefix-1 leaf.

That cannot be done without abandoning prefix routing.

```text
route begins with 0 -> must remain in prefix-0 range
route begins with 1 -> must remain in prefix-1 range
```

Therefore the crowded side grows deeper:

```text
                    root
                  /      \
                 0        1
               /   \       \
             00     01     sparse leaf
            /  \
          000  001
```

The sparse side’s unused space is the cost of obtaining:

```text
order-independent structure
deterministic routing
no global reshuffle
local split operations
```

The meeting explicitly accepts possible skew instead of redistributing records between unrelated hash-prefix ranges. fileciteturn4file1

---

# Part VII: Insert, update, and delete after dynamic Merkle

## 18. Insert

```text
INSERT row
   |
   v
compute route_digest(key)
   |
   v
choose fixed partition
   |
   v
start at partition root
   |
   v
consume route bits and follow child_node_ids[]
   |
   v
reach physical leaf
   |
   v
append (key, route_digest, tuple_hash)
   |
   v
increment tuple_count on ancestors
   |
   v
update subtree hashes
   |
   +-- count <= split threshold
   |       -> finish
   |
   +-- count > split threshold
           -> physical split
           -> if one logical slot remains, grow height
```

The split decision must be made by the globally ordered Merkle applier, not independently by concurrent PostgreSQL workers.

---

## 19. Update with unchanged key

```text
old key == new key
      |
      v
same route digest
      |
      v
same dynamic leaf
      |
      v
replace old tuple hash with new tuple hash
      |
      v
propagate hash delta upward
```

With XOR-based leaf aggregation:

```text
delta =
    old_tuple_hash XOR new_tuple_hash
```

The number of tuples does not change.

---

## 20. Update with changed key

```text
old key != new key
      |
      +-- delete old key/hash from old route
      |
      +-- insert new key/hash through new route
```

The operation may cause:

```text
merge on old path
split on new path
```

Both changes must occur atomically with respect to the durable Merkle state.

---

## 21. Delete and merge

The proposed merge works bottom-up.

Suppose:

```text
Internal node N
  |
  +-- child A: leaf, 6 tuples
  +-- child B: leaf, 5 tuples
  +-- child C: leaf, 4 tuples

subtree total = 15
leaf capacity = 32
```

If all children are leaves and the combined content fits in one leaf:

```text
Before

Grandparent
     |
     v
 Internal N
  /   |   \
 A    B    C


After

Grandparent
     |
     v
   Leaf N
 containing entries from A+B+C
```

The grandparent pointer does not change:

```text
node N remains node N
is_leaf changes false -> true
children disappear
leaf entries are installed
```

That was the merge architecture stated in the July 13 meeting. fileciteturn4file1

---

# Part VIII: Recovery after dynamic Merkle

## 22. Key improvement: leaves store summaries

The dynamic leaf stores:

```text
primary key
tuple-integrity hash
```

Therefore, finding the exact differing keys does not require reading every full heap row in the leaf.

```text
Static recovery

bad leaf
   |
   +-- fetch complete healthy rows
   |
   +-- fetch complete damaged rows
   |
   v
compare every complete payload


Dynamic recovery

bad leaf
   |
   +-- fetch bounded healthy key/hash summaries
   |
   +-- fetch bounded damaged key/hash summaries
   |
   v
find exact differing keys
   |
   v
fetch complete rows only for repair keys
```

The idea of storing key/hash pairs specifically to avoid fetching every tuple from the database was a major conclusion of the July 9 discussion. fileciteturn3file9turn4file0

---

## 23. Dynamic recovery flow

```text
Healthy dynamic root             Damaged dynamic root
          |                                |
          +---------------+----------------+
                          |
                    compare hashes
                          |
                          v
             descend mismatching prefixes
                          |
                          v
                mismatching leaf ranges
                          |
             +------------+------------+
             |                         |
             v                         v
  healthy key/hash summaries  damaged key/hash summaries
             |                         |
             +------------+------------+
                          |
                          v
                compare key -> hash maps
                          |
             +------------+------------+
             |            |            |
             v            v            v
          missing       changed       extra
          damaged       hash          damaged
             |            |            |
             v            v            v
           INSERT       UPDATE        DELETE
             \            |            /
              \           |           /
               +----------+----------+
                          |
                          v
       fetch full healthy rows only for INSERT/UPDATE keys
                          |
                          v
               apply set-based repair DML
                          |
                          v
              verify summaries and roots
```

For a physical leaf capacity `C` and `Kbad` corrupted leaves:

```text
summary rows compared
    <= approximately 2 × Kbad × C
```

If:

```text
Kbad = 75
C    = 32
```

then:

```text
maximum ordinary summary entries
    ≈ 2 × 75 × 32
    = 4,800
```

That remains approximately constant at:

```text
1M rows
10M rows
50M rows
500M rows
```

provided the number of corrupted leaves remains fixed and leaf capacity is enforced.

Full heap-row fetches become proportional to actual differing keys rather than all candidate rows:

```text
heap rows fetched = O(actual corruption count)
summary rows       = O(bad leaves × leaf capacity)
tree descent       = O(bad paths × dynamic height)
```

---

# Part IX: Before-versus-after comparison

| Property | Current static architecture | Proposed dynamic architecture |
|---|---|---|
| Partitions | Fixed | Fixed |
| Fanout | Fixed physical and logical fanout | Fixed logical fanout |
| Leaves | Fixed at index creation | Created and merged dynamically |
| Height | Uniform and fixed | Path-specific and dynamic |
| Routing | First 64 route bits modulo total leaves | Prefix traversal over full route digest |
| Node layout | `nodeId + hash` only | Common node header plus leaf/internal payload |
| Leaf contents | Only aggregate hash in native index | Bounded key/tuple-hash summaries |
| Internal navigation | Arithmetic perfect-tree geometry | Explicit `child_node_ids[K]` |
| Physical occupancy | One physical node per logical node | One physical leaf may represent many logical slots |
| Split | Not supported structurally | Binary physical split |
| Height growth | Requires rebuilding geometry | Leaf becomes internal on demand |
| Merge | No dynamic tree merge | Internal subtree collapses into leaf |
| Candidate fetch | All full rows in bad static leaves | Bounded summaries, then exact rows only |
| Expected candidate growth | `O(K × N / fixed_leaves)` | `O(K × leaf_capacity)` |
| Current implementation status | Implemented in live v7 | Discussed, not yet implemented |

---

# Part X: Important flaws and unresolved details

This is where the architecture needs sharpening before production implementation.

## 24. Critical flaw: repeated child IDs plus naïve XOR

The meetings propose an internal array such as:

```text
child[0..15]  = L0
child[16..31] = L1
```

If the parent hash is computed by XORing all 32 logical child slots:

```text
parent =
    H(L0) XOR H(L0) XOR ... 16 times
    XOR
    H(L1) XOR H(L1) XOR ... 16 times
```

then:

```text
16 copies of H(L0) XOR to zero
16 copies of H(L1) XOR to zero
```

because any even number of identical XOR operands cancels.

The result can become:

```text
parent_hash = 0
```

regardless of the actual data. This is a genuine correctness blocker.

### Correct alternatives

Use a positional cryptographic node hash:

```text
parent_hash =
    BLAKE3(
        domain_separator
        || node_prefix
        || slot_0_commitment
        || slot_1_commitment
        ...
        || slot_31_commitment
    )
```

or hash each logical range with its slot identity:

```text
slot_commitment[i] =
    BLAKE3(slot_number || child_hash || represented_prefix_range)
```

Then aggregate those commitments.

The hash must commit to:

```text
logical slot number
represented prefix range
child subtree hash
```

It must not merely XOR repeated physical-child hashes.

---

## 25. Root hash must be independent of physical compression

These two representations may contain the same logical data:

```text
Representation A:
all 32 logical slots -> one physical leaf

Representation B:
slots 0..15 -> L0
slots 16..31 -> L1
```

A healthy and damaged replica might temporarily have different physical compression due to split timing, restart, or repair history.

Their logical Merkle root should still be comparable.

Therefore:

```text
root hash must represent logical prefix contents,
not physical page arrangement
```

Otherwise a harmless structural difference looks like corruption.

This was not fully resolved in the meetings.

---

## 26. The meetings’ 32-bit or 64-bit tuple-hash suggestion is unsafe

The July 9 discussion considers reducing stored tuple hashes to 32 or 64 bits to save space. fileciteturn4file0

For accidental bit errors, 64 bits may look statistically adequate. But this project explicitly addresses compromised or malicious replicas. An attacker who can choose modified values can target collisions much more effectively than random hardware corruption.

The live repository already uses BLAKE3-256 and describes collision security around a 128-bit birthday bound. The authoritative integrity summary should remain 256 bits. citeturn946078view0

A safe policy is:

```text
route digest:          BLAKE3-256
tuple integrity hash:  BLAKE3-256
node commitment:       BLAKE3-256

optional cached tag:
    64-bit value may be used only as a prefilter,
    never as the authoritative correctness hash
```

---

## 27. Deterministic split timing is not yet fully solved

The meeting correctly rejects ordinary B-tree layout because two transactions touching different user rows may still land in the same Merkle region.

Replica A might physically execute:

```text
insert A
insert B
```

while replica B executes:

```text
insert B
insert A
```

Even when final database contents are identical, an insertion-order-dependent tree can split differently. The transcript explicitly calls this out. fileciteturn4file1

The safest integration with the current repository is:

```text
user transactions
      |
      v
produce semantic Merkle item deltas
      |
      v
commit deltas with globally ordered sequence
      |
      v
single deterministic dynamic Merkle applier
      |
      v
apply insert/delete/update and structural changes
in exact sequence order
```

This naturally extends the current version-7 committed-delta applier. citeturn946078view0turn946078view1

Even better, structure should be canonical for the current item set so that equivalent data cannot lead to different roots merely because historical operation order differed.

---

## 28. Split and merge thresholds need hysteresis

The meetings roughly suggest:

```text
split when count > 32
merge when subtree count < 32
```

That can thrash:

```text
count 33 -> split
delete two rows -> count 31 -> merge
insert two rows -> count 33 -> split
```

Use separate thresholds:

```text
leaf_capacity        = 32
split_threshold      = 32
merge_threshold      = 12 or 16
```

For example:

```text
split when count > 32
merge only when combined subtree count <= 16
```

The exact numbers should remain configurable, but the invariant must require:

```text
merge_threshold < split_threshold
```

---

## 29. Empty-side and no-progress splits need a formal rule

A binary split can theoretically send all entries to one side:

```text
left:  33 entries
right:  0 entries
```

The meeting mentions either creating an empty placeholder or delaying allocation. fileciteturn4file1

A production algorithm needs a precise rule:

```text
1. Consume the next route bit.
2. If both sides are non-empty, create both leaves.
3. If only one side is non-empty:
      advance through additional prefix bits until:
          a) entries separate, or
          b) maximum digest depth is reached.
4. If digest bits are exhausted:
      use a collision/overflow representation and fail closed
      if distinct keys cannot be distinguished.
```

Empty child pointers should use an explicit sentinel, not an ordinary valid node ID.

---

## 30. `node_id` and prefix representation remain ambiguous

The discussion alternates among:

```text
node ID as sequential array position
node ID as prefix bits
separate prefix length
suffix length
parent computed by removing bits
```

These need one canonical definition.

A clean representation is:

```text
partition_id
prefix_bit_length
prefix_value
```

with a packed integer node ID only as an encoding:

```text
node_id = encode(prefix_value, prefix_bit_length)
```

The identity must be deterministic and derived from the logical prefix, not from allocation order, SQL sequences, page numbers, transaction IDs, or TIDs.

---

## 31. Current functional bucket index cannot serve dynamic routing

The static benchmark can create an expression index based on:

```text
merkle_bucket_for_key(index, key)
```

because static geometry never changes.

In a dynamic tree:

```text
key -> leaf
```

depends on mutable split state. After a split, the previously indexed expression value can become stale.

Dynamic recovery instead needs a materialized mapping:

```text
leaf item table/index

(partition_id, physical_leaf_node_id, key, tuple_hash)
```

with a normal B-tree index such as:

```text
(partition_id, physical_leaf_node_id)
```

or:

```text
(index_oid, partition_id, physical_leaf_node_id)
```

The live static recovery relies heavily on its immutable functional bucket index, so this is a real integration boundary, not a cosmetic change. citeturn648025view2

---

## 32. Recovery cannot assume identical physical shape

Update-only corruption may preserve tree shape, but insert/delete corruption can alter counts and splits.

Possible state:

```text
healthy:
    logical range represented by two leaves

damaged:
    same range represented by one leaf
```

Recovery must compare logical prefix ranges, not blindly pair physical node IDs.

A robust comparison algorithm should align ranges:

```text
compare logical range R

if both representations are leaves:
    compare summaries

if both are internal:
    compare corresponding logical slots

if healthy is internal and damaged is leaf:
    expand damaged leaf summary by route prefix
    and compare against healthy children

if healthy is leaf and damaged is internal:
    perform the symmetric operation
```

This shape-mismatch case is not worked out fully in either meeting.

---

# Part XI: Correct consolidated architecture

Putting the sound parts of both meetings together, and repairing the gaps, gives this architecture:

```text
┌──────────────────────────────────────────────────────────────┐
│                 One dynamic Merkle per relation              │
├──────────────────────────────────────────────────────────────┤
│ Fixed partition count                                       │
│ Fixed logical fanout K, initially test K=32                 │
│ Dynamic path-specific height                                │
│ Physical leaf capacity around 32                            │
│ Binary physical splitting inside K-way logical directories  │
│ Prefix-based routing over BLAKE3-256 key digest             │
│ BLAKE3-256 tuple integrity summaries                        │
│ Position-sensitive BLAKE3 internal commitments              │
└──────────────────────────────────────────────────────────────┘
                              |
                              v
┌──────────────────────────────────────────────────────────────┐
│ Dynamic node                                                │
├──────────────────────────────────────────────────────────────┤
│ partition_id                                                │
│ logical_prefix                                              │
│ prefix_length                                               │
│ is_leaf                                                     │
│ tuple_count                                                 │
│ subtree_hash_256                                            │
│ structural_version                                          │
├──────────────────────────────────────────────────────────────┤
│ LEAF:                                                       │
│   bounded [(key, route_digest, tuple_hash_256)]              │
│                                                             │
│ INTERNAL:                                                   │
│   child_node_ids[K]                                         │
│   logical-slot commitments or equivalent canonical hashes   │
└──────────────────────────────────────────────────────────────┘
                              |
                              v
┌──────────────────────────────────────────────────────────────┐
│ Durable mutation path                                      │
├──────────────────────────────────────────────────────────────┤
│ user DML                                                    │
│   -> committed semantic item delta                          │
│   -> globally ordered applier                               │
│   -> deterministic split/merge                              │
│   -> WAL-atomic structure and hash updates                  │
└──────────────────────────────────────────────────────────────┘
                              |
                              v
┌──────────────────────────────────────────────────────────────┐
│ Recovery                                                    │
├──────────────────────────────────────────────────────────────┤
│ compare logical roots                                       │
│   -> descend logical prefix ranges                          │
│   -> fetch bounded key/hash summaries                       │
│   -> identify exact differing keys                          │
│   -> fetch only necessary full rows                         │
│   -> perform set-based repair                               │
│   -> confirm summaries and roots                            │
└──────────────────────────────────────────────────────────────┘
```

---

# Precise status of the project

```text
Already implemented in live main:
  ✓ BLAKE3-256 canonical row hashing
  ✓ BLAKE3-256 uniform route digest
  ✓ full route digest retained
  ✓ static F32/L1024 tree
  ✓ batched recovery localisation
  ✓ indexed static leaf-row fetch
  ✓ durable committed Merkle deltas
  ✓ ordered page applier
  ✓ WAL-safe per-page application sequence
  ✓ stale-index freshness handling

Discussed but not implemented:
  ✗ dynamic node representation
  ✗ explicit child-pointer arrays
  ✗ bounded key/hash leaf summaries
  ✗ physical binary split
  ✗ local height growth
  ✗ deterministic merge
  ✗ logical-versus-physical hash commitment
  ✗ dynamic recovery summary comparison
  ✗ shape-independent comparison
```

The live code is therefore **static Merkle v7 with several useful dynamic prerequisites**, not a partially implemented dynamic tree.

The meeting architecture is intellectually strong: it replaces global rebuilding with local prefix growth and turns recovery candidate work from table-size-dependent to leaf-capacity-dependent. The dangerous gap is that the whiteboard structure is not yet a complete cryptographic and transactional specification. In particular, repeated child pointers cannot be combined with naïve XOR, and deterministic split/merge execution must be attached to the existing globally ordered v7 applier rather than ordinary concurrent worker timing.