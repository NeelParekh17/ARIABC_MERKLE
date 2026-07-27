# Current dynamic Merkle architecture and reproducible DML walkthrough

Date: 2026-07-27
Live server used: `127.0.0.1:5438`, database `postgres`, user `neel`
Data directory: `/work/ARIABC/pgdata`

This document describes the implementation that is currently running in this
checkout. It distinguishes the native v8 `synchronous_cow` path from the older
side-table implementation and records commands actually run against the local
server.

## Executive summary

The current dynamic index is a native v8 append-only Merkle structure stored in
the Merkle index relation's pages. `merkle_dynamic_state` is used for
configuration, counters, sequence/profiling metadata, and reports; it is not
the authoritative node store when `update_mode = 'synchronous_cow'`. The live
stats output explicitly reports `authority: "native_index_pages"`.

Each indexed row contributes:

1. a canonical key encoding;
2. a route digest used to choose a partition and locate a prefix bucket; and
3. a row/tuple digest covering the heap row.

Leaves store bounded item records. Internal records summarize their children.
The data commitment is maintained with XOR-style item commitments, while the
structure commitment includes topology. The combined root commits to both.

The implementation has two different fanout notions:

| Field | Current meaning |
| --- | --- |
| `logical_fanout = 32` | Route geometry is 32-way logically, so one logical level consumes 5 route bits. |
| `physical_node_fanout = 2` | Native node records use left/right child locators and are physically binary. |

These values must not be conflated. The live v8 stats from the final `test.sql`
plus `split.sql` run reported `logical_fanout=32` and
`physical_node_fanout=2`.

## The complete data path

The easiest way to understand the implementation is to follow one row from
SQL to the committed root:

```text
                         SQL statement
                              |
                              v
                     PostgreSQL heap row
                              |
              +---------------+----------------+
              |                                |
              v                                v
      indexed key columns                all heap columns
              |                                |
              v                                v
   canonical key serialization       canonical row serialization
              |                                |
              v                                v
       BLAKE3 route digest              BLAKE3 tuple hash
              |                                |
              +---------------+----------------+
                              |
                              v
             transaction-local Merkle transition
          (partition, route, key, old hash, new hash)
                              |
                    PRE_COMMIT for native v8
                              |
                              v
        lock partition -> read visible root -> copy-on-write
        changed path -> append records -> publish new root
                              |
                              v
             ordered partition roots -> global roots
```

The important consequence is that the Merkle AM is not maintaining a separate
key-only index whose hash is occasionally recomputed. The index stores the
canonical key and the row hash needed to prove both routing and row content.
The indexed key determines *where* the item lives; every heap column determines
the item's content commitment.

## On-disk native v8 layout

The native index relation contains immutable append records. A locator is not
just a block number: it contains `(block, offset, page_generation)`. The page
generation prevents an old locator from silently referring to a reused page.
Every record has a magic value, format version, type, size, and CRC checksum.

```text
Merkle index relation
+
  metapage
  +--------------------------------------------------------------+
  | dynamic marker | layout v8 | partitions | fanout | capacities |
  +--------------------------------------------------------------+

  native pages
  +------------------+  +------------------+  +------------------+
  | page header      |  | page header      |  | page header      |
  | generation = 11  |  | generation = 4   |  | generation = 9   |
  | ROOT version     |  | INTERNAL record  |  | LEAF record      |
  | ROOT version     |  | ITEM chunks      |  | ITEM chunks      |
  +------------------+  +------------------+  +------------------+

  per-partition visible root chain
  root(version N)
       |
       +--> root_node locator ----------------------+
       |                                             |
       +--> previous_version locator                 v
                                             internal / leaf records
```

The root-version record contains the transaction/XID visibility information,
sequence provenance, tuple count, byte count, `data_xor`, `content_xor`,
`structure_hash`, and the locator of the visible tree root. A new mutation
does not edit the old visible tree records. It appends replacement records and
publishes a new root version that points at them.

The record types are:

```text
INTERNAL  : prefix, tuple_count, subtree_bytes, hashes, 32 logical slots
LEAF      : prefix, tuple_count, subtree_bytes, hashes, item_head
ITEM      : route_digest, tuple_hash, key length, canonical key bytes
CHUNK     : packed item payloads linked by locators
ROOT      : transaction-visible partition commitment and root locator
```

The current v8 item chunk format can compact fixed-width single-key values.
For ordinary integer `ycsb_key` demonstrations, the key bytes are therefore
much smaller than a general composite canonical key. Large or mixed-key items
fall back to the general packed representation, and the byte-capacity check is
performed on the actual encoded item size.

## Routing and prefix geometry

For the `test.sql` index, let a route digest begin with the following bits:

```text
route digest:  1 0 1 1  0 0 1 0  1 1 0 0  ...
               \______/  \______/  \______/
                first     second    third
             logical slot logical slot logical slot
```

With logical fanout 32, `native_logical_width(32)` returns 5. The first five
route bits select one of 32 logical slots at the partition root. The next five
bits select a slot below that logical directory, and so on. The partition is
selected separately from the first eight route bytes by the current dynamic
identity code:

```text
canonical indexed key
        |
        +--> route_digest = BLAKE3(canonical key)
        |
        +--> route_value = first 8 digest bytes interpreted as an integer
        |
        +--> partition_id = route_value % partitions
```

For `partitions=2`, this is a modulo partition mapping, not a range partition
on the integer key. Two numerically adjacent keys can land in different
partitions, and two far-apart keys can share one partition.

An internal prefix is stored as `(prefix_len, prefix[32])`; unused bits after
`prefix_len` are canonicalized to zero. A leaf can cover a contiguous range of
logical slots when its prefix is shorter than the directory width. The same
physical locator may consequently appear in adjacent logical slots. Readers
and verifiers deliberately de-duplicate equal locators while aggregating.

```text
logical directory at prefix_len = 0, width = 5 bits

slot:     0   1   2   3   4   5   6   7   ... 30  31
          |   |   |   |   |   |   |   |       |   |
          +---+---+---+---+---+---+---+-------+---+
              one leaf locator may cover a contiguous run

example: slots 8..11 -> locator L17
         slots 12..31 -> locator L22
```

This is why `logical_fanout=32` does not imply that every internal record has
32 distinct physical subtrees, and why a report must show both logical and
physical fanout.

## Initial index build

`CREATE INDEX ... USING merkle` enters the dynamic build path in
`merklebuild.c`. The build scans the heap, computes canonical identities and
row hashes, sorts/spools items by partition and route, builds each partition,
and publishes a baseline root.

```text
heap scan
   |
   +--> (partition, route_digest, canonical_key, tuple_hash)
             |
             v
      sort by route/key within each partition
             |
             v
      recursive native_build_spooled_range()
             |
       +-----+----------------------+
       |                            |
       v                            v
  bounded item set             over-capacity set
  -> write LEAF                -> split by route bits
                                  -> recurse
             |
             v
      write INTERNAL summaries
             |
             v
      publish baseline ROOT_VERSION
```

On an empty partition, the build still materializes an empty leaf/root
commitment so that the global root has one canonical partition entry for every
configured partition. This is why a 150-partition `merkle_demo.sql` build
reports 150 partitions even when most contain no rows.

## Split algorithm, step by step

The small reproduction uses `leaf_capacity=6`. Conceptually, suppose one
leaf receives seven items whose route bits are ordered as follows:

```text
old leaf prefix P

route suffixes after P:
  000...  001...  001...  010...  011...  100...  101...
  <-------------------- 7 items ------------------------->

count = 7 > leaf_capacity = 6
```

The active native algorithm is recursive and route-ordered:

```text
native_build_subtree(P, items[7])
        |
        | count/bytes do not fit a leaf
        v
native_build_item_segment(P, bit = P)
        |
        | inspect the current route bit and find the first 1
        +----------------------+----------------------+
        |                      |
        v                      v
   items with bit 0       items with bit 1
   recurse at bit+1       recurse at bit+1
        |                      |
        +----------+-----------+
                   v
        assign each resulting leaf to its
        logical slot/range in the 32-slot directory
                   |
                   v
        build one INTERNAL summary for prefix P
```

There are three important stopping rules:

1. If `count <= leaf_capacity` *and* encoded bytes fit, write a leaf.
2. If the current bit does not separate the items, advance to the next bit
   until a separating bit is found.
3. Once a logical five-bit directory has been traversed, recurse into the next
   logical directory rather than attaching a child at the same prefix. This
   preserves the slot-to-prefix mapping.

The result is copy-on-write:

```text
BEFORE                                  AFTER

root R0                                 root R1 (new record)
  |                                       |
  +--> leaf L0 (7 items)                  +--> internal I1 (new)
                                              |        |
                                              |        +--> leaf L1
                                              |        +--> leaf L2

R0 and L0 remain readable as historical records.
Only R1, I1, L1, and L2 are appended.
```

The split counter is incremented when the algorithm actually divides a
non-fitting segment or creates an internal build summary. The script comments
describe expected trigger inserts, but the counter and returned frontier are
the reliable evidence.

## Delete and merge algorithm

A delete is first converted into an old-item transition. At apply time, the
native writer loads only the affected path and applies the transition to the
affected leaf/subtree:

```text
root
  |
  +--> internal P
          |
          +--> child A  -- delete old item --> A'
          |
          +--> child B  ---------------------> B
          |
          +--> other children reused
                       |
                       v
              recompute P' summary
```

If a child becomes empty, its locator is removed. Then the parent is tested
for merge eligibility. With `merge_threshold=3`, the relevant condition is:

```text
count(A') + count(B)
        <= 3                         count threshold
        <= leaf_capacity             physical leaf bound
and bytes(A') + bytes(B)
        <= leaf_byte_capacity        encoded-size bound
```

When all conditions pass, both child subtrees are collected into one leaf at
the parent prefix:

```text
BEFORE                                  AFTER

internal prefix 9000...                leaf prefix 9000...
       /       \                              |
  leaf 9000...  leaf 9000...                  +--> 3 items
    1 item        2 items

one new leaf record replaces the parent subtree in the new root path
```

That is the exact shape observed in `split_merge.sql`: after deleting six
keys, the live frontier showed one depth-5 `9000...` leaf containing three
tuples, `merge_count` increased from 0 to 1, and node/leaf counts decreased.

## Update algorithm and transaction timing

The update path is deliberately two-sided because the old tuple is needed to
remove the old commitment and the new tuple is needed to add the new one:

```text
UPDATE usertable_small SET field1 = 'updated-value' WHERE ycsb_key=999998

 executor before heap update
        |
        +--> fetch OLD row
        +--> old canonical key/route
        +--> old tuple_hash
        +--> save delete plan in executor context
        |
        v
     heap update succeeds
        |
        +--> stage OLD transition: has_old=true
        +--> fetch NEW row
        +--> new canonical key/route
        +--> new tuple_hash
        +--> stage NEW transition: has_new=true
        |
        v
      PRE_COMMIT
        |
        +--> merge same-key old/new entries
        +--> sort transitions deterministically
        +--> group by index OID and partition
        +--> native publish
```

For a non-key update, old and new route/key are equal, so the native item
vector replaces the tuple hash in place logically. The physical implementation
still writes a new leaf/path/root record because native records are immutable.
For a key update, the old transition and new transition can target different
partitions, so two partition paths may be published in one transaction.

If a transaction inserts and then deletes the same key before commit, the
transaction-local delta composer can cancel the net item transition. This is
why the normal runtime does not publish every intermediate SQL statement as a
separate durable tree version; it publishes the committed net effect.

## Root construction and visibility

Each partition has its own visible root version. The global native commitment is
constructed in two independent layers:

```text
partition 0 root: content_xor_0, structure_hash_0, count_0
partition 1 root: content_xor_1, structure_hash_1, count_1
                 ...
partition P-1 root: content_xor_P, structure_hash_P, count_P
                                  |
             +--------------------+--------------------+
             |                                         |
             v                                         v
data_root = H(domain, versions,                 structure_root = H(domain,
             partition ids, counts,              layout, partition ids,
             content_xor values)                counts, structure_hashes)
             topology-independent               topology-sensitive
             |                                         |
             +--------------------+--------------------+
                                  v
combined_root = H(domain, layout/route/row versions,
                  data_root, structure_root)
```

This distinction matters. Two trees can contain the same item multiset but
have different leaf topology after different split histories. `data_root`
captures content independently of topology; `structure_root` captures the
topology. `merkle_root_hash()` returns the combined native root for a ready
`synchronous_cow` index.

During PRE_COMMIT, native transitions are sorted by index and partition. The
writer locks all touched partitions, reads the latest writable visible root,
applies each partition batch, then publishes the new root version. A failpoint
exists immediately before root publication, which is the crash-safety boundary
for the native copy-on-write update.

## What verification actually checks

`merkle_dynamic_verify()` dispatches to native verification when the index is
ready and uses `synchronous_cow`. Native verification checks:

```text
for every partition:
  visible root exists
       |
       v
  recursively read every reachable locator
       |
       +--> page generation / record magic / version / checksum
       +--> canonical prefix and partition identity
       +--> leaf item count and byte bound
       +--> leaf data_xor/content_xor/structure_hash
       +--> internal child slot mapping and summaries
       +--> root summary equals reachable tree summary
       |
       v
  scan heap once and compare sorted canonical items
  (spillable tuplesort is used for bounded verification memory)
```

So `verify=t` is stronger than “the root query returned a hash”: it checks the
native records, their summaries, and the heap/index item set under one snapshot.

## Native v8 versus the older dynamic side-table path

The repository still contains older `merkledynamic.c` code and inspection
functions for the compatibility side-table representation. That path uses
`ariabc_internal.merkle_dynamic_node` and
`ariabc_internal.merkle_dynamic_leaf_item` as its authoritative representation.
It is not the active authority for the index created by your SQL scripts because
those scripts request `update_mode='synchronous_cow'` and the native index is
ready. The dispatch is explicit:

```text
dynamic index + synchronous_cow + native ready
       |                         |
       +--> merkle_native_verify_relations()
       +--> merkle_native_root()
       +--> native transition publisher

otherwise, where supported
       +--> compatibility dynamic side-table routines
```

Therefore the SQL inspection functions are useful public probes, but a query
against `ariabc_internal.merkle_dynamic_node` should not be described as proof
that native index pages are the authority. For the current path, use native
stats (`authority=native_index_pages`), native verification, roots, and the
frontier/range APIs backed by native traversal.

## Source map for a guide or code review

These are the implementation locations behind the diagrams above:

| Responsibility | Live implementation |
| --- | --- |
| v8 constants, locators, root versions, node/leaf/item records | [`src/include/access/merkle.h`](/work/ARIABC/AriaBC/src/include/access/merkle.h:133) and [record definitions](/work/ARIABC/AriaBC/src/include/access/merkle.h:220) |
| Canonical key, route digest, modulo partition selection | [`merkle_compute_dynamic_item_identity()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c:594) |
| Native index build entry and baseline publication | [`merklebuild.c`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklebuild.c:360) and [`native_build_spooled_range()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:1871) |
| Logical width and route-slot calculation | [`native_logical_width()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:396) and [`native_route_slot()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:412) |
| Leaf packing and item chunks | [`native_write_leaf()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:1417) |
| Insert hash and semantic new-item staging | [`merkleinsert.c`](/work/ARIABC/AriaBC/src/backend/access/merkle/merkleinsert.c:60) |
| Delete-before/after heap safety | [`CaptureMerkleDeletePlan()`](/work/ARIABC/AriaBC/src/backend/executor/nodeModifyTable.c:253) and [`ApplyMerkleDeletePlan()`](/work/ARIABC/AriaBC/src/backend/executor/nodeModifyTable.c:357) |
| Transaction-local delta composition and PRE_COMMIT publication | [`merkledelta.c`](/work/ARIABC/AriaBC/src/backend/access/merkle/merkledelta.c:582) and [transaction callback](/work/ARIABC/AriaBC/src/backend/access/merkle/merkledelta.c:689) |
| Native path apply, path rebuild, split, and merge | [`native_apply_batch_node()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:2703), [`native_build_subtree()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:1739), and [`native_finish_internal_update()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:2660) |
| New root publication and split/merge profiling | [`native_apply_transitions_authorized()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:3143) |
| Combined data/structure/global roots | [`native_compute_commitments()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:3331) |
| Native structural and heap-item verification | [`native_verify_node()`](/work/ARIABC/AriaBC/src/backend/access/merkle/merklenative.c:3455) and [verification dispatch](/work/ARIABC/AriaBC/src/backend/access/merkle/merkledynamic.c:3936) |

## What each supplied SQL file really does

### `test.sql`

[test.sql](../test.sql) is setup only. It drops `usertable_small`, creates a
table with `ycsb_key` plus `field1` through `field10`, adds a primary key, and
creates a native dynamic index with:

```text
partitions=2
leaves_per_partition=1024
fanout=32
leaf_capacity=6
merge_threshold=3
leaf_byte_capacity=65536
max_key_bytes=1024
update_mode=synchronous_cow
```

It performs no insert, update, delete, verification, or statistics query.

### `split.sql`

[split.sql](../split.sql) assumes that `test.sql` has already created the
table and index. It contains 200 individual inserts. It does not contain any
`UPDATE`, `DELETE`, verification query, or counter query. Its comments predict
which inserts trigger splits; the authoritative result is the live stats query,
not a comment or key-number pattern.

### `split_merge.sql`

[split_merge.sql](../split_merge.sql) is self-contained setup plus 200 inserts.
It enables `merkle_native_profile_enabled`, prints partition-1 internals and
leaf frontiers, then deletes exactly these six keys:

```sql
DELETE FROM public.usertable_small
WHERE ycsb_key IN (292, 414, 487, 330, 303, 168);
```

It does not contain an update. Its comments say that the delete collapses the
`9000...` depth-6 branch. The live run confirmed this: the branch became one
depth-5 leaf with three tuples.

### `merkle_demo.sql`

[merkle_demo.sql](../merkle_demo.sql) assumes that `usertable_small` already
exists. It drops and recreates only the dynamic index, using 150 partitions,
logical fanout 32, capacity 32, merge threshold 8, and
`synchronous_cow`. It then exposes verification, roots, JSON stats, internal
nodes, roots, leaf frontiers, ranges, and item-level inspection functions.

Its DML section runs an insert with an `ON CONFLICT DO UPDATE`, then deletes
key `999999`. In the run recorded here that key did not exist, so the action
took the insert path; it is not a standalone update test. The helper query
that searches for a hard-coded leaf hash is data-dependent and returned no
rows in this run. That is expected when the current data or row-hash inputs do
not match the hard-coded digest.

## Commands to reproduce the live demonstrations

Start the server as in the request:

```bash
cd /work/ARIABC/AriaBC
./scripts/start_server.sh
```

Connect explicitly to the same server:

```bash
/work/ARIABC/install/bin/psql -X -h 127.0.0.1 -p 5438 -d postgres
```

Reproduce the small split run:

```bash
/work/ARIABC/install/bin/psql -X -h 127.0.0.1 -p 5438 -d postgres \
  -v ON_ERROR_STOP=1 -f test.sql -f split.sql
```

Inspect its result:

```sql
SELECT count(*) FROM usertable_small;
SELECT merkle_dynamic_verify('usertable_small_dynamic_merkle_idx'::regclass);
SELECT merkle_dynamic_tree_stats('usertable_small_dynamic_merkle_idx'::regclass);
```

For a controlled insert/update/delete probe that leaves no extra row:

```sql
SELECT merkle_root_hash('usertable_small');

INSERT INTO usertable_small(ycsb_key, field1)
VALUES (999998, 'insert-value');
SELECT merkle_root_hash('usertable_small');

UPDATE usertable_small
SET field1 = 'updated-value'
WHERE ycsb_key = 999998;
SELECT merkle_root_hash('usertable_small');

DELETE FROM usertable_small WHERE ycsb_key = 999998;
SELECT merkle_root_hash('usertable_small');
SELECT merkle_dynamic_verify('usertable_small_dynamic_merkle_idx'::regclass);
```

The final root should equal the first root if no other transaction changes the
table. The update should produce a different root from the insert because the
row hash changes even though the key route does not.

Reproduce the split/merge campaign instead with:

```bash
/work/ARIABC/install/bin/psql -X -h 127.0.0.1 -p 5438 -d postgres \
  -v ON_ERROR_STOP=1 -f split_merge.sql
```

This script drops and recreates `usertable_small`, so do not run it against a
table whose contents you need to preserve.

## Actual execution evidence from this run

### `split_merge.sql`

The live result before the six-key delete was:

```text
split_count=45  merge_count=0  leaf_count=48  node_count=63
```

After the delete:

```text
split_count=45  merge_count=1  leaf_count=47  node_count=61
partition 1, prefix_len 5, prefix 9000..., is_leaf=true, tuple_count=3
```

### Controlled DML probe

The probe returned `verify=t` before and after the operations. The temporary
row changed `item_count` from 194 to 195 on insert, remained 195 on update,
and returned to 194 on delete. The combined roots were:

```text
baseline     13b7352be8822bf312f67cc2921792e159f0b37eeed5722765ff47e108f96b02
after insert 33b57523fa7348dfb879b0966d8cb321ceec9fbdb3d68544644b7e5567db5245
after update 9b328f3de556f85a4d9ed21b591e862e1d85f45599f4f1ea6f25c560bf178096
after delete 13b7352be8822bf312f67cc2921792e159f0b37eeed5722765ff47e108f96b02
```

The stored item lookup for key `999998` showed partition `1`, prefix length
`5`, route digest
`1e5abf96c924aac1c7fda18056510c0f4288599f4abaa5f46e90d4bd5cb92afc`, and a
tuple hash for the updated row.

### Final small reproduction state

After running `test.sql` and `split.sql`, the live server reported:

```text
rows=200
verify=t
partitions=2
logical_fanout=32
physical_node_fanout=2
leaf_capacity=6
merge_threshold=3
split_count=45
merge_count=0
leaf_count=48
node_count=63
layout_version=8
update_mode=synchronous_cow
authority=native_index_pages
```

## Important interpretation limits

- A root hash proves the current commitment, not that a specific script's
  comments were followed. Use the returned topology and stats to prove splits
  and merges.
- `leaf_count`, `node_count`, and split/merge counters are generation state;
  they do not mean that every native record ever written remains live.
- `merkle_dynamic_verify` checks the live native structure against the heap
  under the current implementation. It is the required correctness check for
  these demonstrations.
- The SQL demos use destructive `DROP TABLE` or `DROP INDEX` statements.
  Reproduce them only when replacing `usertable_small` is intended.

## Guide-ready conclusion

The implementation currently demonstrated to a guide is:

```text
PostgreSQL DML
    -> canonical key/route + full-row hash
    -> transaction-local old/new semantic transitions
    -> PRE_COMMIT deterministic grouping by index and partition
    -> native v8 copy-on-write path rebuild
    -> immutable leaf/internal/item records appended to index pages
    -> XID-visible partition root publication
    -> topology-independent data root + topology-sensitive structure root
    -> combined global root and heap/index verification
```

The split/merge campaign is not merely a visualization. With capacity 6 and
merge threshold 3, it exercised the actual native transition publisher and
observed 45 split events followed by one threshold-and-byte-safe merge. The
separate DML probe proved that insert changes membership, update changes row
content without changing membership count, and delete removes the item and
restores the previous combined root.
