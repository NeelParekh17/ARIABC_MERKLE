# Implementation Plan: Meeting-Transcript Hash-Range Dynamic Merkle

**Date:** 27 July 2026
**Source:** `Dynamic_merkle_docs/Meeting_transcript_23rd_July.txt`
**Baseline:** native dynamic Merkle layout v8, synchronous copy-on-write

## 1. Decision

Implement the transcript proposal as an **opt-in `hash_range` mode**, while retaining current native v8 as the correctness baseline.

The proposal is valuable: a Merkle leaf represents a hash-prefix range, while an ordered index supplies the rows in that range. Splitting a prefix changes only Merkle metadata; it does not rewrite every row's ownership mapping.

It is not a small local patch to v8. Current v8 uses key-derived routing, native leaf item records, one transition route, and item-based leaf updates. The new mode changes all four assumptions:

1. Route = prefix of the full canonical tuple hash.
2. Rows are found by an ordered hash locator/index.
3. Leaves store range summaries, not complete row items.
4. An update can touch an old and a new range.

Therefore, allocate a new layout/record version (proposed v9) and never reinterpret v8 records as v9 records.

```text
current v8                         transcript mode
-----------                        ---------------
synchronous_cow                    hash_range (opt-in)
indexed-key route                  full-tuple-hash route
native leaf items                  external hash locator
item ownership                     prefix range summaries
strict v8 checks                   mode-specific checks
```

## 2. Transcript idea in one picture

```text
heap row
   |
   | canonical full tuple hash H(row)
   v
ordered B-tree: tuple_hash -> stable row identity
   |
   | range [lower(prefix), upper(prefix))
   v
rows in one hash prefix
   |
   | count, xor/content commitment, structure commitment
   v
Merkle node for that prefix
```

A prefix `1010` means the half-open interval:

```text
[1010 0000..., 1011 0000...)
```

If the range is too large, the node becomes internal and is replaced by children `10100` and `10101`. The tuple hashes and application rows do not change.

The transcript's important performance requirement is an **index-only range scan**. If a split must fetch every row from the heap, random I/O can remove the intended benefit.

## 3. Current architecture that remains unchanged

The current native-v8 path has:

- layout version 8;
- logical fanout 32 and physical fanout 2;
- synchronous copy-on-write publication;
- native node, leaf, item, and item-chunk records;
- leaf items containing route digest, tuple hash, and canonical key material;
- a route derived from the indexed key;
- old/new DML capture and pre-commit publication;
- separate content/data and structure/topology commitments.

The new mode must be selected explicitly and must report its mode, layout, route definition, and locator generation. Existing v8 tests and runners must continue to exercise v8.

## 4. Target architecture

```text
                 one PostgreSQL transaction
                           |
      +--------------------+--------------------+
      |                    |                    |
      v                    v                    v
   user heap        hash locator B-tree       Merkle v9 COW
   row mutation     tuple_hash -> identity   prefix summaries
      |                    |                    |
      +------------- old/new hashes ----------+
                           |
                           v
                 pre-commit root publication
```

The three states are:

1. User heap: authoritative application row.
2. Hash locator: ordered, index-only-readable tuple hash and identity.
3. Merkle metadata: copy-on-write prefix nodes and commitments.

They must commit atomically. A committed locator move without the corresponding root, or a root without the locator move, is invalid.

### 4.1 Selected locator design

The transcript discusses a generated or maintained hash column on the user table. That is useful for a prototype, but not generic enough for AriaBC: arbitrary user tables cannot safely be altered, multiple Merkle indexes need generation metadata, and a functional index may not prove index-only behavior.

Use an internal relation, conceptually:

```text
ariabc_internal.merkle_tuple_locator
-------------------------------------
index_oid
index_generation
canonical_key
stable_row_identity
tuple_hash                 32 bytes
last_sequence
```

Create a covering B-tree ordered by:

```text
(index_oid, index_generation, tuple_hash,
 canonical_key, stable_row_identity)
```

The tuple hash is the range key. The other columns disambiguate equal hashes and support index-only verification.

#### Identity rule

The first implementation should require a unique indexed key or another stable logical row identity. A raw heap TID is not durable across CLUSTER and table rewrites. If TID is used experimentally, REINDEX/rebuild after rewrite must be mandatory and documented.

Hash uniqueness must not be assumed. A hash collision is not an identity policy.

### 4.2 Optional SQL prototype

A controlled demo can use a stored generated column:

```sql
ALTER TABLE public.usertable_small
  ADD COLUMN merkle_tuple_hash bytea
  GENERATED ALWAYS AS (ariabc_tuple_hash(...)) STORED;

CREATE INDEX usertable_small_merkle_hash_idx
  ON public.usertable_small (merkle_tuple_hash);
```

This proves the SQL/index-only concept. The backend implementation should still use the internal locator so normal user schemas remain supported.

## 5. New native record model

### 5.1 Versioning

Add a new layout version, proposed as:

```c
#define MERKLE_DYNAMIC_LAYOUT_VERSION 9
```

Allocate a new native record/checksum version too. Readers must reject a v8 record in v9 mode and vice versa.

### 5.2 Node fields

A v9 node stores:

```text
locator
prefix length and canonical prefix bytes
is_leaf
tuple_count
estimated subtree bytes
content/data commitment
structure commitment
child locators/references
generation and checksum
```

A v9 leaf does not contain v8's complete item vector. It contains a summary; the locator B-tree supplies the rows for a prefix range.

```text
v8: prefix -> item records -> canonical keys
v9: prefix -> summary; locator B-tree -> rows
```

The tree still needs `is_leaf` and child metadata. Removing row items does not mean removing topology. A map with only commitments cannot traverse arbitrary materialized prefixes unless child existence is represented elsewhere.

## 6. Prefix mathematics

Represent every tuple hash as a fixed-width big-endian 256-bit value. A node has prefix length `p`.

```text
lower(P) = P followed by zero bits to 256 bits
upper(P) = first 256-bit value after P's interval
range    = [lower(P), upper(P))
```

For `1010`:

```text
lower = 1010 0000...0000
upper = 1011 0000...0000
```

Use a half-open range; never construct an inclusive upper bound that overflows for an all-ones prefix.

Add one canonical helper implementation for:

```text
merkle_hash_prefix
merkle_hash_prefix_lower
merkle_hash_prefix_upper
merkle_hash_prefix_contains
merkle_hash_prefix_child
```

Build, split, merge, verification, and recovery must all use these helpers.

## 7. Index-only scan contract

Every prefix operation must be equivalent to:

```sql
SELECT tuple_hash, canonical_key, stable_row_identity
FROM ariabc_internal.merkle_tuple_locator
WHERE index_oid = $1
  AND index_generation = $2
  AND tuple_hash >= $3
  AND tuple_hash <  $4
ORDER BY tuple_hash, canonical_key, stable_row_identity;
```

Prove it with:

```sql
EXPLAIN (ANALYZE, BUFFERS) ...
```

The fixture must show an index-only scan and zero heap fetches. If visibility requires heap fetches, report that fact; do not label the operation index-only.

A locator entry with no heap row, or a heap row whose recomputed hash differs, is a verification error.

## 8. Exact source changes

### 8.1 `src/include/access/merkle.h`

Add:

- `hash_range` mode identifiers;
- v9 node/leaf metadata;
- locator metadata;
- prefix-bound helper declarations;
- mode/version fields in root and verification output;
- old-route and new-route fields in transitions.

The current transition has one route. Hash-range UPDATE needs:

```text
old_route_digest = prefix of old tuple hash
new_route_digest = prefix of new tuple hash
```

Never derive the old route from the new row.

### 8.2 `src/backend/access/merkle/merkle.c`

Add an explicit reloption, proposed as:

```text
merkle_dynamic_mode = 'synchronous_cow' | 'hash_range'
```

Mode is immutable for a physical generation. Changing it requires a rebuild/new generation. Report:

```text
mode, layout_version, logical_fanout, physical_fanout,
route_definition, locator relation, locator generation
```

### 8.3 `src/backend/access/merkle/merkleutil.c`

Implement:

- canonical full tuple hashing;
- hash-to-prefix route derivation;
- lower/upper bounds;
- stable identity comparison;
- deterministic locator ordering;
- duplicate/collision diagnostics.

The same canonical tuple bytes must be used for both hash calculation and route derivation.

### 8.4 Locator module and schema

Add a dedicated locator implementation under `src/backend/access/merkle/` and schema/catalog support in the existing AriaBC SQL infrastructure.

Implement:

- locator relation creation per index generation;
- covering B-tree creation;
- build population;
- insert/delete/move maintenance;
- heap/locator validation;
- rebuild after REINDEX or heap rewrite;
- index-only diagnostics and counts.

Locator changes must be part of the user transaction, not an autocommit side operation.

### 8.5 Build path

Touch `merklebuild.c` and `merklenative.c`. Build order:

```text
1. Create locator generation.
2. Scan heap.
3. Compute canonical tuple hash and identity.
4. Insert locator records.
5. Build covering B-tree.
6. Verify ordering and duplicate policy.
7. Scan ranges in canonical order.
8. Build v9 prefix nodes and commitments.
9. Publish one baseline root.
10. Mark generation active.
```

Do not activate the root before locator construction and verification finish.

### 8.6 Native apply path

Add a path beside v8's item-based apply:

```text
native_apply_hash_range_batch(root, transitions)
  -> group old/new changes by affected prefix
  -> apply removals and insertions
  -> split/merge affected ranges
  -> COW affected nodes
  -> recompute ancestor commitments
  -> return staged root
```

Do not scan the whole table for every transaction. Range-scan only affected prefixes, with incremental commitment updates where safe.

### 8.7 DML and pre-commit

Touch `src/backend/executor/nodeModifyTable.c` and `merkledelta.c`. Preserve the current safe ordering:

```text
DELETE: capture old -> heap delete -> locator delete -> stage removal
INSERT: heap insert -> locator insert -> stage addition
UPDATE: capture old -> heap update -> locator move -> stage old+new
```

Every transition contains:

```text
stable identity
old tuple hash (optional)
new tuple hash (optional)
old route (optional)
new route (optional)
old/new canonical identity data as required
```

At pre-commit:

```text
1. Coalesce repeated operations by stable identity.
2. Apply locator mutations deterministically.
3. Build v9 transitions.
4. Apply v9 COW changes.
5. Publish locator/root generation atomically.
6. Abort on any failure.
```

The chosen internal ordering must never expose a committed half-state.

## 9. Worked DML examples

Assume:

```text
leaf capacity = 3
merge threshold = 3
prefix display = first 4/5 bits
table = public.usertable_small
```

### 9.1 INSERT

Before:

```text
root
└── leaf P=0010, count=2, commitment=C_old
```

Insert `id=100`, with:

```text
h_new = 001011001101...
route = 0010
```

Execution:

```text
1. Heap INSERT succeeds.
2. Insert locator (id=100, h_new, identity).
3. Stage old absent/new h_new/new route 0010.
4. Pre-commit updates leaf 0010.
5. Count 2 -> 3.
6. Recompute content and ancestor commitments.
7. Publish one root.
```

If count becomes 4, invoke split.

### 9.2 DELETE

Delete `id=101`, with:

```text
h_old = 001101110010...
route = 0011
```

Execution:

```text
1. Capture old tuple/hash before heap deletion.
2. Heap DELETE succeeds.
3. Delete locator entry.
4. Stage old removal from 0011.
5. Decrement affected summary.
6. Recompute commitments.
7. Merge eligible siblings.
8. Publish one root.
```

If heap deletion fails, no locator or Merkle deletion is published.

### 9.3 UPDATE within one prefix

```text
h_old = 001001...
h_new = 001111...
old route = new route = 0010
```

The count stays constant but the content commitment changes:

```text
locator: replace h_old by h_new
Merkle: remove old tuple commitment, add new tuple commitment
```

Same prefix is not a no-op.

### 9.4 UPDATE across prefixes

```text
h_old = 001011...
h_new = 110010...
old route = 0010
new route = 1100
```

Execution:

```text
1. Capture old tuple/hash.
2. Heap UPDATE succeeds.
3. Delete old locator; insert new locator.
4. Stage removal from 0010 and insertion into 1100.
5. Apply both paths in one COW publication.
6. Merge old ancestors if underfull.
7. Split new ancestors if overfull.
8. Publish one root.
```

This is the main trade-off: an UPDATE can touch two paths. A split itself does not rewrite all locator rows.

### 9.5 Indexed-key UPDATE

If the indexed key is the stable identity, its update changes identity and hash. Either support an explicit old-identity/new-identity transition, or reject it in the first scope and require delete+insert semantics. Never leave the old locator entry behind.

### 9.6 Repeated operations in one transaction

```sql
BEGIN;
UPDATE usertable_small SET value = 'b' WHERE id = 100;
UPDATE usertable_small SET value = 'c' WHERE id = 100;
COMMIT;
```

Coalesce to one old-hash -> final-hash transition. No intermediate root is visible. Rollback must leave both locator and Merkle state unchanged.

## 10. Split design

### 10.1 Trigger

Split when:

```text
tuple_count > leaf_capacity
OR estimated bytes > leaf_byte_capacity
```

Use hysteresis so split/merge does not oscillate.

### 10.2 Worked split

Capacity is 3. Leaf `0010` contains:

```text
001000...
001001...
001011...
001111...
```

The fourth row triggers:

```text
1. Range-scan locator for [0010...,0011...).
2. Require index-only evidence.
3. Partition by next bit.
4. Next-bit 0 -> child 00100.
5. Next-bit 1 -> child 00101.
6. Compute child counts/commitments.
7. COW-create both children.
8. Rewrite parent 0010 as is_leaf=false.
9. Attach child references.
10. Recompute parent and ancestors.
11. Publish root.
```

Result:

```text
before                         after
------                         -----
leaf 0010                      internal 0010
count=4                        /          \
commitment=C                   leaf 00100  leaf 00101
                               count=2     count=2
```

No user row or locator hash changes. Only Merkle nodes change.

### 10.3 Unbalanced split

If all rows share the next bit, one-bit splitting does not reduce occupancy. Extend by more bits, use a larger logical fanout, or report an unsplittable range. Enforce a maximum prefix depth; never loop indefinitely.

### 10.4 Split failure

Child creation, range scanning, commitment calculation, and root publication are all transaction-scoped. On failure, abort and retain the old leaf.

## 11. Merge design

### 11.1 Trigger

Merge sibling leaves when:

```text
combined count <= merge_threshold
AND combined bytes <= merge_byte_threshold
AND generations are compatible
```

### 11.2 Worked merge

Before:

```text
internal 0010
├── leaf 00100: count=1
└── leaf 00101: count=2
```

After a delete, combined occupancy is eligible:

```text
1. Identify siblings.
2. Range-scan parent prefix 0010.
3. Verify every hash belongs to that range.
4. Compute combined commitment.
5. COW-create leaf 0010.
6. Remove child references.
7. Set is_leaf=true.
8. Recompute ancestors.
9. Publish root.
```

Result:

```text
leaf 0010, count=2, commitment=C_merged
```

Locator rows do not change.

Define an explicit empty-range representation: recommended is a canonical empty commitment plus explicit parent child state.

## 12. Commitments and equality

Keep separate:

```text
content commitment  = tuples in a logical hash range
structure commitment = prefix topology
logical root         = semantic data state
physical topology    = representation state
```

Report:

```text
logical content root
logical prefix/range summaries
structure/topology root
mode/layout/provenance
```

A strict topology comparison may remain as a diagnostic/strict mode, but it must not be confused with semantic equality if different legal split histories are allowed.

## 13. Recovery and verification

Compare ranges recursively:

```text
compare root
  |
  +-- equal: range complete
  +-- both internal: descend mismatching children
  +-- one leaf/internal: expand leaf by locator range scan
  +-- both leaves: compare identities and hashes
```

For a mismatch:

```text
1. Read locator entries on both sides.
2. Compare stable identity, hash, and canonical key.
3. Identify missing/extra/changed rows.
4. Repair through normal transactional DML.
5. Repair/rebuild locator.
6. Rebuild affected v9 summaries.
7. Publish root.
8. Recompare range.
```

Verification must check heap hash, locator ordering, locator-to-heap identity, prefix containment, counts, leaf commitments, parent commitments, and root generation.

## 14. Files and interfaces to add

Do not replace current demos. Add separate SQL/docs:

```text
Dynamic_merkle_docs/hash_range_setup.sql
Dynamic_merkle_docs/hash_range_dml.sql
Dynamic_merkle_docs/hash_range_split_merge.sql
Dynamic_merkle_docs/hash_range_index_only.sql
Dynamic_merkle_docs/hash_range_recovery.sql
```

Expected source touchpoints:

```text
src/include/access/merkle.h
src/backend/access/merkle/merkle.c
src/backend/access/merkle/merkleutil.c
src/backend/access/merkle/merklebuild.c
src/backend/access/merkle/merklenative.c
src/backend/access/merkle/merkledelta.c
src/backend/executor/nodeModifyTable.c
catalog/schema migration for locator relation
scripts/distributed/run_4node_raft_cluster.sh
Merkle/recovery test runners
```

Suggested SQL/backend inspection APIs:

```text
merkle_hash_range_bounds(hash, prefix_len)
merkle_hash_range_scan(index_oid, generation, prefix)
merkle_hash_range_stats(index_oid, generation)
merkle_hash_range_verify(index_oid, generation)
```

Names may follow existing PostgreSQL conventions; the responsibilities are required.

## 15. Test plan

### 15.1 Helpers

Test prefix lengths 0, 1, 7, 8, 255, 256; all-zero/all-one hashes; boundaries immediately inside/outside a range; parent/child containment; and sibling non-overlap.

### 15.2 Locator

Test deterministic hashing, ordering, equal hashes, duplicate policy, index-only scans, missing heap rows, changed heap hashes, REINDEX generations, CLUSTER, and table rewrites.

### 15.3 DML

Test:

```text
insert into empty tree
insert fills leaf
insert triggers split
delete triggers merge
same-prefix update
cross-prefix update
indexed-key update
insert/delete in one transaction
two updates to one row
rollback after locator staging
```

For each compare heap-derived tuples, locator tuples, range summaries, and published root.

### 15.4 Crash/abort

Fail around:

```text
heap mutation
locator mutation
split child creation
merge child removal
before root publication
root metadata publication
```

After restart, prove old complete or new complete state, never a half-state.

### 15.5 Recovery and distributed

Test equal logical roots, different legal split histories, missing locator entries, changed hashes, missing children, stale generations, and cross-prefix repair. Report logical equality separately from topology equality.

### 15.6 Performance

Compare v8 and hash-range mode with identical data/workloads:

```text
build time
locator and native metadata size
insert latency
same/cross-prefix update latency
split/merge latency
heap fetches
recovery work
root publication time
```

Reset split/merge counters before measured operations; do not mix build-time and execution-time counts.

## 16. Runner/reporting changes

Update `scripts/distributed/run_4node_raft_cluster.sh` and related runners with:

```text
merkle_mode
layout_version
route_definition
locator_index_name
logical_root_digest
structure_root_digest
logical_range_mismatch_count
physical_topology_mismatch_count
index_only_scan_count
heap_fetch_count
split_count_build
split_count_execution
merge_count_build
merge_count_execution
```

Do not apply v8 physical leaf-item checks to hash-range mode. A clean run requires mode-specific root evidence, zero divergence, zero permanent failures, and the index-only/locator contract—not merely `valid=true`.

## 17. Rollout and migration

### Stage 0: freeze contracts

Decide stable identity, duplicates, empty ranges, maximum depth, commitment formulas, split/merge hysteresis, locator ownership, and publication order.

### Stage 1: prove locator

Implement prefix arithmetic and an SQL-visible locator prototype. Prove index-only scans before native tree work.

### Stage 2: v9 baseline

Build a complete v9 tree from the locator. Compare every summary with a direct heap calculation.

### Stage 3: insert/delete

Implement single-range DML, then split/merge.

### Stage 4: update

Implement same-prefix, cross-prefix, keyed updates, coalescing, and rollback.

### Stage 5: recovery

Implement logical comparison, repair, and crash failpoints.

### Stage 6: distributed/performance

Run through the cluster harness and compare v8 against hash-range.

Migration is rebuild-based:

```text
create hash-range generation
 -> populate locator
 -> build v9 tree
 -> verify against heap
 -> switch generation
```

An existing v8 generation is never rewritten in place.

## 18. Risks

- Full tuple hashes change on ordinary updates; measure cross-prefix update cost.
- Index-only scans depend on visibility; report heap fetches.
- Hashes are not unique identities; define collision handling.
- TIDs are not stable across all rewrites.
- Generated/functional hash expressions need stable canonical semantics.
- Moving items outside leaves creates a locator consistency obligation; it does not eliminate consistency work.
- Physical topology can diverge even when logical content is equal.
- A “small change” description is misleading for current v8 because route, records, transitions, build/apply, recovery, and runners all change.

## 19. Implementation checklist

### Contracts

- [ ] Add explicit `hash_range` mode and mode/version reporting.
- [ ] Freeze identity and duplicate policies.
- [ ] Freeze prefix bounds and commitment formulas.
- [ ] Freeze split/merge policy and maximum depth.

### Backend

- [ ] Add v9 records and checksums.
- [ ] Add locator generation and covering B-tree.
- [ ] Add canonical hash/prefix helpers.
- [ ] Add old/new route transitions.
- [ ] Add v9 baseline build.
- [ ] Add insert/delete.
- [ ] Add index-only split.
- [ ] Add merge.
- [ ] Add same/cross-prefix update.
- [ ] Add atomic pre-commit publication.
- [ ] Add heap/locator/Merkle verification.

### Evidence

- [ ] Prefix boundary tests.
- [ ] EXPLAIN proof of index-only scan.
- [ ] Insert/delete/update/split/merge tests.
- [ ] Duplicate/rewrite tests.
- [ ] Rollback/crash tests.
- [ ] Recovery and logical-equality tests.
- [ ] Distributed mode-aware tests.
- [ ] v8 versus hash-range performance table.

## 20. Guide-meeting conclusion

The transcript improves one specific property: **tree refinement does not require rewriting row ownership mappings**, because rows are found through full-hash prefix ranges.

The proposal should be implemented as an opt-in v9 `hash_range` architecture, not as an in-place modification of v8. Continue v8 as the current correctness baseline, prove the locator/index-only contract first, then prove DML, split, merge, update, rollback, crash recovery, and distributed equality before considering promotion.

```text
continue native v8 baseline
        +
implement v9 hash_range experimentally
        +
prove index-only lookup
        +
prove all mutations and recovery
        +
compare measured behavior
        =
informed promotion decision
```
