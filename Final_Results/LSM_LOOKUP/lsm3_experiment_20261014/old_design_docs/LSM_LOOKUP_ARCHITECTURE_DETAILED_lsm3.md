# How the LSM lookup index works in AriaBC

**Before, after, implementation, and measured behaviour**

Reviewed on 15 October 2026 against the latest relevant Claude Code chat, the experiment's actual source, saved correctness evidence, and the four remote YCSB D results.

> **The central change:** we replaced the auxiliary index that finds **user rows by Merkle partition and hash range**. Previously it was one large PostgreSQL B-tree. In the experiment it became an `lsm3` index containing a large base B-tree and two small top B-trees. The logical Merkle tree, its additive hashes, and its synchronous node maintenance stayed in place.

The implementation is an **isolated, uncommitted experiment**, based on AriaBC revision `a7fc519dfdd0713247922abff3bc3c80b752e27e`. It lives in [the experiment checkout](../../../../), with Postgres Pro `lsm3` pinned to `3bac10420e7738ddbd78e0eea7e3967afdd35aac`. “After” throughout this document means that experimental configuration; it does not mean that the main repository or every running cluster now uses LSM.

## Reading map

1. [The three structures that must be distinguished](#1-the-three-structures-that-must-be-distinguished)
2. [The logical Merkle tree](#2-the-logical-merkle-tree)
3. [Before: one large lookup B-tree](#3-before-one-large-lookup-b-tree)
4. [What an LSM tree normally does](#4-what-an-lsm-tree-normally-does)
5. [After: the actual three-B-tree implementation](#5-after-the-actual-three-b-tree-implementation)
6. [How we created it](#6-how-we-created-it)
7. [Exactly what happens on a write](#7-exactly-what-happens-on-a-write)
8. [Exactly what happens on a read](#8-exactly-what-happens-on-a-read)
9. [Exactly what happens during a merge](#9-exactly-what-happens-during-a-merge)
10. [Why updates and inserts behave differently](#10-why-updates-and-inserts-behave-differently)
11. [What we changed and fixed](#11-what-we-changed-and-fixed)
12. [What the experiments actually established](#12-what-the-experiments-actually-established)
13. [What is ready, and what remains unproven](#13-what-is-ready-and-what-remains-unproven)
14. [Source and evidence map](#14-source-and-evidence-map)

## 1. The three structures that must be distinguished

AriaBC has several indexes with different jobs. Calling all of them “the Merkle index” hides the architecture.

| Structure in the YCSB experiment | What it stores or does | Before | After |
|---|---|---|---|
| `usertable_pkey1` | Finds a user row by `ycsb_key`; enforces primary-key uniqueness | Ordinary B-tree | Same ordinary B-tree |
| `usertable_merkle_idx` and `ariabc_internal.merkle_node_usertable` | Maintain integrity summaries: node identities, hashes, counts, and tree shape | Custom Merkle AM, metapage, internal node heap and indexes | Same Merkle machinery |
| `usertable_merkle_lookup_idx` | Finds user rows belonging to a partition and a hash interval | One ordinary B-tree | `lsm3`: base B-tree plus two top B-trees |

The lookup index stores keys and PostgreSQL heap tuple identifiers, or **TIDs**. A TID points to a tuple version in the user table. It does not store a Merkle node or its accumulated hash.

![Before and after: the lookup index changes while the user heap, primary key, and Merkle node storage remain.](lsm_lookup_diagrams/01_before_after.png)

*Figure 1. The LSM experiment changes one auxiliary access path. Both variants still use PostgreSQL heap storage, PostgreSQL WAL, the primary-key B-tree, and synchronous Merkle maintenance.*

At the normalized 100-million-row baseline, the sizes were:

| Object | Recorded size |
|---|---:|
| User-table heap | 28,248,276,992 bytes, about 28.25 GB |
| Primary-key B-tree | 2,246,197,248 bytes, about 2.25 GB |
| Lookup B-tree, or LSM base at creation | 3,154,264,064 bytes, about 3.15 GB |
| Merkle node heap plus its three indexes | 38,436,864 bytes, about 38.44 MB |
| Empty LSM top0 / top1 | 8,192 bytes each |
| Custom Merkle index relation/metapage | 8,192 bytes |

These are **decimal GB/MB**. KiB/MiB below use powers of 1024. The small `usertable_merkle_idx` relation does not mean the entire integrity tree occupies one page: the actual nodes live in the separate internal heap and indexes. [Baseline catalogue and sizes][baseline]

## 2. The logical Merkle tree

Two hashes have different purposes:

- **Route hash:** computed from the indexed key; selects the partition and the path/range within that partition. This experiment uses the single-column key `ycsb_key` and an eight-byte routing digest.
- **Row contribution:** a 256-bit hash of the logical row, including its values; contributes to the integrity digest. Changing a non-key value changes this contribution even when the route stays the same.

The experiment uses 200 partitions, fanout 32, split threshold 1,024, and merge threshold 256. At its saved 100M baseline, each partition has a root, 32 intermediate nodes, and 1,024 bucket leaves:

```text
200 × (1 root + 32 intermediate nodes + 1,024 leaves) = 211,400 nodes
200 × 1,024 = 204,800 leaves
100,000,000 / 204,800 ≈ 488 rows per leaf on average
```

This is the measured baseline shape; splits and merges can change it later.

![Logical Merkle routing, bucket leaves, and additive node hashes.](lsm_lookup_diagrams/02_logical_merkle.png)

*Figure 2. The same logical structure is used with either lookup implementation. Bucket leaves summarize many rows; they are not individual entries in the lookup index.*

For a node containing rows `r1 ... rn`, the accumulator is conceptually:

```text
H(node) = [h(r1) + h(r2) + ... + h(rn)] mod 2^256
count(node) = n

INSERT r:           ΔH = +h(r),              Δcount = +1
DELETE r:           ΔH = -h(r),              Δcount = -1
UPDATE old → new:   ΔH = h(new) - h(old),    Δcount =  0
```

If an update changes the routing key, the old and new contributions can affect different paths. The function still named `merkle_hash_xor()` implements **addition modulo 2^256** in this revision. Its name is historical. [Hash implementation][util]

Merkle deltas are already combined within the transaction. The synchronous path applies them to the node tables before PostgreSQL commits. The LSM experiment keeps that path: deferring insertion into the **large lookup base** does not defer the root's logical update. This is an additive integrity tree, so it should not be confused with a textbook binary tree that recomputes each parent as `hash(left || right)`. [Synchronous delta application][delta]

## 3. Before: one large lookup B-tree

The lookup ordering is:

```text
(partition number, route hash, original user key) → heap TID
```

The experiment's original definition is equivalent to:

```sql
CREATE INDEX usertable_merkle_lookup_idx
ON public.usertable USING btree (
    merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200),
    merkle_key_hash(ycsb_key),
    ycsb_key
);
```

The first two columns let the engine find the rows in one Merkle leaf's hash range. The last column preserves the original key in the index. The index covers these lookup keys, **not all user-row values**; computing a full row hash or exporting a repaired row can still require the heap.

### Why have it when we already have a primary key?

The primary key answers “find `ycsb_key = 123`.” A Merkle split or repair asks “find all rows whose **route hash** belongs to partition `p` and lies between `lo` and `hi`.” Consecutive primary keys do not form a consecutive hash interval. A B-tree ordered by the raw primary key therefore cannot directly serve that hash-range predicate.

For example, the split retrieval in `merkleapply.c` has this shape:

```sql
SELECT merkle_key_hash(ycsb_key) AS kh,
       merkle_tuple_hash(u.*) AS th
FROM public.usertable AS u
WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key), $3) = $4
  AND merkle_key_hash(ycsb_key) BETWEEN $1 AND $2;
```

Replica repair uses the same partition/hash-range idea, joined to a set of damaged leaf ranges. It then retrieves the actual user rows. These operations need a correct, complete lookup over both old and recently inserted rows. [Split retrieval][apply], [repair predicates][repair]

### Where random I/O enters

On a new user-row insertion, PostgreSQL updates the heap, the primary key, and the lookup index. A sequential user key can append near the end of the primary-key B-tree, but its hash places the lookup entry at a largely unrelated location in the 3.15 GB lookup B-tree.

If the destination page is absent from the caches, the engine must read it; modifying it dirties the page and generates WAL. With full-page writes enabled, the first modification of a page after a checkpoint can also log a full-page image. It is **not** one physical disk read or one full-page image for every insertion: caching and checkpoint position matter.

Thus, the target of this experiment was **foreground random-page traffic in the large hash-ordered lookup index**, especially for inserts and non-HOT updates.

## 4. What an LSM tree normally does

LSM stands for **Log-Structured Merge-tree**. A common implementation logs new writes, accumulates them in a memory structure, flushes sorted files, and merges files through larger levels. Reads consult the recent state as well as older files. Acknowledged writes can be durable before compaction finishes. [LevelDB's implementation description](https://github.com/google/leveldb/blob/main/doc/impl.md)

![A conventional LSM: log, memtable, sorted files, and background compaction.](lsm_lookup_diagrams/03_generic_lsm.png)

*Figure 3. A conventional LSM layout, shown as background context. These memtables and SSTable levels are not the physical structures used by our `lsm3` experiment.*

The general trade is to collect recent writes in a smaller structure and pay consolidation work later. This adds read-side work and compaction work; it does not make writes free. Our implementation applies that idea inside PostgreSQL using the structures described next.

## 5. After: the actual three-B-tree implementation

We reused **Postgres Pro's `lsm3` extension**, then adapted and fixed it for this fork. We did not implement RocksDB, import the Rust `al8n/lsmtree` sparse-Merkle crate, or build a new general-purpose storage engine from scratch.

The three physical relations are:

```text
usertable_merkle_lookup_idx          large base, exposed through the lsm3 AM
usertable_merkle_lookup_idx_top0     small B-tree, active or merging
usertable_merkle_lookup_idx_top1     small B-tree, active or merging
```

All three contain the same key shape and heap TIDs. The two tops are **real PostgreSQL index relations**. Their pages participate in PostgreSQL buffering and WAL; they are not an application-level, volatile-only memtable. The fact that a small top can remain cache-resident is the useful property.

![The actual lsm3 structure: two alternating top B-trees, one base, and a shared controller.](lsm_lookup_diagrams/04_actual_lsm3.png)

*Figure 4. Top0/top1 are physical names. “Active” and “merging” are roles that alternate; top0 is not permanently the write tree.*

The extension maintains a shared-memory control entry for each LSM index. It includes the two top OIDs, current active top, active-insert access counts, insertion counter, merge state, and background-worker pointer. This control metadata coordinates the physical indexes; it does not contain the user-table data or Merkle node hashes. [Control structures][lsmh], [implementation][lsmc]

| Conventional LSM concept | What this implementation actually uses |
|---|---|
| Recent-write component | Small active PostgreSQL B-tree |
| Frozen component being consolidated | Other top B-tree after a role swap |
| Older persisted state | One large PostgreSQL base B-tree |
| Read across components | Three index cursors merged by key and TID, or bitmap union |
| Consolidation | Background insertion of the inactive top's entries into the base |
| Durability machinery | PostgreSQL relation storage and WAL |

There are no SSTable files, Bloom filters, or multi-level SSTable compaction in the checked extension. The base remains a B-tree, so consolidation still has base-index insertion costs.

## 6. How we created it

The baseline preparation performed these steps in the isolated experiment:

1. **Pin and build the extension.** Add the pinned upstream `lsm3` source under the experiment checkout's `contrib/lsm3`, apply the fork-compatibility changes and defect fixes, and build/install it on the lab host.
2. **Preload it at PostgreSQL startup.** The shared-memory dictionary and hooks require `shared_preload_libraries = 'lsm3'`. The experiment preloaded the same final library in all three modes, including det and B-tree controls.
3. **Create the extension in the experimental database.** This registers the SQL functions, access methods, and operator classes.
4. **Replace the lookup index in the copied LSM baseline.** Keep the exact three key expressions and the original logical index name; change its access method to `lsm3`.
5. **Build the base from existing rows and create empty tops.** `lsm3_build()` delegates the existing-data build to PostgreSQL `btbuild()`. The utility hook creates `_top0` and `_top1` using `lsm3_btree_wrapper`, whose build initializes empty index storage.
6. **Keep the tops under the parent AM's control.** The hook marks the top indexes invalid for independent planner selection. Their wrapper's normal insertion callback is a no-op; the parent LSM insertion callback explicitly inserts into the chosen top. Queries use the parent `lsm3` index so they can see all components.
7. **Normalize and verify the baselines.** Apply equivalent VACUUM/ANALYZE preparation to the three baseline copies, compare sizes and data, verify equal Merkle roots, and confirm the split/repair query plans.

The observed LSM DDL was:

```sql
CREATE EXTENSION lsm3;

DROP INDEX public.usertable_merkle_lookup_idx;

CREATE INDEX usertable_merkle_lookup_idx
ON public.usertable USING lsm3 (
    merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200),
    merkle_key_hash(ycsb_key),
    ycsb_key
)
WITH (top_index_size = 1024);
```

This is a record of the experiment's DDL. Other tables or Merkle routing configurations need their corresponding expressions, rather than blindly copying `200` or `ycsb_key`.

The key experimental settings were:

```conf
shared_preload_libraries = 'lsm3'
lsm3.top_index_size = 1024       # KiB; nominal 1 MiB top-size trigger
max_worker_processes = 32
shared_buffers = '32MB'
default_transaction_isolation = 'serializable'
merkle_apply_synchronous_direct = on
```

Creating the LSM index over 100M rows **does not load 100M entries into a top**. They enter the initial base build; the tops begin at 8 KiB each. The small-write-component benefit applies to subsequent index insertions. [Baseline builder][prepare], [recorded index definitions][baseline]

## 7. Exactly what happens on a write

Consider inserting a new row into `usertable`.

1. PostgreSQL performs the user-heap and primary-key work through the existing execution path.
2. For the auxiliary lookup index, `lsm3_insert()` reads the active-top selection under a spinlock, increments its active-insert access count, and reserves an insertion number.
3. It opens the chosen top and calls PostgreSQL `btinsert()` with the unchanged lookup values and heap TID. The large base is not updated by this normal foreground callback.
4. It periodically checks whether the top has grown beyond the configured size. If needed, it initiates the role swap described in Section 9.
5. Independently, the Merkle path stages/combines the row's hash and count changes and synchronously applies the affected node updates before transaction commit.

![Foreground insertion writes the heap, primary key, and active top, while Merkle nodes are maintained before commit.](lsm_lookup_diagrams/05_insert_path.png)

*Figure 5. These are responsibility branches, not a claim that the callbacks execute in parallel. The foreground lookup entry is already stored in a PostgreSQL index; moving it to the base is a later consolidation step.*

On the ordinary insert path the lookup entry lives in **either the active top or, after consolidation, the base**. During consolidation the same key/TID can temporarily exist in both places, which the read path must handle.

### Does deferring the base update mean the row is missing until the merge?

No. A query uses the parent LSM index, which scans both tops and the base. PostgreSQL's heap visibility rules still determine whether the pointed-to tuple version is visible to the query's snapshot. A committed row can be retrieved while its lookup entry is still in a top.

### What about DELETE and UPDATE?

This remains a PostgreSQL secondary index over an MVCC heap. A DELETE marks a heap tuple version deleted; this design does not introduce RocksDB-style key tombstones. Old index entries can remain until PostgreSQL vacuum removes dead TIDs. A non-HOT UPDATE can require a new lookup entry pointing to the new tuple version, and that insertion goes to the active top. A HOT UPDATE is the important exception described in Section 10.

Using PostgreSQL's native B-tree/WAL machinery avoids adding an independently committed external database. It does **not** establish crash safety of every extension hook, role swap, and truncation path; those still need dedicated validation.

## 8. Exactly what happens on a read

For an ordered index scan, `lsm3_beginscan()` opens three cursors: top0, top1, and base. Rescanning applies the same search keys to each.

`lsm3_gettuple()` then combines them:

1. Locate the next candidate from each component.
2. Compare candidates using the index's key ordering and then heap TID as a tiebreaker.
3. Return the next candidate in the requested forward/backward direction.
4. Advance the selected cursor on the next call.
5. Suppress an identical key/TID copy that appears in both top and base during merging.

![A partition/hash query searches all three components and combines matching tuple identifiers.](lsm_lookup_diagrams/06_read_path.png)

*Figure 6. The example is schematic: keys 10, 20, and 30 represent ordered lookup keys. The same key/TID appearing twice is a merge copy; two distinct row TIDs are not interchangeable duplicates.*

For a **bitmap index scan**, `lsm3_getbitmap()` asks each component to add its matches to the same TID bitmap. The bitmap represents the union, followed by the usual heap access/visibility handling. Both ordered and bitmap access matter: the saved 100M plans used an Index Scan for split retrieval and a Bitmap Index Scan in the repair-transfer query shape.

There is an optional `unique=true` reloption that attempts to skip other components for a full-key lookup. The experiment did **not** set it. It is an application assertion/optimization, not an enforced UNIQUE constraint; the AM advertises `amcanunique = false`. Primary-key uniqueness still comes from `usertable_pkey1`.

The benefit is write locality; the read tradeoff is potentially consulting more index components. An ordinary YCSB primary-key SELECT can continue using the unchanged primary-key B-tree, so “LSM reads three trees” does not imply every application SELECT becomes three-tree work. [Scan and AM callbacks][lsmc]

## 9. Exactly what happens during a merge

Suppose top0 is active and grows large enough.

![Top rotation and background consolidation: top0 freezes, top1 accepts writes, top0 entries are inserted into the base, and top0 is cleared.](lsm_lookup_diagrams/07_merge_cycle.png)

*Figure 7. The next cycle reverses the top roles. The base remains a PostgreSQL B-tree throughout.*

### The trigger is a sampled size check

The experiment sets a nominal top size of **1 MiB**, but the code does not check that size on every insertion. `LSM3_CHECK_TOP_INDEX_SIZE_PERIOD` is `64 * 1024`, or **65,536 index insertions**. The patched code reserves a zero-based insertion number under the spinlock and checks at positions 0, 65,536, 131,072, and so on. Position 0 is the first insertion after control-state initialization.

Consequently, 1 MiB is a **soft trigger, not a hard memory or file-size cap**. The active top can exceed it between checks, and the extension skips overflow checks while a merge is already in progress. The trigger counts index insertion callbacks, not all SQL statements, reads, or HOT updates. [Counter definition][lsmh], [insert implementation][lsmc]

### Rotation and coordination

When an overflow check wins the state check, the callback marks a merge in progress, changes `active_index` to the other top, and increments `n_merges`. New insertions choose the new active top. The extension tracks in-flight insertions into the previous top; once they finish, it requests background work and wakes the merger. The worker is launched lazily when needed.

The insertion counters, spinlock, relation locks, and worker latch have different jobs: choose a write target consistently, avoid starting duplicate merge cycles, wait for old-top insert callbacks, and wake the background process.

### What the merger actually does

Inside a PostgreSQL transaction, the worker:

1. Opens the inactive top and scans it in forward key order using `SnapshotAny`.
2. Calls `_bt_doinsert()` to put each scanned index tuple into the existing base B-tree.
3. Takes an `AccessExclusiveLock` on the merged top, truncates it, and initializes an empty top again.
4. Commits the worker transaction and clears `merge_in_progress`.

**This is not a full rebuild or full sequential rewrite of the entire 3.15 GB base.** The implementation walks the top and performs B-tree insertions into the base. The ordered input can improve locality, but the base can still incur page reads, page splits, dirty pages, and WAL. The saving is the shift from scattered foreground writes to a small foreground index plus later consolidation; its net sustained benefit must be measured.

### Where the deferred base-page work goes

The saved base occupies **385,042 pages of 8 KiB** (`3,154,264,064 / 8,192`). This is its total page count, including internal and metadata pages, rather than an independently measured leaf-page count; the large majority of a B-tree of this size are leaf pages. Hash-ordered lookup keys spread new entries across that large leaf population. A top containing tens of thousands of entries can therefore still touch a different base leaf for most entries, even when those entries are scanned in sorted order. A nominal 1 MiB top can hold entries on that order, depending on tuple width and occupancy, and the sampled trigger can let it grow larger before consolidation starts.

Sorting makes the destination keys monotonic and can group entries that share a leaf; it does **not** turn these sparse changes into an append-only sequential write of the base. The merger still has to locate and modify those base pages, read pages absent from the caches, and generate WAL and eventual writeback. Much of the scattered leaf-page work is thus **deferred from foreground lookup insertion to background consolidation**, rather than eliminated. Cache reuse and batching can reduce the cost, but their actual benefit depends on the workload and storage state.

This is why a long run through repeated merges matters: the merger competes with foreground transactions for cache space, I/O bandwidth, CPU, and locks while the other top accepts new entries. If consolidation cannot keep up, that active top can keep growing. The short YCSB D cases demonstrate cheap insertion into a small top; they do not measure the sustained cost of paying back the deferred base updates. [Base size][baseline], [per-entry merge implementation][lsmc]

The truncate lock can also wait for readers or other holders of conflicting relation locks. “Background” does not mean free of contention or guaranteed never to delay operations. [Merge loop and truncation: `lsm3_merge_indexes()`, `lsm3_truncate_index()`, `lsm3_merger_main()`][lsmc]

### How do reads stay complete during a merge?

While entries are being copied, queries still consult both tops and the base. Ordered scans contain explicit duplicate-copy handling, and bitmap scans union TIDs. The inactive top is cleared only through the locked merge/truncate path. The small tests exercised this mechanism, including concurrent deterministic inserts; they are evidence for those tested streams, not a proof for every concurrent SQL or crash schedule.

### What survives a normal restart?

Base and top contents are PostgreSQL relation data. The shared-memory control entry is reconstructed: `lsm3_get_entry()` locates `_top0`/`_top1` by name and chooses the larger top as active, using top0 on equal sizes. Its insertion/merge counters are reinitialized. Thus, a merge counter read after restart is not a persistent lifetime history; the experiment captured counters before restart and also retained worker logs.

## 10. Why updates and inserts behave differently

PostgreSQL can perform a **HOT**, or Heap-Only Tuple, update when no indexed value/expression needs to change and there is suitable space on the same heap page. The existing index entry can follow the heap's HOT chain, so the update avoids inserting a new entry in the lookup index.

![HOT updates bypass lookup-index insertion; INSERT and non-HOT UPDATE reach either the old B-tree or the new active top.](lsm_lookup_diagrams/08_hot_vs_insert.png)

*Figure 8. Even a HOT update can change the full-row integrity hash. Avoiding lookup-index work does not remove synchronous Merkle-node work.*

`fillfactor = 90` leaves space during the baseline load, which helped value-only workload-A updates stay HOT. It does not guarantee that every later update will be HOT: page space can be exhausted, and indexed-key changes can prevent it.

This gives two distinct cases:

```text
Value-only HOT UPDATE:
    heap tuple changes
    lookup insertion is skipped with either access method
    row hash and Merkle nodes still change
    → little lookup work for LSM to save

INSERT, or a non-HOT UPDATE:
    new lookup entry is required
    before: insert into the large hash-ordered B-tree
    after: insert into the small active top
    → the LSM change can reduce foreground lookup traffic
```

Therefore “an update-heavy benchmark did not improve” and “an insert workload improved” are compatible outcomes. The change targets one particular cost, not every source of Merkle overhead.

## 11. What we changed and fixed

### Reused code and fork adaptation

The implementation came from Postgres Pro `lsm3`. Our experiment added that extension to its isolated checkout and adapted it to this fork's early `13devel` APIs. The version label does not mean it has every API of final PostgreSQL 13.

Adaptations included conditional handling of unsupported B-tree deduplication fields/functions and AM fields, older metapage and utility-hook signatures, SQL operator-class support differences, and the fork source include path needed for `bcdb/globals.h`. The expression-index creation path also needed `transformIndexStmt()` before defining the top indexes.

### Four upstream defects corrected in the experiment

| Defect | Effect | Change |
|---|---|---|
| Top-size helper applied twice | A block count could be treated as a relation OID | Call the relation-size helper once and convert blocks to bytes |
| Worker-registration retry used `||` | Unnecessary retries and potentially unbounded waiting after failure | Use bounded retry with `&&` |
| Periodic insertion check raced with concurrent inserts | Threads could advance the counter past a size-check boundary | Reserve the insertion position under the existing spinlock |
| DROP hook ignored `IF EXISTS` | A missing index could fail a statement that should tolerate absence | Use `relation_openrv_extended(..., drop->missing_ok)` and skip missing relations |

The fourth issue affected preloaded control modes too. The accepted final grid used the rebuilt final library; earlier setup failures were preserved separately. The final `lsm3.so` SHA-256 was `3d5d9913d45a54e164c4fd42540885ab0e95a062ba608c76bfcca0a71b55e8ad`.

### Experiment and harness work

We prepared independent det, Merkle+B-tree, and Merkle+LSM baseline copies; preserved the same routing expressions; normalized VACUUM/visibility-map state across modes; added lookup/HOT/top/merge and WAL measurements; retained failed attempts; and independently audited final row results.

One setup failure came from using stale `pg_class.relpages` to judge whether a rebuilt Merkle node table existed. The harness switched that check to physical `pg_relation_size`. This was a measurement/preflight fix, not an LSM algorithm change.

Another setup failure came from a **telemetry connection mismatch**. The isolated PostgreSQL instance's Unix-domain socket had been moved from `/tmp` into the campaign's private `scratch` directory, but the remote telemetry sampler's `psql` call still relied on the default socket location. Its database-statistics connection therefore failed even though PostgreSQL was running. The sampler was fixed to pass **`-h 127.0.0.1`** and the configured database port explicitly, using TCP loopback on the database host instead of depending on a Unix-socket directory. This corrected telemetry collection; it did not change the LSM access method or database execution semantics. [Telemetry sampler][telemetry], [socket configuration][prepare], [saved setup-fix notes][patches]

No Merkle engine algorithm was modified for this experiment. The extension and harness changes remain uncommitted in the isolated checkout, with saved patches. Existing unrelated edits in the main workspace are a separate matter. [Choice and compatibility notes][choice], [patch notes][patches], [source/patch revisions][revisions]

## 12. What the experiments actually established

### 12.1 Small correctness tests: merges were exercised

The saved evidence reports:

- Paired B-tree/LSM tables passed inserts, updates, deletes, key changes, Merkle splits/merges, exact range comparisons, and an independent additive-root oracle.
- A 150,000-row scalar-index test completed **two automatic LSM merges** and returned the expected count and sum.
- The actual deterministic server/gateway test handled **143,000 commands**, with **one automatic composite-index merge**, zero divergence and permanent failures, exact expected final rows, equal roots, and `merkle_verify = true`.
- The split and repair-transfer query shapes returned the same rows with the new access method, including top-resident rows. The 100M plans used the lookup index in both variants.

These establish tested behaviour through consolidation. They do not constitute a full multi-replica recovery campaign or crash-recovery qualification of the extension. [Correctness evidence][correctness]

### 12.2 Workload A: no consistent benefit

The accepted main grid was:

```text
3 modes × 2 skews × 4 worker counts × 3 repetitions = 72 cases
+ 3 separate smoke cases
```

All three modes used 100M rows, fillfactor 90, SERIALIZABLE isolation, the same final binaries/module, and 32 MiB shared buffers. The database heap exceeded the lab machine's roughly 15 GiB RAM; each case used a physical reset and cold-cache preparation. This is **database-larger-than-memory cold-start evidence**, not proof that the short workload's entire working set exceeded RAM.

Across the eight matched points, LSM/B-tree median TPS changes ranged from **−2.61% to +2.23%**, with overlapping observed trial ranges everywhere. The evidence does not show a consistent improvement or establish formal equivalence.

The mechanism is visible in the counters. Across each Merkle mode's 24 grid cases there were **239,892 updates**, but only **70 B-tree / 71 LSM inferred lookup insertions**. At skew 0 all updates were HOT. These lookup-insertion figures are inferred from heap INSERT/non-HOT UPDATE statistics, not direct AM callback counters.

All 24 LSM grid cases recorded zero requested/completed merges. Tops remained only 8–16 KiB. Changing the write structure of a nearly idle index naturally leaves most work unchanged.

The final saved audit covered **75 accepted cases, 1.5M request/row results, zero divergence/permanent failures, and 50/50 full Merkle verification PASS results**. The audit was inspected for this document; the workload was not rerun. [Final report][report], [independent audit][audit]

### 12.3 Workload D: a promising short-run benefit

I read the four original YCSB D result/counter files from the remote campaign on 15 October and saved a [compact evidence snapshot][dsnapshot]. This was skew 0.99, 16 workers, two trials per Merkle variant, and 20,000 statements per case.

| Measurement | Merkle+B-tree | Merkle+LSM |
|---|---:|---:|
| Trial 1 TPS | 10,293.36 | 11,474.47 |
| Trial 2 TPS | 10,793.31 | 11,454.75 |
| Two-trial midpoint TPS | 10,543.33 | 11,464.61 |
| Average WAL per case, decimal MB | 16.115 | 8.544 |
| Average measured device reads, MiB | 170.12 | 155.71 |
| Average measured device writes, MiB | 30.57 | 20.16 |
| Actual user-row inserts per case | 1,024 | 1,024 |
| Lookup shared-buffer reads per case | 2,002 / 2,003 | 7 / 7, all in top0 |
| LSM base shared-buffer reads/hits | Not applicable | 0 / 0 in both cases |
| Final top0 / top1 size | Not applicable | 56 KiB / 8 KiB |
| Requested LSM merges | Not applicable | 0 in both cases |

With two observations, the arithmetic mean and conventional median coincide. The TPS increase is **8.74%**, WAL falls about **47.0%**, device writes about **34.0%**, and device reads about **8.5%**.

This is the architecture showing up in the measurements: the new entries reached a small top rather than the large lookup base. Both LSM trials exceeded both B-tree trials; all four recorded `divergence_count = 0`, `permanent_failures = 0`, 20,000 validated requests, and `merkle_verify = PASS`.

The boundary is important: these were only **1.743–1.943 seconds of gateway measurement**, with **no consolidation**. The device-measurement windows were wider than the gateway intervals. PostgreSQL buffer reads are not identical to physical device reads. The campaign's qualification report marked both configurations `insufficient_repeats`.

The chat also compared LSM against a published det value of 12,723 TPS and described roughly 40% of the gap as closed. **Det was not rerun in these four D cases.** That is a historical-reference calculation, not a matched three-mode D result. Likewise, an exact fraction of WAL saved by avoiding full-page images needs decoded WAL evidence; the smaller-top mechanism is supported directly by the lookup counters above.

The four-case D evidence is separate from the 75-case workload-A audit. It should not be described as having the same independent full audit coverage.

### 12.4 What “no merges” means here

It means **no merges in the short performance grid and four-case D comparison**. It does not mean the implementation never merged: the smaller correctness tests did.

The fillfactor-100 and sustained performance stages were cancelled in the earlier conversation. Consequently, the data establishes a short-window insertion benefit, but not its size after repeated consolidation. The observed 8.74% is neither a guaranteed sustained gain nor a mathematical upper bound on every workload's benefit.

## 13. What is ready, and what remains unproven

The experiment is a working implementation of a **PostgreSQL-native LSM-style lookup access path** with source patches and meaningful correctness checks. It is reasonable to describe it as an optional experimental configuration that reduced foreground lookup traffic on the tested insert workload.

Before making it the default, the unanswered questions are:

1. **Sustained cost:** what throughput, tail latency, WAL, device I/O, and top growth remain during several merge cycles at 100M rows? Include foreground time, background work, drain, and checkpoint costs.
2. **Read and repair cost:** how do range scans and actual multi-replica repair behave with significant data in both tops and during consolidation?
3. **Crash and lifecycle correctness:** what happens across crashes during copy, role changes, truncation, and catalogue operations? Native PostgreSQL storage helps, but the extension's coordination still needs explicit qualification.
4. **Maintenance and feature limits:** the checked AM does not enforce uniqueness or provide native parallel index scans or array search keys. SQL can still use other planner/executor paths where available. Its planner cost callback is inherited from B-tree rather than a detailed three-component model. We also own the fork-specific patches and any further fixes.

The concrete architectural outcome is therefore: **we retained the Merkle integrity tree and changed how its auxiliary row-lookup entries are buffered and consolidated**. The successful target is foreground lookup-index insertion work. Deferred Merkle-node persistence would be a different experiment with different correctness obligations.

## 14. Source and evidence map

### Chat and revision scope

- Relevant Claude Code session: `95af23bd-1fea-402f-8dae-42a9ee3914f9`, stored in `/home/neel/.claude/projects/-work-ARIABC-AriaBC/95af23bd-1fea-402f-8dae-42a9ee3914f9.jsonl`.
- LSM request: 14 October, `2026-10-14T11:15:25Z`; last relevant assessment: `2026-10-15T09:19:59Z`.
- AriaBC experiment revision: `a7fc519dfdd0713247922abff3bc3c80b752e27e`.
- Upstream extension revision: `3bac10420e7738ddbd78e0eea7e3967afdd35aac`.
- Original upstream project: [Postgres Pro lsm3](https://github.com/postgrespro/lsm3). The pinned, locally patched source below is the authority for this experiment's behaviour.

### Implementation map

Line numbers refer to the inspected experiment files and can move after edits.

| Question | File and entry point |
|---|---|
| What are the three trees and shared state? | [`lsm3.h`][lsmh], `Lsm3DictEntry` and `Lsm3ScanOpaque` |
| How is the existing base built? | [`lsm3.c`][lsmc]:445, `lsm3_build()` |
| How are empty tops created and hidden from separate planner use? | [`lsm3.c`][lsmc]:842 and :960, `lsm3_build_empty()` and `lsm3_process_utility()` |
| Where do foreground index writes go? | [`lsm3.c`][lsmc]:488, `lsm3_insert()` |
| How are ordered/bitmap reads combined? | [`lsm3.c`][lsmc]:606, :689, :765 |
| Does consolidation rebuild the full base? | [`lsm3.c`][lsmc]:244, `lsm3_merge_indexes()` calls `_bt_doinsert()` per top entry |
| How is a merged top cleared? | [`lsm3.c`][lsmc]:224 and :318, truncate/build and worker transaction |
| How is state reconstructed after restart? | [`lsm3.c`][lsmc]:143, `lsm3_get_entry()` |
| What actually hashes and combines row contributions? | [`merkleutil.c`][util]:315, additive hash implementation |
| When are node updates applied? | [`merkledelta.c`][delta]:385, synchronous application and pre-commit callback |
| Which user-row range must a split retrieve? | [`merkleapply.c`][apply]:655, partition/hash-range SQL |
| Which routing predicate does repair use? | [`replica_repair.cxx`][repair]:257, `route_hash_expr()` and `range_pred()` |
| Which DDL was executed for the LSM baseline? | [`prepare_baselines.py`][prepare]:73–75 and [saved catalogue][baseline] |

### Results and qualification

- [LSM choice and compatibility notes][choice]
- [Patch notes][patches] and [revision/patch hashes][revisions]
- [Small correctness and automatic-merge evidence][correctness]
- [Full workload-A report][report], [independent audit][audit], and [per-point medians][medians]
- [YCSB D snapshot extracted from all four remote cases][dsnapshot]
- Original D files: host `10.129.27.111`, directory `/home/neel/claude_ctl/lsm_lookup_20261014/results/ycsb_d/run_20261015_031850_81506eff/`; particularly `summary.csv`, `MEASUREMENT_QUALIFICATION.md`, and each case's `result.json` / `detailed_counters.json`.

The historical experiment ran PostgreSQL/server work on `10.129.148.247` and its gateway/controller on `10.129.27.111`. This document records that provenance. This documentation task read existing source and evidence; it launched no builds, database instances, or new benchmarks.

### Terms used in the diagrams

| Term | Meaning here |
|---|---|
| Merkle root | Integrity summary derived from row contributions |
| Route hash | Key-derived value used to choose partitions and leaf ranges |
| Lookup entry | Partition/hash/key plus a heap TID |
| Base | Large B-tree holding the initial data and consolidated entries |
| Top | Small PostgreSQL B-tree holding recent entries |
| Active top | Top currently chosen for foreground insertions |
| Merge | Copy inactive-top entries into the base and clear that top |
| WAL | PostgreSQL write-ahead log |
| HOT update | Heap-only tuple update that avoids new ordinary index entries |
| Shared-buffer read | PostgreSQL buffer miss; may be served by the OS cache |

[lsmc]: ../lsm3_source/lsm3.c
[lsmh]: ../lsm3_source/lsm3.h
[util]: ../../../../src/backend/access/merkle/merkleutil.c
[delta]: ../../../../src/backend/access/merkle/merkledelta.c
[apply]: ../../../../src/backend/access/merkle/merkleapply.c
[repair]: ../../../../ariabc_pg/src/replica_repair.cxx
[prepare]: ../harness/prepare_baselines.py
[telemetry]: ../../../../scripts/distributed/benchmark_telemetry.py
[baseline]: ../evidence/baseline_evidence/baselines_normalized/base_ff90_lsm.json
[choice]: ../LSM_CHOICE.md
[patches]: ../PATCH_NOTES.md
[revisions]: ../evidence/source_revision.json
[correctness]: ../CORRECTNESS.md
[report]: ../REPORT.md
[audit]: ../AUDIT.md
[medians]: ../data/medians.csv
[dsnapshot]: ../ycsb_d_run_20261015/ycsb_d_verified_snapshot.json
