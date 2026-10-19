# Multi-level lookup: storage, concurrency and recovery

This describes the integrated [merkle_lsm module](../contrib/merkle_lsm/README.md).
Its storage format is separate from core Merkle and does not change the catalog
version. Existing data directories and ordinary B-tree indexes remain usable.

## Components and durable identity

A public `merkle_lsm` index stores the original bulk-built base B-tree. Each
of its `levels-1` upper levels has two ordinary B-trees. A private
`merkle_lsm_meta` relation stores one generic-WAL page with `MlMeta`:

- magic/version, parent OID, level count, L0 target, size ratio;
- a fixed array of component OIDs indexed by level and slot, and active slots;
- one pending merge's source level/slot, COPY/COPIED phase, and epoch;
- flush request, completed counts per level and last duration/start timestamp.

The AM discovers the metadata OID through an internal `pg_depend` edge from the
metadata relation to the parent. An invalidation-aware backend cache avoids
repeating that lookup for successive callbacks on one index. The base
`rd_amcache` belongs to nbtree and is never used for LSM identity. After that
single dependency lookup, every component access uses the stored OID.

Names use `ml_<base OID>_l<level>_s<slot>` and `ml_<base OID>_meta`, comfortably
below NAMEDATALEN and unaffected by base renames. OIDs are database-local and
relation namespaces are database-local, so independent databases cannot collide.
Each component has an internal dependency on the parent. Normal catalog
mechanisms handle DROP; there is no utility-hook special case.

All components have `indisvalid=false`, `indisready=false`, `indislive=false`.
They are excluded from `RelationGetIndexList` and therefore planner selection,
ordinary executor maintenance and COPY's lock list. The parent AM supplies all
maintenance. Parent-only size functions do not include the hidden upper relations; use the
component sizes (plus one 8 KiB metadata page) for LSM storage accounting.
Diagnostic queries using `pg_indexes` must filter `indislive` to
avoid treating implementation components as user indexes. pg_dump should dump
only the public index and reconstruct components during restore.

Metadata is not placed in spare B-tree metapage bytes: nbtree WAL can reconstruct
that page using only `BTMetaPageData`, losing any extension tail. The distinct
metadata AM prevents nbtree from interpreting the generic metadata page.

## Build, insertion and scan

`ml_build` bulk-builds the base from existing heap rows and initializes empty
upper trees. nbtree tuplesort insists on a B-tree AM descriptor. The builder
uses a private copy of `RelationData` and `FormData_pg_class` for that call;
it never changes cached `rd_rel->relam`. Its B-tree cache is private and freed, and its smgr handle is closed on both
normal return and ERROR so no owner points to a dead private descriptor.
Parallel bulk build is disabled. REINDEX rebuilds the base and transactionally
replaces all component/metadata filenodes; changing level count needs DROP/CREATE.

An insert locks the metadata buffer SHARE, selects L0's active OID, calls
`btinsert` and releases the buffer lock before returning. Its component
RowExclusiveLock is released with its open relation. Rotation takes metadata
EXCLUSIVE just long enough to change the active slot and freeze the source.
It waits for in-flight insert callbacks to finish, not for their transactions.
The physical B-tree entry can safely move while its heap transaction remains
uncommitted: index entries have no independent MVCC visibility; the executor
checks the referenced heap version. The merger copies with SnapshotAny.

A scan opens and locks every component before initializing any component cursor.
This prevents source replacement while the scan could need a source entry.
Every subscan explicitly initializes `parallel_scan=NULL`. The k-way merge uses
full key comparisons (opclass comparison functions, collation, DESC/NULL flags)
then heap TID. `_bt_mkscankey(rel,NULL)` initializes `keysz=0`; the AM explicitly
sets the full key count for comparison. Every copy of the last emitted
`(key,TID)` is skipped across all trees, including multiple adjacent copies
within one tree. The AM advertises forward scans only. The planner now copies and honors
`amcanbackward` before generating a reverse ordered index path; previously it
assumed every ordered AM supported that direction. This is a planner-only
capability check, with no catalog format change. Bitmap scans union all
components; bitmap TID insertion is naturally idempotent. Index-only scans
return a copied winning index tuple with the parent's descriptor.

Uniqueness, backward/parallel scans, concurrent builds, unlogged/temp storage,
and CLUSTER through this AM are not advertised or are rejected. Included
columns are copied along with key columns. Public opclasses mirror supported
built-in B-tree opclasses. Custom external opclass families are not supported.
The cost estimator adds root-descent costs for the additional components and
sets correlation to zero; this is an approximation, not a calibrated model.

## Rotation and committed-copy protocol

One merger per database serializes component movement. It chooses the highest
overflowing level first, leaving space below L0. An upper-level target is
`top_index_size * size_ratio^level`. Both slots can grow beyond target if a
merge is slow; the design provides no space bound or write backpressure.

1. Persist active-slot flip and pending COPY, increment the epoch. Writers use
   the spare. No destination or source relation lock is held under the metadata
   buffer's EXCLUSIVE lock.
2. Hold heap ShareUpdateExclusiveLock and copy the frozen source into the next
   level's active tree (or base). Before each insertion, use nbtree's heapkeyspace
   exact search with key attributes and heap TID. The database merger is the
   sole writer to destinations, so no check/insert race exists.
3. Commit the copying transaction. Persist COPIED only afterwards. A crash before
   that marker repeats the copy; existing `(key,TID)` pairs are skipped.
4. Obtain source AccessExclusiveLock **conditionally**. Long scans defer this
   step; the worker does not queue a lock that blocks new readers/writers.
   Transfer predicate locks, allocate a new relfilenode, initialize its empty
   B-tree metapage, then commit.
5. Persist IDLE and completed count/duration only after source replacement
   committed. A crash between replacement and IDLE repeats replacement safely.

A session-level AccessShareLock on the parent spans the internal commits in
steps 3 and 4, preventing DROP/REINDEX from replacing the metadata between commit
and marker publication. PG_TRY cleanup and before_shmem_exit release that lock.
All normal relation locks are transaction-scoped. Merger relation-lock acquisition is
conditional to avoid heap/index DDL lock-order inversion.

Generic WAL state changes deliberately survive transaction abort, as B-tree
structural changes do. Source relfilenode replacement is transactional; in-place
RelationTruncate would be unsafe on ERROR or abort. No uncommitted destination
copy is ever used as the justification for discarding the source.

## Serializable isolation

The parent advertises `ampredlocks=true`: the component B-trees take ordinary
nbtree predicate locks (leaf pages, or the whole tree when it is empty), and the
application insert into the active L0 tree performs nbtree's normal
`CheckForSerializableConflictIn`. This is sufficient because a new logical entry
only ever enters the active L0 tree, and every scan reads both L0 trees. A reader
that scanned a tree before its replacement has its locks moved to the heap by
`TransferPredicateLocksToHeapRelation`, so any later heap write conflicts
(conservative). A reader that scanned it afterwards holds locks on the same
relation OID that later inserts check. Merge insertion represents existing heap
entries, not new logical data, so it needs no conflict check; background workers
use READ COMMITTED.

A whole-parent lock (`ampredlocks=false`) was rejected: every Merkle split runs
its range query inside the inserting SERIALIZABLE transaction, so such a lock
would give each split an rw-conflict with every concurrent insert.

The validation write-skew tests hold SERIALIZABLE readers across an actual
component drain/replacement and require the conflicting transaction to receive
SQLSTATE 40001, both for an update of a read row and for index-range phantoms
(each transaction inserts a key into the range the other one read). Concurrent application writers retry 40001 transactions.

## VACUUM invariant

No source copy overlaps a heap VACUUM. Each copy and source replacement obtains
the heap ShareUpdateExclusiveLock, the same lock ordinary VACUUM owns. Parent
bulkdelete visits the base and **all** hidden components while that heap lock
remains held. Therefore a vacuum cannot remove a dead destination TID, allow its
heap line pointer to be reused, and then have a merger reintroduce the old TID
from an uncleaned source. VACUUM between protocol transactions cleans source and
destination together. A restarted COPY reacquires the heap lock before reading.

Foreground SQL inserts may overlap VACUUM under ordinary PostgreSQL rules.
Long source scans defer replacement; they do not force writers to wait for a
merge. There is no upstream COPY lock-release list, executor hook, or micro-sleep
handoff: hidden components never enter COPY's ordinary index lock list.

## Worker lifecycle and observability

A preloaded launcher connects to `postgres`, discovers connectable databases,
and starts database merger workers. Workers exit when no public LSM indexes
remain. A small shared-memory ownership registry enforces one merger per database.
Normal/FATAL exit clears its PID entry; postmaster crash recovery reinitializes
the registry. The registry is never used to signal a process. The launcher
tracks BackgroundWorkerHandles, not PGPROC pointers. A killed worker is
relaunched, reconstructs all state from WAL-backed metadata, and retries pending
phases. ERROR emits a log message, aborts the transaction, releases cross-commit
locks, discards iteration allocations and backs off (bounded at 30 seconds).
Eight failed attempts exhaust the process retry budget; the worker exits and
the launcher can start another worker against the same durable pending phase.
FATAL/process death uses PostgreSQL resource cleanup and before_shmem_exit;
SIGKILL can trigger normal postmaster crash recovery.

Component diagnostics count physical scan entries, including non-killed dead
or uncommitted entries and cross-component copies. They are not logical row
counts. Merge counts/epochs/duration survive restart. Worker PID is resolved
from `pg_stat_activity`. There is no latch operation on a saved process pointer.

Flush sets a durable request, drains nonempty upper active components through
every level, and waits with latch timeouts plus CHECK_FOR_INTERRUPTS. A quiescent
benchmark drain is required for a full-workload I/O accounting boundary. Worker
slots and competing long transactions can delay completion; statement_timeout
bounds the caller. Test hooks expose partial copy, committed copy and replacement
before commit windows, with delay/error disabled by default.

## Space and write/read trade-offs

The old 100M YCSB base measured 385,042 x 8192 = 3,154,264,064 bytes. A 28-byte
entry contributes 2.8 GB of raw entry+line-pointer payload over 100M rows; that
is a packing estimate, separate from actual B-tree overhead and occupancy.
At 90% dense occupancy, a 1 MiB target corresponds to about 33,700 entries;
16 MiB to 539,000; 256 MiB to 8.63 million. Real random-insert occupancy varies.

| Levels | Trees read | Entry placements before base residence | Approx raw bytes placed per new 28-byte entry | Upper pairs at target |
|---|---:|---:|---:|---:|
| 2 | 3 | L0 + base = 2 | 56 | 2 MiB |
| 3 | 5 | L0 + L1 + base = 3 | 84 | 34 MiB |
| 4 | 7 | L0 + L1 + L2 + base = 4 | 112 | 546 MiB |

These counts exclude WAL records, full-page images, index internal nodes,
page splits and dead-version cleanup. Larger runs can revisit destination pages
less often per incoming entry, but every additional level adds insert and scan
work. No performance ranking follows from this arithmetic. The historical
October lsm3 experiment had one base plus two L0 B-trees, and its accepted
72 workload-A cases performed zero merges. Its measurements are historical
context, not evidence of multi-level compaction performance.

## Source and validation entry points

- [AM, metadata, component build, scan, VACUUM and SQL functions](../contrib/merkle_lsm/merkle_lsm.c)
- [Merger, exact search, crash hooks and workers](../contrib/merkle_lsm/merge.c)
- [Metadata format](../contrib/merkle_lsm/merkle_lsm.h)
- [SQL/opclasses and canonical creation helper](../contrib/merkle_lsm/merkle_lsm--1.0.sql)
- [Regression SQL](../contrib/merkle_lsm/sql/merkle_lsm.sql)
- [OOM setup and drain](../scripts/distributed/run_oom_100m_benchmark.py)
- [TPC-C setup](../scripts/tpcc_v3/tpcc_procs.sql)
- [Split range query](../src/backend/access/merkle/merkleapply.c)
- [Repair transfer query](../ariabc_pg/src/replica_repair.cxx)

Use retained campaign logs to determine which runtime tests passed. Neither
this design explanation nor historical lsm3 results constitute such a test.
