# Integrated multi-level Merkle lookup index

The auxiliary lookup maps `(partition, route hash, user key...)` to heap TIDs.
Merkle hashing and synchronous node maintenance remain in core. The default
setup AM is now `merkle_lsm`; historical comparisons select `btree` explicitly.

```text
INSERT -> heap + primary index + synchronous Merkle integrity maintenance
       -> L0 active B-tree
                  | rotate: old active freezes, spare becomes active
                  v
          L1 active B-tree       (levels=3 or 4)
                  v
          L2 active B-tree       (levels=4)
                  v
          original base B-tree

Each upper level owns two B-trees. Reads merge the base and BOTH slots
at every upper level, suppressing identical (key, heap TID) globally.
```

| Option | 2 levels | 3 levels (default) | 4 levels |
|---|---:|---:|---:|
| Upper-level targets, ratio 16 | 1 MiB | 1 / 16 MiB | 1 / 16 / 256 MiB |
| Maximum scan fan-out | 3 trees | 5 trees | 7 trees |
| Logical entry insertions, including initial L0 | 2 | 3 | 4 |
| Both slots at target, excluding base | 2 MiB | 34 MiB | 546 MiB |

Targets are physical size triggers, checked every 100 ms by each database's
worker. A frozen source remains readable while the active spare accepts writes.
Long readers can delay source replacement, and a slow merger can let active
trees exceed their targets. Foreground writers never wait for copying or source
replacement; rotation briefly synchronizes with active insert callbacks.

For the historical 100M-row YCSB lookup, the base was **385,042 pages =
3,154,264,064 bytes**. At approximately 28 bytes per entry, dense 90%-occupied
1 / 16 / 256 MiB buffers hold roughly 34 thousand / 539 thousand / 8.6 million
entries before accounting for metapages, high keys, and internal nodes. Extra
levels accumulate larger sorted merge runs before touching the roughly 3 GiB
base, but increase entry rewrite work and point-read fan-out. These are design
and packing estimates, not measured speedups. Each merge inserts into an
existing destination B-tree; it does not rebuild the whole destination.

A WAL-logged metadata relation stores component **OIDs**, level/slot roles,
merge epoch/phase, persistent completed counts and duration. Its own internal
dependency points to the parent index; ordinary dependency deletion handles
DROP INDEX, DROP TABLE and DROP EXTENSION CASCADE. Component names are
`ml_<base OID>_l<level>_s<slot>` and `ml_<base OID>_meta`, independent of base
renames and long user names. Hidden components never enter planner candidates
or the executor's ordinary heap-index list.

```text
IDLE -> freeze -> COPY -> copy transaction COMMIT -> COPIED
                                                     |
                          new source relfilenode, COMMIT
                                                     |
                                                    IDLE
```

After a crash, COPY repeats exact `(key,TID)` searches before inserting.
COPIED repeats transactional source replacement. Scans also suppress duplicates
across every component. Component B-trees take ordinary nbtree predicate
locks, and replacement moves them to the heap; heap locks serialize copying
against VACUUM. See the [detailed protocol](LSM_LOOKUP_ARCHITECTURE_DETAILED.md).

Setup and diagnostics:

```sql
CREATE EXTENSION merkle_lsm;
SELECT merkle_create_lookup_index('usertable');
SELECT * FROM merkle_lsm_status;
SELECT merkle_lsm_flush('usertable_merkle_lookup_idx');
```

The helper derives routing expressions from the core Merkle index. Core itself
has no extension dependency. An absent extension is a clear setup error; an
existing B-tree lookup must be converted explicitly or requested explicitly in
the harness. Flush before checkpoint/restart when measuring total deferred I/O.

Source: [AM](../contrib/merkle_lsm/merkle_lsm.c),
[worker and merge protocol](../contrib/merkle_lsm/merge.c),
[SQL helper](../contrib/merkle_lsm/merkle_lsm--1.0.sql),
[split retrieval](../src/backend/access/merkle/merkleapply.c),
[repair transfer](../ariabc_pg/src/replica_repair.cxx).

The earlier October lsm3 experiment used two logical levels (one base and two
L0 trees). Its 72 workload-A cases requested/completed zero merges, and its
mixed TPS results did not establish a compaction performance benefit. Those
historical results do not validate this implementation. Current correctness
claims must refer to logs from the integrated module's validation campaign.
