# Native dynamic Merkle layout v5

Layout v5 is the durable format used by strict (synchronous_cow) dynamic
Merkle indexes. A v4 or older metapage is not opened as native state; it must
be rebuilt with REINDEX.

## Durable objects

| Object | Location | Durable fields |
| --- | --- | --- |
| Metapage | block 0 | dynamic marker, layout version, fanout, partition count, leaf limits, native directory extent, persistent update-mode flags |
| Directory | fixed block range after the metapage | one MerkleNativeLocator root head and last version per partition |
| Root version | append page | magic/version/flags, creator XID, partition, sequence domain/flags/epoch/value, version number, tuple/byte counts, data_xor, content_xor, structure_hash, root locator, previous-version locator, CRC32C |
| Internal node | append page | partition, canonical prefix, child locators, tuple/byte summaries, data_xor, content_xor, structure_hash, CRC32C |
| Leaf node | append page | partition, canonical prefix, item-chunk head, tuple/byte summaries, data_xor, content_xor, structure_hash, CRC32C |
| Item chunk | append page | packed route digest, key length/key bytes, tuple hash, next-chunk locator, CRC32C |

All integer fields use PostgreSQL native fixed-width layout inside the index
record. Hash inputs use explicit big-endian integers and fixed 32-byte
digests. Page envelopes carry a page type, page-format version and
generation. A locator is valid only when the block is in range, the page
envelope matches, and its generation equals the generation stored in the
locator.

## Commitment domains

For each item:

    item_content = BLAKE3(
        "ARIABC_NATIVE_ITEM_CONTENT_V1" ||
        route_digest || uint32_be(key_length) || canonical_key || tuple_hash
    )

content_xor is the XOR of these item commitments. It is independent of tree
shape and is the data-only partition commitment. data_xor remains the legacy
tuple-hash accumulator used in node summaries.

structure_hash commits to the canonical prefix, child prefixes and child
structure hashes. The global values are:

    data_root      = H(layout/route/row versions || ordered partition content_xor)
    structure_root = H(layout version || ordered partition structure_hash)
    combined_root  = H(layout/route/row versions || data_root || structure_root)

Physical block numbers, offsets, page generations and allocation hints are
never included in these logical commitments.

## Sequence provenance

Every published root carries a typed sequence:

| Domain | Meaning |
| --- | --- |
| RAFT | deterministic BCDB order; sequence_epoch is derived from the Raft epoch identifier |
| LOCAL_XID | ordinary PostgreSQL transaction order; sequence_value is the top-level XID |
| BUILD_BASELINE flag | initial materialization in the index's normal domain; it is a compatibility fallback, not a third ordering domain |

`merkle_dynamic_tree_stats()` reports a common non-baseline domain/epoch when
partition roots agree, plus minimum and maximum sequence values. The typed
`merkle_native_partition_roots_at(index, domain, epoch, value)` helper selects
the latest visible root at or before that marker and uses a BUILD_BASELINE root
only when no matching ordered root exists for a partition.

## Visibility and publication

Records are immutable. A writer appends all nodes/chunks first, then publishes
one new root version through a Generic WAL record that updates the directory
head. A root is visible when its creator XID is committed (or current), is not
marked aborted, and is visible to the caller's snapshot. Aborted or superseded
roots remain harmless until VACUUM proves that their pages are unreachable.

The lock order is database object lock, index relation extension lock only for
physical page allocation, then directory/append buffer locks. Partition locks
are acquired in ascending partition order for multi-partition transactions.

## Recovery and operations

Generic WAL registration precedes every page mutation and GenericXLogFinish
precedes the normal transaction commit record. A crash before finish leaves the
old root selected; a crash after finish but before commit leaves the new root
invisible. A committed root is selected after restart without an applier
catch-up step.

Use merkle_dynamic_verify(index) for a full spillable heap/native comparison.
Its tuplesort streams are bounded by work_mem. Strict bulk builds use a
maintenance_work_mem tuplesort ordered by partition, route, canonical key and
tuple hash, so the heap scan does not retain the complete index in backend
memory.

The former `merkle_set_update_mode()` SQL mutation API and session GUC are
removed. Update authority is a persistent index reloption; use
`ALTER INDEX ... SET (update_mode=...)` followed by `REINDEX INDEX ...` when a
mode change is required.
There is no online migration protocol until a drain/compare/reloption/metapage
migration protocol exists. ALTER
TABLE rewrites remain fail-closed; DROP, TRUNCATE and REINDEX use normal
PostgreSQL index lifecycle callbacks.
