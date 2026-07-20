# Native Merkle layout (superseded v4 summary)

This file is retained as a migration note.  The production native format is
layout v5; see `NATIVE_MERKLE_LAYOUT_V5.md` for the durable contract.

The native dynamic index stores immutable root, internal-node, leaf and
packed-item records in PostgreSQL index pages. Each partition owns a directory
page and its root-history head. Append/free pages carry a magic, page type,
format version and monotonic generation; every locator records the generation
and is rejected if the relation bounds, page envelope, generation or record
checksum do not match.

Normal data, structure and combined root reads consume the materialized
partition-root summaries in O(partitions). Logical commitments exclude block,
offset and generation values. The metapage records layout/configuration and
the persistent update mode. A layout mismatch or an unsafe online mode change
fails closed and requires `REINDEX` with the desired reloptions.
