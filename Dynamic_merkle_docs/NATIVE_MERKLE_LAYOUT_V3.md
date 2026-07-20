# Native Merkle layout (historical v3/v4 design)

This historical design is superseded by layout v5. See
`NATIVE_MERKLE_LAYOUT_V5.md` for the production durable contract.

The native dynamic index stores immutable root, internal-node, leaf and item
records in the index relation.  Partition directory pages contain one mutable
root head per partition; this avoids unrelated writers sharing a directory
buffer.  Every append/free page carries a magic, format version, page type and
monotonic `page_generation`.  Locators include that generation, so a locator
into a reused page fails closed instead of reading an ABA page.

Logical hashes never include physical block, offset or generation values.
Page and record checksums cover the complete durable record.

The metapage records layout parameters, route/row hash versions, persistent
update mode and the directory extent.  A layout-version mismatch requires
REINDEX; there is no in-place downgrade.
