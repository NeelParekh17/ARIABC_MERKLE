# Native Merkle operations

`synchronous_cow` is the only supported native-v8 update mode. The former
`pending_log` compatibility queue and legacy static mode are removed. The mode
is persisted in the metapage, and indexes using an older layout must be
reindexed with the native dynamic-v8 options.

The public commitment has three values: `data_root`, `structure_root`, and
`combined_root`.  Tree statistics expose all three, together with topology and
sequence information.  Range/frontier helpers traverse only overlapping
prefixes.  VACUUM validates roots and locators before reclaiming unreachable
append pages.

DDL that changes the indexed row descriptor is rejected until the Merkle index
is dropped and rebuilt.  `CREATE INDEX`, `REINDEX`, `TRUNCATE`, `VACUUM`, and
physical backup/restore are supported through the normal PostgreSQL lifecycle;
unsupported rewrites fail closed with a rebuild hint.
