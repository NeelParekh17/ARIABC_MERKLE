# Native Merkle operations

`synchronous_cow` is the authoritative native-page mode.  `pending_log` uses
the durable compatibility queue and materializes native pages exactly once in
the ordered applier.  The mode is persisted in the metapage; the session GUC
is not consulted for REINDEX/build fallback.  Missing reloptions default to
synchronous COW, explicit pending mode must be stored in reloptions, and
online migration is disabled until a drain-and-compare protocol exists.

The public commitment has three values: `data_root`, `structure_root`, and
`combined_root`.  Tree statistics expose all three, together with topology and
sequence information.  Range/frontier helpers traverse only overlapping
prefixes.  VACUUM validates roots and locators before reclaiming unreachable
append pages.

DDL that changes the indexed row descriptor is rejected until the Merkle index
is dropped and rebuilt.  `CREATE INDEX`, `REINDEX`, `TRUNCATE`, `VACUUM`, and
physical backup/restore are supported through the normal PostgreSQL lifecycle;
unsupported rewrites fail closed with a rebuild hint.
