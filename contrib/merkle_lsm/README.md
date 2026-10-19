# merkle_lsm

`merkle_lsm` is an in-tree PostgreSQL access method for the auxiliary Merkle
row lookup index. It does not replace the Merkle integrity index or its node
storage. The main tree builds and installs it with `make` / `make install`.
For module development, use `make -C contrib/merkle_lsm install`. Add `merkle_lsm` to `shared_preload_libraries`, restart, and run
`CREATE EXTENSION merkle_lsm` in each target database.

```sql
CREATE INDEX usertable_merkle_idx ON usertable USING merkle(ycsb_key);
SELECT merkle_create_lookup_index('usertable');
-- Historical comparison:
-- SELECT merkle_create_lookup_index('usertable', 'btree');
SELECT merkle_lsm_config('usertable_merkle_lookup_idx');
SELECT merkle_lsm_stats('usertable_merkle_lookup_idx');  -- cheap: merge counters, phase, sizes
SELECT * FROM merkle_lsm_components('usertable_merkle_lookup_idx');
SELECT merkle_lsm_flush('usertable_merkle_lookup_idx');
```

The OOM runner refuses both AM and persisted size/level mismatches in an old
baseline; command-line options cannot silently reconfigure a restored index.
`merkle_lsm_config()` reads persisted targets without scanning entries.

The helper derives plain, composite, and leading-key-routed expressions from
one valid Merkle index. Existing lookup indexes are reported with their AM and
must be dropped explicitly before conversion. Core Merkle does not depend on
this extension; calling the helper requires installing the extension.

Options: `levels=2..4` (default 3), `top_index_size` in KiB (default 1024,
minimum 16), `size_ratio=2..1024` (default 16). Every non-base level has an active
and an idle/frozen B-tree. Intermediate targets multiply L0's target by the
ratio at each level. The worker checks actual relation block counts every
100 ms per database; targets are rotation triggers, not hard memory limits.
An overloaded merger allows active components to grow without blocking DML.
`ALTER INDEX ... SET` option changes require REINDEX to update the persisted
size targets. Changing the number of levels requires DROP/CREATE.

Internal component OIDs, roles, merge phase/epoch, counters, and duration are
stored in a separate WAL-logged metadata relation. Names contain the base OID,
level and slot, never a truncated user-provided name. Internal dependencies
provide normal cascading deletion. Components have all of `indisvalid`,
`indisready`, and `indislive` false. Only the parent AM scans, inserts into, or
vacuums them; do not run maintenance directly on an internal component.

Merges copy frozen entries with an exact `(key,TID)` destination search,
commit the copy, persist the copied marker, replace the source relfilenode in
a separate transaction, commit, then clear the marker. Interrupted phases are
redone. The heap ShareUpdateExclusiveLock excludes VACUUM during each copy or
replacement, and the AM vacuums every component in one normal heap-VACUUM
critical section. Serializable reads use the components' own nbtree
predicate locks; source replacement transfers them to the heap.

`merkle_lsm_status` and `merkle_lsm_components()` expose physical bytes and
physical entry counts (including dead/uncommitted entries and cross-component
copies), level/slot/role, persistent completed merge counts, epoch and duration.
Counting entries scans the components: this is diagnostic work, not a cheap
statistics counter. `worker_pid` is resolved through `pg_stat_activity`.
Workers poll, retry errors with bounded exponential backoff, and are relaunched
after process death. No PGPROC pointer is retained or signalled.

`merkle_lsm_flush()` requires index ownership, drains upper levels, and waits
with latch timeouts and interrupt checks. Use a quiescent workload when measuring
a complete drain before checkpoint/restart. Set `statement_timeout` if worker
slots or a long-running transaction could prevent completion. Preloading needs
one launcher slot plus one merger slot per database containing LSM indexes;
allow room for other PostgreSQL workers in `max_worker_processes`.

Supported: forward ordered scans, index-only scans, multicolumn keys, INCLUDE,
NULL keys, bitmap union, ordinary VACUUM and REINDEX. Unique indexes, backward
scans, parallel scans/builds, concurrent builds, temporary and unlogged tables
are rejected or not advertised. No change to the catalog version is required.
The metadata relation has its own magic and version. Ordinary existing B-tree
indexes and data directories remain valid.

For controlled crash tests, SIGHUP GUCs `merkle_lsm.debug_point` (0 off, 1 after
32 copied tuples, 2 after copy commit, 3 after source replacement before commit),
`debug_level`, `debug_delay_ms`, and `debug_error` select a window or one ERROR
per worker. They default off. Never enable them in production configuration.

See [architecture](../../doc/LSM_LOOKUP_ARCHITECTURE.md) and
[protocol and invariants](../../doc/LSM_LOOKUP_ARCHITECTURE_DETAILED.md).
The upstream design and operator definitions derive from Postgres Pro lsm3
commit `3bac10420e7738ddbd78e0eea7e3967afdd35aac`. The upstream PostgreSQL
license is preserved in [LICENSE](LICENSE).
