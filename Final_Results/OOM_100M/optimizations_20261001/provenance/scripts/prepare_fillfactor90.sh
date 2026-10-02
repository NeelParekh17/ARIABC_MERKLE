#!/usr/bin/env bash
# Execute ON .247 by the orchestrator, after provisioning enough same-device space.
# Usage: bash prepare_fillfactor90.sh /tmp/ariabc_oom_ff90_<unique_tag>
set -euo pipefail
export LC_ALL=C
test "$(id -un)" = neel
case " $(hostname -I) " in *" 10.129.148.247 "*) ;; *) echo 'Run on .247 only' >&2; exit 1 ;; esac
OOM_FF_ROOT=${1:?Supply a fresh /tmp/ariabc_oom_ff90_<tag> root}
case "$OOM_FF_ROOT" in /tmp/ariabc_oom_ff90_*) ;; *) exit 1 ;; esac
[[ "$OOM_FF_ROOT" =~ ^/tmp/ariabc_oom_ff90_[a-zA-Z0-9_-]+$ ]]
OOM_FF_SOURCE=/tmp/ariabc_oom_100m/pgdata_base_f32s1024
OOM_FF_INSTALL=/home/neel/claude_opt/install_opt
OOM_FF_PORT=5449
OOM_FF_BASE=$OOM_FF_ROOT/pgdata_base_f32s1024_ff90
export LD_LIBRARY_PATH="$OOM_FF_INSTALL/lib:/home/neel/Desktop/rdkafka_local/lib:${LD_LIBRARY_PATH:-}"
test ! -e "$OOM_FF_ROOT"
test ! -d /tmp/ariabc_oom_100m/benchmark.lock
test -f "$OOM_FF_SOURCE/PG_VERSION"
test ! -f "$OOM_FF_SOURCE/postmaster.pid"
"$OOM_FF_INSTALL/bin/pg_controldata" "$OOM_FF_SOURCE" | grep -Eq 'Database cluster state:[[:space:]]+shut down$'
if fuser "$OOM_FF_PORT"/tcp >/dev/null 2>&1; then exit 1; fi
awk '/MemAvailable:/ {if ($2 < 12582912) exit 1; ok=1} END {if (!ok) exit 1}' /proc/meminfo
# Conservative allowance for new golden/plain/working/check copies, rewrite,
# index-sort temp, and 20GiB WAL. The reported 62GB free is insufficient.
OOM_FF_FREE=$(df -B1 --output=avail /tmp | tail -n 1)
test "$OOM_FF_FREE" -gt 214748364800
mkdir "$OOM_FF_ROOT"
mkdir "$OOM_FF_ROOT/preparation"
OOM_FF_EVIDENCE=$OOM_FF_ROOT/preparation
cp -a --reflink=never "$OOM_FF_SOURCE" "$OOM_FF_BASE"
cp -a "$OOM_FF_BASE/postgresql.auto.conf" "$OOM_FF_EVIDENCE/original.auto.conf"
sha256sum "$OOM_FF_INSTALL/bin/postgres" > "$OOM_FF_EVIDENCE/postgres.sha256"
cat > "$OOM_FF_BASE/postgresql.auto.conf" <<'CONF'
port = 5449
listen_addresses = '127.0.0.1'
shared_buffers = '32MB'
maintenance_work_mem = '512MB'
work_mem = '4MB'
max_parallel_maintenance_workers = 0
max_parallel_workers_per_gather = 0
bcdb_worker_count = 1
bcdb_ledger_trace = off
enable_merkle_index = on
merkle_apply_synchronous_direct = on
default_transaction_isolation = 'serializable'
autovacuum = off
fsync = on
full_page_writes = on
synchronous_commit = on
wal_compression = off
checkpoint_timeout = '30min'
max_wal_size = '20GB'
CONF
oom_ff_stop() {
    if [ -f "$OOM_FF_BASE/postmaster.pid" ]; then
        "$OOM_FF_INSTALL/bin/pg_ctl" -D "$OOM_FF_BASE" -w -t 120 stop -m fast
    fi
}
trap oom_ff_stop EXIT
"$OOM_FF_INSTALL/bin/pg_ctl" -D "$OOM_FF_BASE" -l "$OOM_FF_EVIDENCE/postgres.log" -w -t 120 start
OOM_FF_PSQL=("$OOM_FF_INSTALL/bin/psql" -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$OOM_FF_PORT" -U postgres -d postgres)
"${OOM_FF_PSQL[@]}" -Atc "SELECT merkle_verify('usertable');" > "$OOM_FF_EVIDENCE/verify.before.txt"
test "$(cat "$OOM_FF_EVIDENCE/verify.before.txt")" = t
OOM_FF_ROOT_SQL="SELECT partition_id, tuple_count, encode(hash, 'hex') FROM ariabc_internal.merkle_node_usertable WHERE prefix_len=0 ORDER BY partition_id;"
"${OOM_FF_PSQL[@]}" -Atc "$OOM_FF_ROOT_SQL" > "$OOM_FF_EVIDENCE/roots.before.txt"
"${OOM_FF_PSQL[@]}" <<'SQL' | tee "$OOM_FF_EVIDENCE/rewrite.txt"
\timing on
SELECT indexname, indexdef FROM pg_indexes WHERE tablename='usertable' ORDER BY indexname;
SELECT pg_relation_size('usertable') AS heap_before;
-- Avoid an implicit Merkle rebuild during VACUUM FULL. Rebuild once explicitly
-- with persisted geometry, after the heap rewrite and ordinary-index build.
DROP INDEX usertable_merkle_idx;
DROP INDEX usertable_merkle_lookup_idx;
ALTER TABLE usertable SET (fillfactor=90);
VACUUM (FULL, ANALYZE) usertable;
CREATE INDEX usertable_merkle_lookup_idx ON usertable
    (merkle_partition_for_hash(merkle_key_hash(ycsb_key),200),
     merkle_key_hash(ycsb_key), ycsb_key);
CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key)
    WITH (partitions=200, fanout=32, split_threshold=1024, merge_threshold=256);
ANALYZE usertable;
ANALYZE ariabc_internal.merkle_node_usertable;
SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable;
SELECT pg_relation_size('usertable') AS heap_after,
       pg_relation_size('usertable_pkey1') AS pk_after,
       pg_relation_size('usertable_merkle_lookup_idx') AS lookup_after,
       pg_total_relation_size('ariabc_internal.merkle_node_usertable') AS node_after;
SELECT reloptions FROM pg_class WHERE oid='usertable'::regclass;
SQL
"${OOM_FF_PSQL[@]}" -Atc "SELECT count(*),min(ycsb_key),max(ycsb_key) FROM usertable;" > "$OOM_FF_EVIDENCE/keyspace.txt"
test "$(cat "$OOM_FF_EVIDENCE/keyspace.txt")" = '100000000|1|100000000'
"${OOM_FF_PSQL[@]}" -Atc "SELECT merkle_tree_stats('usertable');" > "$OOM_FF_EVIDENCE/tree_stats.json"
python3 - "$OOM_FF_EVIDENCE/tree_stats.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
assert all(s[k] == v for k,v in dict(partitions=200, fanout=32, split_threshold=1024, merge_threshold=256).items()), s
assert s['total_nodes'] > 0, s
PY
"${OOM_FF_PSQL[@]}" -Atc "SELECT merkle_verify('usertable');" > "$OOM_FF_EVIDENCE/verify.after.txt"
test "$(cat "$OOM_FF_EVIDENCE/verify.after.txt")" = t
"${OOM_FF_PSQL[@]}" -Atc "$OOM_FF_ROOT_SQL" > "$OOM_FF_EVIDENCE/roots.after.txt"
cmp "$OOM_FF_EVIDENCE/roots.before.txt" "$OOM_FF_EVIDENCE/roots.after.txt"
"${OOM_FF_PSQL[@]}" -c 'CHECKPOINT;'
oom_ff_stop
trap - EXIT
cp -a "$OOM_FF_EVIDENCE/original.auto.conf" "$OOM_FF_BASE/postgresql.auto.conf"
sync
"$OOM_FF_INSTALL/bin/pg_controldata" "$OOM_FF_BASE" > "$OOM_FF_EVIDENCE/pg_controldata.txt"
grep -Eq 'Database cluster state:[[:space:]]+shut down$' "$OOM_FF_EVIDENCE/pg_controldata.txt"
du -sb "$OOM_FF_BASE" > "$OOM_FF_EVIDENCE/baseline_bytes.txt"
printf '%s\n' "$OOM_FF_BASE"
