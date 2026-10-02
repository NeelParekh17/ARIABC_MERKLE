#!/bin/bash
# Run ON .247 (detached). Creates pgdata_base_f32s1024_ff90c: copy of the ff90 baseline whose
# Merkle index is rebuilt with the fixed (compact-storage) build path. Source baseline untouched.
set -euo pipefail
R=/tmp/ariabc_oom_100m; SRC=$R/pgdata_base_f32s1024_ff90; DST=$R/pgdata_base_f32s1024_ff90c
I=/home/neel/claude_opt/install_opt2; PORT=5449; EV=$1
export LD_LIBRARY_PATH=$I/lib:/home/neel/Desktop/rdkafka_local/lib
mkdir -p "$EV"; exec > >(tee -a "$EV/prep.log") 2>&1
fail() { echo "PREP_FAIL: $*"; echo FAIL > "$EV/PREP_STATUS"; exit 1; }
date
test ! -d $R/benchmark.lock || fail "benchmark lock present"
test ! -e "$DST" || fail "$DST exists"
test ! -f "$SRC/postmaster.pid" || fail "source running"
$I/bin/pg_controldata "$SRC" | grep -Eq 'Database cluster state:[[:space:]]+shut down$' || fail "source not cleanly shut down"
! ss -ltn | grep -q ":$PORT " || fail "port $PORT busy"
awk '/MemAvailable:/ {exit !($2 > 11534336)}' /proc/meminfo || fail "MemAvailable < 11 GiB"
sha256sum $I/bin/postgres /home/neel/claude_opt/install_opt/bin/postgres > "$EV/postgres_sha256.txt"
echo "copying..."; cp -a --reflink=never "$SRC" "$DST.tmp" && mv "$DST.tmp" "$DST"; sync; date
$I/bin/pg_ctl -D "$DST" -o "-p $PORT -c listen_addresses=127.0.0.1 -c shared_buffers=32MB -c maintenance_work_mem=512MB -c max_parallel_maintenance_workers=0" -l "$EV/postgres.log" -w -t 300 start
trap '$I/bin/pg_ctl -D "$DST" -m fast -w stop >/dev/null 2>&1 || true' EXIT
P="$I/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $PORT -U postgres -d postgres"
SZ="SELECT 'heap', pg_relation_size('ariabc_internal.merkle_node_usertable') UNION ALL SELECT indexrelid::regclass::text, pg_relation_size(indexrelid) FROM pg_index WHERE indrelid='ariabc_internal.merkle_node_usertable'::regclass UNION ALL SELECT 'total', pg_total_relation_size('ariabc_internal.merkle_node_usertable')"
ROOTS="SELECT partition_id, tuple_count, encode(hash,'hex') FROM ariabc_internal.merkle_node_usertable WHERE prefix_len=0 ORDER BY 1"
$P -At -F' ' -c "$SZ" > "$EV/node_sizes_before.txt"; $P -At -c "$ROOTS" > "$EV/roots_before.txt"
$P -At -c "SELECT count(*), count(*) FILTER (WHERE is_leaf) FROM ariabc_internal.merkle_node_usertable" > "$EV/node_counts_before.txt"
echo "REINDEX..."; date; $P -c '\timing on' -c "REINDEX INDEX usertable_merkle_idx"; date
$P -c "ANALYZE ariabc_internal.merkle_node_usertable"
$P -At -F' ' -c "$SZ" > "$EV/node_sizes_after.txt"; $P -At -c "$ROOTS" > "$EV/roots_after.txt"
$P -At -c "SELECT count(*), count(*) FILTER (WHERE is_leaf) FROM ariabc_internal.merkle_node_usertable" > "$EV/node_counts_after.txt"
$P -At -c "SELECT merkle_tree_stats('usertable'::regclass)" > "$EV/tree_stats.json"
python3 -c "import json,sys; s=json.load(open('$EV/tree_stats.json')); assert (s['fanout'],s['split_threshold'],s['merge_threshold'],s['partitions'])==(32,1024,256,200), s; print('geometry ok', s['total_nodes'], s['leaf_nodes'])" || fail geometry
cmp "$EV/roots_before.txt" "$EV/roots_after.txt" || fail "partition roots changed"
[ "$($P -At -c "SELECT merkle_verify('usertable')")" = t ] || fail "merkle_verify"
echo "merkle_verify=t roots_identical=yes"
echo "sizes before:"; cat "$EV/node_sizes_before.txt"; echo "sizes after:"; cat "$EV/node_sizes_after.txt"
$P -c CHECKPOINT
$I/bin/pg_ctl -D "$DST" -m fast -w stop; trap - EXIT
$I/bin/pg_controldata "$DST" > "$EV/pg_controldata.txt"
grep -Eq 'Database cluster state:[[:space:]]+shut down$' "$EV/pg_controldata.txt" || fail "not cleanly shut down"
rm -f "$DST/postmaster.opts"; sync; date
echo OK > "$EV/PREP_STATUS"; echo PREP_OK
