#!/usr/bin/env bash
# Spec-standard TPC-C workload (scripts/tpcc_v3) measured with the ORIGINAL v2
# 20k-transaction method (scripts/distributed/tpcc_v2/sweep_run.sh): fresh
# database per run, CHECKPOINT then prewarm before measurement, autovacuum off,
# checkpoint_timeout 30min, no CPU pinning, identical server/gateway flags, TPS =
# gateway completed_tps over the whole 20k run.  Ranking (protectdr@10.129.7.57) only.
#
# Each warehouse count is loaded once per kind (plain for pg/det, merkle) into a
# stopped base cluster; every run copies that clean-shutdown base, so it starts
# from the same logical/physical state a fresh restore produces.
#
#   std20k.sh base  <ROOT> <W> <plain|merkle>
#   std20k.sh run   <ROOT> <W> <workers> <pg|det|merkle> <trial>
set -euo pipefail
[[ $(id -un) == protectdr && " $(hostname -I) " == *' 10.129.7.57 '* ]] || { echo 'ranking only' >&2; exit 2; }
CMD=$1 ROOT=$2 W=$3
V=$HOME/claude_checks/v3
INST=${INST:-$V/A_install} BINDIR=${BINDIR:-$V/A_build/bin} SRC=${SRC:-$V/A_src}
PORT=55439 CLIENT_PORT=18100 RAFT_PORT=19100 N=20000 SEED=42 FILLFACTOR=90
P=16384 PKC=1 SUB=16 SPLIT=1024 MERGE=256
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
PSQL=("$INST/bin/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres)
[[ $ROOT == "$V"/std20k_* ]] || { echo "ROOT must be $V/std20k_*" >&2; exit 2; }

# The shared home NVMe on ranking can sit at ~100% util with >600 ms write latency
# after bulk writes (copies, loads, other users). Measure only on a quiet disk.
DISK=$(lsblk -no pkname "$(df --output=source "$V" | tail -1)" 2>/dev/null || echo nvme0n1)
diskstat() { awk -v d="$DISK" '$3==d {print $8, $11, $13}' /proc/diskstats; }  # writes, write_ms, io_ticks
settle() {  # wait until disk util < 15% for 3 consecutive 5 s windows (max 1800 s)
  local start=$(date +%s) ok=0 a b util
  sync
  while (( ok < 3 && $(date +%s) - start < 1800 )); do
    a=($(diskstat)); sleep 5; b=($(diskstat))
    util=$(( (b[2]-a[2]) * 100 / 5000 ))
    if (( util < 15 )); then ok=$((ok+1)); else ok=0; fi
  done
  echo "settle_s=$(( $(date +%s) - start )) settle_last_util_pct=$util settle_ok=$(( ok >= 3 ))"
}

ports_free() {
  python3 - "$PORT" "$CLIENT_PORT" "$RAFT_PORT" <<'PY'
import socket, sys
for port in map(int, sys.argv[1:]):
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', port))
PY
}

# Same postgresql.conf as tpcc_v2/sweep_run.sh; per-run lines appended later override.
write_conf() {
  cat >> "$1/postgresql.conf" <<CONF
port = $PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = ''
shared_buffers = 32GB
max_connections = 832
autovacuum = off
synchronous_commit = on
fsync = on
full_page_writes = on
maintenance_work_mem = 2GB
max_parallel_maintenance_workers = 4
checkpoint_timeout = 30min
max_wal_size = 20GB
bcdb_worker_count = 1
enable_merkle_index = $2
bcdb_serial_gate_mode = 1
bcdb_serial_gate_source = 0
bcdb_dt_conflict_tracking = on
bcdb_result_ring_slots = 2048
bcdb_dt_completion_only_skip_reads = off
bcdb_dt_hashtab_switch_threshold = 1500
bcdb_gate_telemetry = off
bcdb_gate_snapshot_each_block = off
merkle_apply_synchronous_direct = on
track_io_timing = on
enable_seqscan = off
log_min_messages = warning
default_transaction_isolation = 'serializable'
max_locks_per_transaction = 4012
max_pred_locks_per_transaction = 4012
max_pred_locks_per_page = 512
bcdb_advance_commit_watermark = on
log_checkpoints = on
CONF
}

if [[ $CMD == base ]]; then
  KIND=$4; [[ $KIND == plain || $KIND == merkle ]] || exit 2
  B=$ROOT/base_w${W}_${KIND}
  [[ ! -e $B ]] || { echo "base exists: $B" >&2; exit 2; }
  ports_free; mkdir -p "$B"
  M=0 MG=off; [[ $KIND == merkle ]] && { M=1; MG=on; }
  STARTED=0
  trap '[[ $STARTED == 1 ]] && "$INST/bin/pg_ctl" -D "$B/pgdata" -m fast -w -t 900 stop >/dev/null 2>&1 || true' EXIT
  t0=$(date +%s)
  "$INST/bin/initdb" -D "$B/pgdata" -U postgres >"$B/initdb.log" 2>&1
  write_conf "$B/pgdata" "$MG"
  STARTED=1
  "$INST/bin/pg_ctl" -D "$B/pgdata" -l "$B/postgres.log" -w -t 300 start >/dev/null
  "${PSQL[@]}" -f "$SRC/scripts/distributed/sql/raft_apply_ledger_schema.sql" >"$B/ledger.log" 2>&1 || true
  "${PSQL[@]}" -f "$SRC/scripts/tpcc_v3/tpcc_schema.sql" >"$B/schema.log" 2>&1
  python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" --warehouses "$W" --seed "$SEED" --host 127.0.0.1 --port "$PORT" \
    --user postgres --db postgres --cache-dir "$ROOT/load_cache" >"$B/load.log" 2>&1
  for t in warehouse district customer history item stock oorder new_order order_line; do
    "${PSQL[@]}" -c "ALTER TABLE public.$t SET (fillfactor=$FILLFACTOR); ALTER TABLE public.$t SET LOGGED;" >>"$B/logged.log"
  done
  "${PSQL[@]}" -v bench_enable_merkle=$M -v bench_merkle_fanout=32 -v bench_merkle_partitions=$P \
    -v bench_merkle_split_threshold=$SPLIT -v bench_merkle_merge_threshold=$MERGE \
    -v bench_merkle_partition_key_columns=$PKC -v bench_merkle_subpartitions=$SUB \
    -f "$SRC/scripts/tpcc_v3/tpcc_procs.sql" >"$B/procs.log" 2>&1
  "${PSQL[@]}" -c 'ANALYZE public.warehouse,public.district,public.customer,public.stock,public.item,public.oorder,public.new_order,public.order_line,public.history;'
  "${PSQL[@]}" -c 'CREATE EXTENSION IF NOT EXISTS pg_prewarm' >/dev/null
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" >"$B/consistency.txt"
  grep -qx 'consistency_ok=t' "$B/consistency.txt"
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" >"$B/state.hash"
  "${PSQL[@]}" --csv -c "SELECT c.relname,c.relpersistence,COALESCE(array_to_string(c.reloptions,';'),'') AS reloptions,pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r' ORDER BY 1" >"$B/relations.csv"
  "$INST/bin/pg_ctl" -D "$B/pgdata" -m fast -w -t 900 stop >/dev/null
  STARTED=0
  sync
  echo "base_build_s=$(( $(date +%s) - t0 ))" >"$B/BASE_OK"
  exit 0
fi

[[ $CMD == run ]] || exit 2
WORKERS=$4 MODE=$5 TRIAL=$6
case $MODE in pg|det) KIND=plain;; merkle) KIND=merkle;; *) exit 2;; esac
B=$ROOT/base_w${W}_${KIND}
[[ -f $B/BASE_OK && -d $B/pgdata ]] || { echo "missing base $B" >&2; exit 2; }
RUN=$ROOT/runs/${MODE}_w${W}_k${WORKERS}_t${TRIAL}
[[ ! -e $RUN ]] || { echo "run exists: $RUN" >&2; exit 2; }
ports_free; mkdir -p "$RUN"
MERKLE=0; [[ $MODE == merkle ]] && MERKLE=1
BCDB_WORKERS=$WORKERS; [[ $MODE == pg ]] && BCDB_WORKERS=1
WL=$ROOT/workloads/tpcc-$N-w$W-seed$SEED.sql
mkdir -p "$ROOT/workloads"
[[ -f $WL ]] || python3 "$SRC/scripts/tpcc_v3/generate_workload.py" --count $N --warehouses "$W" --seed $SEED -o "$WL" >"$WL.gen.log"
SERVER_PID= PG_STARTED=0
stop_all() {
  if [[ -n $SERVER_PID ]]; then kill -KILL "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=; fi
  if [[ $PG_STARTED == 1 ]]; then "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w -t 600 stop >/dev/null 2>&1 || true; PG_STARTED=0; fi
}
trap stop_all EXIT
{ date -Is; uptime; ps -eo user,pid,pcpu,args --sort=-pcpu | head -8; } >"$RUN/host_before.txt"
echo "mode=$MODE warehouses=$W workers=$WORKERS trial=$TRIAL tx=$N fillfactor=$FILLFACTOR base=$B" >"$RUN/config.txt"
t0=$(date +%s)
cp -a "$B/pgdata" "$RUN/pgdata"
sync
echo "copy_s=$(( $(date +%s) - t0 ))" >"$RUN/result.txt"
cat >>"$RUN/pgdata/postgresql.conf" <<CONF
bcdb_worker_count = $BCDB_WORKERS
CONF
PG_STARTED=1
BCDB_PHASE_TRACE=$RUN/ptrace "$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 300 start >/dev/null
"${PSQL[@]}" --csv -c "SELECT name,setting FROM pg_settings WHERE name IN ('default_transaction_isolation','synchronous_commit','fsync','full_page_writes','autovacuum','checkpoint_timeout','merkle_apply_synchronous_direct','shared_buffers','bcdb_worker_count','enable_merkle_index','enable_seqscan') ORDER BY name" >"$RUN/settings.csv"
# v2 method: flush restore WAL, then prewarm every relation (TPCC_PREWARM_SQL).
settle >>"$RUN/result.txt"
"${PSQL[@]}" -c 'CHECKPOINT' >/dev/null
"${PSQL[@]}" -At -c "SELECT sum(pg_prewarm(c.oid::regclass,'buffer')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','ariabc_internal') AND c.relkind IN ('r','i','m')" >"$RUN/prewarm_blocks.txt"
rm -f "$RUN"/ptrace.[0-9]*
"${PSQL[@]}" -c 'select pg_stat_reset()' >/dev/null
settle | sed 's/settle/presettle/g' >>"$RUN/result.txt"
D0=$("${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district')
L0=$("${PSQL[@]}" -At -c 'select pg_current_wal_lsn()')
if [[ $MODE == pg ]]; then
  BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 ARIABC_PG_MAX_RETRIES=100 \
  nohup "$BINDIR/ariabc_pg_server" --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort $CLIENT_PORT \
    --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort $PORT \
    --dbUser postgres --dbType 0 --safedb 0 --dbConnPoolSize $WORKERS --bcdbInitBlockSize $WORKERS \
    --pgExecMode event --bypassRaft 1 </dev/null >"$RUN/server.log" 2>"$RUN/server.err.log" &
  SERVER_PID=$! DBTYPE=0
else
  BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 \
  ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 \
  ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1 \
  nohup "$BINDIR/ariabc_pg_server" --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort $CLIENT_PORT \
    --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort $PORT \
    --dbUser postgres --dbType 1 --safedb 1 --dbConnPoolSize $WORKERS --bcdbInitBlockSize $WORKERS \
    --pgExecMode event --bypassRaft 1 </dev/null >"$RUN/server.log" 2>"$RUN/server.err.log" &
  SERVER_PID=$! DBTYPE=1
fi
for i in $(seq 1 50); do kill -0 "$SERVER_PID"; fuser $CLIENT_PORT/tcp >/dev/null 2>&1 && break; sleep 0.2; done
GATEWAY_RC=0
DS0=($(diskstat)); T0=$(date +%s%N)
ARIABC_WAIT_RESULT_TIMEOUT_MS=180000 timeout 1800 "$BINDIR/ariabc_pg_gateway" --nodes 127.0.0.1:$CLIENT_PORT \
  --queryFrom "$WL" --dbType $DBTYPE --detStartSeq 0 --reqIdOffset 1 --detWindow 1024 --detBatchSize 256 \
  --dbConnPoolSize $WORKERS --submitMode event --detSubmitPipeline 1 --detPipelineDepth 1024 \
  --detClientMode event --detClientWorkers 96 --detClientInflight 16 --clientId single-gateway-direct \
  --numTerminals 96 --connFanout 1 --waitMajority 0 --completionPath direct --totalNodes 1 \
  >"$RUN/gateway.log" 2>&1 || GATEWAY_RC=$?
DS1=($(diskstat)); T1=$(date +%s%N)
echo "gateway_rc=$GATEWAY_RC" >>"$RUN/result.txt"
python3 - "${DS0[@]}" "${DS1[@]}" "$T0" "$T1" >>"$RUN/result.txt" <<'PY'
import sys
w0, m0, t0, w1, m1, t1, n0, n1 = map(int, sys.argv[1:])
el_ms = (n1 - n0) / 1e6
print(f"run_disk_util_pct={100*(t1-t0)/el_ms:.1f} run_disk_w_await_ms={(m1-m0)/max(1,w1-w0):.2f}")
PY
L1=$("${PSQL[@]}" -At -c 'select pg_current_wal_lsn()')
D1=$("${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district')
kill -KILL "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=
sleep 2
echo "wal_bytes=$("${PSQL[@]}" -At -c "select pg_wal_lsn_diff('$L1','$L0')") wal_start=$L0 wal_end=$L1" >>"$RUN/result.txt"
"$INST/bin/pg_waldump" -p "$RUN/pgdata/pg_wal" -s "$L0" -e "$L1" --stats=record >"$RUN/walstats.txt" 2>&1 || true
"${PSQL[@]}" -At -F, -c "select schemaname,relname,n_tup_ins,n_tup_upd,n_tup_hot_upd,n_tup_del from pg_stat_all_tables where schemaname in ('public','ariabc_internal') and (n_tup_upd+n_tup_ins+n_tup_del)>0 order by 1,2" >"$RUN/tabstats.csv"
FINAL=$(grep 'PROGRESS_GATEWAY_DET' "$RUN/gateway.log" | grep 'final=1' | tail -1 || true)
for k in elapsed_s completed completed_tps user_aborts permanent_failures divergence_count user_abort_counter_supported; do
  v=$(grep -Eo "\b$k=[0-9.]+" <<<"$FINAL" | tail -1 | cut -d= -f2 || true); echo "$k=${v:-missing}" >>"$RUN/result.txt"
done
echo "new_orders_committed=$((D1 - D0))" >>"$RUN/result.txt"
CONS_RC=0; "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" >"$RUN/consistency.txt" 2>"$RUN/consistency.err" || CONS_RC=$?
"${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" >"$RUN/state.hash"
if [[ $MERKLE == 1 ]]; then
  "${PSQL[@]}" -At -c "SELECT t.relname||'='||merkle_verify_index(c.oid)::text FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle' AND t.relnamespace='public'::regnamespace ORDER BY 1" >"$RUN/merkle_verify.txt" || true
fi
{ cat "$RUN"/ptrace.[0-9]* 2>/dev/null || true; } | { grep -v '^tx_id' || true; } | awk -F, '{n++; r+=$2} END {printf "traced_tx=%d total_restarts=%d\n", n, r}' >>"$RUN/result.txt"
[[ $MODE == pg ]] && { grep PROFILE_SERVER "$RUN/server.log" | tail -1 | grep -Eo 'retryable_sqlstate_40001=[0-9]+|retry_exhausted_total=[0-9]+' >>"$RUN/result.txt" || true; }
cp "$RUN/postgres.log" "$RUN/postgres_workload.log"
stop_all
python3 - "$RUN" "$WL.meta.json" "$CONS_RC" "$MERKLE" <<'PY' >"$RUN/acceptance.txt"
import json, sys
from pathlib import Path
run, meta, cons_rc, merkle = Path(sys.argv[1]), json.load(open(sys.argv[2])), int(sys.argv[3]), sys.argv[4] == '1'
r = dict(l.split('=', 1) for l in (run/'result.txt').read_text().split() if '=' in l)
exp_ab = meta['expected_rollbacks']; exp_no = meta['counts']['new_order'] - exp_ab
checks = {
  'gateway_rc_zero': r.get('gateway_rc') == '0',
  'completed_all': r.get('completed') == str(meta['count']),
  'user_aborts_expected': r.get('user_aborts') == str(exp_ab),
  'permanent_failures_zero': r.get('permanent_failures') == '0',
  'divergence_zero': r.get('divergence_count') == '0',
  'committed_new_orders': r.get('new_orders_committed') == str(exp_no),
  'consistency': cons_rc == 0 and 'consistency_ok=t' in (run/'consistency.txt').read_text(),
}
if merkle:
    lines = (run/'merkle_verify.txt').read_text().split() if (run/'merkle_verify.txt').exists() else []
    checks['merkle_verify_9'] = len(lines) == 9 and all(l.endswith('=true') for l in lines)
ok = all(checks.values())
print('accepted=' + ('1' if ok else '0'))
for k, v in checks.items(): print(f'{k}={int(v)}')
PY
cat "$RUN/acceptance.txt" >>"$RUN/result.txt"
if grep -qx 'accepted=1' "$RUN/acceptance.txt"; then rm -rf "$RUN/pgdata"; fi
cat "$RUN/result.txt"
