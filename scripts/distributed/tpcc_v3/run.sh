#!/usr/bin/env bash
# One isolated ranking-only attempt. Never execute this on the workstation.
set -euo pipefail
if (( $# != 5 )); then echo 'usage: run.sh <pg|det|merkle> <W> <N> <workers> <trial>' >&2; exit 2; fi
MODE=$1 W=$2 N=$3 WORKERS=$4 TRIAL=$5
case $MODE in pg|det|merkle) ;; *) exit 2;; esac
for value in "$W" "$N" "$WORKERS" "$TRIAL"; do [[ $value =~ ^[1-9][0-9]*$ ]] || exit 2; done
[[ $(id -un) == protectdr && " $(hostname -I) " == *' 10.129.7.57 '* ]] || {
  echo 'TPC-C v3 execution is restricted to protectdr on ranking (10.129.7.57).' >&2; exit 2;
}
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
C=${CAMPAIGN_HOME:-$HOME/claude_checks/v3}
[[ $C == "$HOME/claude_checks/paper_20261010_tpcc" || $C == "$HOME/claude_checks/tpcc_20261020" || $C == "$HOME/claude_checks/v3" ]] || exit 2
INST=${INST:?set INST from Agent A readiness / validation plan}
BINDIR=${BINDIR:?set BINDIR to newly built v3 binaries}
SRC=${SRC:?set SRC to v3 source checkout}
RUNROOT=${RUNROOT:?set a fresh ~/claude_checks/v3/B_* root}
CPUSET=${CPUSET:?set a fixed allowed CPU set}
PORT=${PORT:-55439} CLIENT_PORT=${CLIENT_PORT:-18100} RAFT_PORT=${RAFT_PORT:-19100}
PREPARED_CACHE=${PREPARED_CACHE:-}
WARMUP_S=${WARMUP_S:-60} MIN_WINDOW_S=${MIN_WINDOW_S:-300} SMOKE=${SMOKE:-0}
MEASUREMENT_METHOD=${MEASUREMENT_METHOD:-legacy_parity}
RUN_KIND=${RUN_KIND:-parity}
FILLFACTOR=${FILLFACTOR:-90} SPLIT=${SPLIT:-1024} MERGE=${MERGE:-256}
P=${P:-16384} PKC=${PKC:-1} SUB=${SUB:-16} SEED=${SEED:-42}
SHARED_BUFFERS=${SHARED_BUFFERS:-32GB} CHECKPOINT_TIMEOUT=${CHECKPOINT_TIMEOUT:-5min}
MAX_WAL_SIZE=${MAX_WAL_SIZE:-64GB} GATEWAY_TIMEOUT_S=${GATEWAY_TIMEOUT_S:-7200}
MERKLE_LOOKUP_AM=${MERKLE_LOOKUP_AM:-merkle_lsm}
MERKLE_LOOKUP_LEVELS=${MERKLE_LOOKUP_LEVELS:-3}
MERKLE_LOOKUP_SIZE=${MERKLE_LOOKUP_SIZE:-1024}
MERKLE_LOOKUP_RATIO=${MERKLE_LOOKUP_RATIO:-16}
case $MERKLE_LOOKUP_AM in btree|merkle_lsm) ;; *) exit 2;; esac
[[ $MERKLE_LOOKUP_LEVELS =~ ^[234]$ && $MERKLE_LOOKUP_SIZE =~ ^[0-9]+$ && $MERKLE_LOOKUP_RATIO =~ ^[0-9]+$ ]] || exit 2
(( MERKLE_LOOKUP_SIZE >= 16 && MERKLE_LOOKUP_RATIO >= 2 && MERKLE_LOOKUP_RATIO <= 1024 )) || exit 2
export MERKLE_LOOKUP_AM MERKLE_LOOKUP_LEVELS MERKLE_LOOKUP_SIZE MERKLE_LOOKUP_RATIO
DET_WINDOW=${DET_WINDOW:-1024} DET_PIPELINE_DEPTH=${DET_PIPELINE_DEPTH:-1024}
# Per-transaction phase trace (one CSV row per tx per worker) is a diagnostic,
# not timed-run configuration: off by default and identical for every mode.
PHASE_TRACE=${PHASE_TRACE:-0}
case $PHASE_TRACE in 0) TRACE_ENV=(env -u BCDB_PHASE_TRACE) ;; 1) TRACE_ENV=(env BCDB_PHASE_TRACE="$RUN/ptrace") ;; *) exit 2;; esac
[[ ($RUNROOT == "$C"/B_* || $RUNROOT == "$C"/deck_20261012/B_* || $RUNROOT == "$C"/main_20261020/B_*) && $RUNROOT != *"'"* && $RUNROOT != *$'\n'* && $RUNROOT != *'/../'* ]] || exit 2
for value in "$PORT" "$CLIENT_PORT" "$RAFT_PORT" "$FILLFACTOR" "$SPLIT" "$MERGE" "$P" "$PKC" "$SUB" "$SEED" "$GATEWAY_TIMEOUT_S" "$DET_WINDOW" "$DET_PIPELINE_DEPTH"; do
  [[ $value =~ ^[0-9]+$ ]] || exit 2
done
(( FILLFACTOR >= 10 && FILLFACTOR <= 100 )) || exit 2
[[ $CHECKPOINT_TIMEOUT =~ ^[0-9]+(s|min)$ && $MAX_WAL_SIZE =~ ^[0-9]+GB$ && $SHARED_BUFFERS =~ ^[0-9]+GB$ ]] || exit 2
if [[ $SMOKE != 1 ]]; then
  [[ $MEASUREMENT_METHOD == direct_fixed && $WARMUP_S == 300 && ($MIN_WINDOW_S == 600 || $MIN_WINDOW_S == 7200) && $SHARED_BUFFERS == 32GB && $CHECKPOINT_TIMEOUT == 5min && $DET_WINDOW == 1024 && $DET_PIPELINE_DEPTH == 1024 && $PHASE_TRACE == 0 ]] || {
    echo 'Shrunk measurement/settings require SMOKE=1; final defaults are fixed.' >&2; exit 2;
  }
fi
# One lock holder; daemon children close fd 9 so cleanup releases the lock.
if [[ ${CAMPAIGN_LOCK_HELD:-0} == 1 ]]; then exec 9>&-; else
  mkdir -p "$C"; exec 9>"$C/B_campaign.lock"; flock -n 9 || exit 2
fi
ATTEMPT_SUFFIX=${ATTEMPT_SUFFIX:-}
[[ $ATTEMPT_SUFFIX =~ ^(_attempt[1-9][0-9]*)?$ ]] || exit 2
RUN=$RUNROOT/${MODE}_w${W}_n${N}_workers${WORKERS}_trial${TRIAL}${ATTEMPT_SUFFIX}
[[ ! -e $RUN ]] || { echo "Refusing existing attempt: $RUN" >&2; exit 2; }
mkdir -p "$RUN" "$RUN/wal_archive"
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
export LC_ALL=C TZ=UTC
export PYTHONPATH=$HERE${PYTHONPATH:+:$PYTHONPATH}
export RUN MODE W N WORKERS TRIAL INST BINDIR SRC CPUSET PORT CLIENT_PORT RAFT_PORT
export WARMUP_S MIN_WINDOW_S SMOKE SHARED_BUFFERS CHECKPOINT_TIMEOUT MAX_WAL_SIZE FILLFACTOR
export MEASUREMENT_METHOD RUN_KIND
export SPLIT MERGE P PKC SUB SEED
export DET_WINDOW DET_PIPELINE_DEPTH PHASE_TRACE
PSQL=("$INST/bin/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres)
PIN=(taskset -c "$CPUSET")
MERKLE=0 MERKLE_GUC=off DBTYPE=1 BCDB_WORKERS=$WORKERS
[[ $MODE != merkle ]] || { MERKLE=1; MERKLE_GUC=on; }
[[ $MODE != pg ]] || { DBTYPE=0; BCDB_WORKERS=1; }
export BCDB_WORKERS MERKLE_GUC
PG_STARTED=0 SERVER_PID= GATEWAY_PID= SAMPLER_PID= IOSTAT_PID= WAL_COLLECTOR_PID= LSM_SAMPLER_PID=
LSM_SAMPLE=0
[[ $MODE != merkle || $MERKLE_LOOKUP_AM != merkle_lsm ]] || LSM_SAMPLE=1
GATEWAY_RC=999 CONSISTENCY_RC=999 STATE_HASH_RC=999 WALDUMP_RC=999 FINISHED=0
SAMPLER_RC=999 STOP_RC=0
write_runtime() {
  python3 - "$RUN" "$GATEWAY_RC" "$CONSISTENCY_RC" "$STATE_HASH_RC" "$WALDUMP_RC" "$FINISHED" "$SAMPLER_RC" "$STOP_RC" "$PG_STARTED" <<'PY'
import json, sys
from pathlib import Path
Path(sys.argv[1], 'runtime.json').write_text(json.dumps(dict(gateway_rc=int(sys.argv[2]),
 consistency_rc=int(sys.argv[3]), state_hash_rc=int(sys.argv[4]), waldump_rc=int(sys.argv[5]),
 finished=sys.argv[6]=='1', sampler_rc=int(sys.argv[7]), stop_rc=int(sys.argv[8]),
 postgres_stopped=sys.argv[9]=='0'), indent=2)+'\n')
PY
}
stop_children() {
  if [[ -n $GATEWAY_PID ]]; then stop_pid "$GATEWAY_PID"; GATEWAY_PID=; fi
  if [[ -n $SAMPLER_PID ]]; then
    touch "$RUN/sampler.stop"; SAMPLER_RC=0
    wait "$SAMPLER_PID" 2>/dev/null || SAMPLER_RC=$?
    SAMPLER_PID=
  fi
  if [[ -n $WAL_COLLECTOR_PID ]]; then
    touch "$RUN/wal_collector.stop"
    wait "$WAL_COLLECTOR_PID" || WALDUMP_RC=$?
    WAL_COLLECTOR_PID=
  fi
  if [[ -n $LSM_SAMPLER_PID ]]; then
    touch "$RUN/lsm_sampler.stop"
    wait "$LSM_SAMPLER_PID" 2>/dev/null || true
    LSM_SAMPLER_PID=
  fi
  if [[ -n $SERVER_PID ]]; then stop_pid "$SERVER_PID"; SERVER_PID=; fi
  if [[ $PG_STARTED == 1 ]]; then
    "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w -t 600 stop >>"$RUN/stop.log" 2>&1 || STOP_RC=$?
    if [[ ! -e $RUN/pgdata/postmaster.pid ]]; then PG_STARTED=0; fi
  fi
  if [[ -n $IOSTAT_PID ]]; then stop_pid "$IOSTAT_PID"; IOSTAT_PID=; fi
}
stop_pid() {
  local pid=$1 i
  kill -TERM "$pid" 2>/dev/null || true
  for ((i=0; i<100; i++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep .1
  done
  if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
  wait "$pid" 2>/dev/null || true
}
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  stop_children
  write_runtime
  python3 "$HERE/summary.py" "$RUN" >>"$RUN/summary.log" 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 - <<'PYCONFIG'
import json, os
from pathlib import Path
Path(os.environ['RUN'], 'config.json').write_text(json.dumps(dict(
    mode=os.environ['MODE'], warehouses=int(os.environ['W']), count=int(os.environ['N']),
    workers=int(os.environ['WORKERS']), trial=int(os.environ['TRIAL']),
    warmup_s=float(os.environ['WARMUP_S']), min_window_s=float(os.environ['MIN_WINDOW_S']),
    smoke=os.environ['SMOKE']=='1', setup_incomplete=True), indent=2)+'\n')
PYCONFIG
python3 "$HERE/provenance.py" "$RUN"
python3 - <<'PY'
import json, os, socket
from pathlib import Path
from sampler import cpuset
allowed = set(os.sched_getaffinity(0))
requested = cpuset(os.environ['CPUSET'])
if not requested or not requested <= allowed:
    raise SystemExit('CPUSET must be a nonempty subset of allowed CPUs')
ports = [int(os.environ[k]) for k in ('PORT', 'CLIENT_PORT', 'RAFT_PORT')]
if len(set(ports)) != 3:
    raise SystemExit('ports must be distinct')
for port in ports:
    with socket.socket() as s:
        s.bind(('127.0.0.1', port))
mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines() if line.split()[1].isdigit()}
buf = int(os.environ['SHARED_BUFFERS'][:-2])*1024*1024
if mem['MemAvailable'] < buf*2:
    raise SystemExit('Need available memory >= twice shared_buffers; do not disturb other users')
expected = dict(shared_preload_libraries='merkle_lsm', default_transaction_isolation='serializable', synchronous_commit='on', fsync='on',
    full_page_writes='on', autovacuum='on', enable_seqscan='off', log_checkpoints='on',
    shared_buffers=str(buf//8), checkpoint_timeout=str(int(os.environ['CHECKPOINT_TIMEOUT'][:-3])*60
    if os.environ['CHECKPOINT_TIMEOUT'].endswith('min') else int(os.environ['CHECKPOINT_TIMEOUT'][:-1])),
    max_wal_size=str(int(os.environ['MAX_WAL_SIZE'][:-2])*1024), wal_keep_segments='0',
    bcdb_worker_count=os.environ['BCDB_WORKERS'], enable_merkle_index=os.environ['MERKLE_GUC'],
    merkle_apply_synchronous_direct='on', bcdb_dt_conflict_tracking='on',
    bcdb_dt_completion_only_skip_reads='off', bcdb_serial_gate_mode='1', bcdb_serial_gate_source='0',
    bcdb_result_ring_slots='2048', bcdb_dt_hashtab_switch_threshold='1500',
    bcdb_gate_telemetry='off', bcdb_gate_snapshot_each_block='off', bcdb_advance_commit_watermark='on')
cfg = dict(mode=os.environ['MODE'], warehouses=int(os.environ['W']), count=int(os.environ['N']),
    workers=int(os.environ['WORKERS']), trial=int(os.environ['TRIAL']), seed=int(os.environ['SEED']),
    warmup_s=float(os.environ['WARMUP_S']), min_window_s=float(os.environ['MIN_WINDOW_S']),
    fillfactor=int(os.environ['FILLFACTOR']),
    smoke=os.environ['SMOKE']=='1', cpuset=os.environ['CPUSET'], expected_settings=expected,
    load_noise_threshold=os.cpu_count(), other_cpu_noise_threshold=100,
    progress_interval_ms=1000, completion_counter_includes_aborts=True)
cfg['merkle_lookup_am'] = os.environ['MERKLE_LOOKUP_AM']
cfg['merkle_lookup_levels'] = int(os.environ['MERKLE_LOOKUP_LEVELS'])
cfg['merkle_lookup_size'] = int(os.environ['MERKLE_LOOKUP_SIZE'])
cfg['merkle_lookup_size_ratio'] = int(os.environ['MERKLE_LOOKUP_RATIO'])
cfg['det_window'] = int(os.environ['DET_WINDOW'])
cfg['det_pipeline_depth'] = int(os.environ['DET_PIPELINE_DEPTH'])
cfg['phase_trace'] = os.environ['PHASE_TRACE'] == '1'
cfg['measurement_method'] = os.environ['MEASUREMENT_METHOD']
cfg['wal_accounting'] = 'incremental_record_ranges_v1' if os.environ['MEASUREMENT_METHOD']=='direct_fixed' else 'native_monolithic'
cfg['run_kind'] = os.environ['RUN_KIND']
Path(os.environ['RUN'], 'config.json').write_text(json.dumps(cfg, indent=2)+'\n')
PY
# Inline helpers import sampler from this known directory.
{
  date -Is
  free -g
  uptime
  lscpu
  ps -eo user,pid,pcpu,pmem,etimes,args --sort=-pcpu | head -21 || true
} >"$RUN/host_before.txt"
cat /sys/devices/system/clocksource/clocksource0/current_clocksource > "$RUN/clocksource.txt"
iostat -x -y -t 1 > "$RUN/iostat.log" 2>&1 9>&- &
IOSTAT_PID=$!
echo "$IOSTAT_PID" > "$RUN/iostat.pid"
BASE_KIND=plain
[[ $MODE != merkle ]] || BASE_KIND=merkle
# The lookup AM is part of the initial image: never restore an LSM image for B-tree.
[[ $MODE != merkle || $MERKLE_LOOKUP_AM != btree ]] || BASE_KIND=merkle_btree
BASE_ARCHIVE=
if [[ -n $PREPARED_CACHE ]]; then
  [[ $PREPARED_CACHE == "$C"/prepared_20261011 || $PREPARED_CACHE == "$C"/prepared ]] || exit 2
  mkdir -p "$PREPARED_CACHE"
  BASE_ARCHIVE=$PREPARED_CACHE/${BASE_KIND}_w${W}_seed${SEED}.tar.gz
fi
RESTORED=0
if [[ -n $BASE_ARCHIVE && -f $BASE_ARCHIVE ]]; then
  python3 "$HERE/prepared_cache.py" restore "$RUN" "$BASE_ARCHIVE" "$W" "$BASE_KIND"
  RESTORED=1
else
  "$INST/bin/initdb" -D "$RUN/pgdata" -U postgres >"$RUN/initdb.log" 2>&1
fi
cat >> "$RUN/pgdata/postgresql.conf" <<CONF
port = $PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = ''
shared_buffers = '$SHARED_BUFFERS'
max_connections = 832
autovacuum = on
synchronous_commit = on
fsync = on
full_page_writes = on
maintenance_work_mem = 2GB
max_parallel_maintenance_workers = 4
checkpoint_timeout = '$CHECKPOINT_TIMEOUT'
max_wal_size = '$MAX_WAL_SIZE'
# Incremental record-aligned statistics preserve exact WAL counts before
# recycling; retain normal max_wal_size rather than a full multi-hour history.
wal_keep_segments = 0
bcdb_worker_count = $BCDB_WORKERS
enable_merkle_index = $MERKLE_GUC
shared_preload_libraries = 'merkle_lsm'
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
log_timezone = 'UTC'
log_line_prefix = '%m [%p] '
CONF
PG_STARTED=1
"${TRACE_ENV[@]}" "${PIN[@]}" "$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 300 start 9>&- >"$RUN/start.log" 2>&1
if [[ $RESTORED == 0 ]]; then
"${PSQL[@]}" -f "$SRC/scripts/distributed/sql/raft_apply_ledger_schema.sql" >"$RUN/ledger.log" 2>&1
"${PSQL[@]}" -f "$SRC/scripts/tpcc_v3/tpcc_schema.sql" >"$RUN/schema.log" 2>&1
python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" --warehouses "$W" --seed "$SEED" \
  --host 127.0.0.1 --port "$PORT" --user postgres --db postgres --cache-dir "$RUNROOT/load_cache" >"$RUN/load.log" 2>&1
for table in warehouse district customer stock oorder order_line new_order history item; do
  "${PSQL[@]}" -c "ALTER TABLE public.$table SET (fillfactor=$FILLFACTOR); ALTER TABLE public.$table SET LOGGED;" >>"$RUN/logged.log" 2>&1
done
"${PSQL[@]}" -v bench_merkle_lookup_am="$MERKLE_LOOKUP_AM" -v bench_merkle_lookup_levels="$MERKLE_LOOKUP_LEVELS" -v bench_merkle_lookup_size="$MERKLE_LOOKUP_SIZE" -v bench_merkle_lookup_ratio="$MERKLE_LOOKUP_RATIO" -v bench_enable_merkle="$MERKLE" -v bench_merkle_fanout=32 -v bench_merkle_partitions="$P" \
  -v bench_merkle_split_threshold="$SPLIT" -v bench_merkle_merge_threshold="$MERGE" \
  -v bench_merkle_partition_key_columns="$PKC" -v bench_merkle_subpartitions="$SUB" \
  -f "$SRC/scripts/tpcc_v3/tpcc_procs.sql" >"$RUN/procs.log" 2>&1
fi
"${PSQL[@]}" --csv -c "SELECT c.relname,c.relpersistence,COALESCE(array_to_string(c.reloptions,';'),'') AS reloptions, pg_relation_size(c.oid) AS heap_bytes,pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('warehouse','district','customer','stock','oorder','order_line','new_order','history','item') ORDER BY c.relname" >"$RUN/relations.csv"
"${PSQL[@]}" --csv -c "SELECT c.relname,COALESCE(array_to_string(c.reloptions,';'),'') AS reloptions FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle' ORDER BY c.relname" >"$RUN/merkle_options.csv"
"${PSQL[@]}" --csv -c "SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname" >"$RUN/indexes.csv"
SETTING_NAMES=$(python3 - "$RUN/config.json" <<'PY'
import json, sys
print(','.join("'"+k+"'" for k in json.load(open(sys.argv[1]))['expected_settings']))
PY
)
"${PSQL[@]}" --csv -c "SELECT name,setting FROM pg_settings WHERE name IN ($SETTING_NAMES) ORDER BY name" >"$RUN/settings.csv"
"${PSQL[@]}" -c 'ANALYZE public.warehouse,public.district,public.customer,public.stock,public.item,public.oorder,public.new_order,public.order_line,public.history;' >"$RUN/analyze.log" 2>&1
"${PSQL[@]}" -c 'CREATE EXTENSION IF NOT EXISTS pg_prewarm' >"$RUN/prewarm.log" 2>&1
"${PSQL[@]}" -At -c "SELECT sum(pg_prewarm(c.oid::regclass,'buffer')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','ariabc_internal') AND c.relkind IN ('r','i','m')" >"$RUN/prewarm_blocks.txt"
# Initial validation/cache preparation is unmeasured; no explicit restart, sync
# or requested checkpoint is performed during the measurement interval.
PGOPTIONS='-c enable_seqscan=on -c enable_nestloop=off -c work_mem=128MB' "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" >"$RUN/initial_consistency.txt"
PGOPTIONS='-c enable_seqscan=on -c enable_nestloop=off -c work_mem=128MB' "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" >"$RUN/initial_state.hash"
if [[ $RESTORED == 1 ]]; then
  python3 "$HERE/prepared_cache.py" verify-state "$RUN" "$BASE_ARCHIVE" "$W" "$BASE_KIND"
elif [[ -n $BASE_ARCHIVE ]]; then
  "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w -t 600 stop > "$RUN/base_stop.log" 2>&1
  PG_STARTED=0
  python3 "$HERE/prepared_cache.py" save "$RUN" "$BASE_ARCHIVE" "$W" "$BASE_KIND"
  PG_STARTED=1
  "${TRACE_ENV[@]}" "${PIN[@]}" "$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 300 start 9>&- >"$RUN/base_restart.log" 2>&1
  "${PSQL[@]}" -At -c "SELECT sum(pg_prewarm(c.oid::regclass,'buffer')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','ariabc_internal') AND c.relkind IN ('r','i','m')" >"$RUN/base_prewarm_blocks.txt"
fi
"${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district' >"$RUN/initial_district_next.txt"
"${PSQL[@]}" -c 'SELECT pg_stat_reset()' >"$RUN/stat_reset.log"
python3 "$SRC/scripts/tpcc_v3/generate_workload.py" --count "$N" --warehouses "$W" --seed "$SEED" --terminals 96 -o "$RUN/workload.sql" >"$RUN/workload_generation.log" 2>&1
sha256sum "$RUN/workload.sql" >"$RUN/workload.sha256"
# Server persistence lives in this attempt, never in shared source / home.
mkdir "$RUN/server_state"
pushd "$RUN/server_state" >/dev/null
if [[ $MODE == pg ]]; then
  BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 ARIABC_PG_MAX_RETRIES=100 \
  "${PIN[@]}" "$BINDIR/ariabc_pg_server" --id 1 --raftEndpoint "127.0.0.1:$RAFT_PORT" --clientPort "$CLIENT_PORT" \
    --raftMembers "1=127.0.0.1:$RAFT_PORT" --dbName postgres --dbHost 127.0.0.1 --dbPort "$PORT" \
    --dbUser postgres --dbType 0 --safedb 0 --dbConnPoolSize "$WORKERS" --bcdbInitBlockSize "$WORKERS" \
    --pgExecMode event --bypassRaft 1 </dev/null >"$RUN/server.log" 2>"$RUN/server.err.log" 9>&- &
else
  BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 \
  ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 \
  ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1 \
  "${PIN[@]}" "$BINDIR/ariabc_pg_server" --id 1 --raftEndpoint "127.0.0.1:$RAFT_PORT" --clientPort "$CLIENT_PORT" \
    --raftMembers "1=127.0.0.1:$RAFT_PORT" --dbName postgres --dbHost 127.0.0.1 --dbPort "$PORT" \
    --dbUser postgres --dbType 1 --safedb 1 --dbConnPoolSize "$WORKERS" --bcdbInitBlockSize "$WORKERS" \
    --pgExecMode event --bypassRaft 1 </dev/null >"$RUN/server.log" 2>"$RUN/server.err.log" 9>&- &
fi
SERVER_PID=$!
echo "$SERVER_PID" > "$RUN/server.pid"
popd >/dev/null
READY=0
for ((i=0; i<100; i++)); do
  kill -0 "$SERVER_PID"
  if python3 - "$CLIENT_PORT" <<'PY'
import socket, sys
with socket.create_connection(('127.0.0.1',int(sys.argv[1])),timeout=.2): pass
PY
  then READY=1; break; fi
  sleep .2
done
[[ $READY == 1 ]] || { echo 'server readiness timeout' >&2; exit 1; }
: >"$RUN/gateway.log"
python3 "$HERE/sampler.py" --run "$RUN" --psql "$INST/bin/psql" --port "$PORT" \
  --pg-pid "$(head -1 "$RUN/pgdata/postmaster.pid")" --server-pid "$SERVER_PID" --cpuset "$CPUSET" \
  >"$RUN/sampler.log" 2>&1 9>&- &
SAMPLER_PID=$!
echo "$SAMPLER_PID" > "$RUN/sampler.pid"
for ((i=0; i<100; i++)); do
  kill -0 "$SAMPLER_PID"
  [[ ! -e $RUN/sampler.ready ]] || break
  sleep .1
done
[[ -e $RUN/sampler.ready ]] || exit 1
if [[ $MEASUREMENT_METHOD == direct_fixed ]]; then
  "${PIN[@]}" python3 "$HERE/wal_collector.py" "$RUN" --ramp "$WARMUP_S" --duration "$MIN_WINDOW_S" >"$RUN/wal_collector.log" 2>&1 9>&- &
  WAL_COLLECTOR_PID=$!
  echo "$WAL_COLLECTOR_PID" >"$RUN/wal_collector.pid"
fi
if [[ $LSM_SAMPLE == 1 ]]; then
  python3 "$HERE/lsm_sampler.py" --run "$RUN" --psql "$INST/bin/psql" --port "$PORT" >"$RUN/lsm_sampler.log" 2>&1 9>&- &
  LSM_SAMPLER_PID=$!
  echo "$LSM_SAMPLER_PID" >"$RUN/lsm_sampler.pid"
  for ((i=0; i<100; i++)); do
    kill -0 "$LSM_SAMPLER_PID"
    [[ ! -e $RUN/lsm_sampler.ready ]] || break
    sleep .1
  done
  [[ -e $RUN/lsm_sampler.ready ]] || exit 1
fi
# Phase-align the checkpoint cycle: checkpoint_timeout counts from the last
# checkpoint start, so with a ramp of whole checkpoint_timeout periods every
# window covers the same whole number of identical cycles (start, write
# spread, sync stall) instead of a run-dependent slice of one. Outside the
# window; the in-window checkpoints stay time-driven. The first CHECKPOINT
# flushes setup/hint-bit dirt; the second is near-instant and sets the phase.
"${PSQL[@]}" -c 'CHECKPOINT' >"$RUN/checkpoint_align.txt" 2>&1
"${PSQL[@]}" -At -c "SELECT extract(epoch FROM clock_timestamp())" >>"$RUN/checkpoint_align.txt"
"${PSQL[@]}" -c 'CHECKPOINT' >>"$RUN/checkpoint_align.txt" 2>&1
"${PSQL[@]}" -At -c "SELECT extract(epoch FROM clock_timestamp())" >>"$RUN/checkpoint_align.txt"
ARIABC_WAIT_RESULT_TIMEOUT_MS=180000 "${PIN[@]}" timeout --signal=TERM --kill-after=10 "$GATEWAY_TIMEOUT_S" \
  "$BINDIR/ariabc_pg_gateway" --nodes "127.0.0.1:$CLIENT_PORT" --queryFrom "$RUN/workload.sql" \
  --dbType "$DBTYPE" --detStartSeq 0 --reqIdOffset 1 --detWindow "$DET_WINDOW" --detBatchSize 256 \
  --dbConnPoolSize "$WORKERS" --submitMode event --detSubmitPipeline 1 --detPipelineDepth "$DET_PIPELINE_DEPTH" \
  --detClientMode event --detClientWorkers 96 --detClientInflight 16 --clientId single-gateway-direct \
  --numTerminals 96 --connFanout 1 --waitMajority 0 --completionPath direct --totalNodes 1 \
  --progressIntervalMs 1000 --txLatencyCsv "$RUN/tx_timeline.csv" >"$RUN/gateway.log" 2>&1 9>&- &
GATEWAY_PID=$!
echo "$GATEWAY_PID" >"$RUN/gateway.pid"
GATEWAY_RC=0
wait "$GATEWAY_PID" || GATEWAY_RC=$?
GATEWAY_PID=
sleep 1
# Keep only workload/checkpoint log before normal shutdown checkpoint messages.
touch "$RUN/sampler.stop"
SAMPLER_RC=0
wait "$SAMPLER_PID" || SAMPLER_RC=$?
SAMPLER_PID=
if [[ -n $WAL_COLLECTOR_PID ]]; then
  touch "$RUN/wal_collector.stop"
  WALDUMP_RC=0
  wait "$WAL_COLLECTOR_PID" || WALDUMP_RC=$?
  WAL_COLLECTOR_PID=
fi
cp "$RUN/postgres.log" "$RUN/postgres_workload.log"
# Drain outside the sampled throughput/WAL window, with separate evidence.
if [[ $MODE == merkle && $MERKLE_LOOKUP_AM == merkle_lsm ]]; then
  date -Is >"$RUN/lsm_drain.log"
  "${PSQL[@]}" -c "SET statement_timeout='10min'; SELECT merkle_lsm_flush(c.oid) FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle_lsm';" >>"$RUN/lsm_drain.log" 2>&1
  date -Is >>"$RUN/lsm_drain.log"
fi
if [[ -n $LSM_SAMPLER_PID ]]; then
  touch "$RUN/lsm_sampler.stop"
  wait "$LSM_SAMPLER_PID" || true
  LSM_SAMPLER_PID=
fi
# Dump exactly the sampled steady window, not init/load/warmup/drain.
if python3 "$HERE/summary.py" "$RUN" --window-only >"$RUN/window_selection.log" 2>&1; then
  if [[ $LSM_SAMPLE == 1 ]]; then
    python3 "$HERE/lsm_sampler.py" --run "$RUN" --summarize >"$RUN/lsm_summary.log" 2>&1 || true
  fi
  read -r L0 L1 < <(python3 - "$RUN/window.json" <<'PY'
import json, sys
w=json.load(open(sys.argv[1])); print(w['start']['wal_lsn'], w['end']['wal_lsn'])
PY
  )
  if [[ $MEASUREMENT_METHOD == direct_fixed ]]; then
    if ! python3 - "$RUN" "$L0" "$L1" <<'PY'
import json,sys
from pathlib import Path
m=json.loads((Path(sys.argv[1])/'wal_collection.json').read_text())
assert m['covered'] and m['start']['wal_lsn']==sys.argv[2] and m['end']['wal_lsn']==sys.argv[3]
PY
    then WALDUMP_RC=1; fi
  else
  mkdir "$RUN/wal_view"
  # Archived segments retain the window even when PostgreSQL recycles older WAL.
  for wal in "$RUN"/wal_archive/* "$RUN"/pgdata/pg_wal/*; do
    [[ $(basename "$wal") =~ ^[0-9A-F]{24}$ ]] || continue
    [[ -e $RUN/wal_view/$(basename "$wal") ]] || ln -s "$wal" "$RUN/wal_view/$(basename "$wal")"
  done
  WALDUMP_RC=0
  "$INST/bin/pg_waldump" -p "$RUN/wal_view" -s "$L0" -e "$L1" --stats=record >"$RUN/walstats.txt" 2>&1 || WALDUMP_RC=$?
  fi
fi
"${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district' >"$RUN/final_district_next.txt"
CONSISTENCY_RC=0
PGOPTIONS='-c enable_seqscan=on -c enable_nestloop=off -c work_mem=128MB' "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" >"$RUN/consistency.txt" 2>"$RUN/consistency.err" || CONSISTENCY_RC=$?
STATE_HASH_RC=0
PGOPTIONS='-c enable_seqscan=on -c enable_nestloop=off -c work_mem=128MB' "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" >"$RUN/state.hash" 2>"$RUN/state_hash.err" || STATE_HASH_RC=$?
if [[ $MODE == merkle ]]; then
  "${PSQL[@]}" -At -c "SELECT t.relname,merkle_verify_index(c.oid) FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle' AND n.nspname='public' AND t.relname IN ('warehouse','district','customer','history','item','stock','oorder','new_order','order_line') ORDER BY t.relname" >"$RUN/merkle_verify.txt" 2>"$RUN/merkle_verify.err" || true
fi
"${PSQL[@]}" --csv -c "SELECT schemaname,relname,n_tup_ins,n_tup_upd,n_tup_hot_upd,n_tup_del,n_dead_tup,autovacuum_count,autoanalyze_count FROM pg_stat_all_tables WHERE schemaname IN ('public','ariabc_internal') ORDER BY 1,2" >"$RUN/tabstats.csv"
{
  date -Is
  free -g
  uptime
  ps -eo user,pid,pcpu,pmem,etimes,args --sort=-pcpu | head -21 || true
} >"$RUN/host_after.txt"
stop_children
FINISHED=1
write_runtime
python3 "$HERE/summary.py" "$RUN" >"$RUN/summary.log" 2>&1
python3 - "$RUN/acceptance.json" <<'PY'
import json, sys
r=json.load(open(sys.argv[1])); print(json.dumps(r['acceptance'],indent=2))
# Smoke relaxes ONLY duration/checkpoint requirements in the shell exit status.
# acceptance.json remains the strict 300s evidence gate and is never marked PASS.
failed=set(r['acceptance']['failed_checks'])
if r['acceptance']['smoke']:
    failed -= {'window_300s', 'time_driven_checkpoint'}
sys.exit(1 if failed else 0)
PY
