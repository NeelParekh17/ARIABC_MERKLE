#!/usr/bin/env bash
# Integrator-only validation on ranking. This file is never executed locally.
set -euo pipefail
[[ $(id -un) == protectdr && " $(hostname -I) " == *' 10.129.7.57 '* ]] || {
  echo 'Run only as protectdr on ranking (10.129.7.57)' >&2; exit 2;
}
SRC=${SRC:?set the new A_src source snapshot}
INST=${INST:?set the new A_install with worker/middleware changes}
BINDIR=${BINDIR:?set the new A_build/bin}
ROOT=$HOME/claude_checks/v3
CPUSET=${CPUSET:?set a fixed subset of the ranking CPU affinity}
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
export LC_ALL=C TZ=UTC
PORT=55449 CLIENT_PORT=18110 RAFT_PORT=19110 WORKERS=8
TAG=${TAG:-$(date -u +%Y%m%dT%H%M%SZ)}
RUNROOT=$ROOT/A_validation_$TAG
[[ ! -e $RUNROOT ]] || { echo 'Refusing existing output root' >&2; exit 2; }
mkdir -p "$RUNROOT"
free -g | tee "$RUNROOT/free_before.txt"
python3 - "$CPUSET" <<'PY'
import os,sys
allowed=os.sched_getaffinity(0)
requested=set()
for part in sys.argv[1].split(','):
    ends=list(map(int,part.split('-')))
    requested.update(range(ends[0],ends[-1]+1))
assert requested and requested<=allowed
mem={l.split(':')[0]:int(l.split()[1]) for l in open('/proc/meminfo') if l.startswith(('MemAvailable:','MemTotal:'))}
assert mem['MemAvailable']>=16*1024*1024,mem
PY
# These are static checks; preserve their exact outputs on ranking too.
python3 -m py_compile "$SRC/scripts/tpcc_v3/generate_workload.py" "$SRC/scripts/tpcc_v3/tpcc_load.py"
python3 "$SRC/scripts/tpcc_v3/generate_workload.py" --self-test > "$RUNROOT/generator_selftest.json"
python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" --self-test > "$RUNROOT/loader_selftest.json"
if grep -Ein 'CURRENT_TIMESTAMP|now[[:space:]]*\(|clock_timestamp[[:space:]]*\(|random[[:space:]]*\(' "$SRC/scripts/tpcc_v3/tpcc_procs.sql" > "$RUNROOT/forbidden_calls.txt"; then
  echo 'Forbidden procedure call' >&2; exit 1
fi
python3 "$SRC/scripts/tpcc_v3/generate_workload.py" --count 20000 --warehouses 2 --seed 42 -o "$RUNROOT/workload.sql" > "$RUNROOT/generator.json"
cat > "$RUNROOT/abort.sql" <<'SQL'
SELECT public.new_order_proc_exec(1,1,1,ARRAY[1,2,3,4,100001],ARRAY[1,1,1,1,1],ARRAY[1,1,1,1,1],'2025-01-01 00:00:00'::timestamp);
SELECT public.stock_level_exec(1,1,15);
SQL
PSQL=("$INST/bin/psql" -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PORT" -U postgres -d postgres)
PIN=(taskset -c "$CPUSET")
PG_STARTED=0 SERVER_PID= RUN=
cleanup() {
  if [[ -n $SERVER_PID ]]; then kill -TERM "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; SERVER_PID=; fi
  if [[ $PG_STARTED == 1 ]]; then "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w -t 600 stop; PG_STARTED=0; fi
}
trap cleanup EXIT
for label in load1 load2 abort_pg abort_det abort_merkle pg det merkle; do
  RUN=$RUNROOT/A_$label
  mkdir "$RUN"
  # Never kill another user's listener. Check ports before starting anything.
  python3 - <<'PY'
import socket
for port in (55449,18110,19110):
    with socket.socket() as s: s.bind(('127.0.0.1',port))
PY
  MODE=${label#abort_}; [[ $label != load* ]] || MODE=pg
  MERKLE=0 MERKLE_GUC=off DBTYPE=1 BCDB_WORKERS=$WORKERS
  [[ $MODE != merkle ]] || { MERKLE=1; MERKLE_GUC=on; }
  [[ $MODE != pg ]] || { DBTYPE=0; BCDB_WORKERS=1; }
  "$INST/bin/initdb" -D "$RUN/pgdata" -U postgres > "$RUN/initdb.log" 2>&1
  cat >> "$RUN/pgdata/postgresql.conf" <<CONF
port=$PORT
listen_addresses='127.0.0.1'
unix_socket_directories='$RUN'
shared_buffers=8GB
max_connections=832
autovacuum=on
synchronous_commit=on
fsync=on
full_page_writes=on
maintenance_work_mem=2GB
max_parallel_maintenance_workers=4
checkpoint_timeout=5min
max_wal_size=64GB
bcdb_worker_count=$BCDB_WORKERS
enable_merkle_index=$MERKLE_GUC
bcdb_serial_gate_mode=1
bcdb_serial_gate_source=0
bcdb_dt_conflict_tracking=on
bcdb_result_ring_slots=2048
bcdb_dt_completion_only_skip_reads=off
bcdb_dt_hashtab_switch_threshold=1500
bcdb_gate_telemetry=off
bcdb_gate_snapshot_each_block=off
merkle_apply_synchronous_direct=on
track_io_timing=on
enable_seqscan=off
log_min_messages=warning
default_transaction_isolation='serializable'
max_locks_per_transaction=4012
max_pred_locks_per_transaction=4012
max_pred_locks_per_page=512
bcdb_advance_commit_watermark=on
log_checkpoints=on
CONF
  PG_STARTED=1
  BCDB_PHASE_TRACE="$RUN/ptrace" "${PIN[@]}" "$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 300 start
  "${PSQL[@]}" -f "$SRC/scripts/distributed/sql/raft_apply_ledger_schema.sql" > "$RUN/ledger.log" 2>&1
  "${PSQL[@]}" -f "$SRC/scripts/tpcc_v3/tpcc_schema.sql" > "$RUN/schema.log" 2>&1
  /usr/bin/time -p python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" --warehouses 2 --seed 42 \
    --host 127.0.0.1 --port "$PORT" --user postgres --db postgres --cache-dir "$RUNROOT/cache" > "$RUN/load.log" 2> "$RUN/load.time"
  for table in warehouse district customer history item stock oorder new_order order_line; do
    "${PSQL[@]}" -c "ALTER TABLE public.$table SET (fillfactor=90); ALTER TABLE public.$table SET LOGGED;" >> "$RUN/logged.log"
  done
  "${PSQL[@]}" -v bench_enable_merkle="$MERKLE" -v bench_merkle_fanout=32 -v bench_merkle_partitions=16384 \
    -v bench_merkle_split_threshold=1024 -v bench_merkle_merge_threshold=256 \
    -v bench_merkle_partition_key_columns=1 -v bench_merkle_subpartitions=16 \
    -f "$SRC/scripts/tpcc_v3/tpcc_procs.sql" > "$RUN/procs.log" 2>&1
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" > "$RUN/initial_consistency.txt"
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" > "$RUN/initial_state.hash"
  "${PSQL[@]}" --csv -c "SELECT name,setting FROM pg_settings WHERE name IN ('default_transaction_isolation','autovacuum','synchronous_commit','fsync','full_page_writes','enable_seqscan','shared_buffers','bcdb_worker_count','enable_merkle_index','merkle_apply_synchronous_direct') ORDER BY name" > "$RUN/settings.csv"
  if [[ $label == load* ]]; then cleanup; continue; fi
  "${PSQL[@]}" -c 'ANALYZE warehouse,district,customer,history,item,stock,oorder,new_order,order_line; CREATE EXTENSION pg_prewarm;'
  "${PSQL[@]}" -At -c "SELECT sum(pg_prewarm(c.oid::regclass,'buffer')) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('public','ariabc_internal') AND c.relkind IN ('r','i','m')" > "$RUN/prewarm.txt"
  "${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district' > "$RUN/district_before.txt"
  # Every process in the pipeline runs on ranking and inherits this CPU set.
  env BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 \
    ARIABC_PG_MAX_RETRIES=100 ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 \
    ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1 \
    "${PIN[@]}" "$BINDIR/ariabc_pg_server" --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort "$CLIENT_PORT" \
    --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort "$PORT" --dbUser postgres \
    --dbType "$DBTYPE" --safedb "$DBTYPE" --dbConnPoolSize "$WORKERS" --bcdbInitBlockSize "$WORKERS" \
    --pgExecMode event --bypassRaft 1 </dev/null > "$RUN/server.log" 2> "$RUN/server.err.log" &
  SERVER_PID=$!
  for i in $(seq 1 100); do
    kill -0 "$SERVER_PID"
    if python3 - "$CLIENT_PORT" <<'PY'
import socket,sys
with socket.create_connection(('127.0.0.1',int(sys.argv[1])),timeout=.2): pass
PY
    then break; fi
    sleep .2
  done
  WL=$RUNROOT/workload.sql
  [[ $label != abort_* ]] || WL=$RUNROOT/abort.sql
  GW_RC=0
  env ARIABC_WAIT_RESULT_TIMEOUT_MS=180000 timeout 1800 "${PIN[@]}" "$BINDIR/ariabc_pg_gateway" \
    --nodes 127.0.0.1:$CLIENT_PORT --queryFrom "$WL" --dbType "$DBTYPE" --detStartSeq 0 --reqIdOffset 1 \
    --detWindow 1024 --detBatchSize 256 --dbConnPoolSize "$WORKERS" --submitMode event --detSubmitPipeline 1 \
    --detPipelineDepth 1024 --detClientMode event --detClientWorkers 96 --detClientInflight 16 \
    --clientId A-validation --numTerminals 96 --connFanout 1 --waitMajority 0 --completionPath direct \
    --totalNodes 1 --progressIntervalMs 1000 > "$RUN/gateway.log" 2>&1 || GW_RC=$?
  printf 'gateway_rc=%s\n' "$GW_RC" > "$RUN/result.txt"
  "${PSQL[@]}" -At -c 'SELECT sum(d_next_o_id) FROM public.district' > "$RUN/district_after.txt"
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/consistency_check.sql" > "$RUN/consistency.txt"
  "${PSQL[@]}" -At -f "$SRC/scripts/tpcc_v3/state_hash.sql" > "$RUN/state.hash"
  if [[ $MERKLE == 1 ]]; then
    "${PSQL[@]}" -At -c "SELECT c.relname||'='||merkle_verify_index(c.oid)::text FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle' AND c.relnamespace='public'::regnamespace ORDER BY c.relname" > "$RUN/merkle_verify.txt"
  fi
  cp "$RUN/postgres.log" "$RUN/postgres_workload.log"
  cleanup
  [[ $GW_RC == 0 ]]
  python3 - "$RUN" "$WL" <<'PY'
import json,re,sys
from pathlib import Path
run,wl=map(Path,sys.argv[1:]); log=(run/'gateway.log').read_text()
expected=1 if wl.name=='abort.sql' else json.loads(Path(str(wl)+'.meta.json').read_text())['expected_rollbacks']
for key,value in [('user_aborts',expected),('user_abort_counter_supported',1),('user_abort_poll_failures',0),('permanent_failures',0),('divergence_count',0)]:
    found=re.findall(r'\b'+key+r'=(\d+)',log); assert found and int(found[-1])==value,(key,found,value)
progress=[dict(re.findall(r'(\w+)=([^\s]+)',l)) for l in log.splitlines() if l.startswith('PROGRESS_GATEWAY_DET') and 'final=1' in l]
assert progress and int(progress[-1]['completed'])==(2 if wl.name=='abort.sql' else 20000),progress
assert 'consistency_ok=t' in (run/'consistency.txt').read_text()
if (run/'merkle_verify.txt').exists():
    lines=(run/'merkle_verify.txt').read_text().splitlines(); assert len(lines)==9 and all(l.endswith('=true') for l in lines),lines
if wl.name=='abort.sql': assert (run/'state.hash').read_bytes()==(run/'initial_state.hash').read_bytes()
else:
    meta=json.loads(Path(str(wl)+'.meta.json').read_text())
    delta=int((run/'district_after.txt').read_text())-int((run/'district_before.txt').read_text())
    assert delta==meta['counts']['new_order']-expected,(delta,meta)
print('A validation PASS',run.name,'user_aborts=',expected)
PY
done
cmp "$RUNROOT/A_load1/initial_state.hash" "$RUNROOT/A_load2/initial_state.hash"
for label in load1 load2; do grep -Fx 'consistency_ok=t' "$RUNROOT/A_$label/initial_consistency.txt"; done
cmp "$RUNROOT/A_det/state.hash" "$RUNROOT/A_merkle/state.hash"
printf 'INST=%s\nBINDIR=%s\nSRC=%s\nPROCS=%s\nLOADER=%s\nEVIDENCE=%s\n' \
  "$INST" "$BINDIR" "$SRC" "$SRC/scripts/tpcc_v3/tpcc_procs.sql" "$SRC/scripts/tpcc_v3/tpcc_load.py" "$RUNROOT" > "$ROOT/A_READY"
echo "All Agent A remote checks PASS: $RUNROOT"
