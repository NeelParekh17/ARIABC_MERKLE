#!/usr/bin/env bash
# Post-publication re-execution reproducer (lab host .247 only).
# Runs the workload once serially through psql (the reference order) and once
# in det mode through ariabc_pg_server + gateway, then compares table states.
# usage: run_repro.sh <root> <label> <workers> <workload> [VAR=VALUE ...]
#   VAR=VALUE pairs go to the postmaster env, e.g. BCDB_DT_POST_PUBLISH_SETTLE=0
#   BCDB_FAILPOINT_POST_PUBLISH_APPLY=7
set -euo pipefail
ROOT=$1; LABEL=$2; WORKERS=$3; WL=$4; shift 4
PG_ENV=("$@")
INST=$ROOT/install; SRC=$ROOT/src; BIN=$SRC/ariabc_pg/build/bin
HERE=$(cd "$(dirname "$0")" && pwd)
RUN=$ROOT/runs/$LABEL
PORT=${PORT:-55471}; CLIENT_PORT=${CLIENT_PORT:-18171}; RAFT_PORT=${RAFT_PORT:-19171}
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/rdkafka_local/lib
PSQL="$INST/bin/psql -X -q -h 127.0.0.1 -p $PORT -U postgres -d postgres"
[[ ! -e $RUN ]] || { echo "refusing existing $RUN"; exit 2; }
mkdir -p "$RUN"
(( $(free -g | awk '/^Mem:/{print $7}') >= 4 )) || { echo "less than 4 GB available"; exit 2; }

SERVER_PID=
stop_all() {
  [[ -n $SERVER_PID ]] && kill -KILL "$SERVER_PID" 2>/dev/null || true
  "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w stop >/dev/null 2>&1 || true
}
trap stop_all EXIT

"$INST/bin/initdb" -D "$RUN/pgdata" -U postgres >"$RUN/initdb.log" 2>&1
cat >> "$RUN/pgdata/postgresql.conf" <<CONF
port = $PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$RUN'
shared_buffers = 256MB
maintenance_work_mem = 64MB
max_parallel_maintenance_workers = 0
max_connections = 200
autovacuum = off
default_transaction_isolation = 'serializable'
bcdb_worker_count = $WORKERS
enable_merkle_index = off
bcdb_serial_gate_mode = 1
bcdb_serial_gate_source = 0
bcdb_dt_conflict_tracking = on
bcdb_result_ring_slots = 2048
bcdb_dt_completion_only_skip_reads = off
bcdb_dt_hashtab_switch_threshold = 1500
bcdb_advance_commit_watermark = on
log_min_messages = warning
CONF
printf '%s\n' "${PG_ENV[@]}" > "$RUN/pg_env.txt"
env "${PG_ENV[@]}" BCDB_PHASE_TRACE="$RUN/ptrace" "$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 120 start >/dev/null
$PSQL -f "$SRC/scripts/distributed/sql/raft_apply_ledger_schema.sql" >/dev/null 2>"$RUN/ledger.err" || true

state_hash() {
  $PSQL -At -c "SELECT string_agg(t || '=' || h, ' ' ORDER BY t) FROM (
    SELECT 'acct' t, md5(string_agg(x::text, '|' ORDER BY x::text)) h FROM acct x UNION ALL
    SELECT 'outt', md5(string_agg(x::text, '|' ORDER BY x::text)) FROM outt x UNION ALL
    SELECT 'uq', md5(string_agg(x::text, '|' ORDER BY x::text)) FROM uq x) s"
}

# 1. serial reference: one ordinary session, workload order (errors do not stop it)
$PSQL -f "$HERE/schema.sql" >/dev/null
$PSQL -f "$WL" >/dev/null 2>"$RUN/serial.err" || true
echo "serial_errors=$(grep -c ERROR "$RUN/serial.err" || true)" > "$RUN/result.txt"
state_hash > "$RUN/serial.hash"

# 2. det run (same flags as the TPC-C v2 sweep)
$PSQL -f "$HERE/schema.sql" >/dev/null
rm -f "$RUN"/ptrace.[0-9]*
BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 \
ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 \
ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1 \
nohup "$BIN/ariabc_pg_server" --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort $CLIENT_PORT \
  --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort $PORT \
  --dbUser postgres --dbType 1 --safedb 1 --dbConnPoolSize $WORKERS --bcdbInitBlockSize $WORKERS \
  --pgExecMode event --bypassRaft 1 </dev/null >"$RUN/server.log" 2>"$RUN/server.err.log" &
SERVER_PID=$!
for i in $(seq 1 50); do fuser $CLIENT_PORT/tcp >/dev/null 2>&1 && break; sleep 0.2; done
ARIABC_WAIT_RESULT_TIMEOUT_MS=180000 timeout 900 "$BIN/ariabc_pg_gateway" --nodes 127.0.0.1:$CLIENT_PORT \
  --queryFrom "$WL" --dbType 1 --detStartSeq 0 --reqIdOffset 1 --detWindow 1024 --detBatchSize 256 \
  --dbConnPoolSize $WORKERS --submitMode event --detSubmitPipeline 1 --detPipelineDepth 1024 \
  --detClientMode event --detClientWorkers 32 --detClientInflight 16 --clientId single-gateway-direct \
  --numTerminals 32 --connFanout 1 --waitMajority 0 --completionPath direct --totalNodes 1 \
  >"$RUN/gateway.log" 2>&1 || echo "gateway_rc=$?" >> "$RUN/result.txt"
kill -KILL "$SERVER_PID" 2>/dev/null || true; SERVER_PID=
sleep 1
state_hash > "$RUN/det.hash"
grep -Eo 'completed_tps=[0-9.]+' "$RUN/gateway.log" | tail -1 >> "$RUN/result.txt" || true
grep -Eo '(permanent_failures|divergence_count)=[0-9]+' "$RUN/gateway.log" | sort -u | tr '\n' ' ' >> "$RUN/result.txt"; echo >> "$RUN/result.txt"
echo "state_equal=$(cmp -s "$RUN/serial.hash" "$RUN/det.hash" && echo yes || echo NO)" >> "$RUN/result.txt"
"$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w stop >/dev/null 2>&1 || true
# counters: restarts=col2; post-publish counters are the last three columns
cat "$RUN"/ptrace.[0-9]* 2>/dev/null | grep -v '^tx_id' | awk -F, '{n++; r+=$2; s+=$(NF-2); u+=$(NF-1); v+=$NF} END {printf "traced_tx=%d restarts=%d post_publish_settles=%d terminal_unique=%d invariant=%d\n", n, r, s, u, v}' >> "$RUN/result.txt"
echo "restart_log_lines=$(grep -c 'apply_retry_restart\|apply_unique_conflict_full_restart\|apply_unique_settled_retry_restart' "$RUN/postgres.log" || true) invariant_warnings=$(grep -c BCDB_INVARIANT_POST_PUBLISH_APPLY "$RUN/postgres.log" || true)" >> "$RUN/result.txt"
rm -rf "$RUN/pgdata"
cat "$RUN/result.txt"
