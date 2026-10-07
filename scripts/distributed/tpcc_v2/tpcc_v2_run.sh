#!/usr/bin/env bash
# Merkle-mode TPC-C on ranking, isolated in ~/claude_checks (campaign flags).
# Optional env: FILLFACTOR (unset preserves reference), INST, SRCDIR, BINDIR, RUNROOT.
# usage: tpcc_v2_run.sh <label> <warehouses> <tx> <workers> <partitions> <partition_key_columns> <subpartitions> [mode=merkle|det|pg]
set -euo pipefail
# Only the master holds the campaign lock; PostgreSQL must not inherit it.
exec 9>&-
LABEL=$1; W=$2; N=$3; WORKERS=$4; P=$5; PKC=$6; SUB=$7; MODE=${8:-merkle}
MERKLE=0; [ $MODE = merkle ] && MERKLE=1
BCDB_WORKERS=$WORKERS; [ $MODE = pg ] && BCDB_WORKERS=1
MERKLE_GUC=off; [ $MERKLE = 1 ] && MERKLE_GUC=on
C=$HOME/claude_checks
INST=${INST:-$C/install}
SRC=${SRCDIR:-$C/src}
BIN=${BINDIR:-$C/src/ariabc_pg/build/bin}
FILLFACTOR=${FILLFACTOR:-}
if [[ -n $FILLFACTOR && ! $FILLFACTOR =~ ^[0-9]+$ ]]; then exit 2; fi
if [[ -n $FILLFACTOR ]] && (( FILLFACTOR < 10 || FILLFACTOR > 100 )); then exit 2; fi
SPLIT=${SPLIT:-32}
MERGE=${MERGE:-$((SPLIT/4))}
RUN=${RUNROOT:-$C/probe_merkle_cost}/${MODE}_${LABEL}_w${W}
PORT=55439; CLIENT_PORT=18100; RAFT_PORT=19100
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
PSQL="$INST/bin/psql -X -q -v ON_ERROR_STOP=1 -h 127.0.0.1 -p $PORT -U postgres -d postgres"
WL=${WORKLOAD_ROOT:-${RUNROOT:?RUNROOT must be a fresh v2 output root}/workloads}/tpcc-$N-w$W-seed42.txt
[[ $RUN == "$C"/tpcc_v2_*/* && ! -e $RUN ]] || { echo "Refusing existing/non-v2 run: $RUN" >&2; exit 2; }
mkdir -p "$RUN" "$(dirname "$WL")"
echo "inst=$INST src=$SRC bin=$BIN split=$SPLIT merge=$MERGE fillfactor=${FILLFACTOR:-default} mode=$MODE warehouses=$W workers=$WORKERS tx=$N" > "$RUN/config.txt"
# Refuse occupied ports before registering cleanup; never kill a foreign listener.
python3 - <<'PORTCHECK'
import socket
for port in (55439, 18100, 19100):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', port))
PORTCHECK
SERVER_PID=
PG_STARTED=0
{
  date -Is
  uptime
  ps -eo user,pid,pcpu,pmem,etimes,args --sort=-pcpu | head -21 || true
} > "$RUN/host_before.txt"

stop_all() {
  if [[ -n $SERVER_PID ]]; then
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=
  fi
  if [[ $PG_STARTED == 1 ]]; then
    if [[ -f $RUN/pgdata/postmaster.pid ]]; then
      "$INST/bin/pg_ctl" -D "$RUN/pgdata" -m fast -w stop >/dev/null 2>&1
    fi
    [[ ! -e $RUN/pgdata/postmaster.pid ]]
    PG_STARTED=0
  fi
}
trap stop_all EXIT

[ -f $WL ] || python3 $SRC/scripts/generate_tpcc_workload.py --count $N --warehouses $W --seed 42 \
  --remote-payment-pct 15 --remote-new-order-pct 1 -o $WL >/dev/null

$INST/bin/initdb -D $RUN/pgdata -U postgres >$RUN/initdb.log 2>&1
cat >> $RUN/pgdata/postgresql.conf <<CONF
port = $PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$RUN'
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
bcdb_worker_count = $BCDB_WORKERS
enable_merkle_index = $MERKLE_GUC
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
PG_STARTED=1
BCDB_PHASE_TRACE=$RUN/ptrace $INST/bin/pg_ctl -D $RUN/pgdata -l $RUN/postgres.log -w -t 300 start >/dev/null
$PSQL -f $SRC/scripts/distributed/sql/raft_apply_ledger_schema.sql >/dev/null 2>$RUN/ledger.err || true

# Restore exactly as the sweep harness does (restore_tpcc_db).
$PSQL -f $SRC/scripts/restore_tpcc_drop.sql >/dev/null 2>&1 || true
sed '/^ALTER TABLE public\.[a-z_]* OWNER TO admin;$/d' $SRC/scripts/tpcc/tpcc-pgdump-full.sql | $PSQL >/dev/null 2>>$RUN/restore.err
$PSQL -c "ALTER TABLE public.warehouse SET UNLOGGED; ALTER TABLE public.customer SET UNLOGGED; ALTER TABLE public.history SET UNLOGGED; ALTER TABLE public.oorder SET UNLOGGED; ALTER TABLE public.order_line SET UNLOGGED; ALTER TABLE public.new_order SET UNLOGGED; ALTER TABLE public.stock SET UNLOGGED; ALTER TABLE public.item SET UNLOGGED;" >/dev/null
python3 $SRC/scripts/restore_tpcc_scale.py --host 127.0.0.1 --port $PORT --user postgres --db postgres --warehouses $W >/dev/null
# district was already LOGGED in the reference restore. Force its rewrite only
# for fillfactor runs, then use the original SET LOGGED for all tables.
if [[ -n $FILLFACTOR ]]; then
  $PSQL -c 'ALTER TABLE public.district SET UNLOGGED' >/dev/null
  for table in warehouse district customer stock oorder order_line new_order history; do
    $PSQL -c "ALTER TABLE public.$table SET (fillfactor=$FILLFACTOR)" >/dev/null
  done
fi
$PSQL -c 'ALTER TABLE warehouse SET LOGGED; ALTER TABLE district SET LOGGED; ALTER TABLE customer SET LOGGED; ALTER TABLE history SET LOGGED; ALTER TABLE oorder SET LOGGED; ALTER TABLE order_line SET LOGGED; ALTER TABLE new_order SET LOGGED; ALTER TABLE stock SET LOGGED; ALTER TABLE item SET LOGGED;' >/dev/null
$PSQL --csv -c "SELECT c.relname,c.relpersistence,COALESCE(array_to_string(c.reloptions,';'),'') AS reloptions,pg_relation_size(c.oid) AS heap_bytes,pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('warehouse','district','customer','stock','oorder','order_line','new_order','history','item') ORDER BY c.relname" > "$RUN/restore_relations.csv"
restore_start=$(date +%s)
$PSQL -v bench_enable_merkle=$MERKLE -v bench_merkle_fanout=32 -v bench_merkle_partitions=$P \
  -v bench_merkle_split_threshold=$SPLIT -v bench_merkle_merge_threshold=$MERGE \
  -v bench_merkle_partition_key_columns=$PKC -v bench_merkle_subpartitions=$SUB \
  -f $SRC/scripts/restore_tpcc_procs.sql >$RUN/procs.log 2>&1
echo "merkle_build_s=$(( $(date +%s) - restore_start ))" > $RUN/result.txt
$PSQL --csv -c "SELECT c.relname,COALESCE(array_to_string(c.reloptions,';'),'') AS reloptions FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle' ORDER BY c.relname" > "$RUN/merkle_options.csv"
$PSQL --csv -c "SELECT name,setting FROM pg_settings WHERE name IN ('default_transaction_isolation','synchronous_commit','fsync','full_page_writes','merkle_apply_synchronous_direct','shared_buffers','bcdb_worker_count') ORDER BY name" > "$RUN/settings.csv"
$PSQL -c 'ANALYZE public.warehouse, public.district, public.customer, public.stock, public.item, public.oorder, public.new_order, public.order_line, public.history;' >/dev/null
# The harness restarts PostgreSQL after the restore (cold reset), which
# checkpoints; flush the restore's WAL now so no checkpoint lands mid-run.
$PSQL -c 'CHECKPOINT' >/dev/null
# Optional: let the kernel write back the checkpointed pages before measuring,
# so OS writeback does not compete with WAL fsyncs during the run.
if [ "${SYNC_BEFORE_MEASURE:-0}" = 1 ]; then
  sync
  for i in $(seq 1 180); do
    [ "$(awk '/^Dirty:/{print $2}' /proc/meminfo)" -lt 262144 ] && break
    sleep 1
  done
  echo "dirty_kb_at_start=$(awk '/^Dirty:/{print $2}' /proc/meminfo) sync_wait_s=$i" >> $RUN/result.txt
fi
echo "measure_start=$(date +%T)" >> $RUN/result.txt

# Prewarm every relation the workload touches (harness TPCC_PREWARM_SQL).
$PSQL -c 'CREATE EXTENSION IF NOT EXISTS pg_prewarm' >/dev/null
$PSQL -At -c "SELECT sum(pg_prewarm(c.oid::regclass, 'buffer')) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname IN ('public','ariabc_internal') AND c.relkind IN ('r','i','m')" > $RUN/prewarm_blocks.txt
echo "prewarm_blocks=$(cat $RUN/prewarm_blocks.txt)" >> $RUN/result.txt

rm -f $RUN/ptrace.[0-9]*
$PSQL -c "select pg_stat_reset()" >/dev/null
L0=$($PSQL -At -c "select pg_current_wal_lsn()")
if [ $MODE = pg ]; then
BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 ARIABC_PG_MAX_RETRIES=100 \
nohup $BIN/ariabc_pg_server --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort $CLIENT_PORT \
  --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort $PORT \
  --dbUser postgres --dbType 0 --safedb 0 --dbConnPoolSize $WORKERS \
  --pgExecMode event --bypassRaft 1 </dev/null >$RUN/server.log 2>$RUN/server.err.log &
SERVER_PID=$!
DBTYPE=0
else
BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_PROFILE=1 \
ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 \
ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1 \
nohup $BIN/ariabc_pg_server --id 1 --raftEndpoint 127.0.0.1:$RAFT_PORT --clientPort $CLIENT_PORT \
  --raftMembers 1=127.0.0.1:$RAFT_PORT --dbName postgres --dbHost 127.0.0.1 --dbPort $PORT \
  --dbUser postgres --dbType 1 --safedb 1 --dbConnPoolSize $WORKERS --bcdbInitBlockSize $WORKERS \
  --pgExecMode event --bypassRaft 1 </dev/null >$RUN/server.log 2>$RUN/server.err.log &
SERVER_PID=$!
DBTYPE=1
fi
for i in $(seq 1 50); do
  kill -0 "$SERVER_PID"
  fuser $CLIENT_PORT/tcp >/dev/null 2>&1 && break
  sleep 0.2
 done
GATEWAY_RC=0
ARIABC_WAIT_RESULT_TIMEOUT_MS=180000 timeout 1800 $BIN/ariabc_pg_gateway --nodes 127.0.0.1:$CLIENT_PORT \
  --queryFrom $WL --dbType $DBTYPE --detStartSeq 0 --reqIdOffset 1 --detWindow 1024 --detBatchSize 256 \
  --dbConnPoolSize $WORKERS --submitMode event --detSubmitPipeline 1 --detPipelineDepth 1024 \
  --detClientMode event --detClientWorkers 96 --detClientInflight 16 --clientId single-gateway-direct \
  --numTerminals 96 --connFanout 1 --waitMajority 0 --completionPath direct --totalNodes 1 \
  >$RUN/gateway.log 2>&1 || GATEWAY_RC=$?
echo "gateway_rc=$GATEWAY_RC" >> "$RUN/result.txt"
L1=$($PSQL -At -c "select pg_current_wal_lsn()")
kill -KILL "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
# BCDB workers remain alive until pg_ctl; allow PG13's asynchronous stats flush.
sleep 2
echo "wal_bytes=$($PSQL -At -c "select pg_wal_lsn_diff('$L1','$L0')")" >> $RUN/result.txt
$PSQL -At -F, -c "select schemaname,relname,n_tup_ins,n_tup_upd,n_tup_hot_upd,n_tup_del from pg_stat_all_tables where schemaname in ('public','ariabc_internal') and (n_tup_upd+n_tup_ins+n_tup_del)>0 order by 1,2" > $RUN/tabstats.csv
$PSQL -At -F, -c "select schemaname,relname,indexrelname,idx_scan from pg_stat_all_indexes where schemaname in ('public','ariabc_internal') and idx_scan>0 order by 1,2,3" > $RUN/idxstats.csv
echo "wal_start=$L0 wal_end=$L1" >> "$RUN/result.txt"
$INST/bin/pg_waldump -p $RUN/pgdata/pg_wal -s $L0 -e $L1 --stats=record > $RUN/walstats.txt 2>&1

grep -Eo 'completed_tps=[0-9.]+' $RUN/gateway.log | tail -1 >> $RUN/result.txt || true
grep -Eo '(permanent_failures|divergence_count)=[0-9]+' $RUN/gateway.log | sort -u | tr '\n' ' ' >> $RUN/result.txt; echo >> $RUN/result.txt
[ $MERKLE = 1 ] && echo "merkle_verify=$($PSQL -At -c "SELECT count(*) || ':' || COALESCE(bool_and(merkle_verify_index(c.oid)), false) FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid JOIN pg_class t ON t.oid = i.indrelid JOIN pg_am am ON am.oid = c.relam WHERE am.amname = 'merkle' AND t.relname IN ('warehouse','district','customer','history','item','stock','oorder','new_order','order_line')")" >> $RUN/result.txt
[ $MERKLE = 1 ] && echo "node_rows=$($PSQL -At -c "SELECT sum(n_live_tup) FROM pg_stat_user_tables WHERE schemaname='ariabc_internal' AND relname LIKE 'merkle_node_%'")" >> $RUN/result.txt
[ $MERKLE = 1 ] && echo "leaf_depth_levels=$($PSQL -At -c "SELECT string_agg(t || ':' || d, ' ' ORDER BY t) FROM (SELECT 'stock' t, round(avg(prefix_len/5.0 + 1), 2) d FROM ariabc_internal.merkle_node_stock WHERE is_leaf AND tuple_count > 0 UNION ALL SELECT 'order_line', round(avg(prefix_len/5.0 + 1), 2) FROM ariabc_internal.merkle_node_order_line WHERE is_leaf AND tuple_count > 0) x")" >> $RUN/result.txt
$PSQL -At <<'SQL' > $RUN/state.hash
SELECT string_agg(t || '=' || h, ' ' ORDER BY t) FROM (
  SELECT 'warehouse' t, count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) h FROM (SELECT w_id, w_ytd FROM warehouse) x UNION ALL
  SELECT 'district', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM (SELECT d_w_id, d_id, d_ytd, d_next_o_id FROM district) x UNION ALL
  SELECT 'customer', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM (SELECT c_w_id, c_d_id, c_id, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data FROM customer) x UNION ALL
  SELECT 'history', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM (SELECT h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_data FROM history) x UNION ALL
  SELECT 'oorder', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM (SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local FROM oorder) x UNION ALL
  SELECT 'new_order', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM new_order x UNION ALL
  SELECT 'order_line', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM order_line x UNION ALL
  SELECT 'stock', count(*) || ':' || sum(hashtextextended(x::text, 0)::numeric) FROM (SELECT s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock) x) s;
SQL
{ cat $RUN/ptrace.[0-9]* 2>/dev/null || true; } | { grep -v '^tx_id' || true; } | awk -F, '{n++; r+=$2; if ($2>0) c++} END {printf "traced_tx=%d total_restarts=%d tx_with_restart=%d\n", n, r, c}' >> $RUN/result.txt
# Snapshot pre-shutdown log; normal fast shutdown emits administrator FATAL messages.
cp "$RUN/postgres.log" "$RUN/postgres_workload.log"
# Stop owned workers to flush all phase traces and cumulative table statistics.
stop_all
PG_STARTED=0
mkdir -p "$RUN/ptrace_keep"
for trace in "$RUN"/ptrace.[0-9]*; do
  if [[ -f $trace ]]; then mv "$trace" "$RUN/ptrace_keep/"; fi
done
# Restart only for final PG13 cumulative stats after worker exit; no workload.
PG_STARTED=1
"$INST/bin/pg_ctl" -D "$RUN/pgdata" -l "$RUN/postgres.log" -w -t 300 start >/dev/null
$PSQL -At -F, -c "select schemaname,relname,n_tup_ins,n_tup_upd,n_tup_hot_upd,n_tup_del from pg_stat_all_tables where schemaname in ('public','ariabc_internal') and (n_tup_upd+n_tup_ins+n_tup_del)>0 order by 1,2" > "$RUN/tabstats.csv"
stop_all
PG_STARTED=0
cat "$RUN/result.txt"
exit "$GATEWAY_RC"
