#!/usr/bin/env bash
# run_kafka_node_sweep.sh — Per-node Kafka vs direct TPS sweep.
#
# Each node is started as a single-node ariabc_pg_server (no Raft peers).
# The local ASUS gateway connects to each remote node and sweeps numTerminals.
# Two gateway modes per node:
#   direct        — no Kafka wait  (pure gateway→server RTT)
#   kafka_majority — wait for server to publish result to Kafka broker (1/1 majority)
#
# All 5 nodes run in parallel; each gets its own Kafka topic to avoid cross-talk.
#
# Output: per-node TPS tables + peak-TPS summary in ALL_MACHINES_DETAIL_REPORT.md style.
#
# Usage: bash scripts/distributed/run_kafka_node_sweep.sh [options]
#   --terminals  CSV  numTerminals to sweep   [default: 1 — det mode requires =1]
#   --runs       N    runs per configuration  [default: 3]
#   --batch-size N    detBatchSize            [default: 4]
#   --workload   FILE workload sql file       [default: ycsb-skew0-99-tx-20k-...]
#   --skip-kafka      skip Kafka broker setup (assume already running)
#   --no-kafka        measure direct mode only (no Kafka sweep)
#   --pool-size  N    dbConnPoolSize          [default: 2]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Cluster topology (same as run_4node_raft_cluster.sh)
# ---------------------------------------------------------------------------
declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.247 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(neel-MS kartik utkarsh)
declare -a NODE_USERS=(neel neel neel)
declare -a NODE_IS_U22=(0 1 0)
declare -a NODE_LABELS=("neel-MS (236)" "kartik (54)" "utkarsh (248)")

ARIABC_CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-clusterinfolab123}"
CLUSTER_PASSWORD="$ARIABC_CLUSTER_PASSWORD"

KAFKA_HOST="10.129.148.247"
KAFKA_PORT=9092
KAFKA_HOME_REMOTE="/home/neel/Desktop/kafka_2.13-3.7.0"
KAFKA_BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"

RAFT_PORT=9050          # dedicated port so we don't clash with cluster runs
CLIENT_PORT_BASE=8050   # 8050..8054 per node
DB_PORT=5438
DB_USER=postgres
DB_NAME=postgres
DB_CONN_POOL_SIZE="${DB_CONN_POOL_SIZE:-2}"

REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
REMOTE_BIN_U24="$REMOTE_REPO_ROOT/ariabc_pg/build/bin/ariabc_pg_server"
REMOTE_BIN_U22="/home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server"

LOCAL_GW_BIN="$REPO_ROOT/ariabc_pg/build/bin/ariabc_pg_gateway"
LOCAL_RDKAFKA_LIB="$HOME/Desktop/rdkafka_local/lib"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10 -o ControlMaster=no -o ControlPath=none)

# ---------------------------------------------------------------------------
# Tunables / flags
# ---------------------------------------------------------------------------
TERMINALS="${TERMINALS:-1}"  # det mode requires numTerminals=1; use --terminals for override
RUNS="${RUNS:-3}"
DET_BATCH_SIZE="${DET_BATCH_SIZE:-4}"
DET_WINDOW=32
WORKLOAD_FILE="${WORKLOAD_FILE:-$REPO_ROOT/scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
RESTORE_SQL="$REPO_ROOT/scripts/restore_usertable_small.sql"
SKIP_KAFKA=0
NO_KAFKA=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --terminals)  TERMINALS="${2:-}"; shift 2 ;;
    --runs)       RUNS="${2:-3}"; shift 2 ;;
    --batch-size) DET_BATCH_SIZE="${2:-4}"; shift 2 ;;
    --workload)   WORKLOAD_FILE="${2:-}"; shift 2 ;;
    --skip-kafka) SKIP_KAFKA=1; shift ;;
    --no-kafka)   NO_KAFKA=1; shift ;;
    --pool-size)  DB_CONN_POOL_SIZE="${2:-2}"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

IFS=',' read -ra TERMINAL_ARR <<< "$TERMINALS"

LOG_DIR="$REPO_ROOT/scripts/bench_full_results/kafka_sweep_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"
RESULTS_CSV="$LOG_DIR/results.csv"
echo "node,mode,terminals,run,tps_gateway,submit_ms,kafka_wait_ms,total_ms,txns" > "$RESULTS_CSV"

WORKLOAD_LINES="$(awk 'BEGIN{n=0} /^[[:space:]]*($|--)/{next} {n++} END{print n}' "$WORKLOAD_FILE")"

log()  { echo "[$(date +'%H:%M:%S')] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------
node_ssh() {
  local idx="$1"; shift
  local ip="${NODE_IPS[$idx]}" user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$user@$ip" "$@"
}

# ---------------------------------------------------------------------------
# Phase 0: Ensure Kafka running on admin123
# ---------------------------------------------------------------------------
if [[ "$SKIP_KAFKA" -eq 0 && "$NO_KAFKA" -eq 0 ]]; then
  log "=== Ensuring Kafka (KRaft) on ${KAFKA_HOST} ==="
  node_ssh 0 bash <<KAFKA_EOF
set -euo pipefail
KAFKA_HOME="$KAFKA_HOME_REMOTE"
if ! command -v java >/dev/null 2>&1; then
  export JAVA_HOME="/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64"
  export PATH="\$JAVA_HOME/bin:\$PATH"
fi
TOPICS_SH="\$KAFKA_HOME/bin/kafka-topics.sh"
if "\$TOPICS_SH" --bootstrap-server "${KAFKA_HOST}:${KAFKA_PORT}" --list >/dev/null 2>&1; then
  echo "Kafka already running"
else
  SERVER_PROPS="\$KAFKA_HOME/config/kraft/server.properties"
  sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${KAFKA_HOST}:${KAFKA_PORT}|" "\$SERVER_PROPS" 2>/dev/null || true
  STORAGE_SH="\$KAFKA_HOME/bin/kafka-storage.sh"
  cluster_id="\$("\$STORAGE_SH" random-uuid 2>/dev/null | tail -1 | tr -d '\r')"
  "\$STORAGE_SH" format -t "\$cluster_id" -c "\$SERVER_PROPS" --ignore-formatted >/dev/null 2>&1 || true
  "\$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "\$SERVER_PROPS"
  for i in \$(seq 1 60); do
    "\$TOPICS_SH" --bootstrap-server "${KAFKA_HOST}:${KAFKA_PORT}" --list >/dev/null 2>&1 && { echo "Kafka ready after \${i}s"; break; }
    sleep 1
    [[ "\$i" -eq 60 ]] && { echo "ERROR: Kafka timeout" >&2; exit 1; }
  done
fi
KAFKA_EOF
  log "  Kafka ready at ${KAFKA_BOOTSTRAP}"
fi

# ---------------------------------------------------------------------------
# Phase 1: Verify all nodes reachable and postgres running
# ---------------------------------------------------------------------------
log "=== Phase 1: Verify nodes and ensure postgres ==="
ensure_postgres_remote() {
  local idx="$1"
  node_ssh "$idx" "
    INSTALL_DIR='$REMOTE_INSTALL_DIR'
    PGDATA='$REMOTE_REPO_ROOT/.bench_tmp/single_node_pgdata'
    BIN=\$INSTALL_DIR/bin
    export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
    # Restart postgres to guarantee a clean state (no crash-recovery races from stale backends)
    if \$BIN/pg_isready -h 127.0.0.1 -p $DB_PORT -U $DB_USER >/dev/null 2>&1; then
      echo 'restarting postgres for clean state...'
      \$BIN/pg_ctl -D \$PGDATA -w -t 60 restart -l '$REMOTE_REPO_ROOT/server.log' 2>&1 || true
    else
      echo 'postgres not running — starting...'
      \$BIN/pg_ctl -D \$PGDATA -w -t 60 start -l '$REMOTE_REPO_ROOT/server.log' 2>&1 || true
    fi
    # Wait for postgres to exit recovery and accept real write queries (up to 60s)
    for i in \$(seq 1 30); do
      result=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
        -At -c 'SELECT pg_is_in_recovery();' 2>/dev/null)
      [[ \"\$result\" == 'f' ]] && { echo 'postgres OK'; exit 0; }
      sleep 2
    done
    echo 'postgres not ready after 60s' >&2
    exit 1
  "
}
for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  ip="${NODE_IPS[$idx]}"
  ensure_postgres_remote "$idx" || die "postgres not ready on $name ($ip)"
  log "  $name: postgres OK"
done
log "[local] gateway: $LOCAL_GW_BIN"
[[ -x "$LOCAL_GW_BIN" ]] || die "gateway binary not found: $LOCAL_GW_BIN"

# ---------------------------------------------------------------------------
# Run sweep for a single node (called as background subshell per node)
# ---------------------------------------------------------------------------
run_node_sweep() {
  local idx="$1"
  local name="${NODE_NAMES[$idx]}"
  local ip="${NODE_IPS[$idx]}"
  local label="${NODE_LABELS[$idx]}"
  local is_u22="${NODE_IS_U22[$idx]}"
  local client_port=$(( CLIENT_PORT_BASE + idx ))
  local node_log_dir="$LOG_DIR/${name}"
  mkdir -p "$node_log_dir"

  local srv_bin
  [[ "$is_u22" -eq 1 ]] && srv_bin="$REMOTE_BIN_U22" || srv_bin="$REMOTE_BIN_U24"
  local node_lib_path="/home/neel/Desktop/rdkafka_local/lib:$REMOTE_INSTALL_DIR/lib"

  # Per-node Kafka topic (avoid cross-node contamination)
  local kafka_topic="ariabc_kafka_sweep_node${idx}"

  # ---- Create dedicated topic ----
  if [[ "$NO_KAFKA" -eq 0 ]]; then
    node_ssh 0 bash -s <<TOPIC_EOF 2>/dev/null || true
KAFKA_HOME="$KAFKA_HOME_REMOTE"
if ! command -v java >/dev/null 2>&1; then
  export JAVA_HOME="/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64"
  export PATH="\$JAVA_HOME/bin:\$PATH"
fi
\$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --create --topic "$kafka_topic" --partitions 4 --replication-factor 1 \
  --if-not-exists >/dev/null 2>&1 || true
TOPIC_EOF
  fi

  # ---- Kill any stale server on this node ----
  # Use ps+awk to avoid pkill -f self-matching its own command line
  node_ssh "$idx" "
    ps aux | awk '/ariabc_pg_server/ && !/awk/{print \$2}' | xargs -r kill 2>/dev/null || true
    fuser -k ${RAFT_PORT}/tcp 2>/dev/null || true
    fuser -k ${client_port}/tcp 2>/dev/null || true
    sleep 0.5
  " 2>/dev/null || true

  # ---- Ensure postgres is up (may have been stopped by another process) ----
  ensure_postgres_remote "$idx" >"$node_log_dir/pg_ensure.log" 2>&1 || { echo "[$name] postgres could not be started"; return 1; }

  # ---- Terminate ALL other connections before restore (avoids DROP TABLE lock wait) ----
  # This runs before our restore psql starts, so killing all non-self connections is safe.
  node_ssh "$idx" "
    INSTALL_DIR='$REMOTE_INSTALL_DIR'
    export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
    \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c \
      \"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();\" 2>/dev/null || true
  " 2>/dev/null || true

  # ---- Restore table to known state ----
  node_ssh "$idx" "
    INSTALL_DIR='$REMOTE_INSTALL_DIR'
    export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
    \$INSTALL_DIR/bin/psql -X -v ON_ERROR_STOP=1 -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
      -f '$REMOTE_REPO_ROOT/scripts/restore_usertable_small.sql' >/dev/null && echo 'restore OK'
  " >"$node_log_dir/restore.log" 2>&1 || { echo "[$name] restore FAILED"; return 1; }

  # ---- Start single-node server with Kafka ----
  local srv_log="$node_log_dir/server.log"
  local kafka_args=""
  [[ "$NO_KAFKA" -eq 0 ]] && kafka_args="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $kafka_topic"

  node_ssh "$idx" "
    export LD_LIBRARY_PATH='${node_lib_path}:\${LD_LIBRARY_PATH:-}'
    nohup '$srv_bin' \
      --id ${NODE_IDS[$idx]} \
      --raftEndpoint ${ip}:${RAFT_PORT} \
      --clientPort ${client_port} \
      --raftMembers '${NODE_IDS[$idx]}=${ip}:${RAFT_PORT}' \
      --dbName $DB_NAME --dbHost 127.0.0.1 --dbPort $DB_PORT --dbUser $DB_USER \
      --dbType 1 --safedb 1 --dbConnPoolSize $DB_CONN_POOL_SIZE \
      $kafka_args \
      >'/tmp/ariabc_kafka_sweep_${name}.log' 2>&1 &
    echo \$!
  " >"$node_log_dir/server_pid.txt" 2>&1

  # ---- Wait for client port ----
  local waited=0
  until node_ssh "$idx" "ss -tlnp 2>/dev/null | grep -q ':${client_port}'" 2>/dev/null; do
    (( waited++ )) || true
    [[ "$waited" -gt 30 ]] && { echo "[$name] server did not start (timeout 30s)"; return 1; }
    sleep 1
  done
  sleep 3  # let Raft single-node elect itself leader

  # ---- bcdb_init probe ----
  local probe_sql="$node_log_dir/probe.sql"
  local probe_log="$node_log_dir/probe.log"
  printf 'SELECT 1;\n' > "$probe_sql"
  export LD_LIBRARY_PATH="${LOCAL_RDKAFKA_LIB}:/work/ARIABC/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  "$LOCAL_GW_BIN" \
    --nodes "${ip}:${client_port}" \
    --queryFrom "$probe_sql" \
    --dbType 1 --detStartSeq 99000000 --reqIdOffset 99000000 \
    --detWindow 1 --detBatchSize 1 --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
    --submitMode blocking --detSubmitPipeline 0 \
    --clientId "probe-${name}" --numTerminals 1 \
    --waitMajority 0 --completionPath direct --totalNodes 1 \
    >"$probe_log" 2>&1 || { echo "[$name] bcdb_init probe FAILED"; cat "$probe_log"; return 1; }

  # ---- Run sweeps ----
  # Decide which modes to run
  local modes=("direct")
  [[ "$NO_KAFKA" -eq 0 ]] && modes+=("kafka_majority")

  local seq_offset=1
  local req_offset=1

  for mode in "${modes[@]}"; do
    local gw_extra=""
    if [[ "$mode" == "kafka_majority" ]]; then
      gw_extra="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $kafka_topic --waitMajority 1 --completionPath kafka_majority --totalNodes 1"
    else
      gw_extra="--waitMajority 0 --completionPath direct --totalNodes 1"
    fi

    for t in "${TERMINAL_ARR[@]}"; do
      for run_n in $(seq 1 "$RUNS"); do
        local run_log="$node_log_dir/${mode}_t${t}_run${run_n}.log"
        export LD_LIBRARY_PATH="${LOCAL_RDKAFKA_LIB}:/work/ARIABC/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        "$LOCAL_GW_BIN" \
          --nodes "${ip}:${client_port}" \
          --queryFrom "$WORKLOAD_FILE" \
          --dbType 1 \
          --detStartSeq "$seq_offset" \
          --reqIdOffset "$req_offset" \
          --detWindow "$DET_WINDOW" \
          --detBatchSize "$DET_BATCH_SIZE" \
          --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
          --submitMode blocking \
          --detSubmitPipeline 1 \
          --clientId "kafkasweep-${name}-${mode}" \
          --numTerminals "$t" \
          $gw_extra \
          >"$run_log" 2>&1 || true

        # Extract metrics
        local total_ms submit_ms kafka_ms tps
        total_ms="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$run_log" 2>/dev/null | head -1 || echo 0)"
        submit_ms="$(grep -oP 'submit time \(ms\) \K[0-9]+' "$run_log" 2>/dev/null | head -1 || echo 0)"
        kafka_ms="$(grep -oP 'majority wait time \(ms\) \K[0-9]+' "$run_log" 2>/dev/null | head -1 || echo 0)"
        if [[ "$total_ms" -gt 0 ]]; then
          tps=$(( WORKLOAD_LINES * 1000 / total_ms ))
        else
          tps=0
        fi

        # Append to CSV
        echo "${name},${mode},${t},${run_n},${tps},${submit_ms},${kafka_ms},${total_ms},${WORKLOAD_LINES}" >> "$RESULTS_CSV"
        echo "[$name] mode=$mode t=$t run=$run_n tps=$tps (total=${total_ms}ms submit=${submit_ms}ms kafka_wait=${kafka_ms}ms)"

        # Only advance seq/req offsets for runs that actually committed transactions.
        # Skipping on failure avoids serial-gate gaps when t>1 fails with "argument error"
        # before any transactions reach the server.
        if [[ "$total_ms" -gt 0 ]]; then
          seq_offset=$(( seq_offset + WORKLOAD_LINES ))
          req_offset=$(( req_offset + WORKLOAD_LINES ))
        fi
      done
    done
  done

  # ---- Stop server ----
  node_ssh "$idx" "fuser -k ${RAFT_PORT}/tcp 2>/dev/null || true; fuser -k ${client_port}/tcp 2>/dev/null || true" 2>/dev/null || true
  echo "[$name] sweep complete — logs: $node_log_dir"
}

# ---------------------------------------------------------------------------
# Phase 2: Run all nodes in parallel
# ---------------------------------------------------------------------------
log "=== Phase 2: Launching per-node sweeps in parallel ==="
log "  Nodes     : ${NODE_NAMES[*]}"
log "  Terminals : $TERMINALS"
log "  Modes     : $([ "$NO_KAFKA" -eq 0 ] && echo 'direct, kafka_majority' || echo 'direct only')"
log "  BatchSize : $DET_BATCH_SIZE"
log "  Runs      : $RUNS"
log "  Workload  : $(basename "$WORKLOAD_FILE") ($WORKLOAD_LINES txns)"
log "  Logs      : $LOG_DIR"
echo

declare -a SWEEP_PIDS=()
declare -a SWEEP_LOGS=()

for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  sweep_log="$LOG_DIR/${name}/sweep_stdout.log"
  mkdir -p "$LOG_DIR/${name}"
  run_node_sweep "$idx" 2>&1 | tee "$sweep_log" &
  SWEEP_PIDS+=($!)
  SWEEP_LOGS+=("$sweep_log")
  log "  [$name] started (pid $!)"
done

log "Waiting for all node sweeps to complete..."
ALL_OK=1
for i in "${!SWEEP_PIDS[@]}"; do
  pid="${SWEEP_PIDS[$i]}"
  name="${NODE_NAMES[$i]}"
  if wait "$pid"; then
    log "  [$name] DONE"
  else
    log "  [$name] FAILED — see ${SWEEP_LOGS[$i]}"
    ALL_OK=0
  fi
done

# ---------------------------------------------------------------------------
# Phase 3: Compute median TPS and print tables
# ---------------------------------------------------------------------------
log ""
log "=== Phase 3: Results ==="
log "  Raw CSV: $RESULTS_CSV"
echo ""

python3 - "$RESULTS_CSV" "$TERMINALS" "$NO_KAFKA" <<'PYEOF'
import sys, csv, statistics

csv_path = sys.argv[1]
terminals = [int(t) for t in sys.argv[2].split(',')]
no_kafka = sys.argv[3] == "1"

rows = []
with open(csv_path) as f:
    for r in csv.DictReader(f):
        rows.append(r)

# Group: node -> mode -> terminals -> [tps, ...]
from collections import defaultdict
data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
nodes_order = []
for r in rows:
    node = r['node']
    if node not in nodes_order:
        nodes_order.append(node)
    data[node][r['mode']][int(r['terminals'])].append(int(r['tps_gateway']))

def med(lst):
    return int(statistics.median(lst)) if lst else 0

modes = ['direct'] if no_kafka else ['direct', 'kafka_majority']
mode_label = {'direct': 'det-direct', 'kafka_majority': 'det-kafka'}

# --- Per-node table ---
print("## Per-node TPS table")
print()
for node in nodes_order:
    print(f"### {node}")
    hdr = "| mode | " + " | ".join(f"t={t}" for t in terminals) + " | peak |"
    sep = "|---|" + "|".join(["---:"] * len(terminals)) + "|---:|"
    print(hdr)
    print(sep)
    for mode in modes:
        tps_row = [med(data[node][mode].get(t, [])) for t in terminals]
        peak = max(tps_row) if tps_row else 0
        cells = " | ".join(f"**{v}**" if v == peak else str(v) for v in tps_row)
        print(f"| {mode_label[mode]} | {cells} | **{peak}** |")
    print()

# --- Peak TPS summary ---
print("## Peak TPS Summary")
print()
if not no_kafka:
    hdr = "| node | direct peak | @t | kafka peak | @t | kafka/direct |"
    sep = "|---|---:|---:|---:|---:|---:|"
    print(hdr)
    print(sep)
    for node in nodes_order:
        d_by_t = {t: med(data[node]['direct'].get(t, [])) for t in terminals}
        k_by_t = {t: med(data[node]['kafka_majority'].get(t, [])) for t in terminals}
        d_peak = max(d_by_t.values()) if d_by_t else 0
        k_peak = max(k_by_t.values()) if k_by_t else 0
        d_t = max(d_by_t, key=d_by_t.get) if d_by_t else '-'
        k_t = max(k_by_t, key=k_by_t.get) if k_by_t else '-'
        ratio = f"{k_peak/d_peak*100:.1f}%" if d_peak else "n/a"
        print(f"| {node} | **{d_peak}** | {d_t} | **{k_peak}** | {k_t} | {ratio} |")
else:
    hdr = "| node | direct peak | @t |"
    sep = "|---|---:|---:|"
    print(hdr)
    print(sep)
    for node in nodes_order:
        d_by_t = {t: med(data[node]['direct'].get(t, [])) for t in terminals}
        d_peak = max(d_by_t.values()) if d_by_t else 0
        d_t = max(d_by_t, key=d_by_t.get) if d_by_t else '-'
        print(f"| {node} | **{d_peak}** | {d_t} |")

print()

# --- Kafka latency breakdown (avg across runs) ---
if not no_kafka:
    print("## Kafka latency breakdown (median across runs, peak-terminal config)")
    print()
    print("| node | terminals | total_ms | submit_ms | kafka_wait_ms | kafka_wait% |")
    print("|---|---:|---:|---:|---:|---:|")

    lat_data = defaultdict(lambda: defaultdict(lambda: {'total': [], 'submit': [], 'kafka': []}))
    for r in rows:
        if r['mode'] == 'kafka_majority':
            lat_data[r['node']][int(r['terminals'])]['total'].append(int(r['total_ms']))
            lat_data[r['node']][int(r['terminals'])]['submit'].append(int(r['submit_ms']))
            lat_data[r['node']][int(r['terminals'])]['kafka'].append(int(r['kafka_wait_ms']))

    for node in nodes_order:
        k_by_t = {t: med(data[node]['kafka_majority'].get(t, [])) for t in terminals}
        best_t = max(k_by_t, key=k_by_t.get) if k_by_t else terminals[0]
        d = lat_data[node][best_t]
        if not d['total']:
            continue
        total = int(statistics.median(d['total']))
        submit = int(statistics.median(d['submit']))
        kafka = int(statistics.median(d['kafka']))
        pct = f"{kafka/total*100:.0f}%" if total else "n/a"
        print(f"| {node} | {best_t} | {total} | {submit} | {kafka} | {pct} |")

PYEOF

echo ""
log "=== Sweep complete ==="
log "  Logs + CSV: $LOG_DIR"
[[ "$ALL_OK" -eq 0 ]] && log "WARNING: one or more nodes had errors — check logs above"
