#!/usr/bin/env bash
# run_4node_raft_cluster.sh — Bootstrap and test the full 4-node AriaBC distributed cluster.
#
# Topology (from plan.txt):
#   Node 1 (RAFT ID 1): admin123   10.129.148.236  neel  [Kafka host]  Ubuntu 24.04
#   Node 2 (RAFT ID 2): user4      10.129.27.54    neel               Ubuntu 22.04
#   Node 3 (RAFT ID 3): new-node   10.129.148.179  neel  [password]   Ubuntu 22.04
#   Node 4 (RAFT ID 4): utkarsh    10.129.148.248  neel               Ubuntu 24.04
#   Gateway            : ASUS laptop (this machine, local)
#   Kafka broker       : 10.129.148.236:9092
#
# IMPORTANT KNOWN CONSTRAINTS (confirmed 2026-04-24):
#   - Ubuntu 22.04 nodes (user4, new-node) CANNOT run ASUS-built binary (GLIBC 2.38 required).
#     They use ~/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server built locally.
#     librdkafka built from source at ~/Desktop/rdkafka_local (GLIBC 2.35 compatible).
#   - utkarsh port 8000 is taken by system HP printer snap service.
#     Node 4 uses clientPort=8001 instead.
#   - Use `fuser -k 9000/tcp` to kill servers — pkill -f/-x self-kills the SSH session.
#   - By default postgres bcdb_worker_count follows --pool-size. For profiling,
#     --bcdb-decouple-workers can keep 256-tx blocks while using fewer worker queues.
#   - utkarsh system clock is stuck in March 2026 (hardware issue) — functionally OK.
#
# Phases:
#   0. Cleanup — kill stale ariabc_pg_server via fuser (avoids pkill self-kill bug)
#   1. Sync    — push source files + build on Ubuntu 22.04 nodes if binary missing
#   2. Kafka   — ensure KRaft Kafka broker running on admin123
#   3. Postgres — verify BCDB postgres on :5438 on all 4 nodes
#   4. Servers  — start ariabc_pg_server on each node (background nohup)
#   5. Wait    — poll until Raft leader is elected (all 4 nodes respond)
#   6. Test    — run test workload through gateway (det mode, direct or kafka_majority;
#                --ordering-mode kafka-only bypasses Raft and broadcasts ordered
#                requests to all replicas while still using Kafka majority)
#   7. Results — print TPS, check for divergence, collect logs
#   8. Verify  — submit a barrier marker and compare Merkle roots/counts across nodes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Cluster topology
# ---------------------------------------------------------------------------
declare -a NODE_IDS=(1 2 3 4)
declare -a NODE_IPS=(10.129.148.236 10.129.27.54 10.129.148.179 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 new-node utkarsh)
declare -a NODE_USERS=(neel neel neel neel)
# Ubuntu 22.04 nodes need locally-built binary (GLIBC 2.35, rdkafka from ~/Desktop/rdkafka_local)
declare -a NODE_IS_U22=(0 1 1 0)
# utkarsh port 8000 is taken by HP printer snap; use 8001 instead
declare -a NODE_CLIENT_PORTS=(8000 8000 8000 8001)

NODE_PASS_3="${ARIABC_PASS_NEEL_10_129_148_179:-sunil1165}"  # new-node password

KAFKA_HOST="10.129.148.236"
KAFKA_PORT=9092
KAFKA_RESULT_TOPIC="ariabc_results"
KAFKA_HOME_REMOTE="/home/neel/Desktop/kafka_2.13-3.7.0"
KAFKA_BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"

RAFT_PORT=9000
DB_PORT=5438
DB_USER=postgres
DB_NAME=postgres
DB_CONN_POOL_SIZE="${DB_CONN_POOL_SIZE:-256}" # Gateway dbConnPoolSize and bcdb_init block size
BCDB_WORKER_COUNT="${BCDB_WORKER_COUNT:-}"    # Defaults to DB_CONN_POOL_SIZE after args are parsed

REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"
# Binary path for Ubuntu 24.04 nodes (admin123, utkarsh): use the synced
# ASUS/local build from the remote repo. This matches the last known good
# 4-node Kafka-majority run (nodes 1/4 used ariabc_cluster/ariabc_pg/build).
REMOTE_BIN_U24="/home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server"
# Binary path for Ubuntu 22.04 nodes (user4, new-node): built locally with rdkafka from Desktop
REMOTE_BIN_U22="/home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server"
# Static cmake for Ubuntu 22.04 nodes (no system cmake 3.16+) — stays in /tmp (only needed at build time)
REMOTE_CMAKE_U22="/tmp/cmake-3.28.3-linux-x86_64/bin/cmake"
REMOTE_CMAKE_TARBALL_U22="/tmp/cmake-3.28.3-linux-x86_64.tar.gz"
REMOTE_CMAKE_URL_U22="https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.tar.gz"
# OpenSSL headers pushed from ASUS for Ubuntu 22.04 build — stays in /tmp (build-time only)
REMOTE_OPENSSL_INCLUDE_U22="/tmp/openssl_include"

LOCAL_BIN="$REPO_ROOT/ariabc_pg/build/bin"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10)

LOG_DIR="$REPO_ROOT/scripts/bench_full_results/cluster4_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------
SKIP_SYNC="${SKIP_SYNC:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_KAFKA="${SKIP_KAFKA:-0}"
SKIP_CLEANUP="${SKIP_CLEANUP:-0}"
SKIP_RDKAFKA_SETUP="${SKIP_RDKAFKA_SETUP:-0}"
SKIP_RESTORE="${SKIP_RESTORE:-0}"
SKIP_POST_VERIFY="${SKIP_POST_VERIFY:-0}"
FORCE_PG_RESTART="${FORCE_PG_RESTART:-1}"
NO_KAFKA="${NO_KAFKA:-0}"           # set to 1 to skip kafka and run direct-only test
ORDERING_MODE="${ORDERING_MODE:-${CLUSTER_ORDERING_MODE:-raft-kafka}}" # raft-kafka|kafka-only
TEST_QUERIES="${TEST_QUERIES:-50}"  # number of test transactions
WORKLOAD_FILE="${WORKLOAD_FILE:-$REPO_ROOT/scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
RESTORE_SQL="${RESTORE_SQL:-$REPO_ROOT/scripts/restore_usertable_small.sql}"
VERIFY_TABLE="${VERIFY_TABLE:-usertable_small}"
VERIFY_MARKER_KEY="${VERIFY_MARKER_KEY:-99999999}"
DET_START_SEQ="${DET_START_SEQ:-1}"
REQ_ID_OFFSET="${REQ_ID_OFFSET:-1}"
DET_WINDOW="${DET_WINDOW:-4096}"
DET_BATCH_SIZE="${DET_BATCH_SIZE:-256}"
NUM_TERMINALS="${NUM_TERMINALS:-1}"
CONN_FANOUT="${CONN_FANOUT:-1}"
SUBMIT_MODE="${SUBMIT_MODE:-event}"
DET_SUBMIT_PIPELINE="${DET_SUBMIT_PIPELINE:-1}"
DET_PIPELINE_DEPTH="${DET_PIPELINE_DEPTH:-0}"
PG_EXEC_MODE="${PG_EXEC_MODE:-event}"
DET_BLOCK_PARALLEL="${DET_BLOCK_PARALLEL:-1}"  # parallel PG conns per det block (1=legacy, 4-8=Lever1)
DET_BLOCK_PIPELINE="${DET_BLOCK_PIPELINE:-1}"  # logical BCDB blocks per backend submit call
DET_BLOCK_MAX="${DET_BLOCK_MAX:-2048}"         # max txs per backend deterministic block submit
DET_PARTIAL_BLOCK_MAX_WAIT_US="${DET_PARTIAL_BLOCK_MAX_WAIT_US:-0}"  # low-latency partial deterministic blocks; 0=dispatch immediately
BCDB_BLOCK_PROFILE="${BCDB_BLOCK_PROFILE:-0}"  # postgres-side bcdb_block_submit_results phase logging
BCDB_BLOCK_WAIT_WATERMARK="${BCDB_BLOCK_WAIT_WATERMARK:-0}"  # 1=wait on block commit watermark instead of scanning every slot
BCDB_PHASE_TRACE_ON="${BCDB_PHASE_TRACE_ON:-0}"  # postgres-side per-worker CSV phase traces
BCDB_POLL_MAX_US="${BCDB_POLL_MAX_US:-8}"      # last known good 4-node run used 8us
BCDB_SERIAL_GATE_MODE="${BCDB_SERIAL_GATE_MODE:-0}"  # 0=poll, 1=condvar published-max wakeups
BCDB_SERIAL_GATE_SOURCE="${BCDB_SERIAL_GATE_SOURCE:-0}"  # 0=published-max handoff, 1=last-committed predecessor
BCDB_DT_PARSE_BARRIER="${BCDB_DT_PARSE_BARRIER:-1}"  # 1 enables pre-gate parse barrier when block_txs <= workers
BCDB_BLOCK_ENQUEUE_YIELD_EVERY="${BCDB_BLOCK_ENQUEUE_YIELD_EVERY:-0}"  # 0=off; tiny yield every N block enqueues
BCDB_DECOUPLE_WORKERS="${BCDB_DECOUPLE_WORKERS:-0}"  # 1=use bcdb_worker_count queues independent of bcdb_init block size
BCDB_DT_CONFLICT_TRACKING="${BCDB_DT_CONFLICT_TRACKING:-1}"  # 0 disables DT rs/ws conflict table checks for benchmark A/B
BCDB_DT_LIGHT_SNAPSHOT="${BCDB_DT_LIGHT_SNAPSHOT:-0}"  # 1=READ COMMITTED/no SSI sxact when dt conflict tracking is off
BCDB_DT_SKIP_READONLY_GATE="${BCDB_DT_SKIP_READONLY_GATE:-0}"  # 1=skip no-write txs in the DT publish gate
BCDB_DT_COMPLETION_ONLY_SKIP_READS="${BCDB_DT_COMPLETION_ONLY_SKIP_READS:-0}"  # 1=completion-only block mode bypasses SELECT executor work
BCDB_DT_HASHTAB_SWITCH_THRESHOLD="${BCDB_DT_HASHTAB_SWITCH_THRESHOLD:-1500}"  # DT write-set shard rotation threshold
BCDB_DET_QUEUE_HIGH_WM="${BCDB_DET_QUEUE_HIGH_WM:-0}"  # >0 overrides deterministic server admission high watermark
BCDB_DET_QUEUE_LOW_WM="${BCDB_DET_QUEUE_LOW_WM:-0}"    # >0 overrides deterministic server admission low watermark
BCDB_FLOW_DEBUG="${BCDB_FLOW_DEBUG:-0}"      # 1=emit targeted worker/apply flow logs on cluster replicas
ARIABC_FULL_RESULT_REPLICA_LIMIT="${ARIABC_FULL_RESULT_REPLICA_LIMIT:-0}"  # 0=all replicas include full SQL results in Kafka
ARIABC_PREFERRED_LEADER_ID="${ARIABC_PREFERRED_LEADER_ID:-0}"  # 0=Raft default election priority
RESULT_RING_CAPACITY="${RESULT_RING_CAPACITY:-2048}"
BCDB_OVERWRITE_PROTECTION="${BCDB_OVERWRITE_PROTECTION:-0}"  # 0=off 1=Option-A 2=Option-B
COLLECT_FINAL_SERVER_PROFILE="${COLLECT_FINAL_SERVER_PROFILE:-1}"

usage() {
  cat <<'EOF'
Usage: run_4node_raft_cluster.sh [options]

Options:
  --skip-sync      Skip source sync to remote nodes
  --skip-build     Skip binary build on Ubuntu 22.04 nodes (assume already built)
  --skip-kafka     Skip Kafka setup (assume already running)
  --skip-cleanup   Skip killing stale processes
  --skip-rdkafka-setup
                  Skip building librdkafka on all nodes (assumes already done)
  --skip-restore   Skip restoring the verification table before cluster start
  --skip-post-verify
                  Skip post-workload marker + Merkle root comparison
  --skip-pg-restart
                  Do not restart PostgreSQL before restore (default restarts)
  --no-kafka       Use direct completion (no Kafka majority wait)
  --ordering-mode M
                  Cluster ordering mode:
                    raft-kafka  = normal Raft ordering + Kafka-majority completion
                    kafka-only  = bypass Raft; gateway broadcasts preordered requests
                                  to all replicas and still waits for Kafka majority
  --test-queries N Number of statements in the synthetic fallback workload (only used if --workload FILE is missing; default 50)
  --workload FILE  Workload SQL file (default: scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt)
  --restore-sql FILE
                  SQL used to restore table state before the run
  --verify-table T Table used for post-run root comparison (default: usertable_small)
  --det-start-seq N
                  First 8-digit DET sequence sent to BCDB (default: 1)
  --req-id-offset N
                  First gateway request suffix (default: 1)
  --det-window N   Gateway deterministic in-flight window (default: 4096)
  --det-batch-size N
                  Gateway deterministic Raft batch size (default: 256)
  --num-terminals N
                  Gateway terminal count (default: 1)
  --conn-fanout N Gateway submit sockets per logical node in event submit mode
                  (default: 1)
  --det-pipeline-depth N
                  Per-terminal deterministic in-flight depth; 0 auto-splits
                  detWindow across terminals (default: 0)
  --submit-mode M  Gateway submit mode: blocking|event (default: event)
  --pg-exec-mode M Server pgExecMode: threaded|event (default: event)
  --det-block-parallel N
                  Parallel PG connections per det block on each server (default: 1,
                  set to 4-8 to enable Lever1 parallel block execution)
  --det-block-pipeline N
                  Logical BCDB blocks per backend submit call (default: 1)
  --det-block-max N
                  Max transactions per backend deterministic block submit (default: 2048)
  --det-partial-block-max-wait-us N
                  Max microseconds to wait for a partial deterministic block
                  before dispatch; 0 dispatches immediately (default: 0)
  --bcdb-block-profile N
                  Enable PROFILE_BCDB_BLOCK lines inside PostgreSQL backends (default: 0)
  --bcdb-block-wait-watermark N
                  Wait for the block commit watermark before result submit instead of scanning
                  every result slot: 0|1 (default: 0)
  --bcdb-phase-trace N
                  Enable BCDB_PHASE_TRACE CSVs inside PostgreSQL worker backends (default: 0)
  --bcdb-poll-max-us N
                  Max poll sleep for BCDB ordered waits, 1..64 usec (default: 8)
  --bcdb-serial-gate-mode N
                  Ordered DT gate wait mode: 0=poll, 1=condvar wake (default: 0)
  --bcdb-serial-gate-source N
                  Ordered DT gate source: 0=published-max, 1=last-committed (default: 0)
  --bcdb-dt-parse-barrier N
                  Enable pre-gate deterministic block parse barrier when safe: 0|1 (default: 1)
  --bcdb-block-enqueue-yield-every N
                  Tiny pg_usleep(1) after every N queued block txs, 0 disables (default: 0)
  --bcdb-worker-count N
                  PostgreSQL bcdb_worker_count / BCDB worker queues (default: --pool-size)
  --bcdb-decouple-workers N
                  Use bcdb_worker_count queues independent of bcdb_init block size: 0|1 (default: 0)
  --bcdb-dt-conflict-tracking N
                  Set bcdb_dt_conflict_tracking: 0|1 (default: 1)
  --bcdb-dt-light-snapshot N
                  In DT with bcdb_dt_conflict_tracking=off, skip SSI sxact and use READ COMMITTED snapshot: 0|1 (default: 0)
  --bcdb-dt-skip-readonly-gate N
                  Let no-write DT transactions stop blocking later publish-gate entrants: 0|1 (default: 0)
  --bcdb-dt-completion-only-skip-reads N
                  In completion-only block mode, bypass SELECT executor work: 0|1 (default: 0)
  --bcdb-dt-hashtab-switch-threshold N
                  Set bcdb_dt_hashtab_switch_threshold on every PostgreSQL node
                  before restore/start (default: 1500)
  --bcdb-det-queue-high-wm N
                  Override deterministic server admission high watermark; 0 uses
                  the server default derived from dbConnPoolSize (default: 0)
  --bcdb-det-queue-low-wm N
                  Override deterministic server admission low watermark; 0 uses
                  the server default derived from dbConnPoolSize (default: 0)
  --full-result-replica-limit N
                  Emit full Kafka results only from replica ids <= N, hashes from all replicas; 0=all replicas (default: 0)
  --preferred-leader-id N
                  Set higher Raft election priority for server id N; 0=default election (default: 0)
  --bcdb-overwrite-protection N
                  Result-ring overwrite protection: 0=off, 1=Option-A (per-slot), 2=Option-B (watermark) (default: 0)
  --pool-size N    Gateway dbConnPoolSize and bcdb_init deterministic block size (default: 256)
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-sync)    SKIP_SYNC=1; shift ;;
    --skip-build)   SKIP_BUILD=1; shift ;;
    --skip-kafka)   SKIP_KAFKA=1; shift ;;
    --skip-cleanup) SKIP_CLEANUP=1; shift ;;
    --skip-rdkafka-setup) SKIP_RDKAFKA_SETUP=1; shift ;;
    --skip-restore) SKIP_RESTORE=1; shift ;;
    --skip-post-verify) SKIP_POST_VERIFY=1; shift ;;
    --skip-pg-restart) FORCE_PG_RESTART=0; shift ;;
    --no-kafka)     NO_KAFKA=1; shift ;;
    --ordering-mode) ORDERING_MODE="${2:-raft-kafka}"; shift 2 ;;
    --test-queries) TEST_QUERIES="${2:-50}"; shift 2 ;;
    --workload)     WORKLOAD_FILE="${2:-}"; shift 2 ;;
    --restore-sql)  RESTORE_SQL="${2:-}"; shift 2 ;;
    --verify-table) VERIFY_TABLE="${2:-}"; shift 2 ;;
    --det-start-seq) DET_START_SEQ="${2:-1}"; shift 2 ;;
    --req-id-offset) REQ_ID_OFFSET="${2:-1}"; shift 2 ;;
    --det-window)   DET_WINDOW="${2:-4096}"; shift 2 ;;
    --det-batch-size) DET_BATCH_SIZE="${2:-256}"; shift 2 ;;
    --num-terminals) NUM_TERMINALS="${2:-1}"; shift 2 ;;
    --conn-fanout) CONN_FANOUT="${2:-1}"; shift 2 ;;
    --det-pipeline-depth) DET_PIPELINE_DEPTH="${2:-0}"; shift 2 ;;
    --submit-mode)  SUBMIT_MODE="${2:-event}"; shift 2 ;;
    --pg-exec-mode) PG_EXEC_MODE="${2:-event}"; shift 2 ;;
    --det-block-parallel) DET_BLOCK_PARALLEL="${2:-1}"; shift 2 ;;
    --det-block-pipeline) DET_BLOCK_PIPELINE="${2:-1}"; shift 2 ;;
    --det-block-max) DET_BLOCK_MAX="${2:-2048}"; shift 2 ;;
    --det-partial-block-max-wait-us) DET_PARTIAL_BLOCK_MAX_WAIT_US="${2:-0}"; shift 2 ;;
    --bcdb-block-profile) BCDB_BLOCK_PROFILE="${2:-0}"; shift 2 ;;
    --bcdb-block-wait-watermark) BCDB_BLOCK_WAIT_WATERMARK="${2:-0}"; shift 2 ;;
    --bcdb-phase-trace) BCDB_PHASE_TRACE_ON="${2:-0}"; shift 2 ;;
    --bcdb-poll-max-us) BCDB_POLL_MAX_US="${2:-8}"; shift 2 ;;
    --bcdb-serial-gate-mode) BCDB_SERIAL_GATE_MODE="${2:-0}"; shift 2 ;;
    --bcdb-serial-gate-source) BCDB_SERIAL_GATE_SOURCE="${2:-0}"; shift 2 ;;
    --bcdb-dt-parse-barrier) BCDB_DT_PARSE_BARRIER="${2:-0}"; shift 2 ;;
    --bcdb-block-enqueue-yield-every) BCDB_BLOCK_ENQUEUE_YIELD_EVERY="${2:-0}"; shift 2 ;;
    --bcdb-worker-count) BCDB_WORKER_COUNT="${2:-}"; shift 2 ;;
    --bcdb-decouple-workers) BCDB_DECOUPLE_WORKERS="${2:-0}"; shift 2 ;;
    --bcdb-dt-conflict-tracking) BCDB_DT_CONFLICT_TRACKING="${2:-1}"; shift 2 ;;
    --bcdb-dt-light-snapshot) BCDB_DT_LIGHT_SNAPSHOT="${2:-0}"; shift 2 ;;
    --bcdb-dt-skip-readonly-gate) BCDB_DT_SKIP_READONLY_GATE="${2:-0}"; shift 2 ;;
    --bcdb-dt-completion-only-skip-reads) BCDB_DT_COMPLETION_ONLY_SKIP_READS="${2:-0}"; shift 2 ;;
    --bcdb-dt-hashtab-switch-threshold) BCDB_DT_HASHTAB_SWITCH_THRESHOLD="${2:-1500}"; shift 2 ;;
    --bcdb-det-queue-high-wm) BCDB_DET_QUEUE_HIGH_WM="${2:-0}"; shift 2 ;;
    --bcdb-det-queue-low-wm) BCDB_DET_QUEUE_LOW_WM="${2:-0}"; shift 2 ;;
    --full-result-replica-limit) ARIABC_FULL_RESULT_REPLICA_LIMIT="${2:-0}"; shift 2 ;;
    --preferred-leader-id) ARIABC_PREFERRED_LEADER_ID="${2:-0}"; shift 2 ;;
    --bcdb-overwrite-protection) BCDB_OVERWRITE_PROTECTION="${2:-0}"; shift 2 ;;
    --pool-size)    DB_CONN_POOL_SIZE="${2:-256}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$DET_START_SEQ" -lt 1 || "$REQ_ID_OFFSET" -lt 1 ]]; then
  echo "ERROR: --det-start-seq and --req-id-offset must be >= 1 for deterministic cluster runs" >&2
  exit 2
fi
case "$ORDERING_MODE" in
  raft|raft-kafka|raft_kafka)
    ORDERING_MODE="raft-kafka"
    ;;
  kafka|kafka-only|kafka_only|preordered-direct-broadcast|preordered_direct_broadcast)
    ORDERING_MODE="kafka-only"
    ;;
  *)
    echo "ERROR: --ordering-mode must be raft-kafka or kafka-only (got $ORDERING_MODE)" >&2
    exit 2
    ;;
esac
if [[ "$NO_KAFKA" -eq 1 && "$ORDERING_MODE" == "kafka-only" ]]; then
  echo "ERROR: kafka-only ordering requires Kafka majority; do not combine --ordering-mode kafka-only with --no-kafka" >&2
  exit 2
fi
BYPASS_RAFT=0
GATEWAY_BROADCAST_TO_ALL=0
ORDERING_PATH="raft"
CLUSTER_SERIES="cluster_raft_kafka"
if [[ "$ORDERING_MODE" == "kafka-only" ]]; then
  BYPASS_RAFT=1
  GATEWAY_BROADCAST_TO_ALL=1
  ORDERING_PATH="preordered_direct_broadcast"
  CLUSTER_SERIES="cluster_kafka"
fi
if [[ "$DET_BATCH_SIZE" -lt 1 ]]; then
  echo "ERROR: --det-batch-size must be >= 1" >&2
  exit 2
fi
if [[ "$NUM_TERMINALS" -lt 1 || "$DET_PIPELINE_DEPTH" -lt 0 ]]; then
  echo "ERROR: --num-terminals must be >= 1 and --det-pipeline-depth must be >= 0" >&2
  exit 2
fi
if [[ "$CONN_FANOUT" -lt 1 ]]; then
  echo "ERROR: --conn-fanout must be >= 1" >&2
  exit 2
fi
if [[ -z "$BCDB_WORKER_COUNT" ]]; then
  BCDB_WORKER_COUNT="$DB_CONN_POOL_SIZE"
fi
if [[ "$BCDB_WORKER_COUNT" -lt 1 || "$BCDB_WORKER_COUNT" -gt 1024 ]]; then
  echo "ERROR: --bcdb-worker-count must be between 1 and 1024" >&2
  exit 2
fi
if [[ "$BCDB_DECOUPLE_WORKERS" != "0" && "$BCDB_DECOUPLE_WORKERS" != "1" ]]; then
  echo "ERROR: --bcdb-decouple-workers must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_CONFLICT_TRACKING" != "0" && "$BCDB_DT_CONFLICT_TRACKING" != "1" ]]; then
  echo "ERROR: --bcdb-dt-conflict-tracking must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_LIGHT_SNAPSHOT" != "0" && "$BCDB_DT_LIGHT_SNAPSHOT" != "1" ]]; then
  echo "ERROR: --bcdb-dt-light-snapshot must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_SKIP_READONLY_GATE" != "0" && "$BCDB_DT_SKIP_READONLY_GATE" != "1" ]]; then
  echo "ERROR: --bcdb-dt-skip-readonly-gate must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_COMPLETION_ONLY_SKIP_READS" != "0" && "$BCDB_DT_COMPLETION_ONLY_SKIP_READS" != "1" ]]; then
  echo "ERROR: --bcdb-dt-completion-only-skip-reads must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_HASHTAB_SWITCH_THRESHOLD" -lt 1 ]]; then
  echo "ERROR: --bcdb-dt-hashtab-switch-threshold must be >= 1" >&2
  exit 2
fi
if [[ "$BCDB_DET_QUEUE_HIGH_WM" -lt 0 || "$BCDB_DET_QUEUE_LOW_WM" -lt 0 ]]; then
  echo "ERROR: --bcdb-det-queue-high-wm and --bcdb-det-queue-low-wm must be >= 0" >&2
  exit 2
fi
min_hashtab_threshold=$(( BCDB_WORKER_COUNT * 2 - 1 ))
if [[ "$BCDB_DT_HASHTAB_SWITCH_THRESHOLD" -lt "$min_hashtab_threshold" ]]; then
  echo "ERROR: --bcdb-dt-hashtab-switch-threshold must be >= $min_hashtab_threshold for bcdb_worker_count=$BCDB_WORKER_COUNT" >&2
  exit 2
fi
if [[ "$BCDB_BLOCK_WAIT_WATERMARK" != "0" && "$BCDB_BLOCK_WAIT_WATERMARK" != "1" ]]; then
  echo "ERROR: --bcdb-block-wait-watermark must be 0 or 1" >&2
  exit 2
fi
if [[ "$ARIABC_FULL_RESULT_REPLICA_LIMIT" -lt 0 || "$ARIABC_FULL_RESULT_REPLICA_LIMIT" -gt 4 ]]; then
  echo "ERROR: --full-result-replica-limit must be between 0 and 4" >&2
  exit 2
fi
if [[ "$ARIABC_PREFERRED_LEADER_ID" -lt 0 || "$ARIABC_PREFERRED_LEADER_ID" -gt 4 ]]; then
  echo "ERROR: --preferred-leader-id must be between 0 and 4" >&2
  exit 2
fi
if [[ "$BCDB_DECOUPLE_WORKERS" == "0" && "$BCDB_WORKER_COUNT" != "$DB_CONN_POOL_SIZE" ]]; then
  echo "ERROR: --bcdb-worker-count differs from --pool-size; pass --bcdb-decouple-workers 1 to test decoupled worker queues" >&2
  exit 2
fi
if [[ "$BCDB_SERIAL_GATE_MODE" != "0" && "$BCDB_SERIAL_GATE_MODE" != "1" ]]; then
  echo "ERROR: --bcdb-serial-gate-mode must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_SERIAL_GATE_SOURCE" != "0" && "$BCDB_SERIAL_GATE_SOURCE" != "1" ]]; then
  echo "ERROR: --bcdb-serial-gate-source must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_DT_PARSE_BARRIER" != "0" && "$BCDB_DT_PARSE_BARRIER" != "1" ]]; then
  echo "ERROR: --bcdb-dt-parse-barrier must be 0 or 1" >&2
  exit 2
fi
if [[ "$BCDB_BLOCK_ENQUEUE_YIELD_EVERY" -lt 0 || "$BCDB_BLOCK_ENQUEUE_YIELD_EVERY" -gt 256 ]]; then
  echo "ERROR: --bcdb-block-enqueue-yield-every must be between 0 and 256" >&2
  exit 2
fi
if [[ "$DET_BLOCK_MAX" -lt 1 || "$DET_BLOCK_MAX" -gt 8192 ]]; then
  echo "ERROR: --det-block-max must be between 1 and 8192" >&2
  exit 2
fi
if [[ "$PG_EXEC_MODE" != "threaded" && "$PG_EXEC_MODE" != "event" ]]; then
  echo "ERROR: --pg-exec-mode must be threaded or event" >&2
  exit 2
fi

# Auto-detect expected post-workload values from known workload files.
# These are checked PRE-MARKER in Phase 8 to confirm the workload produced the correct result.
# Can be overridden via env: EXPECTED_ROWS=N EXPECTED_ROOT=hash
if [[ -z "${EXPECTED_ROWS:-}" ]]; then
  case "$WORKLOAD_FILE" in
    *ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq*)
      EXPECTED_ROWS=12498
      EXPECTED_ROOT="125a1bef020ef86d52c7f0038304d2ffde5e298dee89f71cd84703a19147d8dd"
      ;;
    *ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k*)
      EXPECTED_ROWS=12595
      EXPECTED_ROOT="e30d7e8fdfd40c0abbacf7f7a378bc73179b70e72651bc1d693adbbba045acdc"
      ;;
    *)
      EXPECTED_ROWS=""
      EXPECTED_ROOT=""
      ;;
  esac
fi

# ---------------------------------------------------------------------------
# SSH helpers (handles new-node password auth transparently)
# ---------------------------------------------------------------------------
node_ssh() {
  local idx="$1"; shift
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  local cmd=()
  if [[ "$idx" -eq 2 ]]; then
    cmd=(sshpass -p "$NODE_PASS_3" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$user@$ip" "$@")
  else
    cmd=(ssh -i "$SSH_KEY" "${SSH_OPTS[@]}" "$user@$ip" "$@")
  fi
  if [[ -n "${NODE_SSH_COMMAND_TIMEOUT:-}" && "${NODE_SSH_COMMAND_TIMEOUT:-0}" != "0" ]]; then
    timeout "$NODE_SSH_COMMAND_TIMEOUT" "${cmd[@]}"
  else
    "${cmd[@]}"
  fi
}

node_rsync_to() {
  local idx="$1"; local src="$2"; local dst="$3"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  if [[ "$idx" -eq 2 ]]; then
    sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$src" "$user@$ip:$dst"
  else
    rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$src" "$user@$ip:$dst"
  fi
}

node_rsync_from() {
  local idx="$1"; local src="$2"; local dst="$3"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  if [[ "$idx" -eq 2 ]]; then
    sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$user@$ip:$src" "$dst"
  else
    rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$user@$ip:$src" "$dst"
  fi
}

collect_cluster_logs() {
  local label="${1:-Collecting server logs from all nodes...}"
  local log_rsync_timeout="${LOG_RSYNC_TIMEOUT:-20}"
  log "$label"
  for idx in "${!NODE_IDS[@]}"; do
    id="${NODE_IDS[$idx]}"
    name="${NODE_NAMES[$idx]}"
    ip="${NODE_IPS[$idx]}"
    user="${NODE_USERS[$idx]}"
    REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"
    REMOTE_NURAFT_LOG="/home/neel/ariabc_pg_srv${id}.log"
    REMOTE_PG_LOG="$REMOTE_REPO_ROOT/server.log"
    if [[ "$idx" -eq 2 ]]; then
      timeout "$log_rsync_timeout" sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_SRV_LOG" "$LOG_DIR/server_node${id}_${name}.log" 2>/dev/null || true
      timeout "$log_rsync_timeout" sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_NURAFT_LOG" "$LOG_DIR/nuraft_node${id}_${name}.log" 2>/dev/null || true
      timeout "$log_rsync_timeout" sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_PG_LOG" "$LOG_DIR/postgres_node${id}_${name}.log" 2>/dev/null || true
      if [[ "$BCDB_PHASE_TRACE_ON" != "0" ]]; then
        timeout "$log_rsync_timeout" sshpass -p "$NODE_PASS_3" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
          "$user@$ip:$REMOTE_REPO_ROOT/.bench_tmp/bcdb_phase_trace_node${id}.*" \
          "$LOG_DIR/" 2>/dev/null || true
      fi
    else
      timeout "$log_rsync_timeout" rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_SRV_LOG" "$LOG_DIR/server_node${id}_${name}.log" 2>/dev/null || true
      timeout "$log_rsync_timeout" rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_NURAFT_LOG" "$LOG_DIR/nuraft_node${id}_${name}.log" 2>/dev/null || true
      timeout "$log_rsync_timeout" rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_PG_LOG" "$LOG_DIR/postgres_node${id}_${name}.log" 2>/dev/null || true
      if [[ "$BCDB_PHASE_TRACE_ON" != "0" ]]; then
        timeout "$log_rsync_timeout" rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
          "$user@$ip:$REMOTE_REPO_ROOT/.bench_tmp/bcdb_phase_trace_node${id}.*" \
          "$LOG_DIR/" 2>/dev/null || true
      fi
    fi
  done
}

node_rsync_repo() {
  local idx="$1"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  if [[ "$idx" -eq 2 ]]; then
    sshpass -p "$NODE_PASS_3" rsync -az --delete \
      --exclude='.git' \
      --exclude='.venv' \
      --exclude='.bench_tmp' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      --exclude='conftest*' \
      --exclude='scripts/bench_full_results' \
      --exclude='scripts/bench_results' \
      -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$REPO_ROOT/" "$user@$ip:$REMOTE_REPO_ROOT/"
  else
    rsync -az --delete \
      --exclude='.git' \
      --exclude='.venv' \
      --exclude='.bench_tmp' \
      --exclude='__pycache__' \
      --exclude='*.pyc' \
      --exclude='conftest*' \
      --exclude='scripts/bench_full_results' \
      --exclude='scripts/bench_results' \
      -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$REPO_ROOT/" "$user@$ip:$REMOTE_REPO_ROOT/"
  fi
}

node_rsync_install() {
  local idx="$1"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  if [[ "$idx" -eq 2 ]]; then
    sshpass -p "$NODE_PASS_3" rsync -az --delete \
      -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$LOCAL_INSTALL_DIR/" "$user@$ip:$REMOTE_INSTALL_DIR/"
  else
    rsync -az --delete \
      -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$LOCAL_INSTALL_DIR/" "$user@$ip:$REMOTE_INSTALL_DIR/"
  fi
}

log() { echo "[$(date +'%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

collect_final_profiles_before_fail() {
  local reason="${1:-failure}"
  if [[ "$COLLECT_FINAL_SERVER_PROFILE" == "0" ]]; then
    return 0
  fi
  log "  Collecting final server profiles before failing (${reason})"
  for idx in "${!NODE_IDS[@]}"; do
    client_port="${NODE_CLIENT_PORTS[$idx]}"
    NODE_SSH_COMMAND_TIMEOUT="${VERIFY_NODE_SSH_TIMEOUT:-20}" node_ssh "$idx" "
      fuser -k -TERM 9000/tcp 2>/dev/null || true
      fuser -k -TERM ${client_port}/tcp 2>/dev/null || true
    " >/dev/null 2>&1 || true
  done
  sleep 2
  collect_cluster_logs "  Collecting failure-path server logs with PROFILE_SERVER lines..."
}

find_local_cmake_tarball() {
  local candidate
  for candidate in /tmp/cmake-3.28.3-linux-x86_64.tar.gz "$HOME/Desktop/cmake-3.28.3-linux-x86_64.tar.gz"; do
    if [[ -s "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

ensure_u22_cmake() {
  local idx="$1"
  local name="${NODE_NAMES[$idx]}"
  local cmake_tarball

  if node_ssh "$idx" "command -v cmake >/dev/null 2>&1 || command -v cmake3 >/dev/null 2>&1 || test -x '$REMOTE_CMAKE_U22'" 2>/dev/null; then
    return 0
  fi

  log "  Staging portable CMake on $name"
  cmake_tarball="$(find_local_cmake_tarball || true)"
  if [[ -n "$cmake_tarball" ]]; then
    node_rsync_to "$idx" "$cmake_tarball" "$REMOTE_CMAKE_TARBALL_U22"
  fi

  node_ssh "$idx" "
    set -euo pipefail
    if [[ ! -s '$REMOTE_CMAKE_TARBALL_U22' ]]; then
      if command -v wget >/dev/null 2>&1; then
        wget -T 30 -t 2 -q --show-progress -O '$REMOTE_CMAKE_TARBALL_U22' '$REMOTE_CMAKE_URL_U22'
      elif command -v curl >/dev/null 2>&1; then
        curl --connect-timeout 10 --max-time 120 --retry 2 -sSL -o '$REMOTE_CMAKE_TARBALL_U22' '$REMOTE_CMAKE_URL_U22'
      else
        echo 'ERROR: cmake is missing and neither wget nor curl is available to fetch portable CMake' >&2
        exit 1
      fi
    fi
    rm -rf /tmp/cmake-3.28.3-linux-x86_64
    tar -C /tmp -xzf '$REMOTE_CMAKE_TARBALL_U22'
    test -x '$REMOTE_CMAKE_U22'
    '$REMOTE_CMAKE_U22' --version | head -1
  " 2>&1 | sed "s/^/[$name] /" || die "failed to stage portable CMake on $name"
}

build_raft_members() {
  local members=""
  for i in "${!NODE_IDS[@]}"; do
    [[ -n "$members" ]] && members+=","
    members+="${NODE_IDS[$i]}=${NODE_IPS[$i]}:${RAFT_PORT}"
  done
  echo "$members"
}

RAFT_MEMBERS="$(build_raft_members)"

log "Cluster ordering mode: $ORDERING_MODE (ordering_path=$ORDERING_PATH, bypass_raft=$BYPASS_RAFT, gateway_broadcast_to_all=$GATEWAY_BROADCAST_TO_ALL)"
{
  printf 'ordering_mode=%s\n' "$ORDERING_MODE"
  printf 'ordering_path=%s\n' "$ORDERING_PATH"
  printf 'cluster_series=%s\n' "$CLUSTER_SERIES"
  printf 'bypass_raft=%s\n' "$BYPASS_RAFT"
  printf 'gateway_broadcast_to_all=%s\n' "$GATEWAY_BROADCAST_TO_ALL"
  printf 'completion_path=%s\n' "$([[ "$NO_KAFKA" -eq 0 ]] && echo kafka_majority || echo direct)"
  printf 'kafka_bootstrap=%s\n' "$KAFKA_BOOTSTRAP"
  printf 'result_topic=%s\n' "$KAFKA_RESULT_TOPIC"
} > "$LOG_DIR/run_meta.env"

# ---------------------------------------------------------------------------
# Phase 0: Cleanup
# Kill servers by Raft port (9000) rather than by process name — pkill -f/-x
# kills its own SSH session because the binary name appears in bash cmdline.
# fuser -k 9000/tcp avoids this entirely.
# ---------------------------------------------------------------------------
if [[ "$SKIP_CLEANUP" -eq 0 ]]; then
  log "=== Phase 0: Cleanup stale ariabc_pg processes ==="
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    client_port="${NODE_CLIENT_PORTS[$idx]}"
    log "  Killing server on $name (ports 9000, $client_port)"
    node_ssh "$idx" "
      fuser -k 9000/tcp 2>/dev/null || true
      fuser -k ${client_port}/tcp 2>/dev/null || true
      sleep 0.5
    " || true
  done
  sleep 2
  log "  Cleanup done"
fi

# ---------------------------------------------------------------------------
# Phase 0.5: Ensure librdkafka v2.3.0 on all nodes (source-built, no root needed)
# Runs ensure_rdkafka.sh on every node including the local ASUS machine so that
# every binary in the cluster links against the same library regardless of what
# the OS package manager provides (v1.6.1 on U22 apt vs v2.3.0 on U24 apt).
# ---------------------------------------------------------------------------
if [[ "$SKIP_RDKAFKA_SETUP" -eq 0 ]]; then
  log "=== Phase 0.5: Ensure librdkafka v2.3.0 on all nodes ==="

  ENSURE_RDKAFKA_SCRIPT="$SCRIPT_DIR/ensure_rdkafka.sh"
  [[ -f "$ENSURE_RDKAFKA_SCRIPT" ]] || die "ensure_rdkafka.sh not found at $ENSURE_RDKAFKA_SCRIPT"
  chmod +x "$ENSURE_RDKAFKA_SCRIPT"

  # Local machine (gateway binary also uses rdkafka)
  log "  Local machine..."
  "$ENSURE_RDKAFKA_SCRIPT" 2>&1 | sed 's/^/  [local] /' || die "ensure_rdkafka failed on local machine"

  # Locate cmake portable tarball (needed on nodes without a system cmake)
  CMAKE_TARBALL="$(find_local_cmake_tarball || true)"
  if [[ -z "$CMAKE_TARBALL" ]]; then
    log "  cmake-3.28.3 tarball not found locally — will attempt apt-get on remote nodes"
    log "    (pre-download: wget -P /tmp https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.tar.gz)"
  fi
  RDKAFKA_TARBALL="/tmp/librdkafka-v2.3.0.tar.gz"

  # All remote nodes — push cmake + librdkafka tarballs then build in parallel
  declare -a RDKAFKA_PIDS=()
  declare -a RDKAFKA_LOGS=()
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    log "  Pushing ensure_rdkafka.sh to $name..."
    node_rsync_to "$idx" "$ENSURE_RDKAFKA_SCRIPT" "/tmp/ensure_rdkafka.sh" || { log "WARNING: rsync to $name failed"; RDKAFKA_PIDS+=(""); RDKAFKA_LOGS+=(""); continue; }
    # Push cmake tarball so nodes without system cmake can still build
    [[ -s "$CMAKE_TARBALL" ]] && node_rsync_to "$idx" "$CMAKE_TARBALL" "/tmp/cmake-3.28.3-linux-x86_64.tar.gz" 2>/dev/null || true
    # Push librdkafka source tarball if already cached locally (avoids per-node download)
    [[ -s "$RDKAFKA_TARBALL" ]] && node_rsync_to "$idx" "$RDKAFKA_TARBALL" "/tmp/librdkafka-v2.3.0.tar.gz" 2>/dev/null || true
    rdkafka_log="$LOG_DIR/ensure_rdkafka_${name}.log"
    node_ssh "$idx" "
      set -euo pipefail
      if [[ -f /tmp/cmake-3.28.3-linux-x86_64.tar.gz && ! -d /tmp/cmake-3.28.3-linux-x86_64 ]]; then
        tar -C /tmp -xzf /tmp/cmake-3.28.3-linux-x86_64.tar.gz
      fi
      chmod +x /tmp/ensure_rdkafka.sh && /tmp/ensure_rdkafka.sh
    " >"$rdkafka_log" 2>&1 &
    RDKAFKA_PIDS+=("$!")
    RDKAFKA_LOGS+=("$rdkafka_log")
    log "  [$name] building rdkafka in background (pid $!)"
  done
  # Wait for all and report
  RDKAFKA_ALL_OK=1
  for i in "${!RDKAFKA_PIDS[@]}"; do
    pid="${RDKAFKA_PIDS[$i]}"
    [[ -z "$pid" ]] && continue
    name="${NODE_NAMES[$i]}"
    log_file="${RDKAFKA_LOGS[$i]}"
    if wait "$pid"; then
      log "  [$name] rdkafka ready"
    else
      log "  [$name] ensure_rdkafka FAILED — see $log_file"
      tail -10 "$log_file" | sed "s/^/  [$name] /" || true
      RDKAFKA_ALL_OK=0
    fi
  done
  [[ "$RDKAFKA_ALL_OK" -eq 1 ]] || die "ensure_rdkafka failed on one or more nodes — fix above errors then retry with --skip-rdkafka-setup"
  log "  librdkafka v2.3.0 ready on all nodes"
else
  log "=== Phase 0.5: Skipped (--skip-rdkafka-setup) ==="
fi

# ---------------------------------------------------------------------------
# Phase 0.8: Canonicalize local build/install before sync.
# U24 nodes consume the local install and local ariabc_pg/build tree, so the
# benchmark is only reproducible if this machine is rebuilt from the same
# source constants before rsync. This guardrail restored the 5.4k
# Kafka-majority path after nodes 1/4 were using a stale backend.
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  log "=== Phase 0.8: Rebuild local canonical install/binaries ==="
  local_globals="$REPO_ROOT/src/include/bcdb/globals.h"
  [[ -f "$local_globals" ]] || die "missing local globals header: $local_globals"
  current_ring_capacity="$(sed -n -E 's/^#define[[:space:]]+BCDB_RESULT_RING_CAPACITY[[:space:]]+([0-9]+).*/\1/p' "$local_globals" | head -n 1)"
  if [[ "$current_ring_capacity" != "$RESULT_RING_CAPACITY" ]]; then
    sed -i -E "s/^#define[[:space:]]+BCDB_RESULT_RING_CAPACITY[[:space:]]+[0-9]+/#define BCDB_RESULT_RING_CAPACITY $RESULT_RING_CAPACITY/" "$local_globals"
  fi

  log "  Rebuilding local PostgreSQL install at $LOCAL_INSTALL_DIR with BCDB_RESULT_RING_CAPACITY=$RESULT_RING_CAPACITY"
  chmod +x "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh"
  bash "$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh" \
    --repo-root "$REPO_ROOT" \
    --install-dir "$LOCAL_INSTALL_DIR" \
    --force-rebuild \
    --clean-when-rebuild \
    2>&1 | sed 's/^/[local-install] /'

  RDKAFKA_LOCAL="$HOME/Desktop/rdkafka_local"
  LOCAL_KAFKA_CMAKE_OPT="-DKAFKA_OPTIONAL=ON"
  if [[ "$NO_KAFKA" -eq 0 ]]; then
    if [[ -f "$RDKAFKA_LOCAL/lib/librdkafka.so" && -f "$RDKAFKA_LOCAL/include/librdkafka/rdkafka.h" ]]; then
      LOCAL_KAFKA_CMAKE_OPT="-DRDKAFKA_INCLUDE_DIR=$RDKAFKA_LOCAL/include -DRDKAFKA_LIBRARY=$RDKAFKA_LOCAL/lib/librdkafka.so"
    else
      die "Kafka majority requested but local $RDKAFKA_LOCAL is missing; rerun without --skip-rdkafka-setup or install rdkafka_local"
    fi
  fi

  log "  Configuring local ariabc_pg build against $LOCAL_INSTALL_DIR"
  cmake -S "$REPO_ROOT/ariabc_pg" -B "$REPO_ROOT/ariabc_pg/build" \
    -DCMAKE_BUILD_TYPE=Release \
    $LOCAL_KAFKA_CMAKE_OPT \
    -DLIBPQ_INCLUDE_DIR="$LOCAL_INSTALL_DIR/include" \
    -DPOSTGRES_INCLUDE_DIR="$LOCAL_INSTALL_DIR/include/postgresql/server" \
    -DLIBPQ_LIBRARY="$LOCAL_INSTALL_DIR/lib/libpq.so" \
    >/dev/null

  log "  Building local ariabc_pg_gateway and ariabc_pg_server"
  cmake --build "$REPO_ROOT/ariabc_pg/build" --target ariabc_pg_gateway ariabc_pg_server -j"$(nproc)" \
    2>&1 | tail -20
fi

# ---------------------------------------------------------------------------
# Phase 1: Sync source files
# Sync the full working tree so BCDB/PostgreSQL backend changes are not lost.
# Phase 0.8 rebuilds the local U24-consumed install/binaries; Phase 1.5
# rebuilds U22 nodes on-host so all replicas run the same PostgreSQL-side
# constants and Kafka-capable server code.
# ---------------------------------------------------------------------------
if [[ "$SKIP_SYNC" -eq 0 ]]; then
  log "=== Phase 1: Sync source and binaries ==="

  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    is_u22="${NODE_IS_U22[$idx]}"
    log "  Syncing to $name (is_u22=$is_u22)"
    node_ssh "$idx" "mkdir -p '$REMOTE_REPO_ROOT' '$REMOTE_INSTALL_DIR'" || true

    node_rsync_repo "$idx"

    if [[ "$is_u22" -eq 0 ]]; then
      node_rsync_install "$idx"
    fi

    if [[ -f "$WORKLOAD_FILE" ]]; then
      node_rsync_to "$idx" "$WORKLOAD_FILE" "$REMOTE_REPO_ROOT/scripts/cluster_test_workload.sql"
    fi
    if [[ -f "$RESTORE_SQL" ]]; then
      node_rsync_to "$idx" "$RESTORE_SQL" "$REMOTE_REPO_ROOT/scripts/restore_usertable_small.sql"
    fi
  done
  log "  Sync done"
fi

# ---------------------------------------------------------------------------
# Phase 1.5: Rebuild Ubuntu 22.04 nodes on-host.
# Ubuntu 24.04 nodes use the synced local install/binary rebuilt in Phase 0.8.
# They intentionally do not rebuild on-host because at least admin123 does not
# have the full build tool chain installed. Phase 3 verifies the recovered
# 1024-slot result ring before any measurement is trusted.
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  log "=== Phase 1.5: Build on Ubuntu 22.04 nodes (user4, new-node) ==="

  for idx in "${!NODE_IDS[@]}"; do
    is_u22="${NODE_IS_U22[$idx]}"
    [[ "$is_u22" -eq 0 ]] && continue
    name="${NODE_NAMES[$idx]}"
    ip="${NODE_IPS[$idx]}"
    log "  Building on $name ($ip)"

    log "  Rebuilding custom PostgreSQL install on $name"
    node_ssh "$idx" "
      chmod +x '$REMOTE_REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh'
      sed -i -E 's/^#define[[:space:]]+BCDB_RESULT_RING_CAPACITY[[:space:]]+[0-9]+/#define BCDB_RESULT_RING_CAPACITY $RESULT_RING_CAPACITY/' '$REMOTE_REPO_ROOT/src/include/bcdb/globals.h'
      bash '$REMOTE_REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh' \
        --repo-root '$REMOTE_REPO_ROOT' \
        --install-dir '$REMOTE_INSTALL_DIR' \
        --force-rebuild \
        --clean-when-rebuild
    " 2>&1 | sed "s/^/[$name] /"

    ensure_u22_cmake "$idx"

    # Push OpenSSL headers from ASUS if not already there (needed by NuRaft TLS)
    node_ssh "$idx" "mkdir -p '$REMOTE_OPENSSL_INCLUDE_U22/openssl'" 2>/dev/null || true
    node_rsync_to "$idx" "/usr/include/openssl/" "$REMOTE_OPENSSL_INCLUDE_U22/openssl/"
    node_rsync_to "$idx" "/usr/include/x86_64-linux-gnu/openssl/" "$REMOTE_OPENSSL_INCLUDE_U22/openssl/"

    # Phase 0.5 (ensure_rdkafka.sh) guarantees ~/Desktop/rdkafka_local on all nodes.
    # Just check it's present; if somehow missing, warn and fall back to stubs.
    RDKAFKA_DESKTOP="/home/neel/Desktop/rdkafka_local"
    KAFKA_CMAKE_OPT="-DKAFKA_OPTIONAL=ON"
    if node_ssh "$idx" "test -f $RDKAFKA_DESKTOP/lib/librdkafka.so && test -f $RDKAFKA_DESKTOP/include/librdkafka/rdkafka.h" 2>/dev/null; then
      log "  Found rdkafka_local on $name — building WITH Kafka majority support"
      KAFKA_CMAKE_OPT="-DRDKAFKA_INCLUDE_DIR=$RDKAFKA_DESKTOP/include -DRDKAFKA_LIBRARY=$RDKAFKA_DESKTOP/lib/librdkafka.so"
    else
      if [[ "$NO_KAFKA" -eq 0 ]]; then
        die "$RDKAFKA_DESKTOP not found on $name; Kafka majority cannot be trusted with stub binaries. Rerun without --skip-rdkafka-setup."
      fi
      log "  WARNING: $RDKAFKA_DESKTOP not found on $name — building with stubs for --no-kafka mode"
    fi

    # Build
    node_ssh "$idx" bash -s <<BUILDSSH
set -euo pipefail
if command -v cmake >/dev/null 2>&1; then
  CMAKE="\$(command -v cmake)"
elif [[ -x "$REMOTE_CMAKE_U22" ]]; then
  CMAKE="$REMOTE_CMAKE_U22"
else
  echo "[$name] ERROR: cmake not found (also missing $REMOTE_CMAKE_U22)" >&2
  exit 1
fi
REPO="$REMOTE_REPO_ROOT"
INSTALL="$REMOTE_INSTALL_DIR"
BUILD_DIR="/tmp/ariabc_pg_build_u22"
DESKTOP_BIN_DIR="/home/neel/Desktop/ariabc_pg_build_u22/bin"
EXTRA_CMAKE_ARGS="$KAFKA_CMAKE_OPT"

rm -rf "\$BUILD_DIR"
echo "[$name] cmake configure..."
\$CMAKE -S "\$REPO/ariabc_pg" -B "\$BUILD_DIR" \\
  -DCMAKE_BUILD_TYPE=Release \\
  \$EXTRA_CMAKE_ARGS \\
  -DLIBPQ_INCLUDE_DIR="\$INSTALL/include" \\
  -DPOSTGRES_INCLUDE_DIR="\$INSTALL/include/postgresql/server" \\
  -DLIBPQ_LIBRARY="\$INSTALL/lib/libpq.so" \\
  -DOPENSSL_INCLUDE_DIR="$REMOTE_OPENSSL_INCLUDE_U22" \\
  -DOPENSSL_SSL_LIBRARY="/usr/lib/x86_64-linux-gnu/libssl.so.3" \\
  -DOPENSSL_CRYPTO_LIBRARY="/usr/lib/x86_64-linux-gnu/libcrypto.so.3" \\
  2>&1 | tail -5

echo "[$name] building benchmark binaries (j\$(nproc))..."
\$CMAKE --build "\$BUILD_DIR" --target ariabc_pg_server ariabc_pg_gateway -j\$(nproc) 2>&1 | tail -10
echo "[$name] build complete: \$(ls \$BUILD_DIR/bin/)"

mkdir -p "\$DESKTOP_BIN_DIR"
cp -f "\$BUILD_DIR/bin/ariabc_pg_server" "\$DESKTOP_BIN_DIR/ariabc_pg_server"
if [[ -x "\$BUILD_DIR/bin/ariabc_pg_gateway" ]]; then
  cp -f "\$BUILD_DIR/bin/ariabc_pg_gateway" "\$DESKTOP_BIN_DIR/ariabc_pg_gateway"
fi
echo "[$name] installed to \$DESKTOP_BIN_DIR: \$(ls \$DESKTOP_BIN_DIR/)"
BUILDSSH
    log "  Build on $name complete"
  done

  log "  Ubuntu 22.04 builds complete; Ubuntu 24.04 nodes will use synced ariabc_cluster build"
fi

# ---------------------------------------------------------------------------
# Phase 2: Kafka on admin123
# ---------------------------------------------------------------------------
if [[ "$NO_KAFKA" -eq 0 && "$SKIP_KAFKA" -eq 0 ]]; then
  log "=== Phase 2: Ensure Kafka (KRaft) running on admin123 (${KAFKA_HOST}) ==="
  node_ssh 0 bash <<KAFKA_EOF
set -euo pipefail
KAFKA_HOME="$KAFKA_HOME_REMOTE"
KAFKA_BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"
TOPICS_SH="\$KAFKA_HOME/bin/kafka-topics.sh"
# Use Desktop JDK if system java is missing
if ! command -v java >/dev/null 2>&1; then
  export JAVA_HOME="/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64"
  export PATH="\$JAVA_HOME/bin:\$PATH"
fi

if [[ ! -f "\$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  echo "ERROR: Kafka not found at \$KAFKA_HOME — run setup first:" >&2
  echo "  On admin123: wget -P ~/Desktop https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz && tar -C ~/Desktop -xzf ~/Desktop/kafka_2.13-3.7.0.tgz" >&2
  exit 1
fi

SERVER_PROPS="\$KAFKA_HOME/config/kraft/server.properties"
GW_IP="${KAFKA_HOST}"

sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://\$GW_IP:${KAFKA_PORT}|" "\$SERVER_PROPS" 2>/dev/null || \
  echo "advertised.listeners=PLAINTEXT://\$GW_IP:${KAFKA_PORT}" >> "\$SERVER_PROPS"

if "\$TOPICS_SH" --bootstrap-server "\$GW_IP:${KAFKA_PORT}" --list >/dev/null 2>&1; then
  echo "Kafka already running at \$GW_IP:${KAFKA_PORT}"
else
  echo "Starting Kafka..."
  STORAGE_SH="\$KAFKA_HOME/bin/kafka-storage.sh"
  SERVER_SH="\$KAFKA_HOME/bin/kafka-server-start.sh"
  cluster_id="\$("\$STORAGE_SH" random-uuid 2>/dev/null | tail -1 | tr -d '\r')"
  [[ -z "\$cluster_id" ]] && { echo "ERROR: failed to generate cluster ID" >&2; exit 1; }
  "\$STORAGE_SH" format -t "\$cluster_id" -c "\$SERVER_PROPS" --ignore-formatted >/dev/null 2>&1 || true
  "\$SERVER_SH" -daemon "\$SERVER_PROPS"
  for i in \$(seq 1 60); do
    if "\$TOPICS_SH" --bootstrap-server "\$GW_IP:${KAFKA_PORT}" --list >/dev/null 2>&1; then
      echo "Kafka ready after \${i}s"
      break
    fi
    sleep 1
    [[ "\$i" -eq 60 ]] && { echo "ERROR: Kafka did not start" >&2; exit 1; }
  done
fi

"\$TOPICS_SH" --bootstrap-server "\$GW_IP:${KAFKA_PORT}" \
  --create --topic "$KAFKA_RESULT_TOPIC" --partitions 4 --replication-factor 1 \
  --if-not-exists >/dev/null 2>&1 || true
echo "Topic '$KAFKA_RESULT_TOPIC' ready"
KAFKA_EOF
  log "  Kafka ready"
else
  [[ "$NO_KAFKA" -eq 1 ]] && log "  Skipping Kafka (--no-kafka mode)"
  [[ "$SKIP_KAFKA" -eq 1 ]] && log "  Skipping Kafka setup (--skip-kafka)"
fi

# ---------------------------------------------------------------------------
# Phase 3: Verify BCDB postgres on all 4 nodes
# ---------------------------------------------------------------------------
log "=== Phase 3: Verify BCDB postgres on all 4 nodes ==="
for idx in "${!NODE_IDS[@]}"; do
  ip="${NODE_IPS[$idx]}"
  id="${NODE_IDS[$idx]}"
  log "  Checking ${NODE_NAMES[$idx]} (${ip}:${DB_PORT})"
  status_line="$(node_ssh "$idx" "
    INSTALL_DIR='$REMOTE_INSTALL_DIR'
    PGDATA='$REMOTE_REPO_ROOT/.bench_tmp/single_node_pgdata'
    BIN=\$INSTALL_DIR/bin
    export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
    export BCDB_BLOCK_PROFILE='$BCDB_BLOCK_PROFILE'
    export BCDB_BLOCK_WAIT_WATERMARK='$BCDB_BLOCK_WAIT_WATERMARK'
    export BCDB_POLL_MAX_US='$BCDB_POLL_MAX_US'
    export BCDB_DT_PARSE_BARRIER='$BCDB_DT_PARSE_BARRIER'
    export BCDB_FLOW_DEBUG='$BCDB_FLOW_DEBUG'
    export BCDB_BLOCK_ENQUEUE_YIELD_EVERY='$BCDB_BLOCK_ENQUEUE_YIELD_EVERY'
    export BCDB_DECOUPLE_WORKERS='$BCDB_DECOUPLE_WORKERS'
    export BCDB_DT_LIGHT_SNAPSHOT='$BCDB_DT_LIGHT_SNAPSHOT'
    export BCDB_DT_SKIP_READONLY_GATE='$BCDB_DT_SKIP_READONLY_GATE'
    if [[ -f \"\$PGDATA/postgresql.auto.conf\" ]]; then
      sed -i -E \"s/^(bcdb_result_ring_slots[[:space:]]*=[[:space:]]*)'?[0-9]+'?/\\1'$RESULT_RING_CAPACITY'/\" \"\$PGDATA/postgresql.auto.conf\"
      if [[ '$BCDB_OVERWRITE_PROTECTION' == '0' ]]; then
        sed -i -E \"/^[[:space:]]*bcdb_overwrite_protection[[:space:]]*=/d\" \"\$PGDATA/postgresql.auto.conf\"
      fi
    fi
    if [[ '$BCDB_PHASE_TRACE_ON' != '0' ]]; then
      mkdir -p '$REMOTE_REPO_ROOT/.bench_tmp'
      rm -f '$REMOTE_REPO_ROOT/.bench_tmp/bcdb_phase_trace_node${id}.'*
      export BCDB_PHASE_TRACE='$REMOTE_REPO_ROOT/.bench_tmp/bcdb_phase_trace_node${id}'
    else
      unset BCDB_PHASE_TRACE
    fi
    hard_stop_benchmark_postgres() {
      echo '  hard-stopping stale benchmark postgres if needed'
      old_pid=''
      old_state=''
      if [[ -f \"\$PGDATA/postmaster.pid\" ]]; then
        old_pid=\$(head -n 1 \"\$PGDATA/postmaster.pid\" 2>/dev/null || true)
        old_state=\$(sed -n '8p' \"\$PGDATA/postmaster.pid\" 2>/dev/null | tr -d '[:space:]' || true)
        old_shmid=\$(sed -n '7p' \"\$PGDATA/postmaster.pid\" 2>/dev/null | awk '{print \$2}' || true)
        if [[ \"\$old_pid\" =~ ^[0-9]+$ ]] && ! kill -0 \"\$old_pid\" 2>/dev/null; then
          echo \"  removing stale postmaster.pid for dead pid \$old_pid\"
          rm -f \"\$PGDATA/postmaster.pid\"
        fi
      fi
      if [[ -f \"\$PGDATA/postmaster.pid\" && \"\$old_state\" == \"stopping\" ]]; then
        echo \"  postmaster.pid is stuck in stopping state; skipping pg_ctl wait\"
      else
        timeout 25s \$BIN/pg_ctl -D \$PGDATA -w -t 20 stop -m fast 2>&1 || true
      fi
      if [[ -f \"\$PGDATA/postmaster.pid\" ]]; then
        old_pid=\$(head -n 1 \"\$PGDATA/postmaster.pid\" 2>/dev/null || true)
        if [[ \"\$old_pid\" =~ ^[0-9]+$ ]] && kill -0 \"\$old_pid\" 2>/dev/null; then
          echo \"  postmaster pid \$old_pid still alive; sending TERM\"
          kill -TERM \"\$old_pid\" 2>/dev/null || true
          for _i in \$(seq 1 20); do
            kill -0 \"\$old_pid\" 2>/dev/null || break
            sleep 1
          done
          if kill -0 \"\$old_pid\" 2>/dev/null; then
            echo \"  postmaster pid \$old_pid still alive after TERM; sending KILL\"
            kill -KILL \"\$old_pid\" 2>/dev/null || true
          fi
        fi
        if [[ -n \"\${old_pid:-}\" ]] && ! kill -0 \"\$old_pid\" 2>/dev/null; then
          rm -f \"\$PGDATA/postmaster.pid\"
        fi
        if [[ -f \"\$PGDATA/postmaster.pid\" ]]; then
          old_state=\$(sed -n '8p' \"\$PGDATA/postmaster.pid\" 2>/dev/null | tr -d '[:space:]' || true)
          old_shmid=\$(sed -n '7p' \"\$PGDATA/postmaster.pid\" 2>/dev/null | awk '{print \$2}' || true)
          if [[ \"\$old_state\" == \"stopping\" ]]; then
            echo '  removing postmaster.pid left in stopping state'
            rm -f \"\$PGDATA/postmaster.pid\"
          fi
        fi
      fi
      # Benchmark postgres children can remain after a killed postmaster and
      # keep the old SysV shared memory segment attached.  They run as the
      # benchmark user; the system PostgreSQL service runs as user postgres.
      pkill -TERM -u \"\$(id -u)\" -f '^postgres:' 2>/dev/null || true
      sleep 1
      pkill -KILL -u \"\$(id -u)\" -f '^postgres:' 2>/dev/null || true
      if [[ \"\${old_shmid:-}\" =~ ^[0-9]+$ ]]; then
        if ipcs -m 2>/dev/null | awk '{print \$2}' | grep -qx \"\$old_shmid\"; then
          echo \"  removing stale shared memory segment shmid=\$old_shmid\"
          ipcrm -m \"\$old_shmid\" 2>/dev/null || true
        fi
      fi
    }
    ensure_ready() {
      if \$BIN/pg_isready -h 127.0.0.1 -p $DB_PORT -U $DB_USER >/dev/null 2>&1; then
        return 0
      fi
      echo '  postgres not ready — clearing stale benchmark postmaster before start'
      hard_stop_benchmark_postgres
      echo '  attempting postgres start'
      \$BIN/pg_ctl -D \$PGDATA -w -t 60 start -l '$REMOTE_REPO_ROOT/server.log' 2>&1 || echo 'start attempted'
      sleep 3
      \$BIN/pg_isready -h 127.0.0.1 -p $DB_PORT -U $DB_USER >/dev/null 2>&1 || {
        echo 'WARNING: postgres may not be ready'
        exit 1
      }
    }
    ensure_ready
    if [[ "$FORCE_PG_RESTART" -eq 1 ]]; then
      echo '  restarting postgres to clear stale benchmark backends'
      if ! \$BIN/pg_ctl -D \$PGDATA -w -t 60 restart -l '$REMOTE_REPO_ROOT/server.log'; then
        hard_stop_benchmark_postgres
        \$BIN/pg_ctl -D \$PGDATA -w -t 60 start -l '$REMOTE_REPO_ROOT/server.log'
      fi
      ensure_ready
    fi
    worker_count=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_worker_count;' | tr -d '[:space:]')
    serial_gate=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_serial_gate_mode;' | tr -d '[:space:]')
    serial_gate_source=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_serial_gate_source;' | tr -d '[:space:]')
    dt_conflict=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_conflict_tracking;' | tr -d '[:space:]')
    dt_skip_reads=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_completion_only_skip_reads;' | tr -d '[:space:]')
    hashtab_threshold=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_hashtab_switch_threshold;' | tr -d '[:space:]')
    ring_slots=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_result_ring_slots;' | tr -d '[:space:]')
    owp=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_overwrite_protection;' 2>/dev/null | tr -d '[:space:]' || echo '')
    if [[ -z \"\$owp\" && '$BCDB_OVERWRITE_PROTECTION' != '0' ]]; then
      echo \"ERROR: --bcdb-overwrite-protection was requested, but this PostgreSQL build does not expose bcdb_overwrite_protection\" >&2
      exit 1
    fi
    max_connections=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show max_connections;' | tr -d '[:space:]')
    min_max_connections=$(( $DB_CONN_POOL_SIZE * 3 + 64 ))
    worker_min_max_connections=$(( $BCDB_WORKER_COUNT + $DB_CONN_POOL_SIZE + 64 ))
    if [[ "\$min_max_connections" -lt "\$worker_min_max_connections" ]]; then
      min_max_connections="\$worker_min_max_connections"
    fi
    if [[ "\$min_max_connections" -lt 256 ]]; then
      min_max_connections=256
    fi
    target_ring_slots=\"$RESULT_RING_CAPACITY\"
    target_owp=\"$BCDB_OVERWRITE_PROTECTION\"
    needs_restart=0
    if [[ \"\$worker_count\" != \"$BCDB_WORKER_COUNT\" ]]; then
      needs_restart=1
    fi
    if [[ \"\$serial_gate\" != \"$BCDB_SERIAL_GATE_MODE\" ]]; then
      needs_restart=1
    fi
    if [[ \"\$serial_gate_source\" != \"$BCDB_SERIAL_GATE_SOURCE\" ]]; then
      needs_restart=1
    fi
    target_dt_conflict=\"$([[ "$BCDB_DT_CONFLICT_TRACKING" == "1" ]] && echo on || echo off)\"
    if [[ \"\$dt_conflict\" != \"\$target_dt_conflict\" ]]; then
      needs_restart=1
    fi
    target_dt_skip_reads=\"$([[ "$BCDB_DT_COMPLETION_ONLY_SKIP_READS" == "1" ]] && echo on || echo off)\"
    if [[ \"\$dt_skip_reads\" != \"\$target_dt_skip_reads\" ]]; then
      needs_restart=1
    fi
    if [[ \"\$hashtab_threshold\" != \"$BCDB_DT_HASHTAB_SWITCH_THRESHOLD\" ]]; then
      needs_restart=1
    fi
    if [[ \"\$ring_slots\" != \"\$target_ring_slots\" ]]; then
      needs_restart=1
    fi
    if [[ -n \"\$owp\" && \"\$owp\" != \"\$target_owp\" ]]; then
      needs_restart=1
    fi
    if [[ -z \"\$max_connections\" || \"\$max_connections\" -lt \"\$min_max_connections\" ]]; then
      needs_restart=1
    fi
    if [[ \"\$needs_restart\" -eq 1 ]]; then
      echo \"reconfiguring bcdb_worker_count=\$worker_count -> $BCDB_WORKER_COUNT bcdb_serial_gate_mode=\$serial_gate -> $BCDB_SERIAL_GATE_MODE bcdb_serial_gate_source=\$serial_gate_source -> $BCDB_SERIAL_GATE_SOURCE bcdb_dt_conflict_tracking=\$dt_conflict -> \$target_dt_conflict bcdb_dt_completion_only_skip_reads=\$dt_skip_reads -> \$target_dt_skip_reads bcdb_dt_hashtab_switch_threshold=\$hashtab_threshold -> $BCDB_DT_HASHTAB_SWITCH_THRESHOLD bcdb_result_ring_slots=\$ring_slots -> \$target_ring_slots bcdb_overwrite_protection=\$owp -> \$target_owp max_connections=\$max_connections -> >=\$min_max_connections\"
      if [[ \"\$worker_count\" != \"$BCDB_WORKER_COUNT\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_worker_count = '$BCDB_WORKER_COUNT';\"
      fi
      if [[ \"\$serial_gate\" != \"$BCDB_SERIAL_GATE_MODE\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_mode = '$BCDB_SERIAL_GATE_MODE';\"
      fi
      if [[ \"\$serial_gate_source\" != \"$BCDB_SERIAL_GATE_SOURCE\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_source = '$BCDB_SERIAL_GATE_SOURCE';\"
      fi
      if [[ \"\$dt_conflict\" != \"\$target_dt_conflict\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_conflict_tracking = '\$target_dt_conflict';\"
      fi
      if [[ \"\$dt_skip_reads\" != \"\$target_dt_skip_reads\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_completion_only_skip_reads = '\$target_dt_skip_reads';\"
      fi
      if [[ \"\$hashtab_threshold\" != \"$BCDB_DT_HASHTAB_SWITCH_THRESHOLD\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_hashtab_switch_threshold = '$BCDB_DT_HASHTAB_SWITCH_THRESHOLD';\"
      fi
      if [[ \"\$ring_slots\" != \"\$target_ring_slots\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_result_ring_slots = '\$target_ring_slots';\"
      fi
      if [[ -n \"\$owp\" && \"\$owp\" != \"\$target_owp\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_overwrite_protection = '\$target_owp';\"
      fi
      if [[ -z \"\$max_connections\" || \"\$max_connections\" -lt \"\$min_max_connections\" ]]; then
        \$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET max_connections = '\$min_max_connections';\"
      fi
      if ! \$BIN/pg_ctl -D \$PGDATA -w -t 60 restart -l '$REMOTE_REPO_ROOT/server.log'; then
        hard_stop_benchmark_postgres
        \$BIN/pg_ctl -D \$PGDATA -w -t 60 start -l '$REMOTE_REPO_ROOT/server.log'
      fi
      ensure_ready
      worker_count=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_worker_count;' | tr -d '[:space:]')
      serial_gate=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_serial_gate_mode;' | tr -d '[:space:]')
      serial_gate_source=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_serial_gate_source;' | tr -d '[:space:]')
      dt_conflict=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_conflict_tracking;' | tr -d '[:space:]')
      dt_skip_reads=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_completion_only_skip_reads;' | tr -d '[:space:]')
      hashtab_threshold=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_dt_hashtab_switch_threshold;' | tr -d '[:space:]')
      ring_slots=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_result_ring_slots;' | tr -d '[:space:]')
      owp=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_overwrite_protection;' 2>/dev/null | tr -d '[:space:]' || echo '')
      max_connections=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show max_connections;' | tr -d '[:space:]')
    fi
    echo \"postgres OK bcdb_worker_count=\$worker_count bcdb_serial_gate_mode=\$serial_gate bcdb_serial_gate_source=\$serial_gate_source bcdb_dt_conflict_tracking=\$dt_conflict bcdb_dt_completion_only_skip_reads=\$dt_skip_reads bcdb_dt_hashtab_switch_threshold=\$hashtab_threshold bcdb_result_ring_slots=\$ring_slots bcdb_overwrite_protection=\$owp max_connections=\$max_connections\"
  " 2>&1)" || die "could not verify postgres on ${NODE_NAMES[$idx]}"
  log "  ${NODE_NAMES[$idx]}: $status_line"
  actual_workers="$(sed -n 's/.*bcdb_worker_count=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  if [[ -n "$actual_workers" && "$actual_workers" != "$BCDB_WORKER_COUNT" ]]; then
    die "bcdb_worker_count mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_workers expected=$BCDB_WORKER_COUNT"
  fi
  actual_serial_gate="$(sed -n 's/.*bcdb_serial_gate_mode=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  if [[ -n "$actual_serial_gate" && "$actual_serial_gate" != "$BCDB_SERIAL_GATE_MODE" ]]; then
    die "bcdb_serial_gate_mode mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_serial_gate expected=$BCDB_SERIAL_GATE_MODE"
  fi
  actual_serial_gate_source="$(sed -n 's/.*bcdb_serial_gate_source=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  if [[ -n "$actual_serial_gate_source" && "$actual_serial_gate_source" != "$BCDB_SERIAL_GATE_SOURCE" ]]; then
    die "bcdb_serial_gate_source mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_serial_gate_source expected=$BCDB_SERIAL_GATE_SOURCE"
  fi
  actual_dt_conflict="$(sed -n 's/.*bcdb_dt_conflict_tracking=\([^[:space:]]*\).*/\1/p' <<<"$status_line" | tail -1)"
  expected_dt_conflict="$([[ "$BCDB_DT_CONFLICT_TRACKING" == "1" ]] && echo on || echo off)"
  if [[ -n "$actual_dt_conflict" && "$actual_dt_conflict" != "$expected_dt_conflict" ]]; then
    die "bcdb_dt_conflict_tracking mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_dt_conflict expected=$expected_dt_conflict"
  fi
  actual_dt_skip_reads="$(sed -n 's/.*bcdb_dt_completion_only_skip_reads=\([^[:space:]]*\).*/\1/p' <<<"$status_line" | tail -1)"
  expected_dt_skip_reads="$([[ "$BCDB_DT_COMPLETION_ONLY_SKIP_READS" == "1" ]] && echo on || echo off)"
  if [[ -n "$actual_dt_skip_reads" && "$actual_dt_skip_reads" != "$expected_dt_skip_reads" ]]; then
    die "bcdb_dt_completion_only_skip_reads mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_dt_skip_reads expected=$expected_dt_skip_reads"
  fi
  actual_hashtab_threshold="$(sed -n 's/.*bcdb_dt_hashtab_switch_threshold=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  if [[ -n "$actual_hashtab_threshold" && "$actual_hashtab_threshold" != "$BCDB_DT_HASHTAB_SWITCH_THRESHOLD" ]]; then
    die "bcdb_dt_hashtab_switch_threshold mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_hashtab_threshold expected=$BCDB_DT_HASHTAB_SWITCH_THRESHOLD"
  fi
  actual_max_connections="$(sed -n 's/.*max_connections=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  required_max_connections=$(( DB_CONN_POOL_SIZE * 3 + 64 ))
  worker_required_max_connections=$(( BCDB_WORKER_COUNT + DB_CONN_POOL_SIZE + 64 ))
  if [[ "$required_max_connections" -lt "$worker_required_max_connections" ]]; then
    required_max_connections="$worker_required_max_connections"
  fi
  if [[ "$required_max_connections" -lt 256 ]]; then
    required_max_connections=256
  fi
  if [[ -n "$actual_max_connections" && "$actual_max_connections" -lt "$required_max_connections" ]]; then
    die "max_connections too low on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_max_connections required>=$required_max_connections"
  fi
  actual_ring_slots="$(sed -n 's/.*bcdb_result_ring_slots=\([0-9][0-9]*\).*/\1/p' <<<"$status_line" | tail -1)"
  if [[ -n "$actual_ring_slots" && "$actual_ring_slots" != "$RESULT_RING_CAPACITY" ]]; then
    die "bcdb_result_ring_slots mismatch on ${NODE_NAMES[$idx]} after reconfigure: postgres=$actual_ring_slots expected=$RESULT_RING_CAPACITY"
  fi
done

# ---------------------------------------------------------------------------
# Phase 3.2: Ensure the local OS login role exists in Postgres
# Current BCDB worker bootstrap still opens internal libpq connections without
# overriding the role, so they fall back to the service account (`neel` on the
# benchmark nodes). Create that role if it is missing so bcdb_init can start.
# ---------------------------------------------------------------------------
log "=== Phase 3.2: Ensure local benchmark role exists on all 4 nodes ==="
for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  log "  Ensuring role neel exists on $name"
  node_ssh "$idx" "
    INSTALL_DIR='$REMOTE_INSTALL_DIR'
    export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
    \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"
      DO \\\$\\\$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'neel') THEN
          CREATE ROLE neel LOGIN SUPERUSER;
        END IF;
      END
      \\\$\\\$
    \"
  " >/dev/null || die "failed to ensure role neel on $name"
done

# ---------------------------------------------------------------------------
# Phase 3.5: Restore benchmark table state on all 4 nodes
# The distributed run is meaningful only if every replica starts from the same
# table contents and Merkle index.  The restore SQL also calls bcdb_reset().
# ---------------------------------------------------------------------------
if [[ "$SKIP_RESTORE" -eq 0 ]]; then
  log "=== Phase 3.5: Restore $VERIFY_TABLE on all 4 nodes ==="
  [[ -f "$RESTORE_SQL" ]] || die "restore SQL not found: $RESTORE_SQL"

  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    remote_restore="$REMOTE_REPO_ROOT/scripts/restore_usertable_small.sql"
    log "  Restoring $VERIFY_TABLE on $name"
    node_ssh "$idx" "
      INSTALL_DIR='$REMOTE_INSTALL_DIR'
      export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
      test -f '$remote_restore' || { echo 'missing restore SQL: $remote_restore' >&2; exit 1; }
      \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -f '$remote_restore'
      cnt=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc 'SELECT count(*) FROM $VERIFY_TABLE')
      root=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_root_hash('$VERIFY_TABLE')\")
      verify=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_verify('$VERIFY_TABLE')\")
      echo \"count=\$cnt root=\$root verify=\$verify\"
    " 2>&1 | sed "s/^/  [$name] /" || die "restore failed on $name"
  done
else
  log "=== Phase 3.5: Restore skipped (--skip-restore) ==="
fi

# ---------------------------------------------------------------------------
# Phase 4: Start ariabc_pg_server on each node
# Binary selection:
#   - Ubuntu 24.04 (admin123, utkarsh): REMOTE_BIN_U24 (synced ASUS/local build)
#   - Ubuntu 22.04 (user4, new-node):   REMOTE_BIN_U22 (/tmp build, KAFKA_OPTIONAL=ON)
# Port selection:
#   - Nodes 1-3: clientPort=8000
#   - Node 4 (utkarsh): clientPort=8001 (8000 taken by HP printer snap)
# ---------------------------------------------------------------------------
log "=== Phase 4: Starting ariabc_pg_server on all 4 nodes ==="

REMOTE_LOG_DIR="/tmp/ariabc_cluster"
KAFKA_ARGS=""
[[ "$NO_KAFKA" -eq 0 ]] && KAFKA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC"

for idx in "${!NODE_IDS[@]}"; do
  id="${NODE_IDS[$idx]}"
  ip="${NODE_IPS[$idx]}"
  name="${NODE_NAMES[$idx]}"
  client_port="${NODE_CLIENT_PORTS[$idx]}"
  is_u22="${NODE_IS_U22[$idx]}"
  [[ "$is_u22" -eq 1 ]] && srv_bin="$REMOTE_BIN_U22" || srv_bin="$REMOTE_BIN_U24"

  log "  Starting server on $name ($ip) — RAFT ID $id, clientPort=$client_port orderingMode=$ORDERING_MODE"
  log "    binary: $srv_bin"
  log "    dbConnPoolSize=$DB_CONN_POOL_SIZE bcdbWorkerCount=$BCDB_WORKER_COUNT bcdbDecoupleWorkers=$BCDB_DECOUPLE_WORKERS bcdbDtConflictTracking=$BCDB_DT_CONFLICT_TRACKING bcdbDtLightSnapshot=$BCDB_DT_LIGHT_SNAPSHOT bcdbDtSkipReadonlyGate=$BCDB_DT_SKIP_READONLY_GATE bcdbDtCompletionOnlySkipReads=$BCDB_DT_COMPLETION_ONLY_SKIP_READS detBlockSkipReadonly=$BCDB_DT_COMPLETION_ONLY_SKIP_READS bcdbDtHashtabSwitchThreshold=$BCDB_DT_HASHTAB_SWITCH_THRESHOLD bcdbDetQueueHighWm=$BCDB_DET_QUEUE_HIGH_WM bcdbDetQueueLowWm=$BCDB_DET_QUEUE_LOW_WM bcdbFlowDebug=$BCDB_FLOW_DEBUG fullResultReplicaLimit=$ARIABC_FULL_RESULT_REPLICA_LIMIT preferredLeaderId=$ARIABC_PREFERRED_LEADER_ID pgExecMode=$PG_EXEC_MODE detBlockParallel=$DET_BLOCK_PARALLEL detBlockPipeline=$DET_BLOCK_PIPELINE detBlockMax=$DET_BLOCK_MAX detPartialBlockMaxWaitUs=$DET_PARTIAL_BLOCK_MAX_WAIT_US bcdbBlockProfile=$BCDB_BLOCK_PROFILE bcdbBlockWaitWatermark=$BCDB_BLOCK_WAIT_WATERMARK bcdbPhaseTrace=$BCDB_PHASE_TRACE_ON bcdbPollMaxUs=$BCDB_POLL_MAX_US bcdbSerialGateMode=$BCDB_SERIAL_GATE_MODE bcdbSerialGateSource=$BCDB_SERIAL_GATE_SOURCE bcdbDtParseBarrier=$BCDB_DT_PARSE_BARRIER bcdbBlockEnqueueYieldEvery=$BCDB_BLOCK_ENQUEUE_YIELD_EVERY"

  REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"

  # rdkafka_local must precede system/install lib paths on ALL nodes so that
  # the source-built v2.3.0 .so is loaded, not whatever the OS happens to have.
  NODE_LIB_PATH="/home/neel/Desktop/rdkafka_local/lib:$REMOTE_INSTALL_DIR/lib"

  node_ssh "$idx" "
	    mkdir -p '$REMOTE_LOG_DIR'
	    rm -f '$REMOTE_SRV_LOG'
	    export LD_LIBRARY_PATH='${NODE_LIB_PATH}:\${LD_LIBRARY_PATH:-}'
	    export ARIABC_PROFILE='${ARIABC_PROFILE:-1}'
	    export ARIABC_DET_BLOCK_PARALLEL='${DET_BLOCK_PARALLEL}'
	    export ARIABC_DET_BLOCK_PIPELINE='${DET_BLOCK_PIPELINE}'
	    export ARIABC_DET_BLOCK_MAX='${DET_BLOCK_MAX}'
	    export ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US='${DET_PARTIAL_BLOCK_MAX_WAIT_US}'
	    export ARIABC_DET_BLOCK_SKIP_READONLY='${BCDB_DT_COMPLETION_ONLY_SKIP_READS}'
	    export ARIABC_FULL_RESULT_REPLICA_LIMIT='${ARIABC_FULL_RESULT_REPLICA_LIMIT}'
	    export ARIABC_PREFERRED_LEADER_ID='${ARIABC_PREFERRED_LEADER_ID}'
	    export BCDB_DT_COMPLETION_ONLY_SKIP_READS='${BCDB_DT_COMPLETION_ONLY_SKIP_READS}'
	    export BCDB_FLOW_DEBUG='${BCDB_FLOW_DEBUG}'
	    export BCDB_DET_QUEUE_HIGH_WM='${BCDB_DET_QUEUE_HIGH_WM}'
	    export BCDB_DET_QUEUE_LOW_WM='${BCDB_DET_QUEUE_LOW_WM}'
	    nohup '$srv_bin' \
      --id $id \
      --raftEndpoint ${ip}:${RAFT_PORT} \
      --clientPort ${client_port} \
      --raftMembers '$RAFT_MEMBERS' \
      --dbName $DB_NAME \
      --dbHost 127.0.0.1 \
      --dbPort $DB_PORT \
      --dbUser $DB_USER \
      --dbType 1 \
      --safedb 1 \
      --dbConnPoolSize $DB_CONN_POOL_SIZE \
      --pgExecMode $PG_EXEC_MODE \
      --bypassRaft $BYPASS_RAFT \
      $KAFKA_ARGS \
      >'$REMOTE_SRV_LOG' 2>&1 &
    echo \"started pid=\$!\"
  " 2>&1 | sed "s/^/  [$name] /"
done

log "  All 4 server launch commands sent"

# ---------------------------------------------------------------------------
# Phase 5: Wait for Raft cluster to stabilize
# ---------------------------------------------------------------------------
if [[ "$BYPASS_RAFT" -eq 1 ]]; then
  log "=== Phase 5: Waiting for bypass-Raft server ports (up to 60s) ==="
else
  log "=== Phase 5: Waiting for Raft cluster (up to 60s) ==="
fi
MAX_WAIT=60
ALL_UP=0

for attempt in $(seq 1 "$MAX_WAIT"); do
  UP=0
  for idx in "${!NODE_IDS[@]}"; do
    client_port="${NODE_CLIENT_PORTS[$idx]}"
    if node_ssh "$idx" "ss -tlnp 2>/dev/null | grep -q ':${client_port}'" 2>/dev/null; then
      (( UP++ )) || true
    fi
  done

  if [[ "$UP" -ge 4 ]]; then
    log "  All 4 server client ports responding (attempt $attempt)"
    ALL_UP=1
    break
  fi

  if [[ $(( attempt % 5 )) -eq 0 ]]; then
    log "  Waiting... $UP/4 servers up (${attempt}s elapsed)"
  fi
  sleep 1
done

[[ "$ALL_UP" -eq 0 ]] && log "WARNING: Not all 4 nodes responded within ${MAX_WAIT}s"

if [[ "$BYPASS_RAFT" -eq 1 ]]; then
  sleep 2
else
  sleep 5  # Let Raft elect a leader and stabilize
fi

# Check for bcdb_init success on the leader node (any node that started)
if [[ "$BYPASS_RAFT" -eq 1 ]]; then
  log "  Checking BCDB init and bypass-server readiness..."
else
  log "  Checking BCDB init and leader status..."
fi
for idx in "${!NODE_IDS[@]}"; do
  id="${NODE_IDS[$idx]}"
  name="${NODE_NAMES[$idx]}"
  REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"
  result="$(node_ssh "$idx" "grep -E 'bcdb_init|leader_probe|ready' '$REMOTE_SRV_LOG' 2>/dev/null | tail -3" 2>/dev/null || true)"
  log "  [$name] $result"
done

# ---------------------------------------------------------------------------
# Phase 6: Gateway test
# ---------------------------------------------------------------------------
log "=== Phase 6: Gateway test (det mode) ==="

# Build node list with per-node client ports
GW_NODES=""
for idx in "${!NODE_IDS[@]}"; do
  [[ -n "$GW_NODES" ]] && GW_NODES+=","
  GW_NODES+="${NODE_IPS[$idx]}:${NODE_CLIENT_PORTS[$idx]}"
done

GW_BIN="$LOCAL_BIN/ariabc_pg_gateway"
GW_LOG="$LOG_DIR/gateway_test.log"

if [[ ! -x "$GW_BIN" ]]; then
  die "ariabc_pg_gateway not found at $GW_BIN — build it: cmake --build ariabc_pg/build -j\$(nproc)"
fi
log "  Gateway binary: $GW_BIN"

if [[ ! -f "$WORKLOAD_FILE" ]]; then
  log "  Workload file not found at $WORKLOAD_FILE — using minimal inline test"
  WORKLOAD_FILE="$LOG_DIR/test_workload.sql"
  for i in $(seq 1 "$TEST_QUERIES"); do
    echo "SELECT $i;"
  done > "$WORKLOAD_FILE"
fi

log "  Running bcdb_init preflight probe before workload..."
PRECHECK_SQL="$LOG_DIR/bcdb_init_probe.sql"
PRECHECK_LOG="$LOG_DIR/bcdb_init_probe.log"
printf 'SELECT 1;\n' > "$PRECHECK_SQL"
GW_PRECHECK_EXTRA_ARGS="--waitMajority 0 --completionPath direct --totalNodes 4"
if [[ "$BYPASS_RAFT" -eq 1 ]]; then
  GW_PRECHECK_EXTRA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC --waitMajority 1 --completionPath kafka_majority --totalNodes 4 --broadcastToAll 1"
fi
if ! "$GW_BIN" \
  --nodes "$GW_NODES" \
  --queryFrom "$PRECHECK_SQL" \
  --dbType 1 \
  --detStartSeq 99000000 \
  --reqIdOffset 99000000 \
  --detWindow "$DET_WINDOW" \
  --detBatchSize 1 \
  --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
  --submitMode "$SUBMIT_MODE" \
  --detSubmitPipeline "$DET_SUBMIT_PIPELINE" \
  --detPipelineDepth 1 \
  --clientId "cluster-bcdb-probe" \
  --numTerminals 1 \
  $GW_PRECHECK_EXTRA_ARGS \
  >"$PRECHECK_LOG" 2>&1; then
  die "bcdb_init preflight probe failed — see $PRECHECK_LOG"
fi

sleep 2
BCDB_ENABLED=0
BCDB_SKIPPED=0
BCDB_MISSING=0
for idx in "${!NODE_IDS[@]}"; do
  id="${NODE_IDS[$idx]}"
  name="${NODE_NAMES[$idx]}"
  REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"
  status_line="$(node_ssh "$idx" "grep -E 'bcdb_init (enabled|skipped)' '$REMOTE_SRV_LOG' | tail -1" 2>/dev/null || true)"
  if [[ "$status_line" == *"bcdb_init enabled"* ]]; then
    (( BCDB_ENABLED++ )) || true
  elif [[ "$status_line" == *"bcdb_init skipped"* ]]; then
    (( BCDB_SKIPPED++ )) || true
  else
    (( BCDB_MISSING++ )) || true
  fi
  log "  [$name] bcdb_init: ${status_line:-missing}"
done

if [[ "$BCDB_ENABLED" -ne 4 || "$BCDB_SKIPPED" -ne 0 || "$BCDB_MISSING" -ne 0 ]]; then
  die "bcdb_init is not uniformly enabled across all 4 nodes (enabled=$BCDB_ENABLED skipped=$BCDB_SKIPPED missing=$BCDB_MISSING)"
fi

GW_EXTRA_ARGS=""
if [[ "$NO_KAFKA" -eq 0 ]]; then
  GW_EXTRA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC --waitMajority 1 --completionPath kafka_majority --totalNodes 4"
  if [[ "$GATEWAY_BROADCAST_TO_ALL" -eq 1 ]]; then
    GW_EXTRA_ARGS="$GW_EXTRA_ARGS --broadcastToAll 1"
  fi
else
  GW_EXTRA_ARGS="--waitMajority 0 --completionPath direct --totalNodes 4"
fi

log "  Gateway nodes: $GW_NODES"
log "  Workload:      $WORKLOAD_FILE ($(wc -l < "$WORKLOAD_FILE") statements)"
log "  Mode:          dbType=1 (det) | orderingMode=$ORDERING_MODE | orderingPath=$ORDERING_PATH | completionPath=$(echo $GW_EXTRA_ARGS | grep -o 'completionPath [^ ]*' | cut -d' ' -f2) | broadcastToAll=$GATEWAY_BROADCAST_TO_ALL"
log "  DET ids:       detStartSeq=$DET_START_SEQ reqIdOffset=$REQ_ID_OFFSET detWindow=$DET_WINDOW detBatchSize=$DET_BATCH_SIZE terminals=$NUM_TERMINALS connFanout=$CONN_FANOUT detPipelineDepth=$DET_PIPELINE_DEPTH submitMode=$SUBMIT_MODE poolSize=$DB_CONN_POOL_SIZE bcdbWorkerCount=$BCDB_WORKER_COUNT bcdbDecoupleWorkers=$BCDB_DECOUPLE_WORKERS bcdbDtConflictTracking=$BCDB_DT_CONFLICT_TRACKING bcdbDtLightSnapshot=$BCDB_DT_LIGHT_SNAPSHOT bcdbDtSkipReadonlyGate=$BCDB_DT_SKIP_READONLY_GATE bcdbDtCompletionOnlySkipReads=$BCDB_DT_COMPLETION_ONLY_SKIP_READS bcdbDtHashtabSwitchThreshold=$BCDB_DT_HASHTAB_SWITCH_THRESHOLD detBlockParallel=$DET_BLOCK_PARALLEL detBlockPipeline=$DET_BLOCK_PIPELINE detBlockMax=$DET_BLOCK_MAX detPartialBlockMaxWaitUs=$DET_PARTIAL_BLOCK_MAX_WAIT_US bcdbBlockProfile=$BCDB_BLOCK_PROFILE bcdbBlockWaitWatermark=$BCDB_BLOCK_WAIT_WATERMARK bcdbPhaseTrace=$BCDB_PHASE_TRACE_ON bcdbPollMaxUs=$BCDB_POLL_MAX_US bcdbSerialGateMode=$BCDB_SERIAL_GATE_MODE bcdbSerialGateSource=$BCDB_SERIAL_GATE_SOURCE bcdbDtParseBarrier=$BCDB_DT_PARSE_BARRIER bcdbBlockEnqueueYieldEvery=$BCDB_BLOCK_ENQUEUE_YIELD_EVERY"

START_S="$(date +%s)"

if ! "$GW_BIN" \
  --nodes "$GW_NODES" \
  --queryFrom "$WORKLOAD_FILE" \
  --dbType 1 \
  --detStartSeq "$DET_START_SEQ" \
  --reqIdOffset "$REQ_ID_OFFSET" \
  --detWindow "$DET_WINDOW" \
  --detBatchSize "$DET_BATCH_SIZE" \
  --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
  --submitMode "$SUBMIT_MODE" \
  --detSubmitPipeline "$DET_SUBMIT_PIPELINE" \
  --detPipelineDepth "$DET_PIPELINE_DEPTH" \
  ${POLL_COUNT:+--pollCount $POLL_COUNT} \
  ${POLL_INTERVAL_US:+--pollIntervalUs $POLL_INTERVAL_US} \
  --clientId "cluster-ycsb" \
  --numTerminals "$NUM_TERMINALS" \
  --connFanout "$CONN_FANOUT" \
  $GW_EXTRA_ARGS \
  2>&1 | tee "$GW_LOG"; then
  log "WARNING: Gateway exited with non-zero status — check $GW_LOG"
fi

END_S="$(date +%s)"
ELAPSED=$(( END_S - START_S ))

# ---------------------------------------------------------------------------
# Phase 7: Results
# ---------------------------------------------------------------------------
log "=== Phase 7: Results ==="

WORKLOAD_LINES="$(awk 'BEGIN{n=0} /^[[:space:]]*($|--)/{next} {n++} END{print n}' "$WORKLOAD_FILE")"

# Prefer gateway's own reported time (excludes restore, file load, and leader probe).
# Falls back to shell wall-clock only when gateway log lacks the line.
GW_MS="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$GW_LOG" 2>/dev/null | head -1 || echo '')"
if [[ -n "$GW_MS" && "$GW_MS" -gt 0 ]]; then
  TPS=$(( WORKLOAD_LINES * 1000 / GW_MS ))
  log "  GW time (ms)  : ${GW_MS}"
  log "  Queries       : ${WORKLOAD_LINES}"
  log "  TPS (gateway) : ~${TPS} tx/s"
elif [[ "$ELAPSED" -gt 0 ]]; then
  TPS=$(( WORKLOAD_LINES / ELAPSED ))
  log "  Wall time     : ${ELAPSED}s (fallback — gateway ms not found in log)"
  log "  Queries       : ${WORKLOAD_LINES}"
  log "  Est TPS       : ~${TPS} tx/s"
fi

DIVERGENCE="$(grep -E '^divergence_count=[0-9]+$' "$GW_LOG" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
FAILURES="$(grep -E '^permanent_failures=[0-9]+$' "$GW_LOG" 2>/dev/null | tail -1 | cut -d= -f2 || true)"
[[ -n "$DIVERGENCE" ]] || DIVERGENCE="?"
[[ -n "$FAILURES" ]] || FAILURES="?"
log "  divergence_count : $DIVERGENCE"
log "  permanent_failures: $FAILURES"

collect_cluster_logs "  Collecting server logs from all nodes..."

log ""
log "=== Cluster logs ==="
log "  Server stdout : $LOG_DIR/server_node*.log"
log "  Postgres logs  : $LOG_DIR/postgres_node*.log"
log "  NuRaft logs   : $LOG_DIR/nuraft_node*.log"
log "  Gateway log   : $GW_LOG"
log ""
log "=== Quick diagnostics ==="
if [[ "$BYPASS_RAFT" -eq 1 ]]; then
  log "  Ordering mode    : bypass-Raft; grep 'ready (bypass-raft)' $LOG_DIR/server_node*.log"
else
  log "  Raft leader      : grep -i 'leader' $LOG_DIR/nuraft_node*.log | grep 'my id'"
fi
log "  BCDB init status : grep 'bcdb_init' $LOG_DIR/server_node*.log"
log "  BCDB block profile: grep 'PROFILE_BCDB_BLOCK' $LOG_DIR/postgres_node*.log"
log "  Divergences      : grep 'divergence' $GW_LOG"
log ""

if [[ "$DIVERGENCE" != "0" && "$DIVERGENCE" != "?" ]] || [[ "$FAILURES" != "0" && "$FAILURES" != "?" ]]; then
  log "WARNING: Cluster correctness issues detected (divergence=$DIVERGENCE failures=$FAILURES)"
  collect_final_profiles_before_fail "gateway correctness issue"
  exit 1
fi

# ---------------------------------------------------------------------------
# Phase 8: Post-workload table consistency verification
# Submit one final marker transaction through the same ordering path.  In
# raft-kafka mode this goes through Raft; in kafka-only mode the gateway
# broadcasts the preordered marker directly to every replica.  When every node
# can read the marker, every previous workload entry has been applied before
# the Merkle root sample.
# ---------------------------------------------------------------------------
if [[ "$SKIP_POST_VERIFY" -eq 0 ]]; then
  log "=== Phase 8: Post-workload $VERIFY_TABLE Merkle verification ==="
  VERIFY_NODE_SSH_TIMEOUT="${VERIFY_NODE_SSH_TIMEOUT:-20}"

  # --- Pre-marker check: diagnostic only.
  #     With waitMajority=1 the gateway is allowed to finish once a 3/4 quorum
  #     has published matching Kafka results.  The fourth replica can still be
  #     applying or publishing late here, so the correctness barrier is the
  #     marker transaction below, not this immediate snapshot.
  log "  Pre-marker row count + Merkle root check..."
  declare -a PRE_COUNTS=()
  declare -a PRE_ROOTS=()
  PRE_PASS=0
  pre_ref_cnt=""
  pre_ref_root=""
  for pre_attempt in $(seq 1 20); do
    PRE_COUNTS=()
    PRE_ROOTS=()
    for idx in "${!NODE_IDS[@]}"; do
      name="${NODE_NAMES[$idx]}"
      readback="$(NODE_SSH_COMMAND_TIMEOUT="$VERIFY_NODE_SSH_TIMEOUT" node_ssh "$idx" "
        INSTALL_DIR='$REMOTE_INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
        cnt=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc 'SELECT count(*) FROM $VERIFY_TABLE')
        root=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_root_hash('$VERIFY_TABLE')\")
        echo \"\$cnt|\$root\"
      " 2>/dev/null | tr -d '[:space:]')" || readback="error|error"
      IFS='|' read -r cnt root <<<"$readback"
      PRE_COUNTS+=("$cnt")
      PRE_ROOTS+=("$root")
      log "  [$name] pre-marker attempt=$pre_attempt: rows=$cnt root=$root"
    done

    PRE_PASS=1
    pre_ref_cnt="${PRE_COUNTS[0]}"
    pre_ref_root="${PRE_ROOTS[0]}"

    for idx in "${!NODE_IDS[@]}"; do
      if [[ "${PRE_COUNTS[$idx]}" != "$pre_ref_cnt" || "${PRE_ROOTS[$idx]}" != "$pre_ref_root" ]]; then
        PRE_PASS=0
      fi
    done

    if [[ -n "${EXPECTED_ROWS:-}" && "$pre_ref_cnt" != "$EXPECTED_ROWS" ]]; then
      PRE_PASS=0
    fi
    if [[ -n "${EXPECTED_ROOT:-}" && "$pre_ref_root" != "$EXPECTED_ROOT" ]]; then
      PRE_PASS=0
    fi
    [[ "$PRE_PASS" -eq 1 ]] && break
    sleep 1
  done

  for idx in "${!NODE_IDS[@]}"; do
    if [[ "${PRE_COUNTS[$idx]}" != "$pre_ref_cnt" || "${PRE_ROOTS[$idx]}" != "$pre_ref_root" ]]; then
      PRE_PASS=0
      log "  PRE-MARKER MISMATCH on ${NODE_NAMES[$idx]}: rows=${PRE_COUNTS[$idx]} root=${PRE_ROOTS[$idx]}"
    fi
  done

  if [[ -n "${EXPECTED_ROWS:-}" && "$pre_ref_cnt" != "$EXPECTED_ROWS" ]]; then
    PRE_PASS=0
    log "  EXPECTED ROWS MISMATCH: got $pre_ref_cnt expected $EXPECTED_ROWS"
  fi
  if [[ -n "${EXPECTED_ROOT:-}" && "$pre_ref_root" != "$EXPECTED_ROOT" ]]; then
    PRE_PASS=0
    log "  EXPECTED ROOT MISMATCH: got $pre_ref_root expected $EXPECTED_ROOT"
  fi

  if [[ "$PRE_PASS" -eq 1 ]]; then
    exp_note=""
    [[ -n "${EXPECTED_ROWS:-}" ]] && exp_note=" (matches expected rows=$EXPECTED_ROWS root=$EXPECTED_ROOT)"
    log "  Pre-marker consistency: PASS rows=$pre_ref_cnt root=$pre_ref_root${exp_note}"
  else
    log "  Pre-marker consistency: DIAGNOSTIC MISMATCH — continuing to marker barrier"
  fi

  MARKER_VAL="cluster_ycsb_done_$(date +%Y%m%d_%H%M%S)"
  MARKER_FILE="$LOG_DIR/post_verify_marker.sql"
  MARKER_SEQ=$(( DET_START_SEQ + WORKLOAD_LINES ))
  MARKER_REQ=$(( REQ_ID_OFFSET + WORKLOAD_LINES ))
  printf "%s\n" "INSERT INTO $VERIFY_TABLE (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) VALUES ($VERIFY_MARKER_KEY, '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL', '$MARKER_VAL') ON CONFLICT (ycsb_key) DO UPDATE SET field1 = EXCLUDED.field1, field2 = EXCLUDED.field2, field3 = EXCLUDED.field3, field4 = EXCLUDED.field4, field5 = EXCLUDED.field5, field6 = EXCLUDED.field6, field7 = EXCLUDED.field7, field8 = EXCLUDED.field8, field9 = EXCLUDED.field9, field10 = EXCLUDED.field10;" > "$MARKER_FILE"

  MARKER_LOG="$LOG_DIR/post_verify_marker_gateway.log"
  log "  Submitting marker key=$VERIFY_MARKER_KEY detStartSeq=$MARKER_SEQ reqIdOffset=$MARKER_REQ"
  if ! "$GW_BIN" \
    --nodes "$GW_NODES" \
    --queryFrom "$MARKER_FILE" \
    --dbType 1 \
    --detStartSeq "$MARKER_SEQ" \
    --reqIdOffset "$MARKER_REQ" \
    --detWindow 1 \
    --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
    --submitMode blocking \
    --detSubmitPipeline 0 \
    --detPipelineDepth 1 \
    --clientId "cluster-ycsb-marker" \
    --numTerminals 1 \
    $GW_EXTRA_ARGS \
    2>&1 | tee "$MARKER_LOG"; then
    log "WARNING: Marker gateway exited non-zero — check $MARKER_LOG"
  fi

  log "  Waiting until marker is visible on all 4 nodes"
  VERIFY_TIMEOUT="${VERIFY_TIMEOUT:-180}"
  VERIFY_START="$(date +%s)"
  declare -a NODE_MARKER_READY=()
  for idx in "${!NODE_IDS[@]}"; do NODE_MARKER_READY+=("0"); done
  while true; do
    elapsed=$(( $(date +%s) - VERIFY_START ))
    all_ready=1
    for idx in "${!NODE_IDS[@]}"; do
      [[ "${NODE_MARKER_READY[$idx]}" -eq 1 ]] && continue
      name="${NODE_NAMES[$idx]}"
      val="$(NODE_SSH_COMMAND_TIMEOUT="$VERIFY_NODE_SSH_TIMEOUT" node_ssh "$idx" "
        INSTALL_DIR='$REMOTE_INSTALL_DIR'
        export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
        \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME \
          -tAc \"SELECT field1 FROM $VERIFY_TABLE WHERE ycsb_key=$VERIFY_MARKER_KEY\"
      " 2>/dev/null | tr -d '[:space:]')" || true
      if [[ "$val" == "$MARKER_VAL" ]]; then
        NODE_MARKER_READY[$idx]=1
        log "  [$name] marker visible (${elapsed}s)"
      else
        all_ready=0
      fi
    done
    [[ "$all_ready" -eq 1 ]] && break
    if [[ "$elapsed" -ge "$VERIFY_TIMEOUT" ]]; then
      log "ERROR: marker was not visible on all nodes after ${VERIFY_TIMEOUT}s"
      collect_final_profiles_before_fail "marker timeout"
      exit 1
    fi
    sleep 2
  done

  declare -a POST_ROOTS=()
  declare -a POST_COUNTS=()
  declare -a POST_VERIFY=()
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    readback="$(NODE_SSH_COMMAND_TIMEOUT="$VERIFY_NODE_SSH_TIMEOUT" node_ssh "$idx" "
      INSTALL_DIR='$REMOTE_INSTALL_DIR'
      export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
      cnt=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc 'SELECT count(*) FROM $VERIFY_TABLE')
      root=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_root_hash('$VERIFY_TABLE')\")
      verify=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_verify('$VERIFY_TABLE')\")
      echo \"\$cnt|\$root|\$verify\"
    " 2>/dev/null | tr -d '[:space:]')" || readback="error|error|error"
    IFS='|' read -r cnt root verify <<<"$readback"
    POST_COUNTS+=("$cnt")
    POST_ROOTS+=("$root")
    POST_VERIFY+=("$verify")
    log "  [$name] rows=$cnt root=$root merkle_verify=$verify"
  done

  reference_count="${POST_COUNTS[0]}"
  reference_root="${POST_ROOTS[0]}"
  POST_PASS=1
  for idx in "${!NODE_IDS[@]}"; do
    if [[ "${POST_COUNTS[$idx]}" != "$reference_count" ||
          "${POST_ROOTS[$idx]}" != "$reference_root" ||
          "${POST_VERIFY[$idx]}" != "t" ]]; then
      POST_PASS=0
      log "  MISMATCH on ${NODE_NAMES[$idx]} expected rows=$reference_count root=$reference_root verify=t"
    fi
  done

  if [[ "$POST_PASS" -ne 1 ]]; then
    log "ERROR: $VERIFY_TABLE Merkle/root consistency failed"
    collect_final_profiles_before_fail "post-marker Merkle mismatch"
    exit 1
  fi
  log "  $VERIFY_TABLE consistency: PASS rows=$reference_count root=$reference_root"
else
  log "=== Phase 8: Post-workload verification skipped (--skip-post-verify) ==="
fi

if [[ "$COLLECT_FINAL_SERVER_PROFILE" != "0" ]]; then
  log "=== Final server profile collection ==="
  log "  Sending SIGTERM to ariabc_pg_server on all nodes so PROFILE_SERVER is flushed"
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    client_port="${NODE_CLIENT_PORTS[$idx]}"
    NODE_SSH_COMMAND_TIMEOUT="${VERIFY_NODE_SSH_TIMEOUT:-20}" node_ssh "$idx" "
      fuser -k -TERM 9000/tcp 2>/dev/null || true
      fuser -k -TERM ${client_port}/tcp 2>/dev/null || true
    " >/dev/null 2>&1 || true
    log "  [$name] stop signal sent"
  done
  sleep 2
  collect_cluster_logs "  Collecting final server logs with PROFILE_SERVER lines..."
  log "  Server profiles: grep 'PROFILE_SERVER' $LOG_DIR/server_node*.log"
  log "  BCDB profiles: grep 'PROFILE_BCDB_BLOCK' $LOG_DIR/postgres_node*.log"
fi

log "=== 4-node cluster test complete ==="
