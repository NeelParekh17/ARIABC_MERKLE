#!/usr/bin/env bash
# run_4node_raft_cluster.sh — Bootstrap and test the AriaBC distributed cluster.
#
# Topology (from plan.txt):
#   Node 1 (RAFT ID 1): admin123   10.129.148.236  neel  [Kafka host]  Ubuntu 24.04
#   Node 2 (RAFT ID 2): user4      10.129.148.246    neel               Ubuntu 22.04
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
#   3. Postgres — verify BCDB postgres on :5438 on all configured nodes
#   4. Servers  — start ariabc_pg_server on each node (background nohup)
#   5. Wait    — poll until Raft leader is elected (all configured nodes respond)
#   6. Test    — run test workload through gateway (det mode, direct or kafka_majority;
#                --ordering-mode kafka-only bypasses Raft and broadcasts ordered
#                requests to all replicas while using Kafka completion/validation)
#   7. Results — print TPS, check for divergence, collect logs
#   8. Verify  — submit a barrier marker and compare Merkle roots/counts across nodes

set -euo pipefail

cleanup_os_profile() {
  [[ "${ARIABC_OS_PROFILE:-0}" -eq 1 ]] || return 0
  [[ -n "${CLUSTER_PASSWORD:-}" ]] || return 0
  [[ -n "${NODE_IDS[@]+has_nodes}" ]] || return 0

  echo "  Stopping OS profiling..."
  for idx in "${!NODE_IDS[@]}"; do
    node_ssh "$idx" "
      for cmd in mpstat iostat sar pidstat; do
        pidfile='$REMOTE_LOG_DIR/os_'\${cmd}'.pid'
        if [[ -f \"\$pidfile\" ]]; then
          kill \"\$(cat \"\$pidfile\")\" 2>/dev/null || true
          rm -f \"\$pidfile\"
        fi
      done
    " || true
  done
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ---------------------------------------------------------------------------
# Cluster topology
# ---------------------------------------------------------------------------
declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.236 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)
# Ubuntu 22.04 nodes need locally-built binary (GLIBC 2.35, rdkafka from ~/Desktop/rdkafka_local)
declare -a NODE_IS_U22=(0 1 0)
# utkarsh port 8000 is taken by HP printer snap; use 8001 instead
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)

CLUSTER_PASSWORD="${ARIABC_CLUSTER_PASSWORD:-sunil1165}"

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
LOG_FILE="$LOG_DIR/runner.log"
REMOTE_LOG_DIR="/tmp/ariabc_cluster"

# ===========================================================================
# Function: log
# Description: Prints a message to stdout prefixed by [HH:MM:SS].
# ===========================================================================
log() { echo "[$(date +'%H:%M:%S')] $*"; }

# ===========================================================================
# Function: die
# Description: Prints an error message to stderr and terminates execution with
#              status code 1.
# ===========================================================================
die() { echo "ERROR: $*" >&2; exit 1; }

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
KAFKA_COMPLETION_MODE="${KAFKA_COMPLETION_MODE:-majority}" # majority|async|majority-async-all3
TEST_QUERIES="${TEST_QUERIES:-50}"  # number of test transactions
WORKLOAD_FILE="${WORKLOAD_FILE:-$REPO_ROOT/scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"
RESTORE_SQL="${RESTORE_SQL:-$REPO_ROOT/scripts/restore_usertable_small.sql}"
VERIFY_TABLE="${VERIFY_TABLE:-usertable_small}"
VERIFY_MARKER_KEY="${VERIFY_MARKER_KEY:-99999999}"
DET_START_SEQ="${DET_START_SEQ:-0}"
REQ_ID_OFFSET="${REQ_ID_OFFSET:-1}"
DET_WINDOW="${DET_WINDOW:-4096}"
DET_BATCH_SIZE="${DET_BATCH_SIZE:-256}"
NUM_TERMINALS="${NUM_TERMINALS:-1}"
THREADS_ARG=""
PER_THREAD_WINDOW="${PER_THREAD_WINDOW:-1024}"
DET_WINDOW_EXPLICIT=0
DET_BATCH_SIZE_EXPLICIT=0
DET_PIPELINE_DEPTH_EXPLICIT=0
CONN_FANOUT="${CONN_FANOUT:-1}"
CONN_FANOUT_EXPLICIT=0
RAFT_ORDERED_FANOUT="${RAFT_ORDERED_FANOUT:-${ARIABC_RAFT_ORDERED_FANOUT:-1}}"
SUBMIT_MODE="${SUBMIT_MODE:-event}"
DET_SUBMIT_PIPELINE="${DET_SUBMIT_PIPELINE:-1}"
DET_PIPELINE_DEPTH="${DET_PIPELINE_DEPTH:-0}"
PG_EXEC_MODE="${PG_EXEC_MODE:-event}"
DET_RAW_SQL="${DET_RAW_SQL:-0}"  # 0=require "s <seq> <SQL>" deterministic path, 1=raw compatibility mode
DET_BLOCK_PARALLEL="${DET_BLOCK_PARALLEL:-64}"  # active per-tx/event PG conns or parallel det blocks
DET_BLOCK_PIPELINE="${DET_BLOCK_PIPELINE:-4}"  # logical BCDB blocks per backend submit call when block fastpath is enabled
DET_BLOCK_MAX="${DET_BLOCK_MAX:-2048}"         # max txs per backend deterministic block submit
DET_PARTIAL_BLOCK_MAX_WAIT_US="${DET_PARTIAL_BLOCK_MAX_WAIT_US:-0}"  # low-latency partial deterministic blocks; 0=dispatch immediately
DET_EVENT_BLOCK_FASTPATH="${DET_EVENT_BLOCK_FASTPATH:-1}"  # 1=enable BCDB block-submit fast path in event mode
DET_PREFIXED_DIRECT_PARALLEL="${DET_PREFIXED_DIRECT_PARALLEL:-1}"  # 1=execute s<seq> SQL directly on multiple PG sockets
DET_COMPLETION_ONLY_SUCCESS="${DET_COMPLETION_ONLY_SUCCESS:-0}"
DET_COMPLETION_ONLY_SUCCESS_EXPLICIT=0
# ---------------------------------------------------------------------------
# Parallelism mode — mirrors how the single-node Python benchmark uses real
# OS threads vs pipeline depth.
#   pipeline   = current behaviour: one gateway process, N terminal lanes,
#                deepening the DET window (pipeline depth scaling only).
#   os-threads = split the workload into N equal sequential shards and launch
#                N independent gateway processes in parallel (background &),
#                each with its own detStartSeq range.  Like the Python script's
#                ThreadPoolExecutor(max_workers=N) where each thread owns its
#                own DB connection and a strided slice of the workload.
# ---------------------------------------------------------------------------
PARALLELISM_MODE="${PARALLELISM_MODE:-pipeline}"  # pipeline|os-threads
BCDB_BLOCK_PROFILE="${BCDB_BLOCK_PROFILE:-0}"  # postgres-side bcdb_block_submit_results phase logging
BCDB_BLOCK_WAIT_WATERMARK="${BCDB_BLOCK_WAIT_WATERMARK:-0}"  # 1=wait on block commit watermark instead of scanning every slot
BCDB_PHASE_TRACE_ON="${BCDB_PHASE_TRACE_ON:-0}"  # postgres-side per-worker CSV phase traces
BCDB_POLL_MAX_US="${BCDB_POLL_MAX_US:-8}"      # last known good 4-node run used 8us
BCDB_SERIAL_GATE_MODE="${BCDB_SERIAL_GATE_MODE:-1}"  # 0=poll, 1=condvar published-max wakeups
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
ARIABC_FULL_RESULT_REPLICA_LIMIT="${ARIABC_FULL_RESULT_REPLICA_LIMIT:-2}"  # 0=all replicas include full SQL results in Kafka; 2 keeps full results on quorum while all replicas still publish hashes
ARIABC_RESULT_PUBLISH_REPLICA_LIMIT="${ARIABC_RESULT_PUBLISH_REPLICA_LIMIT:-0}"  # 0=all replicas publish Kafka result records
ARIABC_PREFERRED_LEADER_ID="${ARIABC_PREFERRED_LEADER_ID:-0}"  # 0=Raft default election priority; 1=pin leader to admin123 (Kafka host)
GATEWAY_BROADCAST_ACCEPT_QUORUM="${GATEWAY_BROADCAST_ACCEPT_QUORUM:-0}"  # 0=gateway legacy majority for broadcast accepts
GATEWAY_BROADCAST_RESULT_QUORUM="${GATEWAY_BROADCAST_RESULT_QUORUM:-0}"  # 0=legacy accept-completion surface
GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN="${GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN:-1}"  # 1=legacy, 0=client-visible quorum time + post-run drain
GATEWAY_DIRECT_COMPLETION_QUORUM="${GATEWAY_DIRECT_COMPLETION_QUORUM:-1}"  # non-broadcast direct completion apply quorum
RESULT_RING_CAPACITY="${RESULT_RING_CAPACITY:-2048}"
BCDB_OVERWRITE_PROTECTION="${BCDB_OVERWRITE_PROTECTION:-0}"  # 0=off 1=Option-A 2=Option-B
COLLECT_FINAL_SERVER_PROFILE="${COLLECT_FINAL_SERVER_PROFILE:-1}"
SKIP_CLUSTER_LOGS="${SKIP_CLUSTER_LOGS:-0}"  # 1=skip fetching server/nuraft/postgres logs from cluster nodes

# ===========================================================================
# Function: usage
# Description: Prints the command-line options and usage manual to stdout.
# ===========================================================================
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
                    raft-kafka  = normal Raft ordering + selected Kafka completion
                    kafka-only  = bypass Raft; gateway broadcasts preordered requests
                                  to all replicas using selected Kafka completion
  --kafka-completion-mode M
                  Kafka completion/validation mode:
                    majority = strict all-3 audit-grade completion
                    majority-async-all3 = client completes at Kafka majority,
                               then drains all-3 audit before marker/Merkle
                    async    = direct completion plus async Kafka hash validation;
                               post-marker Merkle verification still checks all replicas
  --test-queries N Number of statements in the synthetic fallback workload (only used if --workload FILE is missing; default 50)
  --workload FILE  Workload SQL file (default: scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt)
  --restore-sql FILE
                  SQL used to restore table state before the run
  --verify-table T Table used for post-run root comparison (default: usertable_small)
  --det-start-seq N
                  First 8-digit DET sequence sent to BCDB (default: 0 for fresh strict runs)
  --req-id-offset N
                  First gateway request suffix (default: 1)
  --det-window N   Gateway deterministic in-flight window (default: 4096)
  --det-batch-size N
                  Gateway deterministic Raft batch size (default: 256)
  --parallelism-mode M
                  How N --threads/--num-terminals maps to actual concurrency:
                    pipeline   (default) = one gateway process with N terminal
                                lanes sharing one reactor; deepens DET window.
                                Only pipeline depth scales, NOT OS-level
                                parallelism (same as before).
                    os-threads = mirrors the single-node Python script's
                                ThreadPoolExecutor(max_workers=N).  Splits the
                                workload into N equal sequential shards and
                                launches N independent gateway processes in
                                parallel (background &), each with its own
                                socket+detStartSeq range.  Wall time = max
                                across all workers.  True OS-level parallelism.
  --threads N      Alias for --num-terminals N. In deterministic mode this
                  models N client worker lanes, matching the single-machine
                  traffic loader's worker-stride shape.
  --num-terminals N
                  Gateway terminal/client-lane count (default: 1)
  --per-thread-window N
                  Per client-lane deterministic pipeline depth used with
                  --threads when --det-window/--det-pipeline-depth are not
                  explicitly set (default: 512). Effective detWindow becomes
                  threads * per-thread-window.
  --conn-fanout N Gateway submit sockets per logical node in event submit mode
                  (default: 1). In raft-kafka mode, multi-socket fanout is safe
                  only with --raft-ordered-fanout 1 because the leader reorders
                  DET ranges before Raft append. kafka-only can auto-scale this
                  with --threads because bypass-raft servers reorder
                  deterministic ranges before enqueue.
  --raft-ordered-fanout N
                  Enable the server-side deterministic range reorderer before
                  Raft append, allowing raft-kafka connFanout > 1 without
                  reordering batches: 0|1 (default: 1)
  --det-prefixed-direct-parallel N
                  Execute deterministic "s <seq> SQL" directly on multiple PG
                  sockets instead of bcdb_block_submit_results(): 0|1
                  (default: 0). When enabled, the runner disables the block
                  fast path and requires completion-only success receipts.
  --det-completion-only-success N
                  For deterministic per-tx SQL, publish empty success receipts
                  to Kafka and keep errors verbatim: 0|1 (default: follows
                  --det-prefixed-direct-parallel).
  --broadcast-accept-quorum N
                  Broadcast async accept quorum before pipelining; 0=legacy
                  majority. Late accepts are still drained before exit.
  --broadcast-result-quorum N
                  Broadcast direct-completion result quorum. 0 keeps the legacy
                  accept-completion surface; N waits for N accepted replicas to
                  execute each batch before client-visible completion.
  --broadcast-drain-in-timed-run N
                  Include final blocking late-replica accept drain in reported
                  workload time: 0|1 (default: 1). With 0, the gateway reports
                  client-visible accept-quorum time, then drains before exit.
  --direct-completion-quorum N
                  Non-broadcast direct completion apply quorum; default 1.
                  The post-run Raft marker overrides this to all nodes.
  --det-pipeline-depth N
                  Per-terminal deterministic in-flight depth; 0 auto-splits
                  detWindow across terminals (default: 0)
  --submit-mode M  Gateway submit mode: blocking|event (default: event)
  --pg-exec-mode M Server pgExecMode: threaded|event (default: event)
  --det-raw-sql N
                  Deterministic gateway SQL shape: 0=prefix every request as
                  "s <seq> <SQL>", 1=send raw SQL in deterministic order
                  (default: 0)
  --det-block-parallel N
                  Active deterministic PG connections per server. In
                  prefixed-direct mode this caps per-tx in-flight SQL; in
                  block-fastpath mode this caps concurrent block submits
                  (default: 64)
  --det-block-pipeline N
                  Logical BCDB blocks per backend submit call when block fastpath
                  is enabled (default: 4)
  --det-block-max N
                  Max transactions per backend deterministic block submit (default: 2048)
  --det-partial-block-max-wait-us N
                  Max microseconds to wait for a partial deterministic block
                  before dispatch; 0 dispatches immediately (default: 0)
  --det-event-block-fastpath N
                  Enable BCDB event-mode block fast path on every server:
                  0|1 (default: 1, auto-disabled by prefixed-direct mode)
  --det-prefixed-direct-parallel N
                  Execute deterministic "s <seq>" SQL directly on multiple PG
                  sockets instead of waiting on whole bcdb_block_submit_results
                  calls: 0|1 (default: 1)
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
  --result-publish-replica-limit N
                  Publish Kafka result records only from replica ids <= N; 0=all replicas (default: 0)
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
    --kafka-completion-mode) KAFKA_COMPLETION_MODE="${2:-majority}"; shift 2 ;;
    --test-queries) TEST_QUERIES="${2:-50}"; shift 2 ;;
    --workload)     WORKLOAD_FILE="${2:-}"; shift 2 ;;
    --restore-sql)  RESTORE_SQL="${2:-}"; shift 2 ;;
    --verify-table) VERIFY_TABLE="${2:-}"; shift 2 ;;
    --det-start-seq) DET_START_SEQ="${2:-0}"; shift 2 ;;
    --req-id-offset) REQ_ID_OFFSET="${2:-1}"; shift 2 ;;
    --det-window)   DET_WINDOW="${2:-4096}"; DET_WINDOW_EXPLICIT=1; shift 2 ;;
    --det-batch-size) DET_BATCH_SIZE="${2:-256}"; DET_BATCH_SIZE_EXPLICIT=1; shift 2 ;;
    --threads) THREADS_ARG="${2:-1}"; NUM_TERMINALS="${2:-1}"; shift 2 ;;
    --num-terminals) NUM_TERMINALS="${2:-1}"; shift 2 ;;
    --per-thread-window) PER_THREAD_WINDOW="${2:-512}"; shift 2 ;;
    --conn-fanout) CONN_FANOUT="${2:-1}"; CONN_FANOUT_EXPLICIT=1; shift 2 ;;
    --raft-ordered-fanout) RAFT_ORDERED_FANOUT="${2:-1}"; shift 2 ;;
    --broadcast-accept-quorum) GATEWAY_BROADCAST_ACCEPT_QUORUM="${2:-0}"; shift 2 ;;
    --broadcast-result-quorum) GATEWAY_BROADCAST_RESULT_QUORUM="${2:-0}"; shift 2 ;;
    --broadcast-drain-in-timed-run) GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN="${2:-1}"; shift 2 ;;
    --direct-completion-quorum) GATEWAY_DIRECT_COMPLETION_QUORUM="${2:-1}"; shift 2 ;;
    --det-pipeline-depth) DET_PIPELINE_DEPTH="${2:-0}"; DET_PIPELINE_DEPTH_EXPLICIT=1; shift 2 ;;
    --parallelism-mode) PARALLELISM_MODE="${2:-pipeline}"; shift 2 ;;
    --submit-mode)  SUBMIT_MODE="${2:-event}"; shift 2 ;;
    --pg-exec-mode) PG_EXEC_MODE="${2:-event}"; shift 2 ;;
    --det-raw-sql) DET_RAW_SQL="${2:-0}"; shift 2 ;;
    --det-block-parallel) DET_BLOCK_PARALLEL="${2:-1}"; shift 2 ;;
    --det-block-pipeline) DET_BLOCK_PIPELINE="${2:-1}"; shift 2 ;;
    --det-block-max) DET_BLOCK_MAX="${2:-2048}"; shift 2 ;;
    --det-partial-block-max-wait-us) DET_PARTIAL_BLOCK_MAX_WAIT_US="${2:-0}"; shift 2 ;;
    --det-event-block-fastpath) DET_EVENT_BLOCK_FASTPATH="${2:-0}"; shift 2 ;;
    --det-prefixed-direct-parallel) DET_PREFIXED_DIRECT_PARALLEL="${2:-1}"; shift 2 ;;
    --det-completion-only-success) DET_COMPLETION_ONLY_SUCCESS="${2:-1}"; DET_COMPLETION_ONLY_SUCCESS_EXPLICIT=1; shift 2 ;;
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
    --result-publish-replica-limit) ARIABC_RESULT_PUBLISH_REPLICA_LIMIT="${2:-0}"; shift 2 ;;
    --preferred-leader-id) ARIABC_PREFERRED_LEADER_ID="${2:-0}"; shift 2 ;;
    --bcdb-overwrite-protection) BCDB_OVERWRITE_PROTECTION="${2:-0}"; shift 2 ;;
    --pool-size)    DB_CONN_POOL_SIZE="${2:-256}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$DET_START_SEQ" -lt 0 || "$REQ_ID_OFFSET" -lt 1 ]]; then
  echo "ERROR: --det-start-seq must be >= 0 and --req-id-offset must be >= 1 for deterministic cluster runs" >&2
  exit 2
fi
if [[ "$DET_RAW_SQL" == "0" &&
      "$DET_START_SEQ" -ne 0 &&
      "${ARIABC_ALLOW_DET_RESUME:-0}" != "1" ]]; then
  die "Strict deterministic fresh runs must start at --det-start-seq 0. Set ARIABC_ALLOW_DET_RESUME=1 only for a deliberate live-state resume."
fi
if [[ "$DET_RAW_SQL" != "0" && "$DET_RAW_SQL" != "1" ]]; then
  echo "ERROR: --det-raw-sql must be 0 or 1" >&2
  exit 2
fi
if [[ "$DET_EVENT_BLOCK_FASTPATH" != "0" && "$DET_EVENT_BLOCK_FASTPATH" != "1" ]]; then
  echo "ERROR: --det-event-block-fastpath must be 0 or 1" >&2
  exit 2
fi
if [[ "$DET_PREFIXED_DIRECT_PARALLEL" != "0" && "$DET_PREFIXED_DIRECT_PARALLEL" != "1" ]]; then
  echo "ERROR: --det-prefixed-direct-parallel must be 0 or 1" >&2
  exit 2
fi
if [[ "$DET_COMPLETION_ONLY_SUCCESS" != "0" && "$DET_COMPLETION_ONLY_SUCCESS" != "1" ]]; then
  echo "ERROR: --det-completion-only-success must be 0 or 1" >&2
  exit 2
fi
if [[ "$DET_PREFIXED_DIRECT_PARALLEL" == "1" ]]; then
  if [[ "$PG_EXEC_MODE" != "event" ]]; then
    echo "ERROR: --det-prefixed-direct-parallel requires --pg-exec-mode event" >&2
    exit 2
  fi
  if [[ "$DET_RAW_SQL" != "0" ]]; then
    echo "ERROR: --det-prefixed-direct-parallel requires --det-raw-sql 0 so Raft-ordered DET prefixes are preserved" >&2
    exit 2
  fi
  DET_EVENT_BLOCK_FASTPATH=0
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
  echo "ERROR: kafka-only ordering requires Kafka; do not combine --ordering-mode kafka-only with --no-kafka" >&2
  exit 2
fi
case "$KAFKA_COMPLETION_MODE" in
  majority|kafka-majority|kafka_majority|strict-majority|strict_majority)
    KAFKA_COMPLETION_MODE="majority"
    ;;
  majority-async-all3|majority_async_all3|async-all3|async_all3|quorum)
    KAFKA_COMPLETION_MODE="majority_async_all3"
    ;;
  async|async-hash|async_hash|direct)
    KAFKA_COMPLETION_MODE="async"
    ;;
  *)
    echo "ERROR: --kafka-completion-mode must be majority, majority-async-all3, or async (got $KAFKA_COMPLETION_MODE)" >&2
    exit 2
    ;;
esac
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
if [[ "$NUM_TERMINALS" -lt 1 || "$DET_PIPELINE_DEPTH" -lt 0 || "$PER_THREAD_WINDOW" -lt 1 ]]; then
  echo "ERROR: --num-terminals/--threads and --per-thread-window must be >= 1; --det-pipeline-depth must be >= 0" >&2
  exit 2
fi
if [[ -n "$THREADS_ARG" ]]; then
  if [[ "$DET_WINDOW_EXPLICIT" -eq 0 ]]; then
    DET_WINDOW=$(( NUM_TERMINALS * PER_THREAD_WINDOW ))
  fi
  if [[ "$DET_PIPELINE_DEPTH_EXPLICIT" -eq 0 || "$DET_PIPELINE_DEPTH" -eq 0 ]]; then
    DET_PIPELINE_DEPTH="$PER_THREAD_WINDOW"
  fi
  if [[ "$CONN_FANOUT_EXPLICIT" -eq 0 ]]; then
    if [[ "$ORDERING_MODE" == "kafka-only" ]]; then
      CONN_FANOUT="$NUM_TERMINALS"
    fi
  fi
fi
if [[ "$CONN_FANOUT" -lt 1 ]]; then
  echo "ERROR: --conn-fanout must be >= 1" >&2
  exit 2
fi
if [[ "$RAFT_ORDERED_FANOUT" != "0" && "$RAFT_ORDERED_FANOUT" != "1" ]]; then
  echo "ERROR: --raft-ordered-fanout must be 0 or 1" >&2
  exit 2
fi
# os-threads mode does NOT need connFanout > 1 — each gateway subprocess has its
# own single socket.  Only validate the fanout restriction in pipeline mode.
if [[ "$PARALLELISM_MODE" != "os-threads" ]]; then
  if [[ "$ORDERING_MODE" == "raft-kafka" &&
        "$CONN_FANOUT" -gt 1 &&
        "$RAFT_ORDERED_FANOUT" != "1" &&
        "${ARIABC_ALLOW_RAFT_FANOUT:-0}" != "1" ]]; then
    echo "ERROR: --conn-fanout > 1 is disabled for raft-kafka deterministic runs." >&2
    echo "       Enable --raft-ordered-fanout 1 so the leader reorders DET ranges before" >&2
    echo "       Raft append, switch to --ordering-mode kafka-only, --parallelism-mode" >&2
    echo "       os-threads, or set" >&2
    echo "       ARIABC_ALLOW_RAFT_FANOUT=1 for an explicit unsafe experiment." >&2
    exit 2
  fi
fi
case "$PARALLELISM_MODE" in
  pipeline|pipe) PARALLELISM_MODE="pipeline" ;;
  os-threads|os_threads|threads|parallel) PARALLELISM_MODE="os-threads" ;;
  *)
    echo "ERROR: --parallelism-mode must be pipeline or os-threads (got $PARALLELISM_MODE)" >&2
    exit 2
    ;;
esac
if [[ "$GATEWAY_BROADCAST_ACCEPT_QUORUM" -lt 0 ]]; then
  echo "ERROR: --broadcast-accept-quorum must be >= 0" >&2
  exit 2
fi
if [[ "$GATEWAY_BROADCAST_RESULT_QUORUM" -lt 0 ]]; then
  echo "ERROR: --broadcast-result-quorum must be >= 0" >&2
  exit 2
fi
if [[ "$GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN" != "0" && "$GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN" != "1" ]]; then
  echo "ERROR: --broadcast-drain-in-timed-run must be 0 or 1" >&2
  exit 2
fi
if [[ "$GATEWAY_DIRECT_COMPLETION_QUORUM" -lt 1 || "$GATEWAY_DIRECT_COMPLETION_QUORUM" -gt 4 ]]; then
  echo "ERROR: --direct-completion-quorum must be between 1 and 4" >&2
  exit 2
fi
if [[ -z "$BCDB_WORKER_COUNT" ]]; then
  BCDB_WORKER_COUNT="$DB_CONN_POOL_SIZE"
fi
if [[ "$BCDB_WORKER_COUNT" -lt 1 || "$BCDB_WORKER_COUNT" -gt 1024 ]]; then
  echo "ERROR: --bcdb-worker-count must be between 1 and 1024" >&2
  exit 2
fi

# Per-tx event mode dispatches at most DET_BLOCK_PARALLEL queries, while
# BCDB's parse barrier waits for every tx in a bcdb_init-sized block.
# Prevent a 16-active / 256-required circular wait.
if [[ "$PG_EXEC_MODE" == "event" &&
      "$DET_EVENT_BLOCK_FASTPATH" == "0" &&
      "$DET_RAW_SQL" == "0" &&
      "$BCDB_DT_PARSE_BARRIER" == "1" &&
      "$DET_BLOCK_PARALLEL" -lt "$DB_CONN_POOL_SIZE" ]]; then
  log "Auto-disabling BCDB parse barrier: per-tx event cap=$DET_BLOCK_PARALLEL is below BCDB block size=$DB_CONN_POOL_SIZE"
  BCDB_DT_PARSE_BARRIER=0
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
if [[ "$ARIABC_RESULT_PUBLISH_REPLICA_LIMIT" -lt 0 || "$ARIABC_RESULT_PUBLISH_REPLICA_LIMIT" -gt 4 ]]; then
  echo "ERROR: --result-publish-replica-limit must be between 0 and 4" >&2
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
# ===========================================================================
# Function: node_ssh
# Description: Executes a command on a remote node via SSH using sshpass.
# Arguments:
#   $1 (idx)  - Index of the target node (0..3).
#   $@ (rest) - Command to execute.
# Behavior:
#   - Uses sshpass with CLUSTER_PASSWORD.
#   - Optional timeout can be set via NODE_SSH_COMMAND_TIMEOUT.
# ===========================================================================
node_ssh() {
  local idx="$1"; shift
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  local cmd=(sshpass -p "$CLUSTER_PASSWORD" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$user@$ip" "$@")
  if [[ -n "${NODE_SSH_COMMAND_TIMEOUT:-}" && "${NODE_SSH_COMMAND_TIMEOUT:-0}" != "0" ]]; then
    timeout "$NODE_SSH_COMMAND_TIMEOUT" "${cmd[@]}"
  else
    "${cmd[@]}"
  fi
}

# ===========================================================================
# Function: node_rsync_to
# Description: Synchronously syncs a file or directory from the gateway to
#              a remote node using sshpass.
# Arguments:
#   $1 (idx) - Index of the remote node.
#   $2 (src) - Source path on the local gateway.
#   $3 (dst) - Destination path on the remote node.
# ===========================================================================
node_rsync_to() {
  local idx="$1"; local src="$2"; local dst="$3"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "$src" "$user@$ip:$dst"
}

# ===========================================================================
# Function: node_rsync_from
# Description: Synchronously syncs a file or directory from a remote node
#              to the local gateway using sshpass.
# Arguments:
#   $1 (idx) - Index of the remote node.
#   $2 (src) - Source path on the remote node.
#   $3 (dst) - Destination path on the local gateway.
# ===========================================================================
node_rsync_from() {
  local idx="$1"; local src="$2"; local dst="$3"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "$user@$ip:$src" "$dst"
}

# ===========================================================================
# Function: collect_cluster_logs
# Description: Downloads server, NuRaft, PostgreSQL, and optional phase
#              trace logs from all configured remote nodes in the cluster.
# Arguments:
#   $1 (label) - Output header text (optional).
# ===========================================================================
collect_cluster_logs() {
  local label="${1:-Collecting server logs from all nodes...}"
  local log_rsync_timeout="${LOG_RSYNC_TIMEOUT:-20}"
  if [[ "${SKIP_CLUSTER_LOGS:-0}" == "1" ]]; then
    log "Skipping cluster log collection (SKIP_CLUSTER_LOGS=1)"
    return 0
  fi
  log "$label"
  for idx in "${!NODE_IDS[@]}"; do
    id="${NODE_IDS[$idx]}"
    name="${NODE_NAMES[$idx]}"
    ip="${NODE_IPS[$idx]}"
    user="${NODE_USERS[$idx]}"
    REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"
    REMOTE_NURAFT_LOG="/home/neel/ariabc_pg_srv${id}.log"
    REMOTE_PG_LOG="$REMOTE_REPO_ROOT/server.log"
    timeout "$log_rsync_timeout" sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$user@$ip:$REMOTE_SRV_LOG" "$LOG_DIR/server_node${id}_${name}.log" 2>/dev/null || true
    timeout "$log_rsync_timeout" sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$user@$ip:$REMOTE_NURAFT_LOG" "$LOG_DIR/nuraft_node${id}_${name}.log" 2>/dev/null || true
    timeout "$log_rsync_timeout" sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
      "$user@$ip:$REMOTE_PG_LOG" "$LOG_DIR/postgres_node${id}_${name}.log" 2>/dev/null || true
    if [[ "$BCDB_PHASE_TRACE_ON" != "0" ]]; then
      timeout "$log_rsync_timeout" sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_REPO_ROOT/.bench_tmp/bcdb_phase_trace_node${id}.*" \
        "$LOG_DIR/" 2>/dev/null || true
    fi
    if [[ "${ARIABC_OS_PROFILE:-0}" -eq 1 ]]; then
      mkdir -p "$LOG_DIR/os_node${id}_${name}"
      timeout "$log_rsync_timeout" sshpass -p "$CLUSTER_PASSWORD" rsync -az -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "$user@$ip:$REMOTE_LOG_DIR/os_*.log" "$LOG_DIR/os_node${id}_${name}/" 2>/dev/null || true
    fi
  done
}

# ===========================================================================
# Function: node_rsync_repo
# Description: Syncs the local codebase to the remote node using sshpass,
#              omitting git, venv, caches, and test result directories.
# Arguments:
#   $1 (idx) - Index of the remote node.
# ===========================================================================
node_rsync_repo() {
  local idx="$1"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" rsync -az --delete \
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
}

# ===========================================================================
# Function: node_rsync_install
# Description: Syncs the compiled local PostgreSQL installation binaries and
#              library files to the remote node using sshpass.
# Arguments:
#   $1 (idx) - Index of the remote node.
# ===========================================================================
node_rsync_install() {
  local idx="$1"
  local ip="${NODE_IPS[$idx]}"
  local user="${NODE_USERS[$idx]}"
  sshpass -p "$CLUSTER_PASSWORD" rsync -az --delete \
    -e "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "$LOCAL_INSTALL_DIR/" "$user@$ip:$REMOTE_INSTALL_DIR/"
}



# ===========================================================================
# Function: collect_final_profiles_before_fail
# Description: Terminated servers on SIGTERM to flush performance profiles,
#              then fetches all logs from nodes before terminating in a failure path.
# Arguments:
#   $1 (reason) - Reason for failure.
# ===========================================================================
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

# ===========================================================================
# Function: find_local_cmake_tarball
# Description: Checks locally cached locations for a portable CMake tarball
#              to avoid duplicate downloads.
# Returns:
#   Outputs path to stdout and returns 0 if found; returns 1 if not found.
# ===========================================================================
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

# ===========================================================================
# Function: ensure_u22_cmake
# Description: Checks for cmake on the remote Ubuntu 22.04 node, staging and
#              extracting a portable build if missing.
# Arguments:
#   $1 (idx) - Node index.
# ===========================================================================
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

# ===========================================================================
# Function: build_raft_members
# Description: Constructs the list of endpoints to boot the Raft cluster.
# Returns:
#   Outputs a comma-separated list of "id=ip:port" mappings.
# ===========================================================================
build_raft_members() {
  local members=""
  for i in "${!NODE_IDS[@]}"; do
    [[ -n "$members" ]] && members+=","
    members+="${NODE_IDS[$i]}=${NODE_IPS[$i]}:${RAFT_PORT}"
  done
  echo "$members"
}

RAFT_MEMBERS="$(build_raft_members)"

if [[ "$NO_KAFKA" -eq 0 &&
      ( "$KAFKA_COMPLETION_MODE" == "majority" ||
        "$KAFKA_COMPLETION_MODE" == "majority_async_all3" ) ]]; then
  RUN_META_COMPLETION_PATH="kafka_majority"
else
  RUN_META_COMPLETION_PATH="direct"
fi
log "Cluster ordering mode: $ORDERING_MODE (ordering_path=$ORDERING_PATH, bypass_raft=$BYPASS_RAFT, gateway_broadcast_to_all=$GATEWAY_BROADCAST_TO_ALL, kafka_completion_mode=$KAFKA_COMPLETION_MODE)"
{
  printf 'ordering_mode=%s\n' "$ORDERING_MODE"
  printf 'ordering_path=%s\n' "$ORDERING_PATH"
  printf 'cluster_series=%s\n' "$CLUSTER_SERIES"
  printf 'bypass_raft=%s\n' "$BYPASS_RAFT"
  printf 'gateway_broadcast_to_all=%s\n' "$GATEWAY_BROADCAST_TO_ALL"
  printf 'gateway_broadcast_accept_quorum=%s\n' "$GATEWAY_BROADCAST_ACCEPT_QUORUM"
  printf 'gateway_broadcast_result_quorum=%s\n' "$GATEWAY_BROADCAST_RESULT_QUORUM"
  printf 'gateway_broadcast_drain_in_timed_run=%s\n' "$GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN"
  printf 'raft_ordered_fanout=%s\n' "$RAFT_ORDERED_FANOUT"
  printf 'det_event_block_fastpath=%s\n' "$DET_EVENT_BLOCK_FASTPATH"
  printf 'det_prefixed_direct_parallel=%s\n' "$DET_PREFIXED_DIRECT_PARALLEL"
  printf 'det_completion_only_success=%s\n' "$DET_COMPLETION_ONLY_SUCCESS"
  printf 'completion_path=%s\n' "$RUN_META_COMPLETION_PATH"
  printf 'kafka_completion_mode=%s\n' "$KAFKA_COMPLETION_MODE"
  printf 'kafka_bootstrap=%s\n' "$KAFKA_BOOTSTRAP"
  printf 'result_topic=%s\n' "$KAFKA_RESULT_TOPIC"
} > "$LOG_DIR/run_meta.env"

trap cleanup_os_profile EXIT INT TERM

# ---------------------------------------------------------------------------
# Phase 0: Cleanup
# Kill servers by Raft port (9000) rather than by process name — pkill -f/-x
# kills its own SSH session because the binary name appears in bash cmdline.
# fuser -k 9000/tcp avoids this entirely.
# ---------------------------------------------------------------------------
if [[ "$SKIP_CLEANUP" -eq 0 ]]; then
  log "=== Phase 0: Cleanup stale ariabc_pg processes (parallel) ==="
  declare -a CLEANUP_PIDS=()
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    client_port="${NODE_CLIENT_PORTS[$idx]}"
    log "  Killing server on $name (ports 9000, $client_port)"
    node_ssh "$idx" "
      fuser -k 9000/tcp 2>/dev/null || true
      fuser -k ${client_port}/tcp 2>/dev/null || true
      sleep 0.5
    " || true &
    CLEANUP_PIDS+=("$!")
  done
  for pid in "${CLEANUP_PIDS[@]}"; do
    wait "$pid" || true
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
# ---------------------------------------------------------------------------
# Source-hash auto-skip for Phases 0.8 + 1.5
# If the relevant source tree and ring capacity constant haven't changed since
# the last successful build, skip the ~4-minute rebuild automatically.
# Override: FORCE_BUILD=1 to always rebuild even if hash matches.
# Override: SKIP_BUILD=1 to skip rebuild regardless (existing flag).
# ---------------------------------------------------------------------------
BUILD_STAMP_DIR="$REPO_ROOT/scripts/.bench_tmp"
BUILD_STAMP_FILE="$BUILD_STAMP_DIR/build_stamp"
mkdir -p "$BUILD_STAMP_DIR"

# ===========================================================================
# Function: _compute_src_hash
# Description: Computes a SHA256 checksum of code and parameters to determine
#              if remote/local builds can be skipped.
# Returns:
#   Outputs a SHA256 string representing the source tree state.
# ===========================================================================
_compute_src_hash() {
  # Hash C/C++ sources + key header + ring capacity value
  {
    find "$REPO_ROOT/src" "$REPO_ROOT/ariabc_pg" \
      \( -name '*.c' -o -name '*.cpp' -o -name '*.cxx' -o -name '*.h' -o -name 'CMakeLists.txt' \) \
      -not -path '*/build/*' -not -path '*/.git/*' \
      -exec sha256sum {} \; 2>/dev/null | sort
    echo "RESULT_RING_CAPACITY=$RESULT_RING_CAPACITY"
  } | sha256sum | awk '{print $1}'
}

if [[ "${SKIP_BUILD:-0}" -eq 0 ]]; then
  log "  Computing source hash to check if rebuild is needed..."
  _current_hash="$(_compute_src_hash)"
  _stamp_hash=""
  [[ -f "$BUILD_STAMP_FILE" ]] && _stamp_hash="$(cat "$BUILD_STAMP_FILE" 2>/dev/null || true)"
  if [[ "${FORCE_BUILD:-0}" -eq 1 || "$_current_hash" != "$_stamp_hash" ]]; then
    log "  Source changed, first run, or FORCE_BUILD=1 — will rebuild (hash: $(echo "$_current_hash" | head -c 12)...)"
    _BUILD_HASH_TO_SAVE="$_current_hash"
  else
    log "  Source hash unchanged ($(echo "$_current_hash" | head -c 12)...) — skipping Phases 0.8 + 1.5 (pass FORCE_BUILD=1 to override)"
    SKIP_BUILD=1
  fi
fi

if [[ "${SKIP_BUILD:-0}" -eq 0 ]]; then
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
      die "Kafka requested but local $RDKAFKA_LOCAL is missing; rerun without --skip-rdkafka-setup or install rdkafka_local"
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
  # Save build stamp so next run can auto-skip if source unchanged
  [[ -n "${_BUILD_HASH_TO_SAVE:-}" ]] && echo "$_BUILD_HASH_TO_SAVE" > "$BUILD_STAMP_FILE"
fi

# ---------------------------------------------------------------------------
# Phase 1: Sync source files
# Sync the full working tree so BCDB/PostgreSQL backend changes are not lost.
# Phase 0.8 rebuilds the local U24-consumed install/binaries; Phase 1.5
# rebuilds U22 nodes on-host so all replicas run the same PostgreSQL-side
# constants and Kafka-capable server code.
# ---------------------------------------------------------------------------
if [[ "$SKIP_SYNC" -eq 0 ]]; then
  log "=== Phase 1: Sync source and binaries (parallel) ==="

  declare -a SYNC_PIDS=()
  declare -a SYNC_NAMES=()
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    is_u22="${NODE_IS_U22[$idx]}"
    log "  Syncing to $name in background (is_u22=$is_u22)"
    (
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
    ) &
    SYNC_PIDS+=("$!")
    SYNC_NAMES+=("$name")
  done
  SYNC_ALL_OK=1
  for i in "${!SYNC_PIDS[@]}"; do
    if wait "${SYNC_PIDS[$i]}"; then
      log "  [${SYNC_NAMES[$i]}] sync done"
    else
      log "  [${SYNC_NAMES[$i]}] sync FAILED"
      SYNC_ALL_OK=0
    fi
  done
  [[ "$SYNC_ALL_OK" -eq 1 ]] || die "Phase 1 sync failed on one or more nodes"
  log "  Sync done"
fi

# ---------------------------------------------------------------------------
# Phase 1.5: Rebuild Ubuntu 22.04 nodes on-host.
# Ubuntu 24.04 nodes use the synced local install/binary rebuilt in Phase 0.8.
# They intentionally do not rebuild on-host because at least admin123 does not
# have the full build tool chain installed. Phase 3 verifies the recovered
# 1024-slot result ring before any measurement is trusted.
# ---------------------------------------------------------------------------
if [[ "${SKIP_BUILD:-0}" -eq 0 ]]; then
  log "=== Phase 1.5: Build on Ubuntu 22.04 nodes (user4, new-node) — parallel ==="

  # Pre-flight: resolve rdkafka cmake options PER NODE before backgrounding
  # (node_ssh in a subshell is fine; we just can't let die() from a subshell
  #  silently vanish — capture result and check after wait)
  declare -a U22_BUILD_PIDS=()
  declare -a U22_BUILD_LOGS=()
  declare -a U22_BUILD_NAMES=()

  RDKAFKA_DESKTOP="/home/neel/Desktop/rdkafka_local"

  for idx in "${!NODE_IDS[@]}"; do
    is_u22="${NODE_IS_U22[$idx]}"
    [[ "$is_u22" -eq 0 ]] && continue
    name="${NODE_NAMES[$idx]}"
    ip="${NODE_IPS[$idx]}"
    log "  Launching build on $name ($ip) in background"

    # Resolve kafka cmake opt synchronously so we can fail fast before forking
    KAFKA_CMAKE_OPT="-DKAFKA_OPTIONAL=ON"
    if node_ssh "$idx" "test -f $RDKAFKA_DESKTOP/lib/librdkafka.so && test -f $RDKAFKA_DESKTOP/include/librdkafka/rdkafka.h" 2>/dev/null; then
      log "  Found rdkafka_local on $name — will build WITH Kafka support"
      KAFKA_CMAKE_OPT="-DRDKAFKA_INCLUDE_DIR=$RDKAFKA_DESKTOP/include -DRDKAFKA_LIBRARY=$RDKAFKA_DESKTOP/lib/librdkafka.so"
    else
      if [[ "$NO_KAFKA" -eq 0 ]]; then
        die "$RDKAFKA_DESKTOP not found on $name; Kafka cannot be trusted with stub binaries. Rerun without --skip-rdkafka-setup."
      fi
      log "  WARNING: $RDKAFKA_DESKTOP not found on $name — building with stubs for --no-kafka mode"
    fi

    build_log="$LOG_DIR/build_u22_${name}.log"
    # Capture cmake opt in a local so the heredoc closure is correct per iteration
    _kafka_opt="$KAFKA_CMAKE_OPT"
    (
      set -euo pipefail
      echo "[$name] Rebuilding custom PostgreSQL install"
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

      # Push OpenSSL headers
      node_ssh "$idx" "mkdir -p '$REMOTE_OPENSSL_INCLUDE_U22/openssl'" 2>/dev/null || true
      node_rsync_to "$idx" "/usr/include/openssl/" "$REMOTE_OPENSSL_INCLUDE_U22/openssl/"
      node_rsync_to "$idx" "/usr/include/x86_64-linux-gnu/openssl/" "$REMOTE_OPENSSL_INCLUDE_U22/openssl/"

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
EXTRA_CMAKE_ARGS="$_kafka_opt"

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
    ) >"$build_log" 2>&1 &
    U22_BUILD_PIDS+=("$!")
    U22_BUILD_LOGS+=("$build_log")
    U22_BUILD_NAMES+=("$name")
    log "  [$name] build launched (pid $!, log: $build_log)"
  done

  # Wait for all parallel U22 builds
  U22_ALL_OK=1
  for i in "${!U22_BUILD_PIDS[@]}"; do
    pid="${U22_BUILD_PIDS[$i]}"
    name="${U22_BUILD_NAMES[$i]}"
    log_file="${U22_BUILD_LOGS[$i]}"
    if wait "$pid"; then
      log "  [$name] build complete"
      tail -5 "$log_file" | sed "s/^/  [$name] /" || true
    else
      log "  [$name] build FAILED — see $log_file"
      tail -20 "$log_file" | sed "s/^/  [$name] /" || true
      U22_ALL_OK=0
    fi
  done
  [[ "$U22_ALL_OK" -eq 1 ]] || die "Phase 1.5 build failed on one or more U22 nodes"

  log "  Ubuntu 22.04 builds complete; Ubuntu 24.04 nodes will use synced ariabc_cluster build"
fi

# ---------------------------------------------------------------------------
# Phase 1.6: Source and binary provenance.
# U22 nodes use separately built binaries, so a local source hash alone is not
# enough to trust a benchmark. Record executable identities before measurement.
# ---------------------------------------------------------------------------
log "=== Phase 1.6: Source and binary provenance ==="
local_git_head="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
local_gateway_sha="$(sha256sum "$LOCAL_BIN/ariabc_pg_gateway" 2>/dev/null | awk '{print $1}' || echo missing)"
local_server_sha="$(sha256sum "$LOCAL_BIN/ariabc_pg_server" 2>/dev/null | awk '{print $1}' || echo missing)"
local_postgres_sha="$(sha256sum "$LOCAL_INSTALL_DIR/bin/postgres" 2>/dev/null | awk '{print $1}' || echo missing)"
log "  local git_head=$local_git_head"
log "  local ariabc_pg_gateway_sha256=$local_gateway_sha path=$LOCAL_BIN/ariabc_pg_gateway"
log "  local ariabc_pg_server_sha256=$local_server_sha path=$LOCAL_BIN/ariabc_pg_server"
log "  local postgres_sha256=$local_postgres_sha path=$LOCAL_INSTALL_DIR/bin/postgres"

for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  is_u22="${NODE_IS_U22[$idx]}"
  [[ "$is_u22" -eq 1 ]] && srv_bin="$REMOTE_BIN_U22" || srv_bin="$REMOTE_BIN_U24"
  log "  [$name] provenance:"
  node_ssh "$idx" "
    git_head=\$(git -C '$REMOTE_REPO_ROOT' rev-parse HEAD 2>/dev/null || echo unknown)
    srv_sha=\$(sha256sum '$srv_bin' 2>/dev/null | awk '{print \$1}' || echo missing)
    gw_path='$REMOTE_REPO_ROOT/ariabc_pg/build/bin/ariabc_pg_gateway'
    if [[ '$is_u22' -eq 1 ]]; then
      gw_path='/home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_gateway'
    fi
    gw_sha=\$(sha256sum \"\$gw_path\" 2>/dev/null | awk '{print \$1}' || echo missing)
    pg_sha=\$(sha256sum '$REMOTE_INSTALL_DIR/bin/postgres' 2>/dev/null | awk '{print \$1}' || echo missing)
    echo \"git_head=\$git_head\"
    echo \"ariabc_pg_server_sha256=\$srv_sha path=$srv_bin\"
    echo \"ariabc_pg_gateway_sha256=\$gw_sha path=\$gw_path\"
    echo \"postgres_sha256=\$pg_sha path=$REMOTE_INSTALL_DIR/bin/postgres\"
  " 2>&1 | sed "s/^/    /"
done

# ---------------------------------------------------------------------------
# Phase 1.8: Clock Validity Preflight
# ---------------------------------------------------------------------------
log "=== Phase 1.8: Clock Validity Preflight ==="
log "  [local] (gateway) clock metrics:"
(
  date +%s%3N
  timedatectl status || true
  chronyc tracking || true
) 2>&1 | sed "s/^/    /" | tee -a "$LOG_FILE"

for idx in "${!NODE_IDS[@]}"; do
  name="${NODE_NAMES[$idx]}"
  log "  [$name] (server) clock metrics:"
  node_ssh "$idx" "
    date +%s%3N
    timedatectl status || true
    chronyc tracking || true
  " 2>&1 | sed "s/^/    /" | tee -a "$LOG_FILE"
done

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
  --create --topic "$KAFKA_RESULT_TOPIC" --partitions ${#NODE_IDS[@]} --replication-factor 1 \
  --if-not-exists >/dev/null 2>&1 || true
echo "Topic '$KAFKA_RESULT_TOPIC' ready"
KAFKA_EOF
  log "  Kafka ready"

  # --- Kafka consumer-lag preflight (detects stale broker state) ----------
  # After repeated benchmark runs the broker may accumulate old log segments
  # and high-water-mark offsets from prior consumer groups.  A warm broker
  # with 80k+ stale records can add 400-700ms of end-to-end consume latency
  # (vs ~100ms on a clean broker), costing ~1000 TPS in a kafka_majority
  # completion path.  Resetting the topic before each workload eliminates
  # this source of non-determinism.
  log "  Preflight: resetting topic $KAFKA_RESULT_TOPIC to flush stale offsets..."
  node_ssh 0 bash <<KAFKA_FLUSH_EOF
set -euo pipefail
KAFKA_HOME="$KAFKA_HOME_REMOTE"
TOPICS_SH="\$KAFKA_HOME/bin/kafka-topics.sh"
CONSUMER_SH="\$KAFKA_HOME/bin/kafka-console-consumer.sh"
PRODUCER_SH="\$KAFKA_HOME/bin/kafka-console-producer.sh"
BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"
TOPIC="$KAFKA_RESULT_TOPIC"
if ! command -v java >/dev/null 2>&1; then
  export JAVA_HOME="/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64"
  export PATH="\$JAVA_HOME/bin:\$PATH"
fi

# Delete and recreate the topic to zero out all offsets and log segments.
"\$TOPICS_SH" --bootstrap-server "\$BOOTSTRAP" --delete --topic "\$TOPIC" >/dev/null 2>&1 || true
sleep 1
"\$TOPICS_SH" --bootstrap-server "\$BOOTSTRAP" --create --topic "\$TOPIC" --partitions ${#NODE_IDS[@]} --replication-factor 1 --if-not-exists >/dev/null 2>&1
sleep 1

# Quick smoke check: produce+consume a test record to confirm broker is responsive.
TEST_MSG="kafka_preflight_\$(date +%s)"
echo "\$TEST_MSG" | "\$PRODUCER_SH" --bootstrap-server "\$BOOTSTRAP" --topic "\$TOPIC" 2>/dev/null
RESULT="\$("\$CONSUMER_SH" --bootstrap-server "\$BOOTSTRAP" --topic "\$TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null | head -1)"
if [[ "\$RESULT" == *"\$TEST_MSG"* ]]; then
  echo "Kafka preflight PASS (broker responsive, topic fresh)"
else
  echo "Kafka preflight WARN: smoke test message not confirmed (broker may be slow)" >&2
fi
KAFKA_FLUSH_EOF
  log "  Kafka preflight complete"
else
  [[ "$NO_KAFKA" -eq 1 ]] && log "  Skipping Kafka (--no-kafka mode)"
  [[ "$SKIP_KAFKA" -eq 1 ]] && log "  Skipping Kafka setup (--skip-kafka)"
fi

# ---------------------------------------------------------------------------
# Phase 3: Verify BCDB postgres on all configured nodes (parallel)
# Each node's SSH command writes a status line to a temp file so we can run
# all four verify/restart sessions concurrently and validate results serially.
# ---------------------------------------------------------------------------
log "=== Phase 3: Verify BCDB postgres on all ${#NODE_IDS[@]} nodes (parallel) ==="
declare -a PG3_PIDS=()
declare -a PG3_STATUS_FILES=()
for idx in "${!NODE_IDS[@]}"; do
  ip="${NODE_IPS[$idx]}"
  id="${NODE_IDS[$idx]}"
  log "  Checking ${NODE_NAMES[$idx]} (${ip}:${DB_PORT})"
  pg3_status_file="$(mktemp)"
  PG3_STATUS_FILES+=("$pg3_status_file")
  (
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
    > '$REMOTE_REPO_ROOT/server.log'
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
    # -----------------------------------------------------------------------
    # Function: hard_stop_benchmark_postgres (Executed on remote node)
    # Description: Forcibly kills any running PostgreSQL processes associated with
    #              the benchmark user. Cleans up stale postmaster.pid files
    #              and detaches lingering shared memory segments.
    # -----------------------------------------------------------------------
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
    # -----------------------------------------------------------------------
    # Function: ensure_ready (Executed on remote node)
    # Description: Verifies if Postgres is running on DB_PORT. If not, calls
    #              hard_stop_benchmark_postgres and attempts starting it.
    # -----------------------------------------------------------------------
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
    owp=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_overwrite_protection;' 2>/dev/null | tr -d '[:space:]' || true)
    synchronous_commit=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show synchronous_commit;' | tr -d '[:space:]')
    fsync_guc=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show fsync;' | tr -d '[:space:]')
    full_page_writes=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show full_page_writes;' | tr -d '[:space:]')
    wal_level=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show wal_level;' | tr -d '[:space:]')
    if [[ -z \"\$owp\" && '$BCDB_OVERWRITE_PROTECTION' != '0' ]]; then
      echo \"ERROR: --bcdb-overwrite-protection was requested, but this PostgreSQL build does not expose bcdb_overwrite_protection\" >&2
      exit 1
    fi
    owp_display=\"\$owp\"
    [[ -z \"\$owp_display\" ]] && owp_display=unsupported
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
      echo \"reconfiguring bcdb_worker_count=\$worker_count -> $BCDB_WORKER_COUNT bcdb_serial_gate_mode=\$serial_gate -> $BCDB_SERIAL_GATE_MODE bcdb_serial_gate_source=\$serial_gate_source -> $BCDB_SERIAL_GATE_SOURCE bcdb_dt_conflict_tracking=\$dt_conflict -> \$target_dt_conflict bcdb_dt_completion_only_skip_reads=\$dt_skip_reads -> \$target_dt_skip_reads bcdb_dt_hashtab_switch_threshold=\$hashtab_threshold -> $BCDB_DT_HASHTAB_SWITCH_THRESHOLD bcdb_result_ring_slots=\$ring_slots -> \$target_ring_slots bcdb_overwrite_protection=\$owp_display -> \$target_owp max_connections=\$max_connections -> >=\$min_max_connections\"
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
      owp=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show bcdb_overwrite_protection;' 2>/dev/null | tr -d '[:space:]' || true)
      owp_display=\"\$owp\"
      [[ -z \"\$owp_display\" ]] && owp_display=unsupported
      max_connections=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show max_connections;' | tr -d '[:space:]')
      synchronous_commit=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show synchronous_commit;' | tr -d '[:space:]')
      fsync_guc=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show fsync;' | tr -d '[:space:]')
      full_page_writes=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show full_page_writes;' | tr -d '[:space:]')
      wal_level=\$(\$BIN/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -At -c 'show wal_level;' | tr -d '[:space:]')
    fi
    echo \"postgres OK bcdb_worker_count=\$worker_count bcdb_serial_gate_mode=\$serial_gate bcdb_serial_gate_source=\$serial_gate_source bcdb_dt_conflict_tracking=\$dt_conflict bcdb_dt_completion_only_skip_reads=\$dt_skip_reads bcdb_dt_hashtab_switch_threshold=\$hashtab_threshold bcdb_result_ring_slots=\$ring_slots bcdb_overwrite_protection=\$owp_display max_connections=\$max_connections synchronous_commit=\$synchronous_commit fsync=\$fsync_guc full_page_writes=\$full_page_writes wal_level=\$wal_level\"
  " 2>&1)" && echo "$status_line" > "$pg3_status_file" || { echo "FAILED" > "$pg3_status_file"; exit 1; }
  ) &
  PG3_PIDS+=("$!")
done

# --- Wait for all Phase 3 SSH sessions then validate serially ---
PG3_ALL_OK=1
for i in "${!PG3_PIDS[@]}"; do
  idx="$i"
  wait "${PG3_PIDS[$i]}" || { log "  could not verify postgres on ${NODE_NAMES[$i]}"; PG3_ALL_OK=0; }
done
[[ "$PG3_ALL_OK" -eq 1 ]] || die "Phase 3 postgres verify failed on one or more nodes"

for idx in "${!NODE_IDS[@]}"; do
  status_line="$(cat "${PG3_STATUS_FILES[$idx]}" 2>/dev/null || true)"
  rm -f "${PG3_STATUS_FILES[$idx]}"
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
# --- end Phase 3 parallel validation ---

# ---------------------------------------------------------------------------
# Phase 3.2: Ensure the local OS login role exists in Postgres
# Current BCDB worker bootstrap still opens internal libpq connections without
# overriding the role, so they fall back to the service account (`neel` on the
# benchmark nodes). Create that role if it is missing so bcdb_init can start.
# ---------------------------------------------------------------------------

log "=== Phase 3.2: Ensure local benchmark role exists on all ${#NODE_IDS[@]} nodes (parallel) ==="
declare -a ROLE_PIDS=(); declare -a ROLE_NAMES=()
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
  " >/dev/null &
  ROLE_PIDS+=("$!"); ROLE_NAMES+=("$name")
done
for i in "${!ROLE_PIDS[@]}"; do
  wait "${ROLE_PIDS[$i]}" || die "failed to ensure role neel on ${ROLE_NAMES[$i]}"
done

# ---------------------------------------------------------------------------
# Phase 3.5: Restore benchmark table state on all configured nodes
# The distributed run is meaningful only if every replica starts from the same
# table contents and Merkle index.  The restore SQL also calls bcdb_reset().
# ---------------------------------------------------------------------------
if [[ "$SKIP_RESTORE" -eq 0 ]]; then
  log "=== Phase 3.5: Restore $VERIFY_TABLE on all ${#NODE_IDS[@]} nodes (parallel) ==="
  [[ -f "$RESTORE_SQL" ]] || die "restore SQL not found: $RESTORE_SQL"

  declare -a RESTORE_PIDS=(); declare -a RESTORE_NAMES=()
  for idx in "${!NODE_IDS[@]}"; do
    name="${NODE_NAMES[$idx]}"
    remote_restore="$REMOTE_REPO_ROOT/scripts/restore_usertable_small.sql"
    log "  Restoring $VERIFY_TABLE on $name (background)"
    node_ssh "$idx" "
      INSTALL_DIR='$REMOTE_INSTALL_DIR'
      export LD_LIBRARY_PATH=\"\$INSTALL_DIR/lib:\${LD_LIBRARY_PATH:-}\"
      test -f '$remote_restore' || { echo 'missing restore SQL: $remote_restore' >&2; exit 1; }
      \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -f '$remote_restore'
      cnt=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc 'SELECT count(*) FROM $VERIFY_TABLE')
      root=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_root_hash('$VERIFY_TABLE')\")
      verify=\$(\$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -tAc \"SELECT merkle_verify('$VERIFY_TABLE')\")
      echo \"count=\$cnt root=\$root verify=\$verify\"
    " 2>&1 | sed "s/^/  [$name] /" &
    RESTORE_PIDS+=("$!"); RESTORE_NAMES+=("$name")
  done
  RESTORE_ALL_OK=1
  for i in "${!RESTORE_PIDS[@]}"; do
    wait "${RESTORE_PIDS[$i]}" || { log "  restore FAILED on ${RESTORE_NAMES[$i]}"; RESTORE_ALL_OK=0; }
  done
  [[ "$RESTORE_ALL_OK" -eq 1 ]] || die "Phase 3.5 restore failed on one or more nodes"
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
log "=== Phase 4: Starting ariabc_pg_server on all ${#NODE_IDS[@]} nodes ==="

REMOTE_LOG_DIR="/tmp/ariabc_cluster"
KAFKA_ARGS=""
[[ "$NO_KAFKA" -eq 0 ]] && KAFKA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC"

START_ORDER=("${!NODE_IDS[@]}")
if [[ "$ARIABC_PREFERRED_LEADER_ID" -gt 0 ]]; then
  START_ORDER=()
  for idx in "${!NODE_IDS[@]}"; do
    if [[ "${NODE_IDS[$idx]}" == "$ARIABC_PREFERRED_LEADER_ID" ]]; then
      START_ORDER+=("$idx")
    fi
  done
  for idx in "${!NODE_IDS[@]}"; do
    if [[ "${NODE_IDS[$idx]}" != "$ARIABC_PREFERRED_LEADER_ID" ]]; then
      START_ORDER+=("$idx")
    fi
  done
fi
log "  Server start order indices: ${START_ORDER[*]} (preferredLeaderId=$ARIABC_PREFERRED_LEADER_ID)"

for start_pos in "${!START_ORDER[@]}"; do
  idx="${START_ORDER[$start_pos]}"
  id="${NODE_IDS[$idx]}"
  ip="${NODE_IPS[$idx]}"
  name="${NODE_NAMES[$idx]}"
  client_port="${NODE_CLIENT_PORTS[$idx]}"
  is_u22="${NODE_IS_U22[$idx]}"
  [[ "$is_u22" -eq 1 ]] && srv_bin="$REMOTE_BIN_U22" || srv_bin="$REMOTE_BIN_U24"

  log "  Starting server on $name ($ip) — RAFT ID $id, clientPort=$client_port orderingMode=$ORDERING_MODE"
  log "    binary: $srv_bin"
  log "    dbConnPoolSize=$DB_CONN_POOL_SIZE bcdbWorkerCount=$BCDB_WORKER_COUNT bcdbDecoupleWorkers=$BCDB_DECOUPLE_WORKERS bcdbDtConflictTracking=$BCDB_DT_CONFLICT_TRACKING bcdbDtLightSnapshot=$BCDB_DT_LIGHT_SNAPSHOT bcdbDtSkipReadonlyGate=$BCDB_DT_SKIP_READONLY_GATE bcdbDtCompletionOnlySkipReads=$BCDB_DT_COMPLETION_ONLY_SKIP_READS detBlockSkipReadonly=$BCDB_DT_COMPLETION_ONLY_SKIP_READS bcdbDtHashtabSwitchThreshold=$BCDB_DT_HASHTAB_SWITCH_THRESHOLD bcdbDetQueueHighWm=$BCDB_DET_QUEUE_HIGH_WM bcdbDetQueueLowWm=$BCDB_DET_QUEUE_LOW_WM bcdbFlowDebug=$BCDB_FLOW_DEBUG fullResultReplicaLimit=$ARIABC_FULL_RESULT_REPLICA_LIMIT resultPublishReplicaLimit=$ARIABC_RESULT_PUBLISH_REPLICA_LIMIT preferredLeaderId=$ARIABC_PREFERRED_LEADER_ID pgExecMode=$PG_EXEC_MODE raftOrderedFanout=$RAFT_ORDERED_FANOUT detRawSql=$DET_RAW_SQL detBlockParallel=$DET_BLOCK_PARALLEL detBlockPipeline=$DET_BLOCK_PIPELINE detBlockMax=$DET_BLOCK_MAX detPartialBlockMaxWaitUs=$DET_PARTIAL_BLOCK_MAX_WAIT_US detEventBlockFastpath=$DET_EVENT_BLOCK_FASTPATH detPrefixedDirectParallel=$DET_PREFIXED_DIRECT_PARALLEL detCompletionOnlySuccess=$DET_COMPLETION_ONLY_SUCCESS bcdbBlockProfile=$BCDB_BLOCK_PROFILE bcdbBlockWaitWatermark=$BCDB_BLOCK_WAIT_WATERMARK bcdbPhaseTrace=$BCDB_PHASE_TRACE_ON bcdbPollMaxUs=$BCDB_POLL_MAX_US bcdbSerialGateMode=$BCDB_SERIAL_GATE_MODE bcdbSerialGateSource=$BCDB_SERIAL_GATE_SOURCE bcdbDtParseBarrier=$BCDB_DT_PARSE_BARRIER bcdbBlockEnqueueYieldEvery=$BCDB_BLOCK_ENQUEUE_YIELD_EVERY"

  REMOTE_SRV_LOG="$REMOTE_LOG_DIR/server_node${id}.log"

  # rdkafka_local must precede system/install lib paths on ALL nodes so that
  # the source-built v2.3.0 .so is loaded, not whatever the OS happens to have.
  NODE_LIB_PATH="/home/neel/Desktop/rdkafka_local/lib:$REMOTE_INSTALL_DIR/lib"

  node_ssh "$idx" "
	    mkdir -p '$REMOTE_LOG_DIR'
	    rm -f '$REMOTE_SRV_LOG'
	    rm -f '/home/neel/ariabc_pg_srv${id}.log'
	    export LD_LIBRARY_PATH='${NODE_LIB_PATH}:\${LD_LIBRARY_PATH:-}'
	    export ARIABC_PROFILE='${ARIABC_PROFILE:-1}'
	    export ARIABC_DET_BLOCK_PARALLEL='${DET_BLOCK_PARALLEL}'
	    export ARIABC_DET_BLOCK_PIPELINE='${DET_BLOCK_PIPELINE}'
	    export ARIABC_DET_BLOCK_MAX='${DET_BLOCK_MAX}'
	    export ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US='${DET_PARTIAL_BLOCK_MAX_WAIT_US}'
	    export ARIABC_DET_EVENT_BLOCK_FASTPATH='${DET_EVENT_BLOCK_FASTPATH}'
	    export ARIABC_DET_PREFIXED_DIRECT_PARALLEL='${DET_PREFIXED_DIRECT_PARALLEL}'
	    export ARIABC_DET_COMPLETION_ONLY_SUCCESS='${DET_COMPLETION_ONLY_SUCCESS}'
	    export ARIABC_DET_ALLOW_RAW_COMPAT='${DET_RAW_SQL}'
	    export ARIABC_DET_BLOCK_SKIP_READONLY='${BCDB_DT_COMPLETION_ONLY_SKIP_READS}'
	    export ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US='${ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US:-}'
	    export ARIABC_FULL_RESULT_REPLICA_LIMIT='${ARIABC_FULL_RESULT_REPLICA_LIMIT}'
	    export ARIABC_RESULT_PUBLISH_REPLICA_LIMIT='${ARIABC_RESULT_PUBLISH_REPLICA_LIMIT}'
	    export ARIABC_PREFERRED_LEADER_ID='${ARIABC_PREFERRED_LEADER_ID}'
	    export ARIABC_RAFT_ORDERED_FANOUT='${RAFT_ORDERED_FANOUT}'
	    export ARIABC_DET_ORDER_START_SEQ='${DET_START_SEQ}'
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
  if [[ "$ARIABC_PREFERRED_LEADER_ID" -gt 0 && "$start_pos" -eq 0 ]]; then
    sleep 2
  fi
done

log "  All ${#NODE_IDS[@]} server launch commands sent"

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
    if NODE_SSH_COMMAND_TIMEOUT=5 node_ssh "$idx" "ss -tlnp 2>/dev/null | grep -q ':${client_port}'" 2>/dev/null; then
      (( UP++ )) || true
    fi
  done

  if [[ "$UP" -ge ${#NODE_IDS[@]} ]]; then
    log "  All ${#NODE_IDS[@]} server client ports responding (attempt $attempt)"
    ALL_UP=1
    break
  fi

  if [[ $(( attempt % 5 )) -eq 0 ]]; then
    log "  Waiting... $UP/${#NODE_IDS[@]} servers up (${attempt}s elapsed)"
  fi
  sleep 1
done

[[ "$ALL_UP" -eq 0 ]] && log "WARNING: Not all ${#NODE_IDS[@]} nodes responded within ${MAX_WAIT}s"

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
# Skip per-record HMAC signature verification in trusted cluster runs.
# verify_result_signature() is called for every single Kafka reply record
# (N nodes x 20k tx = many HMAC-SHA256 calls per run). In a trusted cluster
# the hash-based majority check is sufficient; full sig verification is only
# needed when running against potentially Byzantine nodes.
export ARIABC_TRUSTED_RESULT_SIG_FASTPATH=1


if [[ ! -f "$WORKLOAD_FILE" ]]; then
  log "  Workload file not found at $WORKLOAD_FILE — using minimal inline test"
  WORKLOAD_FILE="$LOG_DIR/test_workload.sql"
  for i in $(seq 1 "$TEST_QUERIES"); do
    echo "SELECT $i;"
  done > "$WORKLOAD_FILE"
fi

log "  Checking server-startup bcdb_init status..."
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

if [[ "$BCDB_ENABLED" -ne ${#NODE_IDS[@]} || "$BCDB_SKIPPED" -ne 0 || "$BCDB_MISSING" -ne 0 ]]; then
  die "bcdb_init is not uniformly enabled across all ${#NODE_IDS[@]} nodes (enabled=$BCDB_ENABLED skipped=$BCDB_SKIPPED missing=$BCDB_MISSING)"
fi

GW_EXTRA_ARGS=""
if [[ "$NO_KAFKA" -eq 0 ]]; then
  if [[ "$KAFKA_COMPLETION_MODE" == "majority" ]]; then
    GW_EXTRA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC --waitMajority 1 --completionPath kafka_majority --validationMode strict_majority --totalNodes ${#NODE_IDS[@]}"
  elif [[ "$KAFKA_COMPLETION_MODE" == "majority_async_all3" ]]; then
    GW_EXTRA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC --waitMajority 1 --completionPath kafka_majority --validationMode majority_async_all3 --totalNodes ${#NODE_IDS[@]}"
  else
    GW_EXTRA_ARGS="--kafkaBootstrap $KAFKA_BOOTSTRAP --resultTopic $KAFKA_RESULT_TOPIC --waitMajority 0 --completionPath direct --validationMode async_hash --totalNodes ${#NODE_IDS[@]} --directCompletionQuorum $GATEWAY_DIRECT_COMPLETION_QUORUM"
  fi
  if [[ "$GATEWAY_BROADCAST_TO_ALL" -eq 1 ]]; then
    GW_EXTRA_ARGS="$GW_EXTRA_ARGS --broadcastToAll 1"
    if [[ "$GATEWAY_BROADCAST_ACCEPT_QUORUM" -gt 0 ]]; then
      GW_EXTRA_ARGS="$GW_EXTRA_ARGS --broadcastAcceptQuorum $GATEWAY_BROADCAST_ACCEPT_QUORUM"
    fi
    if [[ "$GATEWAY_BROADCAST_RESULT_QUORUM" -gt 0 ]]; then
      GW_EXTRA_ARGS="$GW_EXTRA_ARGS --broadcastResultQuorum $GATEWAY_BROADCAST_RESULT_QUORUM"
    fi
    GW_EXTRA_ARGS="$GW_EXTRA_ARGS --broadcastDrainInTimedRun $GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN"
  fi
else
  GW_EXTRA_ARGS="--waitMajority 0 --completionPath direct --totalNodes ${#NODE_IDS[@]} --directCompletionQuorum $GATEWAY_DIRECT_COMPLETION_QUORUM"
fi

log "  Gateway nodes: $GW_NODES"
log "  Workload:      $WORKLOAD_FILE ($(wc -l < "$WORKLOAD_FILE") statements)"
log "  Mode:          dbType=1 (det) | orderingMode=$ORDERING_MODE | orderingPath=$ORDERING_PATH | kafkaCompletion=$KAFKA_COMPLETION_MODE | completionPath=$(echo $GW_EXTRA_ARGS | grep -o 'completionPath [^ ]*' | cut -d' ' -f2) | broadcastToAll=$GATEWAY_BROADCAST_TO_ALL | broadcastAcceptQuorum=$GATEWAY_BROADCAST_ACCEPT_QUORUM | broadcastResultQuorum=$GATEWAY_BROADCAST_RESULT_QUORUM | broadcastDrainInTimedRun=$GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN | directCompletionQuorum=$GATEWAY_DIRECT_COMPLETION_QUORUM"
log "  DET ids:       detStartSeq=$DET_START_SEQ reqIdOffset=$REQ_ID_OFFSET detWindow=$DET_WINDOW detBatchSize=$DET_BATCH_SIZE terminals=$NUM_TERMINALS connFanout=$CONN_FANOUT raftOrderedFanout=$RAFT_ORDERED_FANOUT broadcastAcceptQuorum=$GATEWAY_BROADCAST_ACCEPT_QUORUM broadcastResultQuorum=$GATEWAY_BROADCAST_RESULT_QUORUM broadcastDrainInTimedRun=$GATEWAY_BROADCAST_DRAIN_IN_TIMED_RUN directCompletionQuorum=$GATEWAY_DIRECT_COMPLETION_QUORUM detPipelineDepth=$DET_PIPELINE_DEPTH submitMode=$SUBMIT_MODE poolSize=$DB_CONN_POOL_SIZE bcdbWorkerCount=$BCDB_WORKER_COUNT bcdbDecoupleWorkers=$BCDB_DECOUPLE_WORKERS bcdbDtConflictTracking=$BCDB_DT_CONFLICT_TRACKING bcdbDtLightSnapshot=$BCDB_DT_LIGHT_SNAPSHOT bcdbDtSkipReadonlyGate=$BCDB_DT_SKIP_READONLY_GATE bcdbDtCompletionOnlySkipReads=$BCDB_DT_COMPLETION_ONLY_SKIP_READS bcdbDtHashtabSwitchThreshold=$BCDB_DT_HASHTAB_SWITCH_THRESHOLD detRawSql=$DET_RAW_SQL detBlockParallel=$DET_BLOCK_PARALLEL detBlockPipeline=$DET_BLOCK_PIPELINE detBlockMax=$DET_BLOCK_MAX detPartialBlockMaxWaitUs=$DET_PARTIAL_BLOCK_MAX_WAIT_US detEventBlockFastpath=$DET_EVENT_BLOCK_FASTPATH detPrefixedDirectParallel=$DET_PREFIXED_DIRECT_PARALLEL detCompletionOnlySuccess=$DET_COMPLETION_ONLY_SUCCESS bcdbBlockProfile=$BCDB_BLOCK_PROFILE bcdbBlockWaitWatermark=$BCDB_BLOCK_WAIT_WATERMARK bcdbPhaseTrace=$BCDB_PHASE_TRACE_ON bcdbPollMaxUs=$BCDB_POLL_MAX_US bcdbSerialGateMode=$BCDB_SERIAL_GATE_MODE bcdbSerialGateSource=$BCDB_SERIAL_GATE_SOURCE bcdbDtParseBarrier=$BCDB_DT_PARSE_BARRIER bcdbBlockEnqueueYieldEvery=$BCDB_BLOCK_ENQUEUE_YIELD_EVERY"
# Print a clear banner that distinguishes pipeline-depth from real OS parallelism
# so this output can be compared honestly against the single-node Python script:
#   pipeline   → N terminal lanes / single reactor (DET window grows, not OS threads)
#   os-threads → N independent gateway subprocesses (real OS-level parallelism)
if [[ "$PARALLELISM_MODE" == "os-threads" ]]; then
  log "  Parallelism:   os-threads (${NUM_TERMINALS} independent gateway procs in parallel, each owns 1/${NUM_TERMINALS} of workload — mirrors Python ThreadPoolExecutor(max_workers=${NUM_TERMINALS}))"
else
  log "  Parallelism:   pipeline (${NUM_TERMINALS} terminal lanes / 1 reactor — pipeline depth scaling only; NOT comparable to OS thread count)"
  log "  NOTE: submit_time in gateway output is the CUMULATIVE sum across async submissions, NOT wall-clock; actual wall time = overall_wall_ms"
fi

START_S="$(date +%s)"

# ---------------------------------------------------------------------------
# Helper: build common gateway args array (shared by both modes)
# ---------------------------------------------------------------------------
_gw_common_args() {
  local det_start="$1"
  local req_offset="$2"
  local num_terms="$3"
  local det_win="$4"
  local client_id="$5"
  printf '%s\n' \
    --nodes "$GW_NODES" \
    --dbType 1 \
    --detRawSql "$DET_RAW_SQL" \
    --detStartSeq "$det_start" \
    --reqIdOffset "$req_offset" \
    --detWindow "$det_win" \
    --detBatchSize "$DET_BATCH_SIZE" \
    --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
    --submitMode "$SUBMIT_MODE" \
    --detSubmitPipeline "$DET_SUBMIT_PIPELINE" \
    --detPipelineDepth "$DET_PIPELINE_DEPTH" \
    --clientId "$client_id" \
    --numTerminals "$num_terms" \
    --connFanout "$CONN_FANOUT"
  [[ -n "${POLL_COUNT:-}" ]] && printf '%s\n' --pollCount "$POLL_COUNT"
  [[ -n "${POLL_INTERVAL_US:-}" ]] && printf '%s\n' --pollIntervalUs "$POLL_INTERVAL_US"
  # Append extra args word-by-word
  for _ga in $GW_EXTRA_ARGS; do printf '%s\n' "$_ga"; done
}

# ---------------------------------------------------------------------------
# Phase 6 execution: OS Profiling setup
# ---------------------------------------------------------------------------
if [[ "${ARIABC_OS_PROFILE:-0}" -eq 1 ]]; then
  log "  Starting OS profiling across servers (mpstat, iostat, sar, pidstat)..."
  for idx in "${!NODE_IDS[@]}"; do
    node_ssh "$idx" "
      mkdir -p '$REMOTE_LOG_DIR'
      nohup mpstat -P ALL 1 > '$REMOTE_LOG_DIR/os_mpstat.log' 2>&1 & echo \$! > '$REMOTE_LOG_DIR/os_mpstat.pid'
      nohup iostat -xz 1 > '$REMOTE_LOG_DIR/os_iostat.log' 2>&1 & echo \$! > '$REMOTE_LOG_DIR/os_iostat.pid'
      nohup sar -n DEV 1 > '$REMOTE_LOG_DIR/os_sar.log' 2>&1 & echo \$! > '$REMOTE_LOG_DIR/os_sar.pid'
      SPID=\$(pgrep -x ariabc_pg_server | head -1 || true)
      if [[ -n \"\$SPID\" ]]; then
        nohup pidstat -dur -p \"\$SPID\" 1 > '$REMOTE_LOG_DIR/os_pidstat.log' 2>&1 & echo \$! > '$REMOTE_LOG_DIR/os_pidstat.pid'
      fi
    "
  done
fi

# ---------------------------------------------------------------------------
# Phase 6 execution: pipeline mode (original — one gateway process)
# ---------------------------------------------------------------------------
if [[ "$PARALLELISM_MODE" == "pipeline" ]]; then
  if ! "$GW_BIN" \
    --nodes "$GW_NODES" \
    --queryFrom "$WORKLOAD_FILE" \
    --dbType 1 \
    --detRawSql "$DET_RAW_SQL" \
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

# ---------------------------------------------------------------------------
# Phase 6 execution: os-threads mode — N independent gateway processes
#
# IMPORTANT ARCHITECTURE NOTE — WHY STRIDED SEQUENCES MATTER:
#   The BCDB deterministic serial gate publishes results in strict ascending
#   DET sequence order across ALL clients simultaneously.
#
#   CONTIGUOUS shards (shard0: seq 1-5k, shard1: 5k-10k) CANNOT provide
#   parallelism: shard1 blocks waiting for shard0's entire range to complete
#   before any of shard1's results are published.  Wall time ≈ sum of shard
#   times → zero speedup.
#
#   STRIDED sequences (shard0: 0,N,2N,...; shard1: 1,N+1,2N+1,...) would
#   work — all shards contribute to every consecutive window of N seqs, so
#   the gate fills in continuously.  BUT the gateway binary has no --detSeqStep
#   flag; it always increments by 1.  We cannot implement strided submission
#   with multiple independent gateway processes.
#
#   The single-gateway --numTerminals N ALREADY implements strided DET sequence
#   assignment internally (terminal i → seqs i, N+i, 2N+i, ...), which is
#   EXACTLY what the Python ThreadPoolExecutor worker stride does.  Pipeline
#   mode IS the correct multi-thread equivalent for raft-kafka.
#
#   For kafka-only (bypass-raft) mode, individual replicas execute and publish
#   results independently without enforcing the cross-client serial gate, so
#   multiple gateway processes with contiguous ranges CAN run in parallel.
# ---------------------------------------------------------------------------
else
  # -------------------------------------------------------------------------
  # Guard: os-threads + raft-kafka serializes at the BCDB serial gate.
  # Redirect to pipeline mode which already implements strided multi-terminal.
  # -------------------------------------------------------------------------
  if [[ "$ORDERING_MODE" == "raft-kafka" ]]; then
    log ""
    log "ERROR: --parallelism-mode os-threads is incompatible with --ordering-mode raft-kafka."
    log ""
    log "  Root cause: The BCDB deterministic serial gate publishes results in strict"
    log "  ascending DET sequence order across ALL clients. Multiple gateway processes"
    log "  with independent sequence ranges cannot receive completions in parallel —"
    log "  each waits for ALL preceding sequences (from other shards) to be published"
    log "  first. Wall time with N shards ≈ N × single-shard time (WORSE than 1 shard)."
    log ""
    log "  The gateway binary does not support --detSeqStep, so strided multi-process"
    log "  sharding (the only approach that would work) cannot be implemented externally."
    log ""
    log "  SOLUTION: --parallelism-mode pipeline already implements strided DET"
    log "  sequence assignment across --numTerminals N inside a single gateway process."
    log "  This is structurally identical to the Python benchmark's ThreadPoolExecutor:"
    log "    Python:   procSeqNum + worker_idx + next_local × N  (strided per thread)"
    log "    Pipeline: terminal_i → seqs i, N+i, 2N+i, ...      (strided per lane)"
    log "  Both fill the serial gate continuously → no blocking → true N× submission."
    log ""
    log "  To test throughput at higher load: use --parallelism-mode pipeline --threads N"
    log "  To test with actual separate processes: use --ordering-mode kafka-only"
    log ""
    echo "FATAL: os-threads is not supported for raft-kafka ordering (see explanation above)" >&2
    exit 2
  fi

  # -------------------------------------------------------------------------
  # kafka-only + os-threads: contiguous shards DO work because bypass-raft
  # servers execute and publish results per-shard independently.
  # -------------------------------------------------------------------------
  log "  [os-threads] Splitting workload into $NUM_TERMINALS contiguous shards (kafka-only mode)..."

  TOTAL_QUERIES="$(awk 'BEGIN{n=0} /^[[:space:]]*($|--)/{next} {n++} END{print n}' "$WORKLOAD_FILE")"
  if [[ "$TOTAL_QUERIES" -lt "$NUM_TERMINALS" ]]; then
    log "  WARNING: workload has $TOTAL_QUERIES statements but NUM_TERMINALS=$NUM_TERMINALS; reducing to 1 shard"
    NUM_TERMINALS=1
  fi

  ACTUAL_SHARDS="$NUM_TERMINALS"
  SHARD_DIR="$LOG_DIR/shards"
  mkdir -p "$SHARD_DIR"
  SHARD_SIZE=$(( (TOTAL_QUERIES + NUM_TERMINALS - 1) / NUM_TERMINALS ))

  for s in $(seq 0 $(( ACTUAL_SHARDS - 1 ))); do > "$SHARD_DIR/shard_${s}.sql"; done

  shard_idx=0; line_count=0
  while IFS= read -r wline; do
    stripped="${wline#"${wline%%[! ]*}"}"
    [[ -z "$stripped" ]] && continue
    [[ "$stripped" == --* ]] && continue
    [[ "$stripped" == /*  ]] && continue
    [[ "$stripped" == \\* ]] && continue
    echo "$wline" >> "$SHARD_DIR/shard_${shard_idx}.sql"
    (( line_count++ )) || true
    if (( line_count >= SHARD_SIZE )) && (( shard_idx + 1 < ACTUAL_SHARDS )); then
      (( shard_idx++ )) || true
      line_count=0
    fi
  done < "$WORKLOAD_FILE"

  for s in $(seq 0 $(( ACTUAL_SHARDS - 1 ))); do
    sc="$(wc -l < "$SHARD_DIR/shard_${s}.sql")"
    log "  [os-threads]   shard_${s}.sql: $sc statements, detStartSeq=$(( DET_START_SEQ + s * SHARD_SIZE ))"
  done

  declare -a OSTH_PIDS=()
  declare -a OSTH_LOGS=()

  for s in $(seq 0 $(( ACTUAL_SHARDS - 1 ))); do
    shard_start_seq=$(( DET_START_SEQ + s * SHARD_SIZE ))
    shard_req_offset=$(( REQ_ID_OFFSET + s * SHARD_SIZE ))
    shard_log="$LOG_DIR/gateway_shard${s}.log"
    OSTH_LOGS+=("$shard_log")
    log "  [os-threads] Launching shard $s (detStartSeq=$shard_start_seq)"

    "$GW_BIN" \
      --nodes "$GW_NODES" \
      --queryFrom "$SHARD_DIR/shard_${s}.sql" \
      --dbType 1 \
      --detRawSql "$DET_RAW_SQL" \
      --detStartSeq "$shard_start_seq" \
      --reqIdOffset "$shard_req_offset" \
      --detWindow "$DET_WINDOW" \
      --detBatchSize "$DET_BATCH_SIZE" \
      --dbConnPoolSize "$DB_CONN_POOL_SIZE" \
      --submitMode "$SUBMIT_MODE" \
      --detSubmitPipeline "$DET_SUBMIT_PIPELINE" \
      --detPipelineDepth "$DET_PIPELINE_DEPTH" \
      ${POLL_COUNT:+--pollCount $POLL_COUNT} \
      ${POLL_INTERVAL_US:+--pollIntervalUs $POLL_INTERVAL_US} \
      --clientId "cluster-ycsb-shard${s}" \
      --numTerminals 1 \
      --connFanout 1 \
      $GW_EXTRA_ARGS \
      >"$shard_log" 2>&1 &
    OSTH_PIDS+=("$!")
    log "  [os-threads]   shard $s pid=$!"
  done

  OSTH_MAX_MS=0; OSTH_TOTAL_QUERIES=0; OSTH_ANY_FAILED=0

  for s in $(seq 0 $(( ACTUAL_SHARDS - 1 ))); do
    pid="${OSTH_PIDS[$s]}"
    shard_log="${OSTH_LOGS[$s]}"
    if wait "$pid"; then shard_status="ok"; else shard_status="failed"; OSTH_ANY_FAILED=1; fi
    shard_ms="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$shard_log" 2>/dev/null | head -1 || echo 0)"
    shard_q="$(grep -oP 'loaded \K[0-9]+(?= queries)' "$shard_log" 2>/dev/null | head -1 || echo 0)"
    (( OSTH_TOTAL_QUERIES += shard_q )) || true
    if [[ "$shard_ms" -gt "$OSTH_MAX_MS" ]]; then OSTH_MAX_MS="$shard_ms"; fi
    log "  [os-threads] shard $s done: status=$shard_status wall_ms=$shard_ms queries=$shard_q"
    grep -E '^PROGRESS_GATEWAY_DET|^overall|^duplicate_key|^permanent_failures|^divergence_count' \
      "$shard_log" 2>/dev/null | sed "s/^/  [shard${s}] /" || true
  done

  cat "${OSTH_LOGS[@]}" > "$GW_LOG" 2>/dev/null || true

  log "  [os-threads] All $ACTUAL_SHARDS shard processes finished"
  log "  [os-threads] Aggregate queries : $OSTH_TOTAL_QUERIES"
  log "  [os-threads] Max shard wall_ms : $OSTH_MAX_MS"
  if [[ "$OSTH_MAX_MS" -gt 0 && "$OSTH_TOTAL_QUERIES" -gt 0 ]]; then
    OSTH_AGG_TPS=$(( OSTH_TOTAL_QUERIES * 1000 / OSTH_MAX_MS ))
    log "  [os-threads] Aggregate TPS     : ~${OSTH_AGG_TPS} tx/s (total_queries/max_shard_ms)"
    {
      echo "OS_THREADS_AGGREGATE queries=$OSTH_TOTAL_QUERIES max_shard_wall_ms=$OSTH_MAX_MS aggregate_tps=$OSTH_AGG_TPS shards=$ACTUAL_SHARDS"
      echo "overall time taken (millisec) = $OSTH_MAX_MS"
    } >> "$GW_LOG"
  fi
  [[ "$OSTH_ANY_FAILED" -ne 0 ]] && log "WARNING: One or more shard gateway processes failed — check $LOG_DIR/gateway_shard*.log"
fi

END_S="$(date +%s)"
ELAPSED=$(( END_S - START_S ))

# OS profiling cleanup is now handled by trap on exit, but we trigger it early here
# so it finishes exactly when the test finishes.
cleanup_os_profile


# ---------------------------------------------------------------------------
# Phase 7: Results
# ---------------------------------------------------------------------------
log "=== Phase 7: Results ==="

WORKLOAD_LINES="$(awk 'BEGIN{n=0} /^[[:space:]]*($|--)/{next} {n++} END{print n}' "$WORKLOAD_FILE")"

# Prefer gateway's own reported time (excludes restore, file load, and leader probe).
# Falls back to shell wall-clock only when gateway log lacks the line.
# NOTE: In pipeline mode, "submit time (ms)" in the gateway output is CUMULATIVE
# across async operations, not wall-clock. The correct wall-clock is overall_wall_ms.
GW_MS="$(grep -oP 'overall time taken \(millisec\) = \K[0-9]+' "$GW_LOG" 2>/dev/null | head -1 || echo '')"
# In os-threads mode the concatenated log contains one 'overall time' per shard;
# head -1 would pick only shard 0's (shortest) time, yielding inflated TPS.
# Use the already-computed OSTH_MAX_MS which is the true wall clock.
if [[ "$PARALLELISM_MODE" == "os-threads" && "${OSTH_MAX_MS:-0}" -gt 0 ]]; then
  GW_MS="$OSTH_MAX_MS"
fi
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
  MARKER_EXTRA_ARGS=()
  MARKER_ALL_NODES="${#NODE_IDS[@]}"
  if [[ "$BYPASS_RAFT" -eq 1 && "$GATEWAY_BROADCAST_TO_ALL" -eq 1 ]]; then
    # The measured workload may use client-visible accept quorum, but the
    # correctness marker is a barrier: every replica must accept and execute it
    # so all prior deterministic sequence numbers are drained before Merkle.
    MARKER_EXTRA_ARGS+=(--broadcastAcceptQuorum "$MARKER_ALL_NODES")
    MARKER_EXTRA_ARGS+=(--broadcastResultQuorum "$MARKER_ALL_NODES")
    MARKER_EXTRA_ARGS+=(--broadcastDrainInTimedRun 1)
    log "  Marker barrier override: broadcastAcceptQuorum=$MARKER_ALL_NODES broadcastResultQuorum=$MARKER_ALL_NODES broadcastDrainInTimedRun=1"
  elif [[ "$BYPASS_RAFT" -eq 0 && "$KAFKA_COMPLETION_MODE" == "async" ]]; then
    # Normal raft-kafka workload completion waits for the submit node only.
    # The marker is the correctness barrier, so require every replica to apply
    # the marker log entry before post-run Merkle/root checks.
    MARKER_EXTRA_ARGS+=(--directCompletionQuorum "$MARKER_ALL_NODES")
    log "  Marker barrier override: directCompletionQuorum=$MARKER_ALL_NODES"
  fi
  if ! "$GW_BIN" \
    --nodes "$GW_NODES" \
    --queryFrom "$MARKER_FILE" \
    --dbType 1 \
    --detRawSql "$DET_RAW_SQL" \
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
    "${MARKER_EXTRA_ARGS[@]}" \
    2>&1 | tee "$MARKER_LOG"; then
    log "WARNING: Marker gateway exited non-zero — check $MARKER_LOG"
  fi

  log "  Waiting until marker is visible on all ${#NODE_IDS[@]} nodes"
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
