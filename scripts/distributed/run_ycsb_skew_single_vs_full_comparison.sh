#!/usr/bin/env bash
set -euo pipefail

#
# Run one graph-ready YCSB-skew comparison:
#   - single machine majority-pivot node: PG and unsigned DET
#   - trusted 4-node cluster modes:
#       * Kafka-only: preordered direct broadcast + Kafka validation + BCDB
#       * Raft+Kafka: Raft ordering + Kafka validation + BCDB
#
# Outputs under scripts/bench_full_results/ycsb_skew_compare_<timestamp>/:
#   results.csv, summary.csv, overhead.csv, ycsb_skew_tps_comparison.png
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TARGET_NODE="${TARGET_NODE:-neel@10.129.148.236}"
TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL:-}"
TARGETS_DEFAULT="neel@10.129.148.236,neel@10.129.27.54,neel@10.129.148.248"
TARGETS="${TARGETS:-}"
REMOTE_REPO="${REMOTE_REPO:-/home/neel/Desktop/ariabc_cluster}"
REMOTE_INSTALL="${REMOTE_INSTALL:-/home/neel/Desktop/ariabc_install}"
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"
TEMPLATE_CONF_LOCAL="${TEMPLATE_CONF_LOCAL:-/work/ARIABC/pgdata/postgresql.conf}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_PORT="${SSH_PORT:-22}"

THREADS="${THREADS:-1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}"
RUNS="${RUNS:-3}"
DEFAULT_WORKLOADS="ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt,ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt"
WORKLOADS="${WORKLOADS:-${WORKLOAD:-$DEFAULT_WORKLOADS}}"
DB_PORT="${DB_PORT:-5438}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"
EXPERIMENT_MODE="${EXPERIMENT_MODE:-pipeline-saturation}" # pipeline-saturation|strict-overhead

# The default graph uses literal deterministic gateway client lanes on the
# x-axis.  FULL_THREAD_KNOB=concurrency remains available for capacity probes
# that sweep ordered in-flight budget while keeping --num-terminals=1.
FULL_THREAD_KNOB="${FULL_THREAD_KNOB:-client-pipeline}"
FULL_POOL_SIZE_MODE="${FULL_POOL_SIZE_MODE:-fixed}" # fixed|sweep
FULL_FIXED_POOL_SIZE="${FULL_FIXED_POOL_SIZE:-256}"
FULL_DET_BATCH_SIZE_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE+x}" ]]; then
  FULL_DET_BATCH_SIZE_WAS_SET=1
fi
FULL_DET_BATCH_SIZE="${FULL_DET_BATCH_SIZE:-256}"
FULL_DET_BATCH_SIZE_KAFKA_ONLY_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE_KAFKA_ONLY+x}" ]]; then
  FULL_DET_BATCH_SIZE_KAFKA_ONLY_WAS_SET=1
fi
FULL_DET_BATCH_SIZE_KAFKA_ONLY="${FULL_DET_BATCH_SIZE_KAFKA_ONLY:-$FULL_DET_BATCH_SIZE}"
FULL_DET_BATCH_SIZE_RAFT_KAFKA_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE_RAFT_KAFKA+x}" ]]; then
  FULL_DET_BATCH_SIZE_RAFT_KAFKA_WAS_SET=1
fi
FULL_DET_BATCH_SIZE_RAFT_KAFKA="${FULL_DET_BATCH_SIZE_RAFT_KAFKA:-$FULL_DET_BATCH_SIZE}"
FULL_DET_PIPELINE_DEPTH_WAS_SET=0
if [[ -n "${FULL_DET_PIPELINE_DEPTH+x}" ]]; then
  FULL_DET_PIPELINE_DEPTH_WAS_SET=1
fi
FULL_DET_PIPELINE_DEPTH="${FULL_DET_PIPELINE_DEPTH:-80}"
FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY="${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY:-64}"
# Keep Kafka-only and Raft+Kafka on the same per-lane depth so the Raft series
# is not structurally capped below Kafka-only by a benchmark knob mismatch.
FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA="${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA:-64}"
# Optional per-thread depth overrides, e.g. FULL_DET_PIPELINE_DEPTH_*_MAP=1:512.
# Defaults are intentionally empty so --threads 1,4,8,12 shows real scaling from
# 64 -> 256 -> 512 -> 768 effective in-flight requests, instead of inflating
# the thread=1 point to match thread=4.
FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP="${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP-}"
FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP="${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP-}"
FULL_DET_WINDOW="${FULL_DET_WINDOW:-4096}"
FULL_DET_WINDOW_MULTIPLIER="${FULL_DET_WINDOW_MULTIPLIER:-256}"
FULL_DET_WINDOW_MAX="${FULL_DET_WINDOW_MAX:-4096}"
FULL_DET_BATCH_SIZE_MAP_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE_MAP+x}" ]]; then
  FULL_DET_BATCH_SIZE_MAP_WAS_SET=1
fi
FULL_DET_BATCH_SIZE_MAP="${FULL_DET_BATCH_SIZE_MAP:-}"
FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP="${FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP:-}"
FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP="${FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP:-}"
FULL_DET_WINDOW_MAP_WAS_SET=0
if [[ -n "${FULL_DET_WINDOW_MAP+x}" ]]; then
  FULL_DET_WINDOW_MAP_WAS_SET=1
fi
FULL_DET_WINDOW_MAP="${FULL_DET_WINDOW_MAP:-}"
FULL_DET_WINDOW_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_DET_WINDOW_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_DET_WINDOW_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_DET_WINDOW_KAFKA_ONLY_MAP="${FULL_DET_WINDOW_KAFKA_ONLY_MAP:-}"
FULL_DET_WINDOW_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_DET_WINDOW_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_DET_WINDOW_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_DET_WINDOW_RAFT_KAFKA_MAP="${FULL_DET_WINDOW_RAFT_KAFKA_MAP:-}"
FULL_DET_BLOCK_PARALLEL="${FULL_DET_BLOCK_PARALLEL:-1}"
FULL_DET_BLOCK_PIPELINE="${FULL_DET_BLOCK_PIPELINE:-8}"
FULL_DET_BLOCK_MAX="${FULL_DET_BLOCK_MAX:-2048}"
FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US="${FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US:-0}"
FULL_BCDB_WORKER_COUNT="${FULL_BCDB_WORKER_COUNT:-512}"
FULL_BCDB_DECOUPLE_WORKERS="${FULL_BCDB_DECOUPLE_WORKERS:-1}"
FULL_TEST_QUERIES="${FULL_TEST_QUERIES:-20512}"
FULL_BCDB_BLOCK_PROFILE="${FULL_BCDB_BLOCK_PROFILE:-0}"
FULL_BCDB_BLOCK_WAIT_WATERMARK="${FULL_BCDB_BLOCK_WAIT_WATERMARK:-0}"
FULL_BCDB_SERIAL_GATE_MODE="${FULL_BCDB_SERIAL_GATE_MODE:-1}"
FULL_BCDB_SERIAL_GATE_SOURCE="${FULL_BCDB_SERIAL_GATE_SOURCE:-0}"
FULL_BCDB_DT_PARSE_BARRIER="${FULL_BCDB_DT_PARSE_BARRIER:-1}"
FULL_BCDB_DT_SKIP_READONLY_GATE="${FULL_BCDB_DT_SKIP_READONLY_GATE:-1}"
FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS="${FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS:-0}"
FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD="${FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD:-65536}"
FULL_BCDB_DET_QUEUE_HIGH_WM="${FULL_BCDB_DET_QUEUE_HIGH_WM:-4096}"
FULL_BCDB_DET_QUEUE_LOW_WM="${FULL_BCDB_DET_QUEUE_LOW_WM:-2048}"
FULL_CONN_FANOUT="${FULL_CONN_FANOUT:-}"
FULL_CONN_FANOUT_KAFKA_ONLY="${FULL_CONN_FANOUT_KAFKA_ONLY:-1}"
FULL_CONN_FANOUT_RAFT_KAFKA="${FULL_CONN_FANOUT_RAFT_KAFKA:-1}"
FULL_CONN_FANOUT_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_CONN_FANOUT_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_CONN_FANOUT_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_CONN_FANOUT_KAFKA_ONLY_MAP="${FULL_CONN_FANOUT_KAFKA_ONLY_MAP:-}"
FULL_CONN_FANOUT_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_CONN_FANOUT_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_CONN_FANOUT_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_CONN_FANOUT_RAFT_KAFKA_MAP="${FULL_CONN_FANOUT_RAFT_KAFKA_MAP:-}"
FULL_BROADCAST_ACCEPT_QUORUM="${FULL_BROADCAST_ACCEPT_QUORUM:-}"
FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY_WAS_SET=0
if [[ -n "${FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY+x}" ]]; then
  FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY_WAS_SET=1
fi
FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY="${FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY:-3}"
FULL_BROADCAST_ACCEPT_QUORUM_RAFT_KAFKA="${FULL_BROADCAST_ACCEPT_QUORUM_RAFT_KAFKA:-0}"
FULL_BROADCAST_RESULT_QUORUM="${FULL_BROADCAST_RESULT_QUORUM:-}"
FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_WAS_SET=0
if [[ -n "${FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY+x}" ]]; then
  FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_WAS_SET=1
fi
FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY="${FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY:-0}"
FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP="${FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP:-}"
FULL_BROADCAST_RESULT_QUORUM_RAFT_KAFKA="${FULL_BROADCAST_RESULT_QUORUM_RAFT_KAFKA:-0}"
FULL_BROADCAST_DRAIN_IN_TIMED_RUN_WAS_SET=0
if [[ -n "${FULL_BROADCAST_DRAIN_IN_TIMED_RUN+x}" ]]; then
  FULL_BROADCAST_DRAIN_IN_TIMED_RUN_WAS_SET=1
fi
FULL_BROADCAST_DRAIN_IN_TIMED_RUN="${FULL_BROADCAST_DRAIN_IN_TIMED_RUN:-}"
FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY_WAS_SET=0
if [[ -n "${FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY+x}" ]]; then
  FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY_WAS_SET=1
fi
FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY="${FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY:-0}"
FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA_WAS_SET=0
if [[ -n "${FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA+x}" ]]; then
  FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA_WAS_SET=1
fi
FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA="${FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA:-1}"
FULL_DIRECT_COMPLETION_QUORUM="${FULL_DIRECT_COMPLETION_QUORUM:-}"
FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY="${FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY:-1}"
FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA="${FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA:-1}"
FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY_MAP="${FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY_MAP:-}"
FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP="${FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP:-}"
FULL_PREFERRED_LEADER_ID="${FULL_PREFERRED_LEADER_ID:-1}"
FULL_RESULT_REPLICA_LIMIT_WAS_SET=0
if [[ -n "${FULL_RESULT_REPLICA_LIMIT+x}" ]]; then
  FULL_RESULT_REPLICA_LIMIT_WAS_SET=1
fi
FULL_RESULT_REPLICA_LIMIT="${FULL_RESULT_REPLICA_LIMIT:-1}"
FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY="${FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY:-$FULL_RESULT_REPLICA_LIMIT}"
FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA="${FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA:-$FULL_RESULT_REPLICA_LIMIT}"
FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP="${FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP:-}"
FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP="${FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP:-}"
FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET=0
if [[ -n "${FULL_RESULT_PUBLISH_REPLICA_LIMIT+x}" ]]; then
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET=1
fi
FULL_RESULT_PUBLISH_REPLICA_LIMIT="${FULL_RESULT_PUBLISH_REPLICA_LIMIT:-3}"
FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_WAS_SET=0
if [[ -n "${FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY+x}" ]]; then
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_WAS_SET=1
fi
FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY="${FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY:-$FULL_RESULT_PUBLISH_REPLICA_LIMIT}"
FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET=0
if [[ -n "${FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP+x}" ]]; then
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET=1
fi
FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP="${FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP:-}"
FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_WAS_SET=0
if [[ -n "${FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA+x}" ]]; then
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_WAS_SET=1
fi
FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA="${FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA:-$FULL_RESULT_PUBLISH_REPLICA_LIMIT}"
FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP_WAS_SET=0
if [[ -n "${FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP+x}" ]]; then
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP_WAS_SET=1
fi
FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP="${FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP:-}"
FULL_CASE_TIMEOUT_S="${FULL_CASE_TIMEOUT_S:-900}"
FULL_SKIP_SYNC="${FULL_SKIP_SYNC:-0}"
FULL_SKIP_BUILD="${FULL_SKIP_BUILD:-0}"
FULL_SKIP_RDKAFKA_SETUP="${FULL_SKIP_RDKAFKA_SETUP:-1}"
FULL_SKIP_CLUSTER_LOGS="${FULL_SKIP_CLUSTER_LOGS:-1}"
POLL_COUNT="${POLL_COUNT:-120000}"
RESULT_RING_CAPACITY="${RESULT_RING_CAPACITY:-2048}"
FULL_CONTINUE_ON_ERROR="${FULL_CONTINUE_ON_ERROR:-0}"
FULL_CLUSTER_MODES="${FULL_CLUSTER_MODES:-kafka-only,raft-kafka}"
FULL_KAFKA_COMPLETION_MODE="${FULL_KAFKA_COMPLETION_MODE:-async}" # async|majority
SINGLE_TARGET_PICK="${SINGLE_TARGET_PICK:-majority-pivot}" # fastest|majority-pivot|slowest|index:N
SINGLE_GATEWAY_DIRECT="${SINGLE_GATEWAY_DIRECT:-1}"
SINGLE_GATEWAY_CLIENT_PORT_BASE="${SINGLE_GATEWAY_CLIENT_PORT_BASE:-19100}"
SINGLE_GATEWAY_RAFT_PORT_BASE="${SINGLE_GATEWAY_RAFT_PORT_BASE:-19200}"
SINGLE_GATEWAY_POOL_SIZE="${SINGLE_GATEWAY_POOL_SIZE:-}"
SINGLE_GATEWAY_BCDB_WORKER_COUNT="${SINGLE_GATEWAY_BCDB_WORKER_COUNT:-}"
SINGLE_GATEWAY_CASE_TIMEOUT_S="${SINGLE_GATEWAY_CASE_TIMEOUT_S:-$FULL_CASE_TIMEOUT_S}"
SINGLE_GATEWAY_CONN_FANOUT="${SINGLE_GATEWAY_CONN_FANOUT:-4}"
SINGLE_BCDB_SERIAL_GATE_MODE="${SINGLE_BCDB_SERIAL_GATE_MODE:-0}"
SINGLE_BCDB_SERIAL_GATE_SOURCE="${SINGLE_BCDB_SERIAL_GATE_SOURCE:-0}"
SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK="${SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK:-on}"
SINGLE_BCDB_POLL_MAX_US="${SINGLE_BCDB_POLL_MAX_US:-1}"
SINGLE_BCDB_DT_PARSE_BARRIER="${SINGLE_BCDB_DT_PARSE_BARRIER:-0}"
SINGLE_BCDB_DT_LIGHT_SNAPSHOT="${SINGLE_BCDB_DT_LIGHT_SNAPSHOT:-0}"
SINGLE_BCDB_DT_SKIP_READONLY_GATE="${SINGLE_BCDB_DT_SKIP_READONLY_GATE:-1}"
SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS="${SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS:-$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS}"
SINGLE_BCDB_RESULT_RING_SLOTS="${SINGLE_BCDB_RESULT_RING_SLOTS:-$RESULT_RING_CAPACITY}"
SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS="${SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS:-0}"
SINGLE_BCDB_FLOW_DEBUG="${SINGLE_BCDB_FLOW_DEBUG:-0}"
SINGLE_BCDB_GATE_DEBUG="${SINGLE_BCDB_GATE_DEBUG:-0}"
SINGLE_BCDB_APPLY_WAIT_DEBUG="${SINGLE_BCDB_APPLY_WAIT_DEBUG:-0}"
SINGLE_TIMEOUT_WORKLOAD_S="${SINGLE_TIMEOUT_WORKLOAD_S:-0}"
SINGLE_TIMEOUT_WORKLOAD_DET_S="${SINGLE_TIMEOUT_WORKLOAD_DET_S:-1800}"
SINGLE_REMOTE_KILL_STALE="${SINGLE_REMOTE_KILL_STALE:-0}"
SINGLE_BCDB_PHASE_TRACE_PREFIX="${SINGLE_BCDB_PHASE_TRACE_PREFIX:-}"
SINGLE_PYTHON_BIN="${SINGLE_PYTHON_BIN:-}"

SKIP_SYNC="${SKIP_SYNC:-0}"
SINGLE_ONLY="${SINGLE_ONLY:-0}"
FULL_ONLY="${FULL_ONLY:-0}"
ANALYZE_ONLY="${ANALYZE_ONLY:-0}"
NO_RESUME="${NO_RESUME:-0}"

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --threads CSV       Default: $THREADS
  --runs N            Default: $RUNS
  --workload FILE     Single workload (back-compat).
  --workloads CSV     Comma-separated list of workload files; each produces its
                      own subdir + graph under OUT_ROOT. Default: $WORKLOADS
  --target NODE       Single-target back-compat shortcut for --targets NODE.
  --targets CSV       Comma-separated user@host list to run the single-node bench
                      on in parallel. Default: $TARGETS_DEFAULT
                      One single_<label>/ subdir + graph per target. The
                      SINGLE_TARGET_PICK node (by peak DET median TPS across
                      the thread sweep) is then used for the gateway-direct
                      baseline and combined comparison plots.
  --target-label NAME Label used in CSV/graph for the chosen comparison target.
  --skip-sync
  --single-only
  --full-only
  --cluster-modes CSV
                      Cluster series to run for the full-system side.
                      Default: $FULL_CLUSTER_MODES
                      Values: kafka-only,raft-kafka
  --experiment-mode MODE
                      Default: $EXPERIMENT_MODE
                      pipeline-saturation keeps the gateway pipeline surface and
                      reports effective in-flight work. strict-overhead forces
                      full-system detPipelineDepth=1 so x-axis concurrency is
                      comparable to direct single-node threads.
  --analyze-only      Rebuild combined CSVs/graph from existing manifest/files.
  --no-resume         Pass --no-resume to the single-machine runner.

Environment:
  FULL_THREAD_KNOB=client-pipeline maps each x-axis thread value to
  --num-terminals and uses a per-terminal outstanding depth.  If
  FULL_DET_PIPELINE_DEPTH is set, it applies to every cluster mode.  Otherwise
  kafka-only defaults to FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY=64. raft-kafka
  defaults to one fixed FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA=64 across thread
  points unless an explicit map is supplied.  The full-system detWindow is set
  to thread * selected_depth unless FULL_DET_WINDOW_MAP overrides it.
  This is the proper terminal-aware graph shape from plan.txt.
  FULL_THREAD_KNOB=concurrency is the older capacity-calibration mode that maps
  each x-axis value to deterministic window while keeping --num-terminals 1.
  FULL_THREAD_KNOB=fixed-window keeps detWindow fixed for all x-axis values.
  FULL_DET_WINDOW_MAX caps the mapped deterministic window; set 0 to disable.
  FULL_DET_WINDOW_MAP applies to every cluster mode; the mode-specific
  FULL_DET_WINDOW_{KAFKA_ONLY,RAFT_KAFKA}_MAP variants are used when the global
  map is unset, so a Kafka-only cap does not also throttle Raft+Kafka.
  The built-in YCSB-skew workloads get calibrated per-thread default depth
  maps in pipeline-saturation mode; explicit FULL_DET_PIPELINE_DEPTH* env vars
  disable those defaults. FULL_DET_WINDOW_MAP, FULL_DET_BATCH_SIZE_MAP, and
  FULL_DET_BATCH_SIZE_{KAFKA_ONLY,RAFT_KAFKA}_MAP accept comma-separated
  thread:value overrides, e.g. FULL_DET_WINDOW_MAP=1:96,2:192. If the global
  FULL_DET_BATCH_SIZE or FULL_DET_BATCH_SIZE_MAP is set, it applies to every
  cluster mode; otherwise mode-specific batch settings are honored.
  FULL_POOL_SIZE_MODE=sweep maps x-axis thread value to --pool-size, with a
  minimum of 2 because bcdb_init requires at least two workers.
  FULL_CONTINUE_ON_ERROR=1 keeps sweeping after an invalid full-system case.
  By default the script stops the full-system sweep after the first invalid
  case so a poisoned replica cannot make the remaining x-axis points bogus.
  EXPERIMENT_MODE=pipeline-saturation is the default comparison mode.
  EXPERIMENT_MODE=strict-overhead forces FULL_THREAD_KNOB=client-pipeline and
  FULL_DET_PIPELINE_DEPTH=1 when a one-outstanding-request comparison is the
  question instead of a near-saturation comparison.
  SINGLE_GATEWAY_DIRECT=1 runs the apples-to-apples single-node
  gateway-direct baseline by sending the same workload through
  ariabc_pg_gateway --completionPath direct --totalNodes 1 against one
  bypass-Raft server. Set SINGLE_GATEWAY_DIRECT=0 only for legacy output.
  SINGLE_GATEWAY_CONN_FANOUT=4 is the default now that the direct bypass-Raft
  server orders multi-socket deterministic requests by their explicit DET
  sequence before enqueueing them.
  FULL_CONN_FANOUT_KAFKA_ONLY=1 keeps the bypass-Raft Kafka-only path on the
  calibrated single-lane submit surface. FULL_CONN_FANOUT_RAFT_KAFKA=1 keeps
  Raft append order single-lane unless you explicitly test a different surface.
  FULL_CONN_FANOUT_{KAFKA_ONLY,RAFT_KAFKA}_MAP accept comma-separated
  thread:value overrides when a workload needs per-thread gateway fanout.
  FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS=0 is the default honest control
  surface: SELECT executor work is included, and completion counts plus final
  Merkle state are verified. Set it to 1 only for explicit capacity experiments
  where returned SELECT rows are intentionally bypassed.
  FULL_RESULT_PUBLISH_REPLICA_LIMIT=3 publishes Kafka result records from only
  a quorum of replicas by default; final marker/Merkle verification still checks
  all replicas. Set 0 to publish result records from every replica. Use
  FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA to override only the Raft+Kafka
  mode on direct-completion calibration runs.
  FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY=3 is the base direct-broadcast
  accept quorum; calibrated workloads may raise it unless set explicitly.
  FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY=0 keeps the legacy Kafka-only
  accept-completion surface. Set it to 1..4 to wait for that many accepted
  broadcast replicas to execute each batch before client-visible completion.
  FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY is calibrated per built-in
  workload unless set explicitly. 0 reports client-visible broadcast
  accept-quorum time, then drains late replica accepts before the post-marker
  Merkle gate. 1 uses the legacy all-accepts-in-timer surface.
  FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA and
  FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP control how many direct Raft
  replicas must finish WAIT_RESULT before client-visible completion in async
  Kafka mode; post-marker Merkle verification still checks every replica.
  FULL_PREFERRED_LEADER_ID=N passes --preferred-leader-id N to the 4-node Raft
  runner and records the placement in artifact notes. 0 leaves election default.
  FULL_KAFKA_COMPLETION_MODE=async is the default full-system completion surface:
  the gateway completes after direct accept while consuming Kafka hashes
  asynchronously, then the post-marker Merkle gate verifies all replicas. Set
  FULL_KAFKA_COMPLETION_MODE=majority to run the stricter per-request Kafka
  majority path; it is valid but includes synchronous Kafka wait in every point.
  SINGLE_TARGET_PICK=majority-pivot picks the 3rd-fastest DET node by default.
  Use fastest, slowest, or index:N when the comparison needs an explicit
  upper/lower-bound single-node target.
  SINGLE_PYTHON_BIN=/path/to/python3 forces the single-node matrix runner to
  use that remote Python when the executable can import psycopg.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="${2:-}"; shift 2 ;;
    --runs) RUNS="${2:-3}"; shift 2 ;;
    --workload) WORKLOADS="${2:-}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --target) TARGET_NODE="${2:-}"; TARGETS="$TARGET_NODE"; shift 2 ;;
    --targets) TARGETS="${2:-}"; shift 2 ;;
    --target-label) TARGET_MACHINE_LABEL="${2:-}"; shift 2 ;;
    --skip-sync) SKIP_SYNC=1; shift ;;
    --single-only) SINGLE_ONLY=1; shift ;;
    --full-only) FULL_ONLY=1; shift ;;
    --cluster-modes) FULL_CLUSTER_MODES="${2:-}"; shift 2 ;;
    --experiment-mode) EXPERIMENT_MODE="${2:-}"; shift 2 ;;
    --analyze-only) ANALYZE_ONLY=1; shift ;;
    --no-resume) NO_RESUME=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$EXPERIMENT_MODE" != "pipeline-saturation" && "$EXPERIMENT_MODE" != "strict-overhead" ]]; then
  echo "ERROR: EXPERIMENT_MODE=$EXPERIMENT_MODE is not supported; use pipeline-saturation or strict-overhead" >&2
  exit 2
fi
if [[ "$EXPERIMENT_MODE" == "strict-overhead" ]]; then
  FULL_THREAD_KNOB="client-pipeline"
  FULL_DET_PIPELINE_DEPTH_WAS_SET=1
  FULL_DET_PIPELINE_DEPTH=1
  FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY=1
  FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA=1
  FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP=""
  FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP=""
  FULL_DET_WINDOW_MAX=0
fi

if [[ "$FULL_THREAD_KNOB" != "client-pipeline" && "$FULL_THREAD_KNOB" != "pool-size" && "$FULL_THREAD_KNOB" != "concurrency" && "$FULL_THREAD_KNOB" != "fixed-window" ]]; then
  echo "ERROR: FULL_THREAD_KNOB=$FULL_THREAD_KNOB is not supported; use client-pipeline, pool-size, concurrency, or fixed-window" >&2
  exit 2
fi
case "$FULL_KAFKA_COMPLETION_MODE" in
  async|async-hash|async_hash|direct)
    FULL_KAFKA_COMPLETION_MODE="async"
    ;;
  majority|kafka-majority|kafka_majority|strict-majority|strict_majority)
    FULL_KAFKA_COMPLETION_MODE="majority"
    ;;
  *)
    echo "ERROR: FULL_KAFKA_COMPLETION_MODE=$FULL_KAFKA_COMPLETION_MODE is not supported; use async or majority" >&2
    exit 2
    ;;
esac
if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
  for depth_name in FULL_DET_PIPELINE_DEPTH FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA; do
    depth_value="${!depth_name}"
    if [[ "$depth_value" -lt 1 ]]; then
      echo "ERROR: $depth_name must be >= 1 for FULL_THREAD_KNOB=client-pipeline" >&2
      exit 2
    fi
  done
fi
if [[ "$FULL_POOL_SIZE_MODE" != "fixed" && "$FULL_POOL_SIZE_MODE" != "sweep" ]]; then
  echo "ERROR: FULL_POOL_SIZE_MODE=$FULL_POOL_SIZE_MODE is not supported; use fixed or sweep" >&2
  exit 2
fi
for quorum_name in FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY FULL_BROADCAST_ACCEPT_QUORUM_RAFT_KAFKA FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY FULL_BROADCAST_RESULT_QUORUM_RAFT_KAFKA; do
  quorum_value="${!quorum_name}"
  if [[ "$quorum_value" -lt 0 ]]; then
    echo "ERROR: $quorum_name must be >= 0" >&2
    exit 2
  fi
done
if [[ -n "$FULL_BROADCAST_ACCEPT_QUORUM" && "$FULL_BROADCAST_ACCEPT_QUORUM" -lt 0 ]]; then
  echo "ERROR: FULL_BROADCAST_ACCEPT_QUORUM must be >= 0" >&2
  exit 2
fi
if [[ -n "$FULL_BROADCAST_RESULT_QUORUM" && "$FULL_BROADCAST_RESULT_QUORUM" -lt 0 ]]; then
  echo "ERROR: FULL_BROADCAST_RESULT_QUORUM must be >= 0" >&2
  exit 2
fi
for direct_quorum_name in FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA; do
  direct_quorum_value="${!direct_quorum_name}"
  if [[ "$direct_quorum_value" -lt 1 || "$direct_quorum_value" -gt 4 ]]; then
    echo "ERROR: $direct_quorum_name must be between 1 and 4" >&2
    exit 2
  fi
done
if [[ -n "$FULL_DIRECT_COMPLETION_QUORUM" &&
      ( "$FULL_DIRECT_COMPLETION_QUORUM" -lt 1 || "$FULL_DIRECT_COMPLETION_QUORUM" -gt 4 ) ]]; then
  echo "ERROR: FULL_DIRECT_COMPLETION_QUORUM must be between 1 and 4" >&2
  exit 2
fi

normalize_cluster_mode() {
  case "$1" in
    raft|raft-kafka|raft_kafka)
      printf '%s\n' "raft-kafka"
      ;;
    kafka|kafka-only|kafka_only|preordered-direct-broadcast|preordered_direct_broadcast)
      printf '%s\n' "kafka-only"
      ;;
    *)
      return 1
      ;;
  esac
}

if [[ -z "$FULL_CLUSTER_MODES" ]]; then
  echo "ERROR: --cluster-modes cannot be empty" >&2
  exit 2
fi
IFS=',' read -ra full_mode_check_arr <<< "$FULL_CLUSTER_MODES"
for mode_check in "${full_mode_check_arr[@]}"; do
  mode_check="${mode_check//[[:space:]]/}"
  [[ -z "$mode_check" ]] && continue
  if ! normalize_cluster_mode "$mode_check" >/dev/null; then
    echo "ERROR: unsupported cluster mode '$mode_check'; use kafka-only or raft-kafka" >&2
    exit 2
  fi
done

if [[ -z "$TARGETS" ]]; then
  TARGETS="$TARGETS_DEFAULT"
fi

declare -a TARGETS_ARR=()
declare -a LABELS_ARR=()
IFS=',' read -ra _targets_split <<< "$TARGETS"
for _t in "${_targets_split[@]}"; do
  _t="${_t//[[:space:]]/}"
  [[ -z "$_t" ]] && continue
  TARGETS_ARR+=("$_t")
  _lbl="${_t##*@}"
  _lbl="${_lbl%%:*}"
  _lbl="${_lbl//./_}"
  LABELS_ARR+=("$_lbl")
done
if [[ "${#TARGETS_ARR[@]}" -eq 0 ]]; then
  echo "ERROR: no targets parsed from TARGETS=$TARGETS" >&2
  exit 2
fi

# TARGET_NODE / TARGET_MACHINE_LABEL default to the first target. They are
# later reassigned to the 3rd-fastest node before the gateway-direct step.
TARGET_NODE="${TARGETS_ARR[0]}"
if [[ -z "$TARGET_MACHINE_LABEL" ]]; then
  TARGET_MACHINE_LABEL="${LABELS_ARR[0]}"
fi

ts="$(date +%Y%m%d_%H%M%S)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/scripts/bench_full_results/ycsb_skew_compare_${ts}}"
mkdir -p "$OUT_ROOT/_run_logs"

# Per-workload paths. set_workload_paths <workload-file> populates:
#   WORKLOAD, WORKLOAD_SLUG, OUT_DIR, SINGLE_LOCAL_DIR,
#   SINGLE_GATEWAY_LOCAL_DIR, SINGLE_GATEWAY_MANIFEST, FULL_MANIFEST
set_workload_paths() {
  WORKLOAD="$1"
  WORKLOAD_SLUG="$(printf '%s' "${WORKLOAD%.*}" | tr -cs '[:alnum:]' '_' | sed 's/^_*//; s/_*$//')"
  [[ -z "$WORKLOAD_SLUG" ]] && WORKLOAD_SLUG="workload"
  OUT_DIR="$OUT_ROOT/$WORKLOAD_SLUG"
  SINGLE_LOCAL_DIR="$OUT_DIR/single_${TARGET_MACHINE_LABEL//./_}"
  SINGLE_GATEWAY_LOCAL_DIR="$OUT_DIR/single_gateway_direct"
  SINGLE_GATEWAY_MANIFEST="$OUT_DIR/single_gateway_direct_manifest.csv"
  FULL_MANIFEST="$OUT_DIR/full_system_manifest.csv"
  RUN_LOG_DIR="$OUT_ROOT/_run_logs/$WORKLOAD_SLUG"
  SINGLE_LOCAL_DIRS=()
  for _lbl in "${LABELS_ARR[@]}"; do
    SINGLE_LOCAL_DIRS+=("$OUT_DIR/single_${_lbl//./_}")
    mkdir -p "$OUT_DIR/single_${_lbl//./_}"
  done
  mkdir -p "$OUT_DIR" "$SINGLE_LOCAL_DIR" "$SINGLE_GATEWAY_LOCAL_DIR" "$RUN_LOG_DIR"
}

single_dir_for_label() {
  echo "$OUT_DIR/single_${1//./_}"
}

log() { echo "[$(date +'%F %T')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

apply_workload_calibrated_defaults() {
  [[ "$EXPERIMENT_MODE" == "pipeline-saturation" ]] || return 0
  [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]] || return 0

  local kafka_depth_map=""
  local raft_depth_map=""
  local raft_batch_size="256"
  local kafka_batch_map=""
  local raft_batch_map=""
  local kafka_det_window_map=""
  local raft_det_window_map=""
  local kafka_conn_fanout_map=""
  local kafka_accept_quorum=""
  local kafka_result_quorum_map=""
  local kafka_drain_in_timed_run="0"
  local kafka_full_result_replica_limit_map=""
  local kafka_result_publish_limit=""
  local kafka_result_publish_limit_map=""
  local raft_result_publish_limit=""
  local raft_result_publish_limit_map=""
  local raft_direct_completion_quorum_map=""
  case "$WORKLOAD_SLUG" in
    ycsbtx_skew_01_24k_pt_intkey_sid_clean_20k)
      kafka_depth_map="1:15,2:12,4:10,6:24"
      kafka_result_quorum_map="1:1,2:1,4:1,6:1,8:1,12:1,14:1"
      raft_depth_map="1:22,2:20,4:32,6:192,8:192,10:112,12:160,14:120,16:120"
      raft_batch_map="6:2048,8:1024,10:1024,12:1024,14:1024,16:1024"
      kafka_drain_in_timed_run="0"
      raft_result_publish_limit="1"
      raft_direct_completion_quorum_map="6:4"
      ;;
    ycsb_skew0_99_tx_20k_point_safedb_intkey_insert12k_uniq)
      kafka_depth_map="1:6,2:5,4:5,6:3,8:4,10:2,12:2,14:2,16:1"
      kafka_batch_map="2:512,6:192,10:160,12:192"
      kafka_det_window_map="4:18,8:28,12:20,14:18"
      raft_depth_map="1:13,2:12,4:48,6:64,8:64,10:32,12:64,14:64,16:16"
      raft_result_publish_limit_map="1:4"
      raft_direct_completion_quorum_map="4:2,6:2,8:3,12:4,14:4,16:2"
      kafka_accept_quorum="4"
      kafka_full_result_replica_limit_map="4:3,6:4,12:3"
      kafka_result_publish_limit_map="2:4,4:4,6:4,10:4,12:4"
      ;;
    *)
      return 0
      ;;
  esac

  local changed=0
  if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" != "1" ]]; then
    if [[ "$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
      FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP="$kafka_depth_map"
      changed=1
    fi
    if [[ "$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP_WAS_SET" != "1" ]]; then
      FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP="$raft_depth_map"
      changed=1
    fi
  fi

  if [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_RAFT_KAFKA_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_BATCH_SIZE_RAFT_KAFKA="$raft_batch_size"
    changed=1
  fi
  if [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_MAP_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP="$kafka_batch_map"
    changed=1
  fi
  if [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_MAP_WAS_SET" != "1" &&
        "$FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP="$raft_batch_map"
    changed=1
  fi
  if [[ "$FULL_DET_WINDOW_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_WINDOW_MAP=""
    changed=1
  fi
  if [[ "$FULL_DET_WINDOW_MAP_WAS_SET" != "1" &&
        "$FULL_DET_WINDOW_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_WINDOW_KAFKA_ONLY_MAP="$kafka_det_window_map"
    changed=1
  fi
  if [[ "$FULL_DET_WINDOW_MAP_WAS_SET" != "1" &&
        "$FULL_DET_WINDOW_RAFT_KAFKA_MAP_WAS_SET" != "1" ]]; then
    FULL_DET_WINDOW_RAFT_KAFKA_MAP="$raft_det_window_map"
    changed=1
  fi
  if [[ -z "$FULL_CONN_FANOUT" &&
        "$FULL_CONN_FANOUT_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_CONN_FANOUT_KAFKA_ONLY_MAP="$kafka_conn_fanout_map"
    changed=1
  fi
  if [[ -z "$FULL_BROADCAST_ACCEPT_QUORUM" &&
        "$FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY_WAS_SET" != "1" ]]; then
    FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY="${kafka_accept_quorum:-3}"
    changed=1
  fi
  if [[ -z "$FULL_BROADCAST_RESULT_QUORUM" &&
        "$FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_WAS_SET" != "1" ]]; then
    FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY="0"
    changed=1
  fi
  if [[ -z "$FULL_BROADCAST_RESULT_QUORUM" &&
        "$FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP="$kafka_result_quorum_map"
    changed=1
  fi
  if [[ "$FULL_BROADCAST_DRAIN_IN_TIMED_RUN_WAS_SET" != "1" &&
        "$FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY_WAS_SET" != "1" ]]; then
    FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY="$kafka_drain_in_timed_run"
    changed=1
  fi
  if [[ "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_WAS_SET" != "1" ]]; then
    FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY="${kafka_result_publish_limit:-$FULL_RESULT_PUBLISH_REPLICA_LIMIT}"
    changed=1
  fi
  if [[ "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP="$kafka_result_publish_limit_map"
    changed=1
  fi
  if [[ "$FULL_RESULT_REPLICA_LIMIT_WAS_SET" != "1" &&
        "$FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP_WAS_SET" != "1" ]]; then
    FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP="$kafka_full_result_replica_limit_map"
    changed=1
  fi
  if [[ "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_WAS_SET" != "1" ]]; then
    FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA="${raft_result_publish_limit:-$FULL_RESULT_PUBLISH_REPLICA_LIMIT}"
    changed=1
  fi
  if [[ "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_WAS_SET" != "1" &&
        "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP_WAS_SET" != "1" ]]; then
    FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP="$raft_result_publish_limit_map"
    changed=1
  fi
  if [[ "$FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP_WAS_SET" != "1" ]]; then
    FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP="$raft_direct_completion_quorum_map"
    changed=1
  fi

  if [[ "$changed" == "1" ]]; then
    log "Applied workload-calibrated full-system defaults for $WORKLOAD_SLUG (kafka-depth-map=${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP:-none} raft-depth-map=${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP:-none} det-window-map=${FULL_DET_WINDOW_MAP:-none} kafka-det-window-map=${FULL_DET_WINDOW_KAFKA_ONLY_MAP:-none} raft-det-window-map=${FULL_DET_WINDOW_RAFT_KAFKA_MAP:-none} kafka-fanout-map=${FULL_CONN_FANOUT_KAFKA_ONLY_MAP:-none} kafka-batch=$FULL_DET_BATCH_SIZE_KAFKA_ONLY kafka-batch-map=${FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP:-none} raft-batch=$FULL_DET_BATCH_SIZE_RAFT_KAFKA raft-batch-map=${FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP:-none} kafka-accept-quorum=$FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY kafka-result-quorum-map=${FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP:-none} kafka-drain-in-timed-run=$FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY kafka-full-result-map=${FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP:-none} kafka-result-publish-limit=$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY kafka-result-publish-map=${FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP:-none} raft-result-publish-limit=$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA raft-result-publish-map=${FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP:-none} raft-direct-completion-map=${FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP:-none})"
  fi
}

ssh_run() {
  ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT" "$TARGET_NODE" "$@"
}

rsync_to_target() {
  local rc

  set +e
  rsync -az "$@" -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT"
  rc=$?
  set -e

  # Autoconf/build scratch files such as ./conftest can disappear while rsync
  # walks the tree. Code 24 means "vanished source files"; the target copy is
  # still usable for this benchmark sync.
  if [[ "$rc" == "24" ]]; then
    log "WARNING: rsync saw vanished source files; continuing"
    return 0
  fi
  return "$rc"
}

local_install_has_bcdb_gucs() {
  local postgres_bin="$LOCAL_INSTALL_DIR/bin/postgres"
  [[ -x "$postgres_bin" ]] || return 1
  # Capture output first, then grep. Piping into `grep -q` causes SIGPIPE on
  # postgres after the first match, which under `set -o pipefail` flips the
  # pipeline's exit status to non-zero — making the function race with itself
  # when called concurrently from multiple background sync jobs.
  local desc
  desc="$(LD_LIBRARY_PATH="$LOCAL_INSTALL_DIR/lib:${LD_LIBRARY_PATH:-}" "$postgres_bin" --describe-config 2>/dev/null)" || return 1
  grep -qE '^bcdb_worker_count([[:space:]]|\|)' <<< "$desc"
}

append_full_manifest_header() {
  if [[ ! -f "$FULL_MANIFEST" ]]; then
    echo "series,mode,experiment_mode,ordering_mode,ordering_path,completion_path,server_bypass_raft,gateway_broadcast_to_all,thread,run,artifact_dir,exit_code,thread_knob,pool_size,bcdb_worker_count,det_batch_size,det_window,num_terminals,det_pipeline_depth,effective_inflight,det_block_pipeline,det_block_max,req_id_offset,tps_denominator_policy,notes" > "$FULL_MANIFEST"
  fi
}

append_single_gateway_manifest_header() {
  if [[ ! -f "$SINGLE_GATEWAY_MANIFEST" ]]; then
    echo "series,mode,experiment_mode,ordering_mode,ordering_path,completion_path,server_bypass_raft,gateway_broadcast_to_all,thread,run,artifact_dir,exit_code,thread_knob,pool_size,bcdb_worker_count,det_batch_size,det_window,num_terminals,det_pipeline_depth,effective_inflight,det_block_pipeline,det_block_max,req_id_offset,tps_denominator_policy,notes" > "$SINGLE_GATEWAY_MANIFEST"
  fi
}

lookup_thread_override() {
  local map="$1"
  local thread="$2"
  local entry key value
  [[ -n "$map" ]] || return 1
  IFS=',' read -ra entries <<< "$map"
  for entry in "${entries[@]}"; do
    entry="${entry//[[:space:]]/}"
    [[ -n "$entry" ]] || continue
    key="${entry%%:*}"
    value="${entry#*:}"
    if [[ "$key" == "$thread" && "$value" != "$entry" && -n "$value" ]]; then
      printf '%s\n' "$value"
      return 0
    fi
  done
  return 1
}

select_full_case_params() {
  local cluster_mode="$1"
  local th="$2"
  local override

  if [[ "$FULL_POOL_SIZE_MODE" == "fixed" ]]; then
    SELECTED_POOL_SIZE="$FULL_FIXED_POOL_SIZE"
  else
    SELECTED_POOL_SIZE="$th"
    if [[ "$SELECTED_POOL_SIZE" -lt 2 ]]; then
      SELECTED_POOL_SIZE=2
    fi
  fi

  SELECTED_NUM_TERMINALS=1
  SELECTED_DET_PIPELINE_DEPTH=0
  SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE"
  if [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" ]]; then
    if [[ "$cluster_mode" == "kafka-only" ]]; then
      SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE_KAFKA_ONLY"
    else
      SELECTED_DET_BATCH_SIZE="$FULL_DET_BATCH_SIZE_RAFT_KAFKA"
    fi
  fi

  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    SELECTED_NUM_TERMINALS="$th"
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH"
    elif [[ "$cluster_mode" == "kafka-only" ]]; then
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY"
      if override="$(lookup_thread_override "$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP" "$th")"; then
        SELECTED_DET_PIPELINE_DEPTH="$override"
      fi
    else
      SELECTED_DET_PIPELINE_DEPTH="$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA"
      if override="$(lookup_thread_override "$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP" "$th")"; then
        SELECTED_DET_PIPELINE_DEPTH="$override"
      fi
    fi
    if [[ "$SELECTED_DET_PIPELINE_DEPTH" -lt 1 ]]; then
      die "selected det-pipeline-depth must be >= 1 (mode=$cluster_mode thread=$th value=$SELECTED_DET_PIPELINE_DEPTH)"
    fi
    SELECTED_DET_WINDOW=$(( th * SELECTED_DET_PIPELINE_DEPTH ))
  elif [[ "$FULL_THREAD_KNOB" == "concurrency" ]]; then
    SELECTED_DET_WINDOW=$(( th * FULL_DET_WINDOW_MULTIPLIER ))
    if [[ "$SELECTED_DET_WINDOW" -lt "$SELECTED_DET_BATCH_SIZE" ]]; then
      SELECTED_DET_WINDOW="$SELECTED_DET_BATCH_SIZE"
    fi
    if [[ "$FULL_DET_WINDOW_MAX" -gt 0 && "$SELECTED_DET_WINDOW" -gt "$FULL_DET_WINDOW_MAX" ]]; then
      SELECTED_DET_WINDOW="$FULL_DET_WINDOW_MAX"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    SELECTED_DET_WINDOW="$FULL_DET_WINDOW"
  else
    SELECTED_DET_WINDOW="$FULL_DET_WINDOW"
  fi

  if [[ "$FULL_DET_BATCH_SIZE_MAP_WAS_SET" == "1" ]]; then
    if override="$(lookup_thread_override "$FULL_DET_BATCH_SIZE_MAP" "$th")"; then
      SELECTED_DET_BATCH_SIZE="$override"
    fi
  elif [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" && "$cluster_mode" == "kafka-only" ]]; then
    if override="$(lookup_thread_override "$FULL_DET_BATCH_SIZE_KAFKA_ONLY_MAP" "$th")"; then
      SELECTED_DET_BATCH_SIZE="$override"
    fi
  elif [[ "$FULL_DET_BATCH_SIZE_WAS_SET" != "1" ]]; then
    if override="$(lookup_thread_override "$FULL_DET_BATCH_SIZE_RAFT_KAFKA_MAP" "$th")"; then
      SELECTED_DET_BATCH_SIZE="$override"
    fi
  fi
  if override="$(lookup_thread_override "$FULL_DET_WINDOW_MAP" "$th")"; then
    SELECTED_DET_WINDOW="$override"
  elif [[ "$cluster_mode" == "kafka-only" ]]; then
    if override="$(lookup_thread_override "$FULL_DET_WINDOW_KAFKA_ONLY_MAP" "$th")"; then
      SELECTED_DET_WINDOW="$override"
    fi
  else
    if override="$(lookup_thread_override "$FULL_DET_WINDOW_RAFT_KAFKA_MAP" "$th")"; then
      SELECTED_DET_WINDOW="$override"
    fi
  fi
  if [[ "$SELECTED_DET_BATCH_SIZE" -lt 1 ]]; then
    die "selected det-batch-size must be >= 1 (mode=$cluster_mode thread=$th value=$SELECTED_DET_BATCH_SIZE)"
  fi

  SELECTED_EFFECTIVE_INFLIGHT="$SELECTED_DET_WINDOW"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    SELECTED_EFFECTIVE_INFLIGHT=$(( SELECTED_NUM_TERMINALS * SELECTED_DET_PIPELINE_DEPTH ))
  fi
  if [[ -n "$FULL_BCDB_WORKER_COUNT" ]]; then
    SELECTED_BCDB_WORKER_COUNT="$FULL_BCDB_WORKER_COUNT"
  else
    SELECTED_BCDB_WORKER_COUNT="$SELECTED_POOL_SIZE"
  fi
}

local_glibc_version() {
  ldd --version 2>/dev/null | awk 'NR==1{print $NF; exit}'
}

remote_glibc_version() {
  local TARGET_NODE="$1"
  ssh_run "ldd --version 2>/dev/null | awk 'NR==1{print \$NF; exit}'" 2>/dev/null || true
}

# Returns 0 if remote GLIBC is at least as new as local, non-zero otherwise.
remote_can_run_local_install() {
  local TARGET_NODE="$1"
  local lv rv
  lv="$(local_glibc_version)"
  rv="$(remote_glibc_version "$TARGET_NODE")"
  [[ -z "$lv" || -z "$rv" ]] && return 0  # unknown -> assume compatible
  # Compare as dotted versions.
  printf '%s\n%s\n' "$lv" "$rv" | sort -V -C
}

single_node_can_trust_synced_install() {
  local TARGET_NODE="$1"
  # With --skip-sync there may be an old copied install on the remote host.
  # Do not blindly trust it: U24-built gateway binaries fail immediately on
  # U22 nodes with missing GLIBC/GLIBCXX symbols. Rebuild on-host unless the
  # remote libc can run the local install.
  remote_can_run_local_install "$TARGET_NODE"
}

_sync_one_target() {
  local TARGET_NODE="$1"
  local local_install_bcdb="$2"
  log "Syncing source/install to $TARGET_NODE"
  ssh_run "mkdir -p '$REMOTE_REPO' '$REMOTE_INSTALL' '$REMOTE_REPO/.bench_tmp' '$REMOTE_REPO/.bench_tmp/deps/lib'"
  rsync_to_target --delete \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='.bench_tmp' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='conftest' \
    --exclude='conftest.*' \
    --exclude='scripts/bench_full_results' \
    --exclude='scripts/bench_results' \
    "$REPO_ROOT/" "$TARGET_NODE:$REMOTE_REPO/"
  if [[ "$local_install_bcdb" != "1" ]]; then
    log "WARNING: local install at $LOCAL_INSTALL_DIR is not BCDB-capable; skipping install sync to $TARGET_NODE to preserve remote custom install"
  elif ! remote_can_run_local_install "$TARGET_NODE"; then
    log "WARNING: $TARGET_NODE has older GLIBC than local ($(local_glibc_version) vs $(remote_glibc_version "$TARGET_NODE")); skipping install sync and letting remote rebuild from source"
    # Wipe any previously-synced (now-broken) binaries so the remote
    # ensure_custom_install_from_repo.sh rebuilds from source.
    ssh_run "rm -rf '$REMOTE_INSTALL'/bin '$REMOTE_INSTALL'/lib 2>/dev/null || true"
  else
    log "Local install looks BCDB-capable; syncing install tree to $TARGET_NODE"
    rsync_to_target --delete "$LOCAL_INSTALL_DIR/" "$TARGET_NODE:$REMOTE_INSTALL/"
  fi
  rsync_to_target "$TEMPLATE_CONF_LOCAL" "$TARGET_NODE:$REMOTE_REPO/.bench_tmp/shared_postgresql.conf"
}

sync_single_target() {
  [[ "$FULL_ONLY" == "1" ]] && { log "Single-node sync skipped (--full-only)"; return; }
  [[ "$SKIP_SYNC" == "1" ]] && { log "Single-node sync skipped"; return; }
  # Evaluate the local install's BCDB capability ONCE here, then pass the
  # cached answer to each parallel subshell. Running the check 4× concurrently
  # used to race (one subshell's pipe SIGPIPE'd postgres under pipefail, so one
  # target arbitrarily decided "not BCDB-capable" and skipped its install sync).
  local local_install_bcdb=0
  if local_install_has_bcdb_gucs; then
    local_install_bcdb=1
  fi
  log "Syncing source/install to ${#TARGETS_ARR[@]} target(s) in parallel: ${TARGETS_ARR[*]} (local_install_bcdb=$local_install_bcdb)"
  local pids=() rc=0
  for tgt in "${TARGETS_ARR[@]}"; do
    ( _sync_one_target "$tgt" "$local_install_bcdb" ) &
    pids+=($!)
  done
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      log "WARNING: sync subprocess pid=$pid failed"
      rc=1
    fi
  done
  if [[ "$rc" != "0" ]]; then
    log "WARNING: one or more sync targets failed; per-node single bench will skip nodes that did not sync"
  fi
}

_run_single_node_one() {
  local TARGET_NODE="$1"
  local TARGET_MACHINE_LABEL="$2"
  local SINGLE_LOCAL_DIR
  SINGLE_LOCAL_DIR="$(single_dir_for_label "$TARGET_MACHINE_LABEL")"
  [[ "$FULL_ONLY" == "1" ]] && return
  log "Running single-node PG/DET benchmark on $TARGET_NODE (label=$TARGET_MACHINE_LABEL workload=$WORKLOAD)"
  local remote_out="$REMOTE_REPO/scripts/bench_results/ycsb_skew_compare_${ts}_${WORKLOAD_SLUG}"
  local remote_log="$REMOTE_REPO/.bench_tmp/ycsb_skew_compare_single_${ts}_${WORKLOAD_SLUG}.log"
  local log_file="$RUN_LOG_DIR/single_node_${TARGET_MACHINE_LABEL//./_}_${WORKLOAD_SLUG}.log"
  local no_resume_arg=""
  local single_extra_gucs="bcdb_serial_gate_mode=$SINGLE_BCDB_SERIAL_GATE_MODE,bcdb_serial_gate_source=$SINGLE_BCDB_SERIAL_GATE_SOURCE,bcdb_advance_commit_watermark=$SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK,bcdb_dt_completion_only_skip_reads=$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS,bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD,bcdb_result_ring_slots=$SINGLE_BCDB_RESULT_RING_SLOTS"
  local trust_synced_install=0
  [[ "$NO_RESUME" == "1" ]] && no_resume_arg="--no-resume"
  if [[ -n "${SINGLE_BCDB_EXTRA_GUCS:-}" ]]; then
    single_extra_gucs="$SINGLE_BCDB_EXTRA_GUCS,$single_extra_gucs"
  fi
  if single_node_can_trust_synced_install "$TARGET_NODE"; then
    trust_synced_install=1
  fi

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO/scripts'
if [[ '$trust_synced_install' == '1' ]]; then
  if ! bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
       --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild --trust-install; then
    echo \"[INFO] --trust-install failed on this host; retrying with a source rebuild\" >&2
    bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
  fi
else
  echo \"[INFO] remote cannot consume the freshly synced local install; ensuring an on-host build\" >&2
  bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
fi
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export BCDB_EXTRA_GUCS='$single_extra_gucs'
export BCDB_BLOCK_RETURN_ACTUAL_RESULTS='$SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS'
export BCDB_POLL_MAX_US='$SINGLE_BCDB_POLL_MAX_US'
export BCDB_DT_PARSE_BARRIER='$SINGLE_BCDB_DT_PARSE_BARRIER'
export BCDB_DT_LIGHT_SNAPSHOT='$SINGLE_BCDB_DT_LIGHT_SNAPSHOT'
export BCDB_DT_SKIP_READONLY_GATE='$SINGLE_BCDB_DT_SKIP_READONLY_GATE'
export BCDB_FLOW_DEBUG='$SINGLE_BCDB_FLOW_DEBUG'
export BCDB_GATE_DEBUG='$SINGLE_BCDB_GATE_DEBUG'
export BCDB_APPLY_WAIT_DEBUG='$SINGLE_BCDB_APPLY_WAIT_DEBUG'
if [[ -n '$SINGLE_BCDB_PHASE_TRACE_PREFIX' ]]; then
  export BCDB_PHASE_TRACE='$remote_out/$SINGLE_BCDB_PHASE_TRACE_PREFIX'
fi
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
export ARIABC_PSYCOPG_CLIENT_CURSOR=1
echo \"SINGLE_NODE_BCDB_KNOBS=serial_gate_mode=$SINGLE_BCDB_SERIAL_GATE_MODE serial_gate_source=$SINGLE_BCDB_SERIAL_GATE_SOURCE advance_commit_watermark=$SINGLE_BCDB_ADVANCE_COMMIT_WATERMARK completion_only_skip_reads=$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS result_ring_slots=$SINGLE_BCDB_RESULT_RING_SLOTS block_return_actual_results=$SINGLE_BCDB_BLOCK_RETURN_ACTUAL_RESULTS poll_max_us=$SINGLE_BCDB_POLL_MAX_US dt_parse_barrier=$SINGLE_BCDB_DT_PARSE_BARRIER dt_light_snapshot=$SINGLE_BCDB_DT_LIGHT_SNAPSHOT dt_skip_readonly_gate=$SINGLE_BCDB_DT_SKIP_READONLY_GATE flow_debug=$SINGLE_BCDB_FLOW_DEBUG gate_debug=$SINGLE_BCDB_GATE_DEBUG apply_wait_debug=$SINGLE_BCDB_APPLY_WAIT_DEBUG timeout_workload_s=$SINGLE_TIMEOUT_WORKLOAD_S timeout_workload_det_s=$SINGLE_TIMEOUT_WORKLOAD_DET_S phase_trace_prefix=$SINGLE_BCDB_PHASE_TRACE_PREFIX extra_gucs=$single_extra_gucs\"
if [[ '$SINGLE_REMOTE_KILL_STALE' == '1' ]]; then
  for pid in \$(pgrep -f '$REMOTE_REPO/scripts/generic-saicopg-traffic-load+logSkip-safedb+pg.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -TERM \"\$pid\" 2>/dev/null || true
  done
  for pid in \$(pgrep -f 'python3 -u bench_threads_matrix.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -TERM \"\$pid\" 2>/dev/null || true
  done
  sleep 1
  for pid in \$(pgrep -f '$REMOTE_REPO/scripts/generic-saicopg-traffic-load+logSkip-safedb+pg.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -KILL \"\$pid\" 2>/dev/null || true
  done
  for pid in \$(pgrep -f 'python3 -u bench_threads_matrix.py' 2>/dev/null || true); do
    [[ \"\$pid\" == \"$$\" ]] && continue
    kill -KILL \"\$pid\" 2>/dev/null || true
  done
fi
PYTHON_BIN=''
if [[ -n '$SINGLE_PYTHON_BIN' ]]; then
  if [[ -x '$SINGLE_PYTHON_BIN' ]] && '$SINGLE_PYTHON_BIN' -c 'import psycopg' >/dev/null 2>&1; then
    PYTHON_BIN='$SINGLE_PYTHON_BIN'
  else
    echo \"ERROR: SINGLE_PYTHON_BIN=$SINGLE_PYTHON_BIN is not executable or cannot import psycopg\" >&2
    exit 1
  fi
elif [[ -x '$REMOTE_REPO/.venv/bin/python3' ]] && '$REMOTE_REPO/.venv/bin/python3' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
elif [[ -x '$REMOTE_REPO/.venv/bin/python' ]] && '$REMOTE_REPO/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
  python3 -m venv --clear '$REMOTE_REPO/.venv'
fi
export ARIABC_PYTHON=\"\$PYTHON_BIN\"
pgdata_line=\$(bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] && export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
pip_install() {
  # Bounded so a slow/hung PyPI fetch can't stall the whole bench.
  timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check \"\$@\" >/dev/null 2>&1 \
    || timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check --user \"\$@\" >/dev/null 2>&1 \
    || timeout 60s \$PYTHON_BIN -m pip install -q --disable-pip-version-check --break-system-packages \"\$@\" >/dev/null 2>&1
}
if ! \$PYTHON_BIN -c 'import psycopg' >/dev/null 2>&1; then
  pip_install 'psycopg[binary]' psycopg || echo \"WARNING: failed to install psycopg into \$PYTHON_BIN\" >&2
fi
# Note: matplotlib intentionally not installed here. The runner generates the
# per-machine TPS-vs-threads graphs locally after collecting summary.csv, so
# the remote does not need matplotlib. A previous version pip-installed it on
# every node in parallel which sometimes hung indefinitely on slow PyPI mirrors.
echo \"ARIABC_PYTHON=\$ARIABC_PYTHON\"
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes pg,det \
  --signing-modes 0 \
  --enforce-signatures 0 \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --workloads '$WORKLOAD' \
  --timeout-workload-s '$SINGLE_TIMEOUT_WORKLOAD_S' \
  --timeout-workload-det-s '$SINGLE_TIMEOUT_WORKLOAD_DET_S' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out' $no_resume_arg
")" > "$log_file" 2>&1

  log "Collecting single-node results from $TARGET_NODE:$remote_out"
  rsync_to_target "$TARGET_NODE:$remote_out/" "$SINGLE_LOCAL_DIR/"
}

generate_local_per_machine_graph() {
  local single_dir="$1"
  local summary="$single_dir/summary.csv"
  if [[ ! -f "$summary" ]]; then
    log "WARNING: skipping local graph for $single_dir (no summary.csv)"
    return
  fi
  log "Generating local per-machine graph from $summary"
  MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" python3 - "$REPO_ROOT" "$summary" "$single_dir" <<'PYEOF' \
    >> "$RUN_LOG_DIR/local_graphs.log" 2>&1 \
    || log "WARNING: local graph generation failed for $single_dir (see $RUN_LOG_DIR/local_graphs.log)"
import sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]) / "scripts"))
from bench_threads_matrix import _generate_tps_graphs
out = _generate_tps_graphs(Path(sys.argv[2]), Path(sys.argv[3]))
for p in out:
    print("wrote", p)
PYEOF
}

run_single_node() {
  [[ "$FULL_ONLY" == "1" ]] && return
  log "Launching single-node bench in parallel across ${#TARGETS_ARR[@]} target(s): ${TARGETS_ARR[*]}"
  local pids=() pid_targets=() pid_labels=()
  for i in "${!TARGETS_ARR[@]}"; do
    local tgt="${TARGETS_ARR[$i]}"
    local lbl="${LABELS_ARR[$i]}"
    ( _run_single_node_one "$tgt" "$lbl" ) &
    pids+=($!)
    pid_targets+=("$tgt")
    pid_labels+=("$lbl")
  done
  for j in "${!pids[@]}"; do
    if wait "${pids[$j]}"; then
      log "Single-node bench succeeded on ${pid_targets[$j]} (label=${pid_labels[$j]})"
    else
      log "WARNING: single-node bench failed on ${pid_targets[$j]} (label=${pid_labels[$j]}); see $RUN_LOG_DIR/single_node_${pid_labels[$j]//./_}_${WORKLOAD_SLUG}.log"
    fi
  done
  for lbl in "${LABELS_ARR[@]}"; do
    generate_local_per_machine_graph "$(single_dir_for_label "$lbl")"
  done
}

pick_comparison_node() {
  local out
  out="$(python3 - "$OUT_DIR" "$SINGLE_TARGET_PICK" "${LABELS_ARR[@]}" "--" "${TARGETS_ARR[@]}" <<'PYEOF' 2> >(tee -a "$RUN_LOG_DIR/comparison_node_pick.log" >&2)
import csv, os, sys
out_dir = sys.argv[1]
pick = sys.argv[2]
sep = sys.argv.index("--", 3)
labels = sys.argv[3:sep]
targets = sys.argv[sep+1:]
results = []
for label, target in zip(labels, targets):
    summary = os.path.join(out_dir, f"single_{label}", "summary.csv")
    if not os.path.exists(summary):
        print(f"  miss: {summary}", file=sys.stderr)
        continue
    peak = 0.0
    with open(summary, newline='') as f:
        for row in csv.DictReader(f):
            if (row.get('mode') or '').strip().lower() != 'det':
                continue
            try:
                tps = float(row.get('median_throughput_tps') or 0)
            except (ValueError, TypeError):
                continue
            if tps > peak:
                peak = tps
    if peak > 0:
        results.append((peak, label, target))
# Rank by peak DET median TPS descending; pick the 3rd entry. If fewer than 3
# nodes produced data, fall back to the slowest one we have.
results.sort(reverse=True)
if not results:
    sys.exit(1)
print("RANKING:", file=sys.stderr)
for r in results:
    print(f"  peak_det_tps={r[0]:.2f}  label={r[1]}  target={r[2]}", file=sys.stderr)
if pick == "fastest":
    idx = 0
elif pick == "slowest":
    idx = len(results) - 1
elif pick == "majority-pivot":
    idx = min(2, len(results) - 1)
elif pick.startswith("index:"):
    try:
        idx = int(pick.split(":", 1)[1])
    except ValueError:
        print(f"unsupported SINGLE_TARGET_PICK={pick!r}", file=sys.stderr)
        sys.exit(2)
    if idx < 0 or idx >= len(results):
        print(f"SINGLE_TARGET_PICK index out of range: {idx} for {len(results)} result(s)", file=sys.stderr)
        sys.exit(2)
else:
    print(f"unsupported SINGLE_TARGET_PICK={pick!r}; use fastest, majority-pivot, slowest, or index:N", file=sys.stderr)
    sys.exit(2)
peak, label, target = results[idx]
print(f"{label}\t{target}\t{peak:.2f}")
PYEOF
  )"
  if [[ -z "$out" ]]; then
    die "could not pick comparison node — no successful single-node summary.csv files under $OUT_DIR"
  fi
  IFS=$'\t' read -r COMPARISON_NODE_LABEL COMPARISON_NODE COMPARISON_NODE_TPS <<< "$out"
  log "Comparison node selected ($SINGLE_TARGET_PICK): $COMPARISON_NODE (label=$COMPARISON_NODE_LABEL, peak DET TPS=$COMPARISON_NODE_TPS)"
  # Reassign the globals used by run_single_gateway_direct and generate_outputs
  TARGET_NODE="$COMPARISON_NODE"
  TARGET_MACHINE_LABEL="$COMPARISON_NODE_LABEL"
  SINGLE_LOCAL_DIR="$(single_dir_for_label "$COMPARISON_NODE_LABEL")"
}

run_single_gateway_direct() {
  [[ "$FULL_ONLY" == "1" ]] && return
  [[ "$SINGLE_ONLY" == "1" ]] && return
  [[ "$SINGLE_GATEWAY_DIRECT" == "1" ]] || { log "Single-node gateway-direct baseline skipped (SINGLE_GATEWAY_DIRECT=$SINGLE_GATEWAY_DIRECT)"; return; }

  append_single_gateway_manifest_header
  log "Running single-node gateway-direct baseline on $TARGET_NODE (workload=$WORKLOAD)"

  local remote_out="$REMOTE_REPO/scripts/bench_results/ycsb_skew_gateway_direct_${ts}_${WORKLOAD_SLUG}"
  local log_file="$RUN_LOG_DIR/single_gateway_direct_${WORKLOAD_SLUG}.log"
  local trust_synced_install=0
  : > "$log_file"
  if single_node_can_trust_synced_install "$TARGET_NODE"; then
    trust_synced_install=1
  fi

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
mkdir -p '$remote_out'
cd '$REMOTE_REPO'
if [[ '$trust_synced_install' == '1' ]]; then
  if ! bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
       --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild --trust-install; then
    echo \"[INFO] --trust-install failed on this host; retrying with a source rebuild\" >&2
    bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
      --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
  fi
else
  echo \"[INFO] remote cannot consume the freshly synced local install; ensuring an on-host build\" >&2
  bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
    --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
fi
srv_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_server'
gw_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_gateway'
build_ok=0
if command -v cmake >/dev/null 2>&1 && [[ -d '$REMOTE_REPO/ariabc_pg/build' ]]; then
  if cmake --build '$REMOTE_REPO/ariabc_pg/build' --target ariabc_pg_server ariabc_pg_gateway -j\$(nproc); then
    build_ok=1
  fi
elif command -v make >/dev/null 2>&1 && [[ -f '$REMOTE_REPO/ariabc_pg/build/Makefile' ]]; then
  if make -C '$REMOTE_REPO/ariabc_pg/build' ariabc_pg_server ariabc_pg_gateway -j\$(nproc); then
    build_ok=1
  fi
fi
if [[ \"\$build_ok\" != '1' && '$trust_synced_install' == '1' && -x \"\$srv_bin\" && -x \"\$gw_bin\" ]]; then
  echo \"[INFO] no remote cmake/build Makefile; using freshly synced compatible ariabc_pg binaries\"
  build_ok=1
fi
if [[ \"\$build_ok\" != '1' ]]; then
  echo \"ERROR: cannot rebuild ariabc_pg_server/gateway on this host; missing cmake and build Makefile\" >&2
  exit 1
fi
[[ -x \"\$srv_bin\" ]] || { echo \"ERROR: missing ariabc_pg_server at \$srv_bin\" >&2; exit 1; }
[[ -x \"\$gw_bin\" ]] || { echo \"ERROR: missing ariabc_pg_gateway at \$gw_bin\" >&2; exit 1; }
")" >> "$log_file" 2>&1

  IFS=',' read -ra thread_arr <<< "$THREADS"
  for th in "${thread_arr[@]}"; do
    th="${th//[[:space:]]/}"
    [[ -z "$th" ]] && continue
    for run in $(seq 1 "$RUNS"); do
      # Match cluster_kafka's pipeline depth: the gateway-direct baseline is
      # the apples-to-apples reference for cluster_kafka minus replication, so
      # it must use the same per-terminal pipeline depth (kafka-only's depth)
      # — not raft-kafka's. Without this match, single-node-gateway-direct was
      # artificially capped below cluster_kafka and the graph ordering looked
      # wrong.
      select_full_case_params "kafka-only" "$th"
      local gateway_pool_size="${SINGLE_GATEWAY_POOL_SIZE:-$SELECTED_POOL_SIZE}"
      # Keep worker_count at the cluster's value (512). Bumping it higher
      # explodes the per-process open-file count and tripped
      #   bcdb_init skipped: Cannot start worker: could not create socket:
      #   Too many open files
      # which silently falls back to the serialized det compat path and caps
      # TPS at ~2.7k. Stay within ulimit -n.
      local gateway_worker_count="${SINGLE_GATEWAY_BCDB_WORKER_COUNT:-$SELECTED_BCDB_WORKER_COUNT}"
      local gateway_num_terminals="$SELECTED_NUM_TERMINALS"
      # Use kafka-only's pipeline depth directly. Effective in-flight then
      # scales linearly with numTerminals.
      # Without the BCDB_DET_QUEUE_HIGH_WM bump exported below, the server
      # would stall on admission control above the formula default
      # (2 * conn_pool_size = 512) — capping single-node throughput.
      local gateway_det_pipeline_depth="$SELECTED_DET_PIPELINE_DEPTH"
      local gateway_det_batch_size="$SELECTED_DET_BATCH_SIZE"
      local gateway_det_window
      local gateway_effective_inflight
      if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
        gateway_det_window=$(( gateway_num_terminals * gateway_det_pipeline_depth ))
        gateway_effective_inflight="$gateway_det_window"
      else
        gateway_det_window="$SELECTED_DET_WINDOW"
        gateway_effective_inflight="$SELECTED_EFFECTIVE_INFLIGHT"
        if [[ "$gateway_det_pipeline_depth" -lt 1 ]]; then
          gateway_det_pipeline_depth="$gateway_det_window"
        fi
      fi
      local req_id_offset=$(( 500000000 + (run * 1000000) + (th * 10000) + 1 ))
      local client_port=$(( SINGLE_GATEWAY_CLIENT_PORT_BASE + (th * 20) + run ))
      local raft_port=$(( SINGLE_GATEWAY_RAFT_PORT_BASE + (th * 20) + run ))
      local required_max_connections=$(( gateway_pool_size * 3 + 64 ))
      local worker_required_max_connections=$(( gateway_worker_count + gateway_pool_size + 64 ))
      if [[ "$required_max_connections" -lt "$worker_required_max_connections" ]]; then
        required_max_connections="$worker_required_max_connections"
      fi
      if [[ "$required_max_connections" -lt 256 ]]; then
        required_max_connections=256
      fi
      local completion_only_skip_reads="off"
      if [[ "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" == "1" ]]; then
        completion_only_skip_reads="on"
      fi

      local remote_case="$remote_out/thread_${th}_run_${run}"
      local local_case="$SINGLE_GATEWAY_LOCAL_DIR/thread_${th}_run_${run}"
      mkdir -p "$local_case"
      local gateway_conn_fanout="$SINGLE_GATEWAY_CONN_FANOUT"
      log "Single gateway-direct case thread=$th run=$run experiment=$EXPERIMENT_MODE (num-terminals=$gateway_num_terminals conn-fanout=$gateway_conn_fanout det-pipeline-depth=$gateway_det_pipeline_depth effective-inflight=$gateway_effective_inflight pool-size=$gateway_pool_size worker-count=$gateway_worker_count det-batch=$gateway_det_batch_size det-window=$gateway_det_window completion_path=direct)"

      set +e
      ssh_run "timeout '$SINGLE_GATEWAY_CASE_TIMEOUT_S' bash -lc $(printf '%q' "
set -euo pipefail
case_dir='$remote_case'
mkdir -p \"\$case_dir\"
cd '$REMOTE_REPO'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
# Decouple BCDB worker queue count from bcdb_init's block_size argument.
# Must be exported BEFORE postgres (re)starts so it's in postgres's env.
# Without this, bcdb_init() rejects with
#   \"bcdb_worker_count mismatch: existing=512 requested=256; restart required\"
# and the server falls back to the serialized det compat path — capping
# single-node-gateway-direct at ~2.7k TPS regardless of fanout, threads, or
# pipeline depth. This mirrors the cluster runner's BCDB_DECOUPLE_WORKERS=1.
export BCDB_DECOUPLE_WORKERS='$FULL_BCDB_DECOUPLE_WORKERS'
# Raise the BCDB det-mode admission-control watermarks for the single-node
# gateway-direct backend so it can absorb the full gateway pipeline
# (numTerminals × detPipelineDepth × detBatchSize outstanding txs) without
# tripping append_stall and capping throughput. The cluster spreads load
# across 4 servers; the single-node case must scale watermarks up to match.
export BCDB_DET_QUEUE_HIGH_WM='${SINGLE_BCDB_DET_QUEUE_HIGH_WM:-4096}'
export BCDB_DET_QUEUE_LOW_WM='${SINGLE_BCDB_DET_QUEUE_LOW_WM:-2048}'
pgdata_line=\$(bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] || { echo \"bad PGDATA line: \$pgdata_line\" >&2; exit 1; }
PGDATA=\${pgdata_line#PGDATA=}
psql_bin='$REMOTE_INSTALL/bin/psql'
pg_ctl_bin='$REMOTE_INSTALL/bin/pg_ctl'
srv_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_server'
gw_bin='$REMOTE_REPO/ariabc_pg/build/bin/ariabc_pg_gateway'

\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_worker_count = '$gateway_worker_count';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_mode = '$FULL_BCDB_SERIAL_GATE_MODE';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_serial_gate_source = '$FULL_BCDB_SERIAL_GATE_SOURCE';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_conflict_tracking = 'on';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_completion_only_skip_reads = '$completion_only_skip_reads';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_dt_hashtab_switch_threshold = '$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET bcdb_result_ring_slots = '$RESULT_RING_CAPACITY';\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"ALTER SYSTEM SET max_connections = '$required_max_connections';\"
\"\$pg_ctl_bin\" -D \"\$PGDATA\" -w -t 120 restart -l \"\$case_dir/postgres.log\"
\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -c \"DO \\\$\\\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'neel') THEN CREATE ROLE neel LOGIN SUPERUSER; END IF; END \\\$\\\$;\"

\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -v ON_ERROR_STOP=1 -f '$REMOTE_REPO/scripts/restore_usertable_small.sql' >\"\$case_dir/restore.log\" 2>&1
if command -v fuser >/dev/null 2>&1; then
  fuser -k -TERM '${client_port}/tcp' '${raft_port}/tcp' >/dev/null 2>&1 || true
  sleep 1
  fuser -k '${client_port}/tcp' '${raft_port}/tcp' >/dev/null 2>&1 || true
fi

export ARIABC_PROFILE=1
export ARIABC_DET_BLOCK_PARALLEL='$FULL_DET_BLOCK_PARALLEL'
export ARIABC_DET_BLOCK_PIPELINE='$FULL_DET_BLOCK_PIPELINE'
export ARIABC_DET_BLOCK_MAX='$FULL_DET_BLOCK_MAX'
export ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US='$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US'
export ARIABC_DET_BLOCK_SKIP_READONLY='$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS'
export ARIABC_FULL_RESULT_REPLICA_LIMIT='$FULL_RESULT_REPLICA_LIMIT'
export BCDB_DT_COMPLETION_ONLY_SKIP_READS='$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS'
nohup \"\$srv_bin\" \
  --id 1 \
  --raftEndpoint 127.0.0.1:'$raft_port' \
  --clientPort '$client_port' \
  --raftMembers '1=127.0.0.1:$raft_port' \
  --dbName '$DB_NAME' \
  --dbHost 127.0.0.1 \
  --dbPort '$DB_PORT' \
  --dbUser '$DB_USER' \
  --dbType 1 \
  --safedb 1 \
  --dbConnPoolSize '$gateway_pool_size' \
  --pgExecMode event \
  --bypassRaft 1 \
  >\"\$case_dir/server.log\" 2>&1 &
srv_pid=\$!
cleanup() {
  kill -TERM \"\$srv_pid\" >/dev/null 2>&1 || true
  wait \"\$srv_pid\" >/dev/null 2>&1 || true
}
trap cleanup EXIT

ready=0
for attempt in \$(seq 1 60); do
  if ss -tlnp 2>/dev/null | grep -q ':$client_port'; then
    ready=1
    break
  fi
  if ! kill -0 \"\$srv_pid\" >/dev/null 2>&1; then
    cat \"\$case_dir/server.log\" >&2 || true
    exit 1
  fi
  sleep 1
done
if [[ \"\$ready\" -ne 1 ]]; then
  cat \"\$case_dir/server.log\" >&2 || true
  echo 'server did not open client port' >&2
  exit 1
fi

printf 'SELECT 1;\n' >\"\$case_dir/precheck.sql\"
\"\$gw_bin\" \
  --nodes '127.0.0.1:$client_port' \
  --queryFrom \"\$case_dir/precheck.sql\" \
  --dbType 1 \
  --detStartSeq 99000000 \
  --reqIdOffset 99000000 \
  --detWindow 1 \
  --detBatchSize 1 \
  --dbConnPoolSize '$gateway_pool_size' \
  --submitMode blocking \
  --detSubmitPipeline 0 \
  --detPipelineDepth 1 \
  --clientId 'single-gateway-direct-probe' \
  --numTerminals 1 \
  --waitMajority 0 \
  --completionPath direct \
  --totalNodes 1 \
  >\"\$case_dir/precheck.log\" 2>&1

set +e
\"\$gw_bin\" \
  --nodes '127.0.0.1:$client_port' \
  --queryFrom '$REMOTE_REPO/scripts/$WORKLOAD' \
  --dbType 1 \
  --detStartSeq 1 \
  --reqIdOffset '$req_id_offset' \
  --detWindow '$gateway_det_window' \
  --detBatchSize '$gateway_det_batch_size' \
  --dbConnPoolSize '$gateway_pool_size' \
  --submitMode event \
  --detSubmitPipeline 1 \
  --detPipelineDepth '$gateway_det_pipeline_depth' \
  --pollCount '$POLL_COUNT' \
  --clientId 'single-gateway-direct' \
  --numTerminals '$gateway_num_terminals' \
  --waitMajority 0 \
  --completionPath direct \
  --totalNodes 1 \
  --connFanout '$gateway_conn_fanout' \
  2>&1 | tee \"\$case_dir/gateway_test.log\"
gw_rc=\${PIPESTATUS[0]}
set -e

cnt=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc 'SELECT count(*) FROM usertable_small' | tr -d '[:space:]')
root=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"SELECT merkle_root_hash('usertable_small')\" | tr -d '[:space:]')
verify=\$(\"\$psql_bin\" -X -q -h 127.0.0.1 -p '$DB_PORT' -U '$DB_USER' '$DB_NAME' -tAc \"SELECT merkle_verify('usertable_small')\" | tr -d '[:space:]')
{
  echo \"rows=\$cnt\"
  echo \"root=\$root\"
  echo \"verify=\$verify\"
  echo \"pool_size=$gateway_pool_size\"
  echo \"bcdb_worker_count=$gateway_worker_count\"
  echo \"max_connections=$required_max_connections\"
} >\"\$case_dir/verify.txt\"

if [[ \"\$gw_rc\" -ne 0 ]]; then
  exit \"\$gw_rc\"
fi
if [[ \"\$verify\" != 't' ]]; then
  echo \"merkle_verify failed: \$verify\" >&2
  exit 1
fi
")" >> "$log_file" 2>&1
      rc=$?
      set -e

      set +e
      rsync_to_target "$TARGET_NODE:$remote_case/" "$local_case/"
      rsync_rc=$?
      set -e
      if [[ "$rsync_rc" != "0" ]]; then
        log "WARNING: failed to collect single gateway-direct artifact thread=$th run=$run rsync_rc=$rsync_rc"
      fi

      notes="single_node_gateway_direct;completion_path=direct;experiment_mode=$EXPERIMENT_MODE;thread_knob=$FULL_THREAD_KNOB;num_terminals=$gateway_num_terminals;conn_fanout=$gateway_conn_fanout;det_pipeline_depth=$gateway_det_pipeline_depth;effective_inflight=$gateway_effective_inflight;pool_size=$gateway_pool_size;bcdb_worker_count=$gateway_worker_count;det_block_parallel=$FULL_DET_BLOCK_PARALLEL;det_block_pipeline=$FULL_DET_BLOCK_PIPELINE;det_block_max=$FULL_DET_BLOCK_MAX;bcdb_serial_gate_mode=$FULL_BCDB_SERIAL_GATE_MODE;bcdb_serial_gate_source=$FULL_BCDB_SERIAL_GATE_SOURCE;bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD;result_ring_capacity=$RESULT_RING_CAPACITY;remote_case=$remote_case"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "single_node_gateway_direct" "gateway_direct_bcdb" "$EXPERIMENT_MODE" "direct" "single_node_gateway" "direct" "1" "0" "$th" "$run" "$local_case" "$rc" "$FULL_THREAD_KNOB" "$gateway_pool_size" "$gateway_worker_count" "$gateway_det_batch_size" "$gateway_det_window" "$gateway_num_terminals" "$gateway_det_pipeline_depth" "$gateway_effective_inflight" "$FULL_DET_BLOCK_PIPELINE" "$FULL_DET_BLOCK_MAX" "$req_id_offset" "completed_eq_loaded_required" "$notes" \
        >> "$SINGLE_GATEWAY_MANIFEST"

      if [[ "$rc" != "0" ]]; then
        die "single gateway-direct case failed (thread=$th run=$run rc=$rc); see $log_file and $local_case"
      fi
      if [[ "$rsync_rc" != "0" ]]; then
        die "single gateway-direct artifact collection failed (thread=$th run=$run rsync_rc=$rsync_rc)"
      fi
    done
  done
}

FULL_SYNC_FIRST=1

run_full_system() {
  [[ "$SINGLE_ONLY" == "1" ]] && return
  append_full_manifest_header
  log "Running cluster sweep (modes=$FULL_CLUSTER_MODES workload=$WORKLOAD)"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      log "Full-system x-axis mapping: thread value -> --num-terminals; detPipelineDepth=$FULL_DET_PIPELINE_DEPTH per terminal (forced for every mode)"
    else
      log "Full-system x-axis mapping: thread value -> --num-terminals; detPipelineDepth by mode: kafka-only=$FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY kafka-only-map=${FULL_DET_PIPELINE_DEPTH_KAFKA_ONLY_MAP:-none} raft-kafka=$FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA raft-kafka-map=${FULL_DET_PIPELINE_DEPTH_RAFT_KAFKA_MAP:-none}"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    log "Full-system x-axis mapping: thread labels with fixed detWindow=$FULL_DET_WINDOW; --num-terminals remains 1"
  else
    log "Full-system x-axis mapping: thread value -> ordered concurrency budget; --num-terminals remains 1"
  fi
  if [[ "$FULL_KAFKA_COMPLETION_MODE" == "async" ]]; then
    log "Full-system completion surface: direct accept + async Kafka hash validation + post-marker Merkle verification"
    cluster_completion_path="direct"
    cluster_validation_mode="async_hash"
    cluster_trusted_gate="async_kafka_post_marker_merkle"
  else
    log "Full-system completion surface: synchronous per-request Kafka majority + post-marker Merkle verification"
    cluster_completion_path="kafka_majority"
    cluster_validation_mode="strict_majority"
    cluster_trusted_gate="kafka_majority_merkle"
  fi

  IFS=',' read -ra full_mode_arr <<< "$FULL_CLUSTER_MODES"
  IFS=',' read -ra thread_arr <<< "$THREADS"
  for requested_mode in "${full_mode_arr[@]}"; do
    requested_mode="${requested_mode//[[:space:]]/}"
    [[ -z "$requested_mode" ]] && continue
    cluster_mode="$(normalize_cluster_mode "$requested_mode")" || die "unsupported cluster mode: $requested_mode"
    if [[ "$cluster_mode" == "kafka-only" ]]; then
      cluster_series="cluster_kafka"
      cluster_mode_label="kafka_only_bcdb"
      ordering_path="preordered_direct_broadcast"
      server_bypass_raft=1
      gateway_broadcast_to_all=1
    else
      cluster_series="cluster_raft_kafka"
      cluster_mode_label="kafka_raft_bcdb"
      ordering_path="raft"
      server_bypass_raft=0
      gateway_broadcast_to_all=0
    fi
    log "Cluster mode: $cluster_mode (series=$cluster_series ordering_path=$ordering_path completion_path=$cluster_completion_path validation_mode=$cluster_validation_mode)"
    for th in "${thread_arr[@]}"; do
      th="${th//[[:space:]]/}"
      [[ -z "$th" ]] && continue
      for run in $(seq 1 "$RUNS"); do
      select_full_case_params "$cluster_mode" "$th"
      full_pool_size="$SELECTED_POOL_SIZE"
      full_num_terminals="$SELECTED_NUM_TERMINALS"
      full_det_pipeline_depth="$SELECTED_DET_PIPELINE_DEPTH"
      full_det_batch_size="$SELECTED_DET_BATCH_SIZE"
      full_det_window="$SELECTED_DET_WINDOW"
      full_effective_inflight="$SELECTED_EFFECTIVE_INFLIGHT"
      full_bcdb_worker_count="$SELECTED_BCDB_WORKER_COUNT"
      if [[ -n "$FULL_CONN_FANOUT" ]]; then
        full_conn_fanout="$FULL_CONN_FANOUT"
      elif [[ "$cluster_mode" == "kafka-only" ]]; then
        full_conn_fanout="$FULL_CONN_FANOUT_KAFKA_ONLY"
        if override="$(lookup_thread_override "$FULL_CONN_FANOUT_KAFKA_ONLY_MAP" "$th")"; then
          full_conn_fanout="$override"
        fi
      else
        full_conn_fanout="$FULL_CONN_FANOUT_RAFT_KAFKA"
        if override="$(lookup_thread_override "$FULL_CONN_FANOUT_RAFT_KAFKA_MAP" "$th")"; then
          full_conn_fanout="$override"
        fi
      fi
      if [[ "$full_conn_fanout" -lt 1 ]]; then
        die "selected conn-fanout must be >= 1 (mode=$cluster_mode thread=$th value=$full_conn_fanout)"
      fi
      if [[ -n "$FULL_BROADCAST_ACCEPT_QUORUM" ]]; then
        full_broadcast_accept_quorum="$FULL_BROADCAST_ACCEPT_QUORUM"
      elif [[ "$cluster_mode" == "kafka-only" ]]; then
        full_broadcast_accept_quorum="$FULL_BROADCAST_ACCEPT_QUORUM_KAFKA_ONLY"
      else
        full_broadcast_accept_quorum="$FULL_BROADCAST_ACCEPT_QUORUM_RAFT_KAFKA"
      fi
      if [[ -n "$FULL_BROADCAST_RESULT_QUORUM" ]]; then
        full_broadcast_result_quorum="$FULL_BROADCAST_RESULT_QUORUM"
      elif [[ "$cluster_mode" == "kafka-only" ]]; then
        full_broadcast_result_quorum="$FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY"
        if override="$(lookup_thread_override "$FULL_BROADCAST_RESULT_QUORUM_KAFKA_ONLY_MAP" "$th")"; then
          full_broadcast_result_quorum="$override"
        fi
      else
        full_broadcast_result_quorum="$FULL_BROADCAST_RESULT_QUORUM_RAFT_KAFKA"
      fi
      if [[ "$full_broadcast_result_quorum" -lt 0 || "$full_broadcast_result_quorum" -gt 4 ]]; then
        die "selected broadcast-result-quorum must be between 0 and 4 (mode=$cluster_mode thread=$th value=$full_broadcast_result_quorum)"
      fi
      if [[ -n "$FULL_BROADCAST_DRAIN_IN_TIMED_RUN" ]]; then
        full_broadcast_drain_in_timed_run="$FULL_BROADCAST_DRAIN_IN_TIMED_RUN"
      elif [[ "$cluster_mode" == "kafka-only" ]]; then
        full_broadcast_drain_in_timed_run="$FULL_BROADCAST_DRAIN_IN_TIMED_RUN_KAFKA_ONLY"
      else
        full_broadcast_drain_in_timed_run="$FULL_BROADCAST_DRAIN_IN_TIMED_RUN_RAFT_KAFKA"
      fi
      if [[ -n "$FULL_DIRECT_COMPLETION_QUORUM" ]]; then
        full_direct_completion_quorum="$FULL_DIRECT_COMPLETION_QUORUM"
      elif [[ "$cluster_mode" == "kafka-only" ]]; then
        full_direct_completion_quorum="$FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY"
        if override="$(lookup_thread_override "$FULL_DIRECT_COMPLETION_QUORUM_KAFKA_ONLY_MAP" "$th")"; then
          full_direct_completion_quorum="$override"
        fi
      else
        full_direct_completion_quorum="$FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA"
        if override="$(lookup_thread_override "$FULL_DIRECT_COMPLETION_QUORUM_RAFT_KAFKA_MAP" "$th")"; then
          full_direct_completion_quorum="$override"
        fi
      fi
      if [[ "$full_direct_completion_quorum" -lt 1 || "$full_direct_completion_quorum" -gt 4 ]]; then
        die "selected direct-completion-quorum must be between 1 and 4 (mode=$cluster_mode thread=$th value=$full_direct_completion_quorum)"
      fi
      if [[ "$cluster_mode" == "kafka-only" ]]; then
        full_result_replica_limit="$FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY"
        if override="$(lookup_thread_override "$FULL_RESULT_REPLICA_LIMIT_KAFKA_ONLY_MAP" "$th")"; then
          full_result_replica_limit="$override"
        fi
        full_result_publish_replica_limit="$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY"
        if override="$(lookup_thread_override "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_KAFKA_ONLY_MAP" "$th")"; then
          full_result_publish_replica_limit="$override"
        fi
      else
        full_result_replica_limit="$FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA"
        if override="$(lookup_thread_override "$FULL_RESULT_REPLICA_LIMIT_RAFT_KAFKA_MAP" "$th")"; then
          full_result_replica_limit="$override"
        fi
        full_result_publish_replica_limit="$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA"
        if override="$(lookup_thread_override "$FULL_RESULT_PUBLISH_REPLICA_LIMIT_RAFT_KAFKA_MAP" "$th")"; then
          full_result_publish_replica_limit="$override"
        fi
      fi
      # Disjoint req-id range per cluster mode so stale Kafka result messages
      # from a kafka-only case cannot contaminate a later raft-kafka case (and
      # vice versa) — the gateway result-matching key is the req id, and the
      # base formula (run*1e6 + th*1e4 + 1) was identical across modes.
      if [[ "$cluster_mode" == "kafka-only" ]]; then
        mode_offset=100000000
      else
        mode_offset=200000000
      fi
      req_id_offset=$(( mode_offset + (run * 1000000) + (th * 10000) + 1 ))
      log "Cluster case mode=$cluster_mode thread=$th run=$run experiment=$EXPERIMENT_MODE (num-terminals=$full_num_terminals conn-fanout=$full_conn_fanout broadcast-accept-quorum=$full_broadcast_accept_quorum broadcast-result-quorum=$full_broadcast_result_quorum broadcast-drain-in-timed-run=$full_broadcast_drain_in_timed_run direct-completion-quorum=$full_direct_completion_quorum preferred-leader-id=$FULL_PREFERRED_LEADER_ID det-pipeline-depth=$full_det_pipeline_depth effective-inflight=$full_effective_inflight pool-size=$full_pool_size worker-count=$full_bcdb_worker_count det-batch=$full_det_batch_size det-window=$full_det_window det-block-parallel=$FULL_DET_BLOCK_PARALLEL det-block-pipeline=$FULL_DET_BLOCK_PIPELINE det-block-max=$FULL_DET_BLOCK_MAX det-partial-block-max-wait-us=$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US serial-gate=$FULL_BCDB_SERIAL_GATE_MODE serial-gate-source=$FULL_BCDB_SERIAL_GATE_SOURCE dt-parse-barrier=$FULL_BCDB_DT_PARSE_BARRIER completion-only-skip-reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS full-result-replica-limit=$full_result_replica_limit result-publish-replica-limit=$full_result_publish_replica_limit kafka-completion-mode=$FULL_KAFKA_COMPLETION_MODE ordering_path=$ordering_path)"
      before_file="$RUN_LOG_DIR/full_${cluster_mode}_before_${th}_${run}.txt"
      after_file="$RUN_LOG_DIR/full_${cluster_mode}_after_${th}_${run}.txt"
      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$before_file" || true

      extra_skip=()
      [[ "$FULL_SKIP_SYNC" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-sync)
      [[ "$FULL_SKIP_BUILD" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-build)
      [[ "$FULL_SKIP_RDKAFKA_SETUP" == "1" || "$FULL_SYNC_FIRST" == "0" ]] && extra_skip+=(--skip-rdkafka-setup)

      set +e
      timeout "$FULL_CASE_TIMEOUT_S" env POLL_COUNT="$POLL_COUNT" RESULT_RING_CAPACITY="$RESULT_RING_CAPACITY" SKIP_CLUSTER_LOGS="$FULL_SKIP_CLUSTER_LOGS" \
      "$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh" \
        "${extra_skip[@]}" \
        --ordering-mode "$cluster_mode" \
        --workload "$REPO_ROOT/scripts/$WORKLOAD" \
        --test-queries "$FULL_TEST_QUERIES" \
        --req-id-offset "$req_id_offset" \
        --pool-size "$full_pool_size" \
        --bcdb-worker-count "$full_bcdb_worker_count" \
        --bcdb-decouple-workers "$FULL_BCDB_DECOUPLE_WORKERS" \
        --det-batch-size "$full_det_batch_size" \
        --det-window "$full_det_window" \
        --det-block-pipeline "$FULL_DET_BLOCK_PIPELINE" \
        --det-block-parallel "$FULL_DET_BLOCK_PARALLEL" \
        --det-block-max "$FULL_DET_BLOCK_MAX" \
        --det-partial-block-max-wait-us "$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US" \
        --num-terminals "$full_num_terminals" \
        --conn-fanout "$full_conn_fanout" \
        --broadcast-accept-quorum "$full_broadcast_accept_quorum" \
        --broadcast-result-quorum "$full_broadcast_result_quorum" \
        --broadcast-drain-in-timed-run "$full_broadcast_drain_in_timed_run" \
        --direct-completion-quorum "$full_direct_completion_quorum" \
        --det-pipeline-depth "$full_det_pipeline_depth" \
        --bcdb-block-profile "$FULL_BCDB_BLOCK_PROFILE" \
        --bcdb-block-wait-watermark "$FULL_BCDB_BLOCK_WAIT_WATERMARK" \
        --bcdb-serial-gate-mode "$FULL_BCDB_SERIAL_GATE_MODE" \
        --bcdb-serial-gate-source "$FULL_BCDB_SERIAL_GATE_SOURCE" \
        --bcdb-dt-parse-barrier "$FULL_BCDB_DT_PARSE_BARRIER" \
        --bcdb-dt-skip-readonly-gate "$FULL_BCDB_DT_SKIP_READONLY_GATE" \
        --bcdb-dt-completion-only-skip-reads "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" \
        --bcdb-dt-hashtab-switch-threshold "$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD" \
        --bcdb-det-queue-high-wm "$FULL_BCDB_DET_QUEUE_HIGH_WM" \
        --bcdb-det-queue-low-wm "$FULL_BCDB_DET_QUEUE_LOW_WM" \
        --full-result-replica-limit "$full_result_replica_limit" \
        --result-publish-replica-limit "$full_result_publish_replica_limit" \
        --preferred-leader-id "$FULL_PREFERRED_LEADER_ID" \
        --kafka-completion-mode "$FULL_KAFKA_COMPLETION_MODE" \
        > "$RUN_LOG_DIR/full_${cluster_mode}_thread_${th}_run_${run}.log" 2>&1
      rc=$?
      set -e
      FULL_SYNC_FIRST=0

      ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$after_file" || true
      artifact="$(grep -vxF -f "$before_file" "$after_file" | head -n 1 || true)"
      if [[ -z "$artifact" ]]; then
        artifact="$(head -n 1 "$after_file" || true)"
      fi
      [[ -n "$artifact" ]] || artifact="$RUN_LOG_DIR/missing_full_artifact_thread_${th}_run_${run}"
      notes="cluster_mode=$cluster_mode;ordering_path=$ordering_path;completion_path=$cluster_completion_path;validation_mode=$cluster_validation_mode;kafka_completion_mode=$FULL_KAFKA_COMPLETION_MODE;experiment_mode=$EXPERIMENT_MODE;full_system_thread_knob=$FULL_THREAD_KNOB;full_pool_size_mode=$FULL_POOL_SIZE_MODE;num_terminals=$full_num_terminals;conn_fanout=$full_conn_fanout;broadcast_accept_quorum=$full_broadcast_accept_quorum;broadcast_result_quorum=$full_broadcast_result_quorum;broadcast_drain_in_timed_run=$full_broadcast_drain_in_timed_run;direct_completion_quorum=$full_direct_completion_quorum;preferred_leader_id=$FULL_PREFERRED_LEADER_ID;det_pipeline_depth=$full_det_pipeline_depth;effective_inflight=$full_effective_inflight;trusted_gate=$cluster_trusted_gate;pool_size_min=2;det_window_multiplier=$FULL_DET_WINDOW_MULTIPLIER;det_window_max=$FULL_DET_WINDOW_MAX;det_window_map=$FULL_DET_WINDOW_MAP;det_window_kafka_only_map=$FULL_DET_WINDOW_KAFKA_ONLY_MAP;det_window_raft_kafka_map=$FULL_DET_WINDOW_RAFT_KAFKA_MAP;det_batch_size_map=$FULL_DET_BATCH_SIZE_MAP;det_block_parallel=$FULL_DET_BLOCK_PARALLEL;det_block_pipeline=$FULL_DET_BLOCK_PIPELINE;det_block_max=$FULL_DET_BLOCK_MAX;det_partial_block_max_wait_us=$FULL_DET_PARTIAL_BLOCK_MAX_WAIT_US;bcdb_block_wait_watermark=$FULL_BCDB_BLOCK_WAIT_WATERMARK;bcdb_serial_gate_mode=$FULL_BCDB_SERIAL_GATE_MODE;bcdb_serial_gate_source=$FULL_BCDB_SERIAL_GATE_SOURCE;bcdb_dt_parse_barrier=$FULL_BCDB_DT_PARSE_BARRIER;bcdb_dt_skip_readonly_gate=$FULL_BCDB_DT_SKIP_READONLY_GATE;bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;det_block_skip_readonly=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS;bcdb_dt_hashtab_switch_threshold=$FULL_BCDB_DT_HASHTAB_SWITCH_THRESHOLD;bcdb_det_queue_high_wm=$FULL_BCDB_DET_QUEUE_HIGH_WM;bcdb_det_queue_low_wm=$FULL_BCDB_DET_QUEUE_LOW_WM;full_result_replica_limit=$full_result_replica_limit;result_publish_replica_limit=$full_result_publish_replica_limit;backend_capacity=normalized"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$cluster_series" "$cluster_mode_label" "$EXPERIMENT_MODE" "$cluster_mode" "$ordering_path" "$cluster_completion_path" "$server_bypass_raft" "$gateway_broadcast_to_all" "$th" "$run" "$artifact" "$rc" "$FULL_THREAD_KNOB" "$full_pool_size" "$full_bcdb_worker_count" "$full_det_batch_size" "$full_det_window" "$full_num_terminals" "$full_det_pipeline_depth" "$full_effective_inflight" "$FULL_DET_BLOCK_PIPELINE" "$FULL_DET_BLOCK_MAX" "$req_id_offset" "completed_eq_loaded_required" "$notes" \
        >> "$FULL_MANIFEST"
      if [[ "$rc" != "0" ]]; then
        if [[ "$FULL_CONTINUE_ON_ERROR" == "1" ]]; then
          log "WARNING: full-system case thread=$th run=$run exited rc=$rc; continuing because FULL_CONTINUE_ON_ERROR=1"
        else
          log "WARNING: full-system case thread=$th run=$run exited rc=$rc; stopping full-system sweep so the failed replica cannot poison later points"
          return 0
        fi
      fi
      done
    done
  done
}

generate_outputs() {
  local single_results="$SINGLE_LOCAL_DIR/results.csv"
  [[ "$FULL_ONLY" == "1" ]] && single_results="${SINGLE_RESULTS_CSV:-$single_results}"
  if [[ ! -f "$single_results" ]]; then
    if [[ "$FULL_ONLY" == "1" ]]; then
      log "Single-node results unavailable in --full-only mode; generating full-system-only outputs"
      single_results="/dev/null"
    else
      die "missing single-node results.csv: $single_results"
    fi
  fi
  [[ -f "$FULL_MANIFEST" ]] || die "missing full-system manifest: $FULL_MANIFEST"
  single_gateway_args=()
  if [[ -f "$SINGLE_GATEWAY_MANIFEST" ]]; then
    single_gateway_args=(--single-gateway-manifest "$SINGLE_GATEWAY_MANIFEST")
  fi
  local x_label="Threads"
  if [[ "$FULL_THREAD_KNOB" == "client-pipeline" ]]; then
    if [[ "$FULL_DET_PIPELINE_DEPTH_WAS_SET" == "1" ]]; then
      x_label="Single-node client threads / full-system numTerminals (detPipelineDepth=$FULL_DET_PIPELINE_DEPTH, experiment=$EXPERIMENT_MODE)"
    else
      x_label="Single-node client threads / full-system numTerminals (mode-specific detPipelineDepth, experiment=$EXPERIMENT_MODE)"
    fi
  elif [[ "$FULL_THREAD_KNOB" == "concurrency" ]]; then
    x_label="Single-node client threads / full-system ordered concurrency budget"
  elif [[ "$FULL_THREAD_KNOB" == "fixed-window" ]]; then
    x_label="Single-node client threads / full-system labeled points (fixed detWindow=$FULL_DET_WINDOW)"
  fi
  MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" \
    python3 "$SCRIPT_DIR/plot_ycsb_skew_tps_comparison.py" \
      --single-results "$single_results" \
      "${single_gateway_args[@]}" \
      --full-manifest "$FULL_MANIFEST" \
      --out-dir "$OUT_DIR" \
      --workload "$WORKLOAD" \
      --machine "$TARGET_MACHINE_LABEL" \
      --threads "$THREADS" \
      --x-label "$x_label"
  if [[ "$FULL_ONLY" != "1" && -f "$SCRIPT_DIR/build_ycsb_capacity_graph.py" ]]; then
    MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/mplconfig}" \
      python3 "$SCRIPT_DIR/build_ycsb_capacity_graph.py" \
        --single-root "$OUT_DIR" \
        --gateway-cluster-summary "$OUT_DIR/summary.csv" \
        --threads "$THREADS" \
        --workload "$WORKLOAD" \
        --out-dir "$OUT_DIR"
  fi
}

log "=== YCSB skew TPS comparison ==="
log "Out root  : $OUT_ROOT"
log "Threads   : $THREADS"
log "Runs      : $RUNS"
log "Workloads : $WORKLOADS"
log "Targets   : ${TARGETS_ARR[*]}"
log "Single target pick: $SINGLE_TARGET_PICK"
log "Cluster modes: $FULL_CLUSTER_MODES"
log "Experiment mode: $EXPERIMENT_MODE"
log "Kafka completion mode: $FULL_KAFKA_COMPLETION_MODE"
log "Skip-read semantics: full/gateway bcdb_dt_completion_only_skip_reads=$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS raw-single-det=$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS"
if [[ "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" == "1" ]]; then
  log "Capacity semantics: completion-only SELECT path with final state/Merkle verification; set FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS=0 for real returned-read control runs"
fi
if [[ "$FULL_BCDB_DT_COMPLETION_ONLY_SKIP_READS" != "$SINGLE_BCDB_DT_COMPLETION_ONLY_SKIP_READS" ]]; then
  log "WARNING: raw single-node DET skip-read semantics differ from full/gateway runs; artifact notes will show the mismatch"
fi

IFS=',' read -ra workload_arr <<< "$WORKLOADS"

if [[ "$ANALYZE_ONLY" != "1" ]]; then
  sync_single_target
fi

for wl in "${workload_arr[@]}"; do
  wl="${wl//[[:space:]]/}"
  [[ -z "$wl" ]] && continue
  set_workload_paths "$wl"
  log "--- Workload: $WORKLOAD (out=$OUT_DIR) ---"
  apply_workload_calibrated_defaults
  if [[ "$ANALYZE_ONLY" != "1" ]]; then
    run_single_node
  fi
  if [[ "$SINGLE_ONLY" == "1" ]]; then
    log "Workload single-only done: $WORKLOAD"
    for _lbl in "${LABELS_ARR[@]}"; do
      _d="$(single_dir_for_label "$_lbl")"
      log "  $_lbl: $_d/results.csv  summary=$_d/summary.csv"
    done
    continue
  fi
  if [[ "$FULL_ONLY" != "1" ]]; then
    pick_comparison_node
  fi
  if [[ "$ANALYZE_ONLY" != "1" ]]; then
    run_single_gateway_direct
    run_full_system
  fi
  generate_outputs
  log "Workload done: $WORKLOAD"
  log "  Results : $OUT_DIR/results.csv"
  log "  Summary : $OUT_DIR/summary.csv"
  log "  Overhead: $OUT_DIR/overhead.csv"
  log "  Graph   : $OUT_DIR/ycsb_skew_pg_vs_det.png"
  log "  Graph   : $OUT_DIR/ycsb_skew_det_vs_cluster.png"
  log "  Graph   : $OUT_DIR/ycsb_skew_all_systems.png"
  [[ -f "$OUT_DIR/ycsb_skew_capacity_all_systems.png" ]] && log "  Graph   : $OUT_DIR/ycsb_skew_capacity_all_systems.png"
done

log "Done"
log "Out root : $OUT_ROOT"
