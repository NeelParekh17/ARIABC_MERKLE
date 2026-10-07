#!/usr/bin/env bash
# run_recovery_cluster_test.sh — online replica recovery run on the real cluster.
#
# Same configuration as bench_cluster_threads.sh (8 server workers, 96 client
# lanes, raft-kafka leader-assigned ordering, majority_async_all3), plus online
# recovery and optional fault injection.  Extra arguments are passed through to
# run_4node_raft_cluster.sh.
#
# Examples:
#   # baseline, no recovery
#   run_recovery_cluster_test.sh --recovery-mode off
#   # recovery enabled, no fault (overhead check)
#   run_recovery_cluster_test.sh --recovery-mode both --skip-sync --skip-build
#   # corrupt 100 tuples on utkarsh 5 s into the run
#   run_recovery_cluster_test.sh --recovery-mode both --inject-fault-node utkarsh \
#       --inject-fault-count 100 --inject-fault-delay-sec 5 --skip-sync --skip-build
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
WORKERS="${WORKERS:-8}"
WORKLOAD="${WORKLOAD:-$REPO_ROOT/scripts/ycsb_recovery/ycsb_workload_a_skew_0_00_160k.txt}"

export ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS="${ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS:-128}"
export ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS="${ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS:-256}"
export ARIABC_FULL_RESULT_REPLICA_LIMIT="${ARIABC_FULL_RESULT_REPLICA_LIMIT:--1}"
export ARIABC_KAFKA_PAYLOAD_FORMAT="${ARIABC_KAFKA_PAYLOAD_FORMAT:-text}"
export BCDB_DET_QUEUE_HIGH_WM="${BCDB_DET_QUEUE_HIGH_WM:-65536}"
export BCDB_DET_QUEUE_LOW_WM="${BCDB_DET_QUEUE_LOW_WM:-32768}"
export GATEWAY_STALL_WATCHDOG="${GATEWAY_STALL_WATCHDOG:-1}"
export GATEWAY_STALL_POLL_SECONDS="${GATEWAY_STALL_POLL_SECONDS:-5}"
export GATEWAY_STALL_MAX_CYCLES="${GATEWAY_STALL_MAX_CYCLES:-12}"
export KAFKA_FAST_RESET="${KAFKA_FAST_RESET:-1}"
export DUMP_VERIFY_CSV="${DUMP_VERIFY_CSV:-0}"
export TX_SIGN="${TX_SIGN:-blake3}"
export BENCH_COLD_CACHE="${BENCH_COLD_CACHE:-0}"
export RECOVERY_INTERVAL_MS="${RECOVERY_INTERVAL_MS:-1000}"

exec bash "$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh" \
  --threads 96 --det-client-workers 96 --det-client-inflight 16 \
  --per-thread-window 256 --det-batch-size 256 \
  --pool-size "$WORKERS" --server-exec-workers "$WORKERS" --server-pg-connections "$WORKERS" \
  --bcdb-workers "$WORKERS" --bcdb-init-block-size "$WORKERS" --bcdb-decouple-workers 1 \
  --conn-fanout 1 --raft-ordered-fanout 1 --raft-ordering-policy leader-assigned \
  --raft-ordered-batch-append 1 --raft-ordered-batch-target-entries 64 \
  --raft-ordered-batch-linger-us 1000 --raft-ordered-coalesce-log 1 \
  --kafka-completion-mode majority_async_all3 --det-window 1024 \
  --enable-merkle-index 1 --tx-sign blake3 --raft-apply-ledger-mode off \
  --db-shared-buffers 32MB --det-pipeline-depth 0 --det-block-parallel 64 \
  --det-event-block-fastpath 0 --submit-mode event --parallelism-mode pipeline \
  --ordering-mode raft-kafka --workload "$WORKLOAD" \
  "$@"
