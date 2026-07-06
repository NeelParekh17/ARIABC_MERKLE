#!/bin/bash
cd /work/ARIABC/AriaBC
set -euo pipefail

echo "=== Building targets ==="
cmake --build ariabc_pg/build \
  --target ariabc_pg_gateway ariabc_pg_server \
  -j"$(nproc)"

echo "=== Checking script syntax ==="
bash -n scripts/distributed/run_4node_raft_cluster.sh

echo "=== Checking git style rules ==="
# If this fails, the script will stop cleanly instead of killing your terminal
git diff --check

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="scripts/bench_full_results/pg_executor_sweep_${STAMP}"
mkdir -p "$OUT"
: > "$OUT/run_dirs.txt"
: > "$OUT/summary.csv"

printf 'pg_executor_workers,rep,artifact\n' > "$OUT/runs.csv"

# Wrapped execution sequence to funnel into out.txt
{
  for E in 1 2 4 8 12 16; do
    for REP in 1 2 3; do
      echo
      echo "=============================================================="
      echo "PG/EXECUTOR WORKERS=$E | REP=$REP"
      echo "=============================================================="

      BEFORE="$(mktemp)"
      find scripts/bench_full_results \
        -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
        -printf '%f\n' | sort > "$BEFORE"

      env \
        SKIP_RDKAFKA_SETUP=1 \
        ARIABC_PREFERRED_LEADER_ID=1 \
        ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1 \
        ARIABC_RAFT_STREAM_GAP=512 \
        ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1 \
        BCDB_DET_QUEUE_HIGH_WM=128 \
        BCDB_DET_QUEUE_LOW_WM=64 \
        ./scripts/distributed/run_4node_raft_cluster.sh \
          --ordering-mode raft-kafka \
          --execution-profile threaded-raft-direct \
          --threads 96 \
          --det-client-workers 96 \
          --det-client-inflight 1 \
          --server-exec-workers "$E" \
          --server-pg-connections "$E" \
          --bcdb-workers 8 \
          --bcdb-init-block-size 8 \
          --bcdb-decouple-workers 1 \
          --raft-ordered-fanout 1 \
          --raft-ordering-policy leader-assigned \
          --raft-ordered-batch-append 1 \
          --raft-ordered-batch-target-entries 32 \
          --raft-ordered-batch-linger-us 1000 \
          --raft-ordered-coalesce-log 0 \
          --kafka-completion-mode async \
          --det-window 65536

      RUN_NAME="$(
        comm -13 "$BEFORE" \
          <(find scripts/bench_full_results \
            -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
            -printf '%f\n' | sort) | tail -n1
      )"
      rm -f "$BEFORE"

      RUN="scripts/bench_full_results/$RUN_NAME"
      [[ -d "$RUN" ]] || { echo "Could not locate fresh benchmark artifact"; exit 1; }

      printf '%s,%s,%s\n' "$E" "$REP" "$RUN" | tee -a "$OUT/runs.csv"
      printf '%s\n' "$RUN" >> "$OUT/run_dirs.txt"

      python3 scripts/distributed/summarize_raft_profile.py "$RUN" | tee -a "$OUT/summary.csv"
    done
  done

  echo
  echo "Done."
  echo "Artifacts: $OUT"

} > out.txt 2>&1
