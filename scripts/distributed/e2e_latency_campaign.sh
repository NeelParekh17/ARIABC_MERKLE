#!/usr/bin/env bash
# e2e_latency_campaign.sh — per-transaction end-to-end latency on the cluster.
#
# Runs the worker-thread sweep (bench_cluster_threads.sh) under three load
# shapes. The gateway's tx_latency.csv carries the per-tx timeline
# (submit → leader accept → first replica result → majority verified →
# client completion):
#   E0 unloaded  — 1 transaction outstanding (pure per-tx path cost)
#   E1 bounded   — closed loop, 256 outstanding (latency under steady load)
#   E2 flood     — legacy config: all transactions submitted at once
#
# Usage: e2e_latency_campaign.sh [OUT_DIR]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/scripts/bench_full_results/e2e_latency_$(date +%Y%m%d_%H%M%S)}"
WORKERS="${WORKERS:-1,4,8,16}"
E0_WORKERS="${E0_WORKERS:-1,16}"
mkdir -p "$OUT_DIR"

run() {
  local name="$1" workers="$2" window="$3"
  echo "[$(date +%H:%M:%S)] === $name workers=$workers det-window=$window ==="
  bash "$SCRIPT_DIR/bench_cluster_threads.sh" \
    --workers "$workers" --out-dir "$OUT_DIR/$name" \
    -- --det-window "$window" 2>&1 | tee "$OUT_DIR/$name.log" ||
    # The sweep exits non-zero when TPS does not scale with workers, which
    # is expected for E0 (one tx outstanding); per-run status is in its CSV.
    echo "[$(date +%H:%M:%S)] $name: sweep exited non-zero (see $name.log)"
}

run E0_unloaded "$E0_WORKERS" 1
run E1_bounded256 "$WORKERS" 256
run E2_flood "$WORKERS" 65536

echo "[$(date +%H:%M:%S)] campaign done: $OUT_DIR"
