#!/usr/bin/env bash
# ==============================================================================
# Full 1,920-Run YCSB Benchmark Campaign Orchestrator
# Executes all 12 workload families x 8 skews x 5 worker counts x 4 modes
# Followed by automatic graph generation and comprehensive report compiling.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/scripts/bench_full_results/ycsb_32mb_$(date -u +%Y%m%dT%H%M%SZ)}"
export OUT_DIR
LOG_FILE="${OUT_DIR}/sweep.log"

mkdir -p "$OUT_DIR"

echo "=============================================================================="
echo "LAUNCHING FULL 1,920-RUN YCSB BENCHMARK CAMPAIGN"
echo "Repo Root:  $REPO_ROOT"
echo "Output Dir: $OUT_DIR"
echo "Log File:   $LOG_FILE"
echo "Start Time: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "=============================================================================="

export PYTHONUNBUFFERED=1

# The sweep writes graphs and a data-derived report into its own output folder.
# Legacy scratch report generators target the old campaign and embed old claims.
exec bash "${REPO_ROOT}/scripts/distributed/run_full_72_ycsb_campaign.sh" "$@"
