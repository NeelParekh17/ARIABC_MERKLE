#!/usr/bin/env bash
# Recovery chain: cmd 6 from COMMANDS.md
# Runs on ranking host, fully independent of gateway chain.
# synchronous_commit=off: recovery latency is the metric; replay if crash.
set -Eeuo pipefail

RESULT_ROOT="/work/ARIABC/AriaBC/Final_Results/reruns/20260922T133957Z_122862"
ROOT="/work/ARIABC/AriaBC"
RECOVERY_REPETITIONS=10

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S%z')] [recovery-chain] $*"; }

cd "$ROOT"

log "=== CMD 6/6: Recovery size-scaling-k75-c300 ==="
mkdir -p "$RESULT_ROOT/Recovery"

RECOVERY_LOG_TEE_ACTIVE=1 \
bash scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh \
  --host ranking \
  --ssh-user protectdr \
  --remote-root /home/protectdr/merkle_recovery_runs \
  --remote-python /usr/bin/python3 \
  --profile size-scaling-k75-c300 \
  --build-profile release \
  --fanout 32 \
  --geometry-label fanout_f32_l16 \
  --partitions 200 \
  --levels-per-batch 1 \
  --leaf-fetch-batch-size 64 \
  --corruption-mode mixed \
  --repetitions "$RECOVERY_REPETITIONS" \
  --profiling off \
  --track-counts on \
  --artifact-mode summary \
  --audit-mode full \
  --synchronous-commit off \
  --cpu-affinity 176-183 \
  --warmup-cycles 6 \
  2>&1 | tee "$RESULT_ROOT/Recovery/runner.log"

# Runner's last stdout line is the fetched artifact directory.
RECOVERY_FETCHED="$(tail -n 1 "$RESULT_ROOT/Recovery/runner.log")"
test -d "$RECOVERY_FETCHED"
cp -a --reflink=never "$RECOVERY_FETCHED" "$RESULT_ROOT/Recovery/"

log "=== CMD 6/6: Recovery DONE — artifact: $RECOVERY_FETCHED ==="
log "=== RECOVERY CHAIN COMPLETE ==="
