#!/usr/bin/env bash
# ==============================================================================
# Exact Replication Script for AriaBC Dynamic Recovery Scaling Evaluation
#
# Suite: Dynamic Merkle Recovery Scaling (1M to 50M rows, 110 runs)
# Geometry: Fanout F=32, split_threshold=32, merge_threshold=8, partitions=200
# Contention: K=75 bad leaves, C=300 corruptions, mixed corruption mode
# Infrastructure: Remote AMD EPYC 9654 node (ranking.cse.iitb.ac.in / user-MZ73-LM0-000)
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
HOST="${RECOVERY_HOST:-ranking}"
SSH_USER="${RECOVERY_SSH_USER:-protectdr}"
FANOUT="${RECOVERY_FANOUT:-32}"
PROFILE="${RECOVERY_PROFILE:-size-scaling-k75-c300}"
BUILD_PROFILE="${RECOVERY_BUILD_PROFILE:-release}"
CORRUPTION_MODE="${RECOVERY_CORRUPTION_MODE:-mixed}"
AUDIT_MODE="${RECOVERY_AUDIT_MODE:-skip}"
CPU_AFFINITY="${RECOVERY_CPU_AFFINITY:-176-183}"
WARMUP_CYCLES="${RECOVERY_WARMUP_CYCLES:-6}"

echo "================================================================================"
echo "AriaBC Dynamic Recovery Benchmark Replication Pipeline"
echo "Timestamp:        ${TIMESTAMP}"
echo "Repository:       ${REPO_ROOT}"
echo "Target Host:      ${SSH_USER}@${HOST}"
echo "Profile:          ${PROFILE}"
echo "Fanout:           F=${FANOUT}"
echo "Build Profile:    ${BUILD_PROFILE}"
echo "Corruption Mode:  ${CORRUPTION_MODE}"
echo "Audit Mode:       ${AUDIT_MODE}"
echo "CPU Affinity:     ${CPU_AFFINITY} (cores pinned for 1.50 GHz base clock)"
echo "Warmup Cycles:    ${WARMUP_CYCLES}"
echo "================================================================================"

# ------------------------------------------------------------------------------
# 1. Execute Remote Synced Recovery Benchmark
# ------------------------------------------------------------------------------
echo "[-] Launching synced remote recovery benchmark on ${HOST}..."
FETCHED_DIR=$(./scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh \
  --host "${HOST}" \
  --ssh-user "${SSH_USER}" \
  --profile "${PROFILE}" \
  --fanout "${FANOUT}" \
  --build-profile "${BUILD_PROFILE}" \
  --corruption-mode "${CORRUPTION_MODE}" \
  --audit-mode "${AUDIT_MODE}" \
  --cpu-affinity "${CPU_AFFINITY}" \
  --warmup-cycles "${WARMUP_CYCLES}" | tail -n 1)

if [[ ! -d "${FETCHED_DIR}" ]]; then
  echo "[!] Error: fetched directory does not exist: ${FETCHED_DIR}" >&2
  exit 1
fi

RUN_ID=$(basename "${FETCHED_DIR}")
echo "[+] Remote benchmark completed successfully. Run ID: ${RUN_ID}"
echo "[+] Local fetched artifacts at: ${FETCHED_DIR}"

# ------------------------------------------------------------------------------
# 2. Archive Artifacts into Final_Results/Recovery/
# ------------------------------------------------------------------------------
TARGET_RECOVERY_DIR="${REPO_ROOT}/Final_Results/Recovery"
mkdir -p "${TARGET_RECOVERY_DIR}/plots"

echo "[-] Archiving raw run artifacts into ${TARGET_RECOVERY_DIR}/${RUN_ID}..."
cp -r "${FETCHED_DIR}" "${TARGET_RECOVERY_DIR}/"

# Copy generated single-run plots
if [[ -d "${REPO_ROOT}/Dynamic_merkle_docs/run_reports/${RUN_ID}/plots" ]]; then
  echo "[-] Copying single-run visualization plots..."
  cp -r "${REPO_ROOT}/Dynamic_merkle_docs/run_reports/${RUN_ID}/plots/"* "${TARGET_RECOVERY_DIR}/plots/"
fi

# ------------------------------------------------------------------------------
# 3. Generate Side-by-Side Comparison vs Aug 24 Baseline (00dad3)
# ------------------------------------------------------------------------------
BASELINE_DIR="${REPO_ROOT}/scripts/benchmark/recovery/fetched/ariabc-recovery-size-scaling-k75-c300-20260824T151600Z-00dad3"
if [[ -d "${BASELINE_DIR}" ]]; then
  echo "[-] Generating comparative delta analysis vs Aug 24 baseline..."
  mkdir -p "${TARGET_RECOVERY_DIR}/plots/comparison_vs_aug24"
  python3 scripts/benchmark/recovery/compare_dynamic_runs.py \
    --old-dir "${BASELINE_DIR}" \
    --new-dir "${FETCHED_DIR}" \
    --old-label "Aug 24 Baseline (F=4)" \
    --new-label "New Codebase (F=32)" \
    --output-dir "${TARGET_RECOVERY_DIR}/plots/comparison_vs_aug24"
fi

SEP19_DIR="${REPO_ROOT}/scripts/benchmark/recovery/fetched/ariabc-recovery-size-scaling-k75-c300-20260919T125919Z-004587"
if [[ -d "${SEP19_DIR}" ]]; then
  echo "[-] Generating comparative delta analysis vs Sep 19 latest run..."
  mkdir -p "${TARGET_RECOVERY_DIR}/plots/comparison_vs_sep19"
  python3 scripts/benchmark/recovery/compare_dynamic_runs.py \
    --old-dir "${SEP19_DIR}" \
    --new-dir "${FETCHED_DIR}" \
    --old-label "Sep 19 Latest (F=4, 004587)" \
    --new-label "New Run (F=32, ${RUN_ID##*-})" \
    --output-dir "${TARGET_RECOVERY_DIR}/plots/comparison_vs_sep19"
fi

echo "================================================================================"
echo "[+] Recovery Replication Complete!"
echo "Raw Results:  ${TARGET_RECOVERY_DIR}/${RUN_ID}"
echo "Report:       ${TARGET_RECOVERY_DIR}/Report.md"
echo "Plots:        ${TARGET_RECOVERY_DIR}/plots/"
echo "================================================================================"
