#!/usr/bin/env bash
# Gateway chain v3: YCSB → OOM-1 → OOM-2 → TPCC-workers → TPCC-warehouses
# Changes vs v1/v2:
#   - FORCE_BUILD=1 for YCSB to rebuild stale cluster manifests
#   - Fresh RESULT_ROOT (campaign-contract change required new dir)
#   - run_4node_raft_cluster.sh now has +3s post-leader stabilization sleep
set -Eeuo pipefail

RESULT_ROOT="/work/ARIABC/AriaBC/Final_Results/reruns/20260922T140053Z_129359"
ROOT="/work/ARIABC/AriaBC"

YCSB_TRIALS=1
OOM_TRIALS=1
TPCC_TRIALS=3

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S%z')] [gateway-chain-v3] $*"; }

cd "$ROOT"
unset SKIP_SYNC SKIP_BUILD DET_CLIENT_WORKERS || true

# FORCE_BUILD=1: cluster node manifests are stale (binary rebuilt after manifest written).
# Kept for ALL cluster cases so every cold-run restart rebuilds correctly.
export FORCE_BUILD=1

# ── 1. YCSB ────────────────────────────────────────────────────────────────────
log "=== CMD 1/5: YCSB ==="
python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark ycsb \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-host 10.129.148.247 \
  --db-user neel \
  --db-port 5438 \
  --server-port 8000 \
  --workloads abcdf_4skews \
  --workers 1,4,8,16 \
  --modes cluster,pg,bcdb_det,bcdb_merkle \
  --run-cluster \
  --db-shared-buffers 32MB \
  --cold-runs \
  --order-seed 42 \
  --trials "$YCSB_TRIALS" \
  --out-dir "$RESULT_ROOT/YCSB"
log "=== CMD 1/5: YCSB DONE ==="

# ── 2. OOM 100M — skews 0 and .99 ──────────────────────────────────────────────
log "=== CMD 2/5: OOM skews 0 and .99 ==="
python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --remote-host 10.129.148.247 \
  --remote-user neel \
  --remote-dir /tmp/ariabc_oom_100m \
  --install-dir /home/neel/Desktop/ariabc_install \
  --cluster-dir /home/neel/Desktop/ariabc_cluster \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-port 5438 \
  --server-port 8000 \
  --db-rows 100000000 \
  --shared-buffers 32MB \
  --txs 20000 \
  --seed 42 \
  --workloads a \
  --skews 0.0 0.99 \
  --workers 1 8 16 \
  --modes pg bcdb_det bcdb_merkle \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_and_0_99"
log "=== CMD 2/5: OOM skews 0 and .99 DONE ==="

# ── 3. OOM 100M — skews .5 and 1.2 ────────────────────────────────────────────
log "=== CMD 3/5: OOM skews .5 and 1.2 ==="
python3 -u scripts/distributed/run_oom_100m_benchmark.py \
  --remote-host 10.129.148.247 \
  --remote-user neel \
  --remote-dir /tmp/ariabc_oom_100m \
  --install-dir /home/neel/Desktop/ariabc_install \
  --cluster-dir /home/neel/Desktop/ariabc_cluster \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --db-port 5438 \
  --server-port 8000 \
  --db-rows 100000000 \
  --shared-buffers 32MB \
  --txs 20000 \
  --seed 42 \
  --workloads a \
  --skews 0.5 1.2 \
  --workers 1 8 16 \
  --modes pg bcdb_det bcdb_merkle \
  --trials "$OOM_TRIALS" \
  --reset-mode cp \
  --verify-mode fast \
  --base-dir-name pgdata_base_fanout32 \
  --skip-gen \
  --gateway-timeout 1800 \
  --reset-timeout 3600 \
  --verify-timeout 1800 \
  --out-dir "$RESULT_ROOT/OOM_100M/skews_0_5_and_1_2"
log "=== CMD 3/5: OOM skews .5 and 1.2 DONE ==="

# ── 4. TPC-C — worker scaling at 100 warehouses ────────────────────────────────
log "=== CMD 4/5: TPC-C workers sweep (100 warehouses) ==="
python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark tpcc \
  --db-host 10.129.7.57 \
  --db-user protectdr \
  --db-port 5438 \
  --server-port 8000 \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --modes pg,bcdb_det,bcdb_merkle \
  --warehouses 100 \
  --tpcc-workers 8,16,24,32 \
  --trials "$TPCC_TRIALS" \
  --tpcc-tx-count 20000 \
  --tpcc-seed 42 \
  --tpcc-remote-payment-pct 15.0 \
  --tpcc-remote-new-order-pct 1.0 \
  --tpcc-merkle-fanout 32 \
  --tpcc-merkle-partitions 200 \
  --tpcc-merkle-split-threshold 32 \
  --tpcc-merkle-merge-threshold 8 \
  --db-shared-buffers 32GB \
  --cold-runs \
  --order-seed 42 \
  --out-dir "$RESULT_ROOT/TPCC/workers_w100"
log "=== CMD 4/5: TPC-C workers sweep DONE ==="

# ── 5. TPC-C — warehouse scaling at 32 workers ────────────────────────────────
log "=== CMD 5/5: TPC-C warehouses sweep (32 workers) ==="
python3 -u scripts/distributed/run_all_modes_gateway_sweep.py \
  --benchmark tpcc \
  --db-host 10.129.7.57 \
  --db-user protectdr \
  --db-port 5438 \
  --server-port 8000 \
  --gateway-host 10.129.27.111 \
  --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC \
  --modes pg,bcdb_det,bcdb_merkle \
  --warehouses 5,10,20,30,50,75,100 \
  --tpcc-workers 32 \
  --trials "$TPCC_TRIALS" \
  --tpcc-tx-count 20000 \
  --tpcc-seed 42 \
  --tpcc-remote-payment-pct 15.0 \
  --tpcc-remote-new-order-pct 1.0 \
  --tpcc-merkle-fanout 32 \
  --tpcc-merkle-partitions 200 \
  --tpcc-merkle-split-threshold 32 \
  --tpcc-merkle-merge-threshold 8 \
  --db-shared-buffers 32GB \
  --cold-runs \
  --order-seed 42 \
  --out-dir "$RESULT_ROOT/TPCC/warehouses_w32"
log "=== CMD 5/5: TPC-C warehouses sweep DONE ==="

log "=== GATEWAY CHAIN v3 COMPLETE ==="
