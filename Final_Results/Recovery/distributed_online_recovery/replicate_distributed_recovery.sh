#!/usr/bin/env bash
# replicate_distributed_recovery.sh
# Reproduces AriaBC Distributed Online Replica Recovery (ProtectDB Algorithm 2) benchmarks.
# Runs against the 3-node Raft-Kafka cluster: admin123 (Node 1), user4 (Node 2), utkarsh (Node 4).

set -euo pipefail
cd /work/ARIABC/AriaBC

RUNNER="scripts/distributed/recovery/run_recovery_cluster_test.sh"
STAMP="$(date +%Y%m%d_%H%M%S)"

echo "=== AriaBC Distributed Online Replica Recovery Replication Suite ==="
echo "Date: $(date -u)"
echo "Cluster nodes: admin123 (10.129.148.247), user4 (10.129.148.246), utkarsh (10.129.148.248)"
echo "Gateway: 10.129.27.111"
echo ""

# 1. Run A: Baseline (Recovery OFF)
echo ">>> [1/7] Running Run A: Baseline (Recovery OFF)..."
$RUNNER --recovery-mode off

# 2. Run B: Recovery Overhead (Recovery BOTH, No Fault)
echo ">>> [2/7] Running Run B: Recovery Overhead (No Fault)..."
$RUNNER --recovery-mode both --skip-build

# 3. Run C: Follower Corruption (Update 100 tuples @ 5s)
echo ">>> [3/7] Running Run C: Follower Corruption (Update)..."
$RUNNER --recovery-mode both \
  --inject-fault-node utkarsh \
  --inject-fault-count 100 \
  --inject-fault-delay-sec 5 \
  --inject-fault-type update \
  --skip-build

# 4. Run M_passive: Follower Corruption (Passive Merkle compare detection)
echo ">>> [4/7] Running Run M_passive: Passive Detection Mode..."
$RUNNER --recovery-mode passive \
  --inject-fault-node utkarsh \
  --inject-fault-count 100 \
  --inject-fault-delay-sec 5 \
  --inject-fault-type update \
  --skip-build

# 5. Run M_active: Follower Corruption (Active vote divergence detection)
echo ">>> [5/7] Running Run M_active: Active Detection Mode..."
$RUNNER --recovery-mode active \
  --inject-fault-node utkarsh \
  --inject-fault-count 100 \
  --inject-fault-delay-sec 5 \
  --inject-fault-type update \
  --skip-build

# 6. Run M_mixed: Follower Mixed Corruption (Update + Delete + Insert)
echo ">>> [6/7] Running Run M_mixed: Follower Mixed Corruption..."
$RUNNER --recovery-mode both \
  --inject-fault-node utkarsh \
  --inject-fault-count 100 \
  --inject-fault-delay-sec 5 \
  --inject-fault-type mixed \
  --skip-build

# 7. Run L_mix (Prioritized): Leader Mixed Corruption
echo ">>> [7/7] Running Run L_mix (Prioritized): Leader Mixed Corruption..."
$RUNNER --recovery-mode both \
  --inject-fault-node admin123 \
  --inject-fault-count 100 \
  --inject-fault-delay-sec 5 \
  --inject-fault-type mixed \
  --skip-build

echo ""
echo "=== All replication runs completed successfully! ==="
