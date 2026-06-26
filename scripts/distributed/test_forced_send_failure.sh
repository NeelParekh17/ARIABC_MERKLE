#!/bin/bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export ARIABC_CLUSTER_PASSWORD=clusterinfolab123
unset ARIABC_TEST_FAIL_DET_BLOCK_SEND_ONCE
export ARIABC_TEST_FAIL_DET_BLOCK_SEND_NODE="${ARIABC_TEST_FAIL_DET_BLOCK_SEND_NODE:-1}"
export ENABLE_FASTPATH_WATCHDOG=1

WORKLOAD_FILE="$SCRIPT_DIR/test_forced_failure_workload.sql"
echo "Generating 768 queries..."
: > "$WORKLOAD_FILE"
for i in $(seq 1 768); do
    printf "UPDATE usertable_small SET field1 = 'forced_fail_%s' WHERE ycsb_key = %s;\n" \
      "$i" "$i" >> "$WORKLOAD_FILE"
done

echo "Running 3-node cluster with forced send failure..."
export FORCE_BUILD=1
export ENABLE_FASTPATH_WATCHDOG=1
export ARIABC_TEST_FAIL_DET_BLOCK_SEND_NODE="${ARIABC_TEST_FAIL_DET_BLOCK_SEND_NODE:-1}"
timeout -k 45s 900s \
./run_4node_raft_cluster.sh \
  --workload "$WORKLOAD_FILE" \
  --threads 1 \
  --det-window 256 \
  --det-batch-size 256 \
  --det-pipeline-depth 256 \
  --pool-size 256 \
  --bcdb-worker-count 64 \
  --bcdb-decouple-workers 1 \
  --det-block-parallel 1 \
  --det-block-pipeline 1 \
  --det-block-max 256 \
  --det-event-block-fastpath 1 \
  --det-prefixed-direct-parallel 0 \
  --bcdb-block-wait-watermark 1 \
  --bcdb-dt-parse-barrier 0 \
  --bcdb-block-profile 1 \
  --bcdb-phase-trace 1 \
  --preferred-leader-id 1 > "forced_failure.log" 2>&1 || {
    echo "Run failed!"
    cat "forced_failure.log"
    exit 1
}

echo "Validating forced failure constraints..."

# Required invariants:
# 768 accepted
# 768 completed
# 3 blocks submitted
# 3 blocks returned
# 3 blocks emitted
# zero divergence
# matching Merkle roots
# Profile counters for forced failures

FAIL=0

# Find the latest result directory
LATEST_DIR=$(ls -td "$SCRIPT_DIR"/../bench_full_results/cluster4_* 2>/dev/null | head -n 1)

if ! grep -q "accepted=768" "forced_failure.log" && ! grep -q "Total accepted : 768" "forced_failure.log" && ! grep -q "accepted=768 " "forced_failure.log"; then
    echo "FAILED: Expected accepted: 768"
    FAIL=1
fi

if ! grep -q "completed=768" "forced_failure.log" && ! grep -q "Total completed: 768" "forced_failure.log" && ! grep -q "completed=768 " "forced_failure.log"; then
    echo "FAILED: Expected completed: 768"
    FAIL=1
fi

if ! grep -q "det_fastpath_blocks_submitted=3" "$LATEST_DIR"/server_node*.log; then
    echo "FAILED: Expected det_fastpath_blocks_submitted=3"
    FAIL=1
fi
if ! grep -q "det_fastpath_blocks_returned=3" "$LATEST_DIR"/server_node*.log; then
    echo "FAILED: Expected det_fastpath_blocks_returned=3"
    FAIL=1
fi
if ! grep -q "det_fastpath_blocks_emitted=3" "$LATEST_DIR"/server_node*.log; then
    echo "FAILED: Expected det_fastpath_blocks_emitted=3"
    FAIL=1
fi
if ! grep -q "det_fastpath_send_failures=1" "$LATEST_DIR"/server_node*.log; then
    echo "FAILED: Expected det_fastpath_send_failures=1"
    FAIL=1
fi
if ! grep -q "det_fastpath_requeues=" "$LATEST_DIR"/server_node*.log; then
    echo "FAILED: Expected det_fastpath_requeues to be present"
    FAIL=1
fi
if ! grep -q "divergence_count : 0" "forced_failure.log"; then
    echo "FAILED: Expected zero divergence"
    FAIL=1
fi
if ! grep -q "usertable_small consistency: PASS" "forced_failure.log"; then
    echo "FAILED: Expected matching Merkle roots"
    FAIL=1
fi

if [ "$FAIL" -eq 1 ]; then
    echo "Validation failed. Test output follows:"
    cat "forced_failure.log"
    exit 1
fi

echo "Forced send failure regression test PASSED."
