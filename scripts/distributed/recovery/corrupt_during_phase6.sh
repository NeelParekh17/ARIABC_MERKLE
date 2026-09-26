#!/usr/bin/env bash
# ==============================================================================
# corrupt_during_phase6.sh
#
# External helper script to trigger data corruption during Phase 6 workload execution.
#
# Usage:
#   # In terminal 2 while run_4node_raft_cluster.sh is starting in terminal 1:
#   ./scripts/distributed/recovery/corrupt_during_phase6.sh --target utkarsh --count 300
#
# Options:
#   --target, -t        Target node (default: utkarsh)
#   --count, -n         Number of tuples to corrupt (default: 300)
#   --fault-type        update | delete | insert | mixed (default: update)
#   --table             Table name (default: usertable_small)
#   --delay-sec         Delay after Phase 6 starts before injection (default: 3.0)
#   --immediate         Inject immediately without waiting for Phase 6
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"

python3 -u "$SCRIPT_DIR/corrupt_during_phase6.py" "$@"
