#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANONICAL_SCRIPT="$SCRIPT_DIR/../../ONLINE_RECOVERY/replicate_distributed_recovery.sh"
if [ ! -f "$CANONICAL_SCRIPT" ]; then
    echo "ERROR: Cannot find canonical replication script at $CANONICAL_SCRIPT" >&2
    exit 1
fi
exec bash "$CANONICAL_SCRIPT" "$@"
