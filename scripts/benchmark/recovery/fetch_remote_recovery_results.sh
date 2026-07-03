#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'MSG'
fetch_remote_recovery_results.sh is deprecated.
Use scripts/benchmark/recovery/fetch_synced_remote_recovery_results.sh instead.
MSG
exit 2
