#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'MSG'
run_remote_recovery_benchmark.sh is deprecated.
Use scripts/benchmark/recovery/run_synced_remote_recovery_benchmark.sh instead.
MSG
exit 2
