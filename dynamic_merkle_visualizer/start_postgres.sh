#!/usr/bin/env bash
# Compatibility entry point: creates a fresh isolated custom-PostgreSQL demo.
set -euo pipefail
VIZ_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VIZ_PYTHON="${MERKLE_VIZ_PYTHON:-${VIZ_DIR}/../.venv/bin/python3}"
if [[ ! -x "$VIZ_PYTHON" ]]; then VIZ_PYTHON=python3; fi
exec "$VIZ_PYTHON" "$VIZ_DIR/demo.py" "$@"
