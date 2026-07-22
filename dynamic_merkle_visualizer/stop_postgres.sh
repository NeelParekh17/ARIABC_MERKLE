#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_BIN="${MERKLE_VIZ_PG_BIN:-/work/ARIABC/install/bin}"
PGDATA="${MERKLE_VIZ_PGDATA:-${ROOT}/.pgdata}"

if [[ ! -f "${PGDATA}/PG_VERSION" ]]; then
    echo "No isolated visualizer PostgreSQL cluster exists at ${PGDATA}."
    exit 0
fi
if ! "${PG_BIN}/pg_ctl" status -D "$PGDATA" >/dev/null 2>&1; then
    echo "Isolated visualizer PostgreSQL is already stopped."
    exit 0
fi
"${PG_BIN}/pg_ctl" stop -D "$PGDATA" -m fast -w -t 60
