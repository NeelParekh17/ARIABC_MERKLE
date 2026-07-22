#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PG_BIN="${MERKLE_VIZ_PG_BIN:-/work/ARIABC/install/bin}"
PGDATA="${MERKLE_VIZ_PGDATA:-${ROOT}/.pgdata}"
PGHOST="${MERKLE_VIZ_PGHOST:-127.0.0.1}"
PGPORT="${MERKLE_VIZ_PGPORT:-5438}"
PGUSER="${MERKLE_VIZ_PGUSER:-postgres}"
PGDATABASE="${MERKLE_VIZ_PGDATABASE:-postgres}"
LOG="${MERKLE_VIZ_PGLOG:-${ROOT}/postgres.log}"

for tool in initdb pg_ctl pg_isready psql; do
    if [[ ! -x "${PG_BIN}/${tool}" ]]; then
        echo "ERROR: missing ${PG_BIN}/${tool}; build/install AriaBC PostgreSQL first" >&2
        exit 1
    fi
done

connection_ready() {
    "${PG_BIN}/psql" -X -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
        -d "$PGDATABASE" -Atc "SELECT 1" >/dev/null 2>&1
}

if connection_ready; then
    echo "PostgreSQL is ready at ${PGHOST}:${PGPORT}."
    exit 0
fi

if "${PG_BIN}/pg_isready" -h "$PGHOST" -p "$PGPORT" -q 2>/dev/null; then
    echo "ERROR: port ${PGPORT} already has PostgreSQL, but the visualizer cannot connect as ${PGUSER}." >&2
    echo "Set MERKLE_VIZ_CONNINFO for that server or choose MERKLE_VIZ_PGPORT for the isolated cluster." >&2
    exit 1
fi

if [[ ! -f "${PGDATA}/PG_VERSION" ]]; then
    echo "Initializing isolated visualizer PostgreSQL data at ${PGDATA}..."
    mkdir -p "$PGDATA"
    "${PG_BIN}/initdb" -D "$PGDATA" -U "$PGUSER" --auth=trust --no-sync
fi

if "${PG_BIN}/pg_ctl" status -D "$PGDATA" >/dev/null 2>&1; then
    echo "ERROR: ${PGDATA} is running on another port but ${PGHOST}:${PGPORT} is unavailable." >&2
    exit 1
fi

echo "Starting isolated visualizer PostgreSQL at ${PGHOST}:${PGPORT}..."
"${PG_BIN}/pg_ctl" start -D "$PGDATA" -w -t 60 -l "$LOG" \
    -o "-p ${PGPORT} -c listen_addresses=${PGHOST}"

if ! connection_ready; then
    echo "ERROR: PostgreSQL started but the visualizer connection check failed; inspect ${LOG}" >&2
    exit 1
fi
echo "PostgreSQL is ready. Log: ${LOG}"
