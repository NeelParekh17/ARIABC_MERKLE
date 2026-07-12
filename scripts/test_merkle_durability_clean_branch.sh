#!/usr/bin/env bash
#
# Clean-source, clean-build Merkle durability test, v5:
#   local merkle/main (or SOURCE_REF override)
#
# The script deliberately avoids the current in-tree build. It:
#   1. pins the requested local repository ref,
#   2. creates a brand-new detached Git worktree,
#   3. performs a fresh full build inside that disposable worktree,
#   4. proves merkleapply.c and merkledelta.c were compiled in that build,
#   5. uses only the newly installed binaries,
#   6. initializes an isolated PGDATA on port 5438,
#   7. runs clean-restart and post-COMMIT postmaster-crash tests,
#   8. records complete source/build/binary/runtime provenance.
#
# It never runs "git clean" in the user's working tree and never crashes the
# user's normal PGDATA. The build is intentionally in-tree only inside the
# disposable worktree because this branch tracks generated exports.list files,
# which break PostgreSQL VPATH/out-of-tree linking.
#
# Usage:
#   chmod +x scripts/test_merkle_durability_clean_branch.sh
#   ./scripts/test_merkle_durability_clean_branch.sh
#
# Useful overrides:
#   NONINTERACTIVE=1
#   KEEP_BUILD=1                 # retain worktree/build/install/test PGDATA
#   JOBS=8
#   SOURCE_REF=merkle/main      # local branch, tag, or commit
#   PORT=5438
#

set -Eeuo pipefail
IFS=$'\n\t'

REPO="${REPO:-/work/ARIABC/AriaBC}"
SOURCE_REF="${SOURCE_REF:-merkle/main}"
PORT="${PORT:-5438}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
DB_USER="${DB_USER:-$(id -un)}"
DB_NAME="${DB_NAME:-postgres}"

# Avoid turning a laptop into a tiny jet engine by default.
if [[ "$JOBS" -gt 16 ]]; then
    JOBS=16
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
SCRATCH_ROOT="${SCRATCH_ROOT:-/work/ARIABC/.merkle_clean_build_runs}"
RUN_ROOT="${SCRATCH_ROOT}/${RUN_ID}"
WORKTREE="${RUN_ROOT}/source"
BUILD="${WORKTREE}"
PREFIX="${RUN_ROOT}/install"
TEST_PGDATA="${RUN_ROOT}/pgdata"
ARTIFACT_DIR="${REPO}/merkle_clean_build_artifacts/${RUN_ID}"
TEST_LOG="${ARTIFACT_DIR}/postgres.log"

ORIGINAL_PGDATA="${ORIGINAL_PGDATA:-/work/ARIABC/pgdata}"
ORIGINAL_PG_CTL="${REPO}/src/bin/pg_ctl/pg_ctl"
ORIGINAL_WAS_RUNNING=0
TEST_RUNNING=0
WORKTREE_ADDED=0
CLEANUP_ACTIVE=0

PG_CTL=""
INITDB=""
PSQL=""
PG_CONFIG=""
POSTGRES=""
LEDGER_SQL=""

TABLE_NAME="merkle_clean_build_demo"
INDEX_NAME="merkle_clean_build_demo_merkle"

bold=$'\033[1m'
blue=$'\033[34m'
green=$'\033[32m'
yellow=$'\033[33m'
red=$'\033[31m'
reset=$'\033[0m'

stage() {
    printf '\n%s%s================================================================%s\n' "$bold" "$blue" "$reset"
    printf '%s%s%s%s\n' "$bold" "$blue" "$*" "$reset"
    printf '%s%s================================================================%s\n' "$bold" "$blue" "$reset"
}

info() { printf '%s[INFO]%s %s\n' "$green" "$reset" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$yellow" "$reset" "$*"; }
die()  { printf '%s[ERROR]%s %s\n' "$red" "$reset" "$*" >&2; exit 1; }

require_exec() { [[ -x "$1" ]] || die "Missing executable: $1"; }
require_file() { [[ -f "$1" ]] || die "Missing file: $1"; }

original_running() {
    [[ -x "$ORIGINAL_PG_CTL" && -f "${ORIGINAL_PGDATA}/PG_VERSION" ]] &&
        "$ORIGINAL_PG_CTL" status -D "$ORIGINAL_PGDATA" >/dev/null 2>&1
}

test_running() {
    [[ -n "$PG_CTL" && -x "$PG_CTL" && -f "${TEST_PGDATA}/PG_VERSION" ]] &&
        "$PG_CTL" status -D "$TEST_PGDATA" >/dev/null 2>&1
}

port_busy() {
    if command -v ss >/dev/null 2>&1; then
        ss -ltnH "sport = :${PORT}" 2>/dev/null | grep -q .
    elif command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1
    else
        return 1
    fi
}

psql_base() {
    "$PSQL" -X -v ON_ERROR_STOP=1 \
        -h 127.0.0.1 -p "$PORT" \
        -U "$DB_USER" -d "$DB_NAME" "$@"
}

sql() {
    psql_base -P pager=off "$@"
}

scalar() {
    psql_base -qAt -c "$1"
}

verify_runtime_binary_and_libraries() {
    local running_pid="$1"
    local expected_exe
    local actual_exe

    expected_exe="$(readlink -f "$POSTGRES")"
    actual_exe="$(readlink -f "/proc/${running_pid}/exe")"

    info "running executable: ${actual_exe}"
    [[ "$actual_exe" == "$expected_exe" ]] ||
        die "Running postmaster is not the freshly installed binary."

    local maps_file="${ARTIFACT_DIR}/runtime_library_maps.txt"
    {
        echo
        echo "============================================================"
        echo "utc_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "pid=${running_pid}"
        echo "executable=${actual_exe}"
        echo "expected_prefix=${PREFIX}"
        echo "[libpq mappings]"
        awk '$NF ~ /\/libpq\.so(\.|$)/ {print $NF}' "/proc/${running_pid}/maps" |
            sort -u
        echo "[all non-system mappings under custom paths]"
        awk '$NF ~ /^\// {print $NF}' "/proc/${running_pid}/maps" |
            sort -u |
            grep -vE '^/(usr/)?lib(64)?/' ||
            true
    } >>"$maps_file"

    mapfile -t libpq_paths < <(
        awk '$NF ~ /\/libpq\.so(\.|$)/ {print $NF}' "/proc/${running_pid}/maps" |
            sort -u
    )

    [[ "${#libpq_paths[@]}" -gt 0 ]] ||
        die "No runtime libpq mapping was found for postmaster PID ${running_pid}."

    local path
    local resolved
    for path in "${libpq_paths[@]}"; do
        resolved="$(readlink -f "$path")"
        info "runtime libpq: ${resolved}"
        [[ "$resolved" == "${PREFIX}/"* ]] ||
            die "Postmaster loaded libpq outside the private install: ${resolved}"
    done
}

start_test_server() {
    local failpoint="${1:-}"
    local action="${2:-}"

    if [[ -n "$failpoint" ]]; then
        info "Starting freshly built PostgreSQL with failpoint:"
        info "  ARIABC_MERKLE_FAILPOINT=${failpoint}"
        info "  ARIABC_MERKLE_FAILPOINT_ACTION=${action}"

        env \
            PATH="${PREFIX}/bin:/usr/bin:/bin" \
            LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/postgresql" \
            ARIABC_MERKLE_FAILPOINT="$failpoint" \
            ARIABC_MERKLE_FAILPOINT_ACTION="$action" \
            "$PG_CTL" start \
                -D "$TEST_PGDATA" \
                -l "$TEST_LOG" \
                -o "-p ${PORT}" \
                -w -t 60
    else
        info "Starting freshly built PostgreSQL without failpoints."
        env \
            PATH="${PREFIX}/bin:/usr/bin:/bin" \
            LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/postgresql" \
            "$PG_CTL" start \
                -D "$TEST_PGDATA" \
                -l "$TEST_LOG" \
                -o "-p ${PORT}" \
                -w -t 60
    fi

    TEST_RUNNING=1
    "$PG_CTL" status -D "$TEST_PGDATA" || true

    local running_pid
    running_pid="$(head -n 1 "${TEST_PGDATA}/postmaster.pid")"
    info "postmaster PID: ${running_pid}"

    # Prove both the executable and dynamically loaded libpq came from
    # this run's private installation.
    [[ -e "/proc/${running_pid}/exe" ]] ||
        die "Cannot inspect /proc/${running_pid}/exe."
    verify_runtime_binary_and_libraries "$running_pid"
}

stop_test_server_fast() {
    if test_running; then
        info "Stopping isolated test server cleanly."
        "$PG_CTL" stop -D "$TEST_PGDATA" -m fast -w -t 60
    fi
    TEST_RUNNING=0
}

restart_original_if_needed() {
    if [[ "$ORIGINAL_WAS_RUNNING" -eq 1 ]] && ! original_running; then
        info "Restarting the original AriaBC cluster."
        "$ORIGINAL_PG_CTL" start \
            -D "$ORIGINAL_PGDATA" \
            -l "${REPO}/server.log" \
            -w -t 60 || {
                warn "Could not restart the original cluster automatically."
                warn "Run: ${REPO}/scripts/start_server.sh"
                return 1
            }
    fi
}

cleanup() {
    local rc=$?
    [[ "$CLEANUP_ACTIVE" -eq 0 ]] || return
    CLEANUP_ACTIVE=1

    stage "CLEANUP"

    if test_running; then
        warn "Stopping isolated clean-build test cluster."
        "$PG_CTL" stop -D "$TEST_PGDATA" -m fast -w -t 60 || true
    fi
    TEST_RUNNING=0

    restart_original_if_needed || true

    if [[ "${KEEP_BUILD:-0}" == "1" ]]; then
        info "KEEP_BUILD=1, preserving:"
        info "  ${RUN_ROOT}"
    else
        if [[ "$WORKTREE_ADDED" -eq 1 ]]; then
            git -C "$REPO" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
            git -C "$REPO" worktree prune >/dev/null 2>&1 || true
        fi
        rm -rf "$RUN_ROOT"
        info "Removed temporary disposable worktree/install/PGDATA."
    fi

    info "Permanent artifacts:"
    info "  ${ARTIFACT_DIR}"
    exit "$rc"
}

trap cleanup EXIT
trap 'die "Interrupted at line ${LINENO}."' INT TERM

record_existing_tree_and_binary() {
    stage "CURRENT WORKING TREE AND EXISTING BINARY PROVENANCE"

    {
        echo "utc_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "repo=${REPO}"
        echo "current_branch=$(git -C "$REPO" branch --show-current || true)"
        echo "current_head=$(git -C "$REPO" rev-parse HEAD)"
        echo "source_repo=${REPO}"
        echo "source_ref=${SOURCE_REF}"
        echo
        echo "[configured git remotes]"
        git -C "$REPO" remote -v || true
        echo
        echo "[git status --short]"
        git -C "$REPO" status --short
        echo
        echo "[current schema markers]"
        grep -nE \
            'merkle_apply_state|merkle_local_delta|terminal_prefix_seq|merkle_apply_seq_base' \
            "${REPO}/scripts/distributed/sql/raft_apply_ledger_schema.sql" 2>/dev/null ||
            true
        echo
        echo "[existing in-tree binary]"
        if [[ -x "${REPO}/src/backend/postgres" ]]; then
            stat "${REPO}/src/backend/postgres"
            sha256sum "${REPO}/src/backend/postgres"
            strings "${REPO}/src/backend/postgres" |
                grep -E \
                    'ordered_committed_delta_wal|after_user_transaction_commit|after_apply_state_commit|BLOCKED_ON_GAP' |
                sort -u ||
                true
        else
            echo "No existing in-tree postgres executable."
        fi
    } | tee "${ARTIFACT_DIR}/existing_tree_and_binary.txt"
}

prepare_local_source_worktree() {
    stage "PIN AND MATERIALIZE LOCAL MERKLE SOURCE"

    git -C "$REPO" rev-parse --verify "${SOURCE_REF}^{commit}" >/dev/null ||
        die "Cannot resolve local source ref ${SOURCE_REF}."

    local pinned_commit
    pinned_commit="$(git -C "$REPO" rev-parse "${SOURCE_REF}^{commit}")"

    info "Pinned source commit: ${pinned_commit}"
    info "Local source ref: ${SOURCE_REF}"

    {
        echo "source_repo=${REPO}"
        echo "source_ref=${SOURCE_REF}"
        echo "pinned_commit=${pinned_commit}"
        git -C "$REPO" show -s \
            --format='commit=%H%nauthor=%an <%ae>%nauthor_date=%aI%ncommitter_date=%cI%nsubject=%s' \
            "$pinned_commit"
    } | tee "${ARTIFACT_DIR}/pinned_source_commit.txt"

    mkdir -p "$RUN_ROOT"
    git -C "$REPO" worktree add --detach "$WORKTREE" "$pinned_commit"
    WORKTREE_ADDED=1

    [[ -z "$(git -C "$WORKTREE" status --porcelain)" ]] ||
        die "Fresh detached worktree is unexpectedly dirty."

    # A clean Git worktree must not contain objects or linked executables.
    local stale_count
    stale_count="$(
        find "$WORKTREE" -type f \
            \( -name '*.o' -o -name '*.a' -o -name '*.so' -o -name postgres \) \
            -print | wc -l
    )"
    [[ "$stale_count" == "0" ]] ||
        die "Fresh worktree unexpectedly contains ${stale_count} compiled artifacts."

    info "Fresh worktree contains zero compiled .o/.a/.so/postgres artifacts."
}

validate_branch_contract() {
    stage "VALIDATE DURABILITY SOURCE CONTRACT BEFORE BUILD"

    require_file "${WORKTREE}/src/backend/access/merkle/merkleapply.c"
    require_file "${WORKTREE}/src/backend/access/merkle/merkledelta.c"
    require_file "${WORKTREE}/src/backend/access/merkle/Makefile"
    require_file "${WORKTREE}/scripts/distributed/sql/raft_apply_ledger_schema.sql"

    grep -Eq 'merkleapply\.o' \
        "${WORKTREE}/src/backend/access/merkle/Makefile" ||
        die "Merkle Makefile does not build merkleapply.o."

    grep -Eq 'merkledelta\.o' \
        "${WORKTREE}/src/backend/access/merkle/Makefile" ||
        die "Merkle Makefile does not build merkledelta.o."

    grep -Eq 'after_user_transaction_commit' \
        "${WORKTREE}/src/backend/access/merkle/merkledelta.c" ||
        die "Expected post-COMMIT crash failpoint is absent."

    grep -Eq 'Generic-WAL-backed|generic_xlog' \
        "${WORKTREE}/src/backend/access/merkle/merkleapply.c" ||
        die "Expected Generic-WAL Merkle applier source is absent."

    local schema="${WORKTREE}/scripts/distributed/sql/raft_apply_ledger_schema.sql"

    for required_pattern in \
        'CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_apply_counter' \
        'CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_apply_state' \
        'CREATE TABLE IF NOT EXISTS ariabc_internal.merkle_local_delta' \
        'terminal_prefix_seq' \
        'merkle_apply_seq_base' \
        'merkle_apply_seq' \
        'merkle_delta_blob'
    do
        grep -Fq "$required_pattern" "$schema" ||
            die "Official branch schema is missing: ${required_pattern}"
    done

    {
        echo "[Merkle Makefile]"
        cat "${WORKTREE}/src/backend/access/merkle/Makefile"
        echo
        echo "[source hashes]"
        sha256sum \
            "${WORKTREE}/src/backend/access/merkle/merkleapply.c" \
            "${WORKTREE}/src/backend/access/merkle/merkledelta.c" \
            "${WORKTREE}/src/backend/access/merkle/Makefile" \
            "$schema"
        echo
        echo "[schema durability markers]"
        grep -nE \
            'merkle_apply_counter|merkle_apply_state|merkle_local_delta|terminal_prefix_seq|merkle_apply_seq_base|merkle_apply_seq|merkle_delta_blob' \
            "$schema"
    } | tee "${ARTIFACT_DIR}/validated_source_contract.txt"
}

clean_build_and_install() {
    stage "FULL FRESH BUILD IN DISPOSABLE WORKTREE + PRIVATE INSTALL"

    mkdir -p "$PREFIX"

    info "Source/build directory: ${WORKTREE}"
    info "Install prefix:         ${PREFIX}"
    info "Parallel jobs:          ${JOBS}"

    # This is a disposable detached worktree, not the user's working tree.
    # Reset and clean it again immediately before configure so the build starts
    # from exactly the pinned commit with zero stale or generated artifacts.
    git -C "$WORKTREE" reset --hard HEAD \
        >"${ARTIFACT_DIR}/prebuild_git_reset.log" 2>&1
    git -C "$WORKTREE" clean -ffdqx \
        >"${ARTIFACT_DIR}/prebuild_git_clean.log" 2>&1

    local stale_before
    stale_before="$(
        find "$WORKTREE" -type f \
            \( -name '*.o' -o -name '*.a' -o -name '*.so' -o -name postgres \) \
            -print | wc -l
    )"

    [[ "$stale_before" == "0" ]] ||
        die "Disposable worktree still contains ${stale_before} compiled artifacts before configure."

    info "Pre-build compiled-artifact count: ${stale_before}"

    # Why build in the disposable source tree rather than VPATH/out-of-tree:
    # this branch tracks src/interfaces/libpq/exports.list. In a VPATH build,
    # make sees that source-tree file as an existing target and skips generating
    # build/src/interfaces/libpq/exports.list, while the linker looks only in the
    # build directory. The result is:
    #   ld: cannot open linker script file exports.list
    #
    # Building inside this brand-new worktree remains fully stale-free and is
    # compatible with the branch's current generated-file layout.
    if ! (
        cd "$WORKTREE"
        ./configure --prefix="$PREFIX"
    ) >"${ARTIFACT_DIR}/configure.log" 2>&1; then
        warn "configure failed. Last 120 lines:"
        tail -n 120 "${ARTIFACT_DIR}/configure.log" || true
        die "Fresh configure failed."
    fi

    if ! (
        cd "$WORKTREE"
        make -j"$JOBS"
    ) >"${ARTIFACT_DIR}/make.log" 2>&1; then
        warn "make failed. Error lines:"
        grep -nEi \
            'fatal error:|(^|[^[:alpha:]])error:|undefined reference|cannot open|No rule to make target|collect2:' \
            "${ARTIFACT_DIR}/make.log" | tail -n 80 || true
        warn "Last 160 make-log lines:"
        tail -n 160 "${ARTIFACT_DIR}/make.log" || true
        die "Fresh full build failed."
    fi

    if ! (
        cd "$WORKTREE"
        make install
    ) >"${ARTIFACT_DIR}/make_install.log" 2>&1; then
        warn "make install failed. Last 120 lines:"
        tail -n 120 "${ARTIFACT_DIR}/make_install.log" || true
        die "Private installation failed."
    fi

    PG_CTL="${PREFIX}/bin/pg_ctl"
    INITDB="${PREFIX}/bin/initdb"
    PSQL="${PREFIX}/bin/psql"
    PG_CONFIG="${PREFIX}/bin/pg_config"
    POSTGRES="${PREFIX}/bin/postgres"
    LEDGER_SQL="${WORKTREE}/scripts/distributed/sql/raft_apply_ledger_schema.sql"

    require_exec "$PG_CTL"
    require_exec "$INITDB"
    require_exec "$PSQL"
    require_exec "$PG_CONFIG"
    require_exec "$POSTGRES"

    require_file "${WORKTREE}/src/backend/access/merkle/merkleapply.o"
    require_file "${WORKTREE}/src/backend/access/merkle/merkledelta.o"

    [[ "${WORKTREE}/src/backend/access/merkle/merkleapply.o" -nt "${ARTIFACT_DIR}/pinned_source_commit.txt" ]] ||
        die "merkleapply.o does not appear freshly built."
    [[ "${WORKTREE}/src/backend/access/merkle/merkledelta.o" -nt "${ARTIFACT_DIR}/pinned_source_commit.txt" ]] ||
        die "merkledelta.o does not appear freshly built."
    [[ "$POSTGRES" -nt "${ARTIFACT_DIR}/pinned_source_commit.txt" ]] ||
        die "Installed postgres binary does not appear freshly built."

    {
        echo "[pinned source]"
        echo "commit=$(git -C "$WORKTREE" rev-parse HEAD)"
        echo "source_build_dir=${WORKTREE}"
        echo "install_prefix=${PREFIX}"
        echo
        echo "[post-build worktree status]"
        git -C "$WORKTREE" status --short || true
        echo
        echo "[pg_config]"
        "$PG_CONFIG" --version
        echo "bindir=$("$PG_CONFIG" --bindir)"
        echo "libdir=$("$PG_CONFIG" --libdir)"
        echo "configure=$("$PG_CONFIG" --configure)"
        echo
        echo "[fresh build products]"
        stat \
            "${WORKTREE}/src/backend/access/merkle/merkleapply.o" \
            "${WORKTREE}/src/backend/access/merkle/merkledelta.o" \
            "$POSTGRES"
        echo
        sha256sum \
            "${WORKTREE}/src/backend/access/merkle/merkleapply.o" \
            "${WORKTREE}/src/backend/access/merkle/merkledelta.o" \
            "$POSTGRES" \
            "$PG_CTL" \
            "$PSQL"
        echo
        echo "[binary durability markers]"
        strings "$POSTGRES" |
            grep -E \
                'ordered_committed_delta_wal|after_user_transaction_commit|after_apply_state_commit|BLOCKED_ON_GAP|merkle_apply_pending' |
            sort -u
        echo
        echo "[private libpq]"
        private_libpq="$(readlink -f "${PREFIX}/lib/libpq.so.5")"
        echo "private_libpq=${private_libpq}"
        sha256sum "$private_libpq"
        echo
        echo "[ELF dynamic tags]"
        if command -v readelf >/dev/null 2>&1; then
            readelf -d "$POSTGRES" |
                grep -E 'NEEDED|RPATH|RUNPATH' ||
                true
        fi
        echo
        echo "[dynamic dependencies with controlled private-library path]"
        env \
            -u LD_PRELOAD \
            LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/postgresql" \
            ldd "$POSTGRES" ||
            true
    } | tee "${ARTIFACT_DIR}/fresh_binary_provenance.txt"

    grep -q 'after_user_transaction_commit' \
        "${ARTIFACT_DIR}/fresh_binary_provenance.txt" ||
        die "Fresh installed binary lacks the expected crash failpoint marker."

    grep -q 'merkle_apply_pending' \
        "${ARTIFACT_DIR}/fresh_binary_provenance.txt" ||
        die "Fresh installed binary lacks the Merkle applier marker."

    grep -Fq "libpq.so.5 => ${PREFIX}/lib/libpq.so.5" \
        "${ARTIFACT_DIR}/fresh_binary_provenance.txt" ||
        die "Controlled dependency check did not resolve libpq from the private install."

    export PATH="${PREFIX}/bin:/usr/bin:/bin"
    export LD_LIBRARY_PATH="${PREFIX}/lib:${PREFIX}/lib/postgresql"
}

free_port_safely() {
    stage "FREE PORT ${PORT} SAFELY"

    if original_running; then
        ORIGINAL_WAS_RUNNING=1
        info "Original AriaBC cluster is running; stopping it cleanly."
        "$ORIGINAL_PG_CTL" stop \
            -D "$ORIGINAL_PGDATA" \
            -m fast -w -t 60
    elif port_busy; then
        die "Port ${PORT} is occupied by an unknown process. Refusing to continue."
    else
        info "Original cluster is not running and port ${PORT} is free."
    fi
}

initialize_isolated_cluster() {
    stage "INITIALIZE ISOLATED CLUSTER WITH FRESHLY BUILT INITDB"

    "$INITDB" \
        -D "$TEST_PGDATA" \
        --username="$DB_USER" \
        --auth=trust \
        >"${ARTIFACT_DIR}/initdb.log" 2>&1

    cat >>"${TEST_PGDATA}/postgresql.conf" <<CONF

port = ${PORT}
listen_addresses = '127.0.0.1'
unix_socket_directories = '/tmp'

fsync = on
synchronous_commit = on
full_page_writes = on
autovacuum = off

log_checkpoints = on
log_min_messages = info
CONF

    start_test_server

    sql -c "
        SELECT version(),
               current_setting('port') AS port,
               pg_postmaster_start_time() AS postmaster_started,
               pg_is_in_recovery() AS in_recovery;
    " | tee "${ARTIFACT_DIR}/fresh_server_identity.txt"
}

bootstrap_official_schema_only() {
    stage "BOOTSTRAP OFFICIAL PINNED-BRANCH LEDGER SCHEMA"

    info "Using only:"
    info "  ${LEDGER_SQL}"

    sql -f "$LEDGER_SQL" |
        tee "${ARTIFACT_DIR}/official_schema_bootstrap.txt"

    sql <<'SQL' | tee "${ARTIFACT_DIR}/official_schema_status.txt"
\pset pager off
SELECT pg_catalog.merkle_recovery_status();

SELECT c.relname AS relation_name,
       c.relkind
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = c.relnamespace
WHERE n.nspname = 'ariabc_internal'
ORDER BY c.relname;

TABLE ariabc_internal.merkle_apply_state;
TABLE ariabc_internal.merkle_apply_counter;
SELECT * FROM ariabc_internal.raft_apply_schema_meta;
SQL

    local status
    status="$(scalar "SELECT pg_catalog.merkle_recovery_status();")"

    [[ "$status" == *'"managed":true'* ]] ||
        die "Official branch schema produced managed=false: ${status}"
    [[ "$status" == *'"state":"READY"'* ]] ||
        die "Official branch schema did not produce READY: ${status}"
}

show_state() {
    local label="$1"
    local file="$2"

    stage "STATE: ${label}"

    sql <<SQL | tee "${ARTIFACT_DIR}/${file}"
\pset pager off
\echo '--- PostgreSQL identity ---'
SELECT version(),
       pg_postmaster_start_time() AS postmaster_started,
       pg_current_wal_lsn() AS current_wal_lsn,
       pg_is_in_recovery() AS in_recovery;

\echo '--- Merkle recovery status ---'
SELECT pg_catalog.merkle_recovery_status();

\echo '--- Apply metadata ---'
TABLE ariabc_internal.merkle_apply_state;
TABLE ariabc_internal.merkle_apply_counter;

\echo '--- Local delta queue ---'
SELECT apply_seq,
       delta_version,
       octet_length(delta_blob) AS delta_bytes,
       committed_at
FROM ariabc_internal.merkle_local_delta
ORDER BY apply_seq;

\echo '--- User table ---'
SELECT count(*) AS rows,
       count(*) FILTER (WHERE version = 1) AS version_1_rows,
       count(*) FILTER (WHERE version = 2) AS version_2_rows,
       count(*) FILTER (WHERE version = 3) AS version_3_rows
FROM ${TABLE_NAME};

SELECT id, version, left(payload, 65) AS payload_prefix
FROM ${TABLE_NAME}
WHERE id IN (1, 42, 100, 1000, 5000, 10000, 50000)
ORDER BY id;
SQL
}

show_root_and_verify() {
    local label="$1"
    local file="$2"

    stage "ROOT + VERIFY: ${label}"

    sql <<SQL | tee "${ARTIFACT_DIR}/${file}"
\pset pager off
SELECT pg_catalog.merkle_recovery_status() AS recovery_status;
SELECT pg_catalog.merkle_root_hash('${TABLE_NAME}'::regclass) AS merkle_root;
SELECT pg_catalog.merkle_verify('${TABLE_NAME}'::regclass) AS heap_matches_merkle;
SELECT pg_catalog.merkle_tree_stats('${TABLE_NAME}'::regclass) AS tree_stats;
SQL
}

create_baseline() {
    stage "CREATE TABLE AND MERKLE V7 BASELINE"

    sql <<SQL
CREATE TABLE ${TABLE_NAME}
(
    id       bigint PRIMARY KEY,
    payload  text NOT NULL,
    version  integer NOT NULL
);

INSERT INTO ${TABLE_NAME}(id, payload, version)
SELECT g,
       repeat(md5(g::text), 8),
       1
FROM generate_series(1, 50000) AS g;

CREATE INDEX ${INDEX_NAME}
ON ${TABLE_NAME}
USING merkle(id)
WITH (
    partitions = 32,
    leaves_per_partition = 1024,
    fanout = 32
);

ANALYZE ${TABLE_NAME};
CHECKPOINT;
SQL

    show_state "initial checkpointed baseline" "state_00_initial.txt"
    show_root_and_verify "initial checkpointed baseline" "verify_00_initial.txt"

    [[ "$(scalar "SELECT pg_catalog.merkle_verify('${TABLE_NAME}'::regclass);")" == "t" ]] ||
        die "Initial Merkle verification failed."
}

clean_restart_control() {
    stage "CLEAN RESTART CONTROL"

    sql <<SQL
UPDATE ${TABLE_NAME}
SET payload = 'clean-v2-' || id || '-' || repeat('C', 100),
    version = 2
WHERE id <= 1000;
SQL

    show_state \
        "after clean-control COMMIT, before apply" \
        "state_01_clean_pending.txt"

    sql <<'SQL' | tee "${ARTIFACT_DIR}/apply_01_clean.txt"
SELECT pg_catalog.merkle_recovery_status() AS before_apply;
SELECT pg_catalog.merkle_apply_pending() AS applied_through;
SELECT pg_catalog.merkle_recovery_status() AS after_apply;
SQL

    show_root_and_verify "after clean delta apply" "verify_01_clean_applied.txt"

    local root_before
    root_before="$(scalar "SELECT pg_catalog.merkle_root_hash('${TABLE_NAME}'::regclass);")"

    stop_test_server_fast
    start_test_server

    show_state "after clean restart" "state_02_after_clean_restart.txt"
    show_root_and_verify "after clean restart" "verify_02_after_clean_restart.txt"

    local root_after
    root_after="$(scalar "SELECT pg_catalog.merkle_root_hash('${TABLE_NAME}'::regclass);")"

    {
        echo "root_before_clean_stop=${root_before}"
        echo "root_after_clean_restart=${root_after}"
    } | tee "${ARTIFACT_DIR}/clean_restart_root_comparison.txt"

    [[ "$root_before" == "$root_after" ]] ||
        die "Merkle root changed across clean restart."
}

post_commit_crash_test() {
    stage "DETERMINISTIC POST-COMMIT POSTMASTER CRASH"

    stop_test_server_fast
    start_test_server "after_user_transaction_commit" "postmaster_kill"

    local log_line_before
    log_line_before="$(wc -l < "$TEST_LOG")"

    info "Executing COMMIT that intentionally kills the postmaster afterward."
    info "Connection loss and psql exit code 2 are expected."

    set +e
    timeout 45 "$PSQL" -X -v ON_ERROR_STOP=1 \
        -h 127.0.0.1 -p "$PORT" \
        -U "$DB_USER" -d "$DB_NAME" \
        -c "
            UPDATE ${TABLE_NAME}
            SET payload = 'crash-v3-' || id || '-' || repeat('X', 140),
                version = 3
            WHERE id <= 10000;
        " >"${ARTIFACT_DIR}/crash_client_output.txt" 2>&1
    local client_rc=$?
    set -e

    cat "${ARTIFACT_DIR}/crash_client_output.txt"
    info "Crash client exit code: ${client_rc}"

    sleep 1

    if test_running; then
        die "Failpoint did not kill the postmaster."
    fi
    TEST_RUNNING=0

    {
        "$PG_CTL" status -D "$TEST_PGDATA" || true
        echo
        if [[ -x "${PREFIX}/bin/pg_controldata" ]]; then
            "${PREFIX}/bin/pg_controldata" "$TEST_PGDATA" |
                grep -E \
                    'Database cluster state|Latest checkpoint location|Latest checkpoint.s REDO location|Time of latest checkpoint' ||
                true
        fi
    } | tee "${ARTIFACT_DIR}/server_state_after_failpoint.txt"

    stage "POSTGRESQL WAL RECOVERY"

    start_test_server

    tail -n +$((log_line_before + 1)) "$TEST_LOG" |
        tee "${ARTIFACT_DIR}/postgres_recovery_log.txt"

    grep -q \
        'MERKLE_FAILPOINT_REACHED name=after_user_transaction_commit' \
        "${ARTIFACT_DIR}/postgres_recovery_log.txt" ||
        die "Server log does not prove the intended failpoint was reached."

    grep -q \
        'automatic recovery in progress' \
        "${ARTIFACT_DIR}/postgres_recovery_log.txt" ||
        die "Server log does not show PostgreSQL crash recovery."

    show_state \
        "after crash recovery, before explicit Merkle apply" \
        "state_03_after_crash_before_apply.txt"

    local survived
    survived="$(scalar "SELECT count(*) FROM ${TABLE_NAME} WHERE version = 3;")"
    [[ "$survived" == "10000" ]] ||
        die "Expected 10000 committed version-3 rows, found ${survived}."

    sql <<'SQL' | tee "${ARTIFACT_DIR}/apply_02_after_crash.txt"
SELECT pg_catalog.merkle_recovery_status() AS before_apply;
SELECT pg_catalog.merkle_apply_pending() AS applied_through;
SELECT pg_catalog.merkle_recovery_status() AS after_apply;
SQL

    show_state \
        "after post-crash Merkle catch-up" \
        "state_04_after_crash_apply.txt"

    show_root_and_verify \
        "final post-crash state" \
        "verify_03_final.txt"

    local final_status
    local final_state
    local final_applied_seq
    local final_target_seq
    local final_terminal_prefix_seq
    local final_blocked_seq
    local final_tree_lag
    local final_queue_rows
    local final_verify

    final_status="$(scalar "SELECT pg_catalog.merkle_recovery_status();")"
    final_state="$(scalar "SELECT (pg_catalog.merkle_recovery_status()::jsonb ->> 'state');")"
    final_applied_seq="$(scalar "SELECT (pg_catalog.merkle_recovery_status()::jsonb ->> 'applied_seq');")"
    final_target_seq="$(scalar "SELECT (pg_catalog.merkle_recovery_status()::jsonb ->> 'target_seq');")"
    final_terminal_prefix_seq="$(scalar "SELECT (pg_catalog.merkle_recovery_status()::jsonb ->> 'terminal_prefix_seq');")"
    final_blocked_seq="$(scalar "SELECT (pg_catalog.merkle_recovery_status()::jsonb ->> 'blocked_seq');")"
    final_tree_lag="$(scalar "SELECT (pg_catalog.merkle_tree_stats('${TABLE_NAME}'::regclass)::jsonb ->> 'lag_items');")"
    final_queue_rows="$(scalar "SELECT count(*) FROM ariabc_internal.merkle_local_delta;")"
    final_verify="$(scalar "SELECT pg_catalog.merkle_verify('${TABLE_NAME}'::regclass);")"

    [[ "$final_state" == "READY" ]] ||
        die "Final recovery state is not READY: ${final_status}"
    [[ "$final_applied_seq" == "$final_target_seq" ]] ||
        die "Applied sequence ${final_applied_seq} differs from target ${final_target_seq}."
    [[ "$final_terminal_prefix_seq" == "$final_target_seq" ]] ||
        die "Terminal prefix ${final_terminal_prefix_seq} differs from target ${final_target_seq}."
    [[ "$final_blocked_seq" == "0" ]] ||
        die "Final blocked_seq is ${final_blocked_seq}, expected 0."
    [[ "$final_tree_lag" == "0" ]] ||
        die "Merkle tree stats report lag_items=${final_tree_lag}, expected 0."
    [[ "$final_queue_rows" == "0" ]] ||
        die "Local Merkle delta queue still has ${final_queue_rows} row(s)."
    [[ "$final_verify" == "t" ]] ||
        die "Final Merkle verification is false."

    {
        echo "pinned_commit=$(git -C "$WORKTREE" rev-parse HEAD)"
        echo "fresh_postgres_sha256=$(sha256sum "$POSTGRES" | awk '{print $1}')"
        echo "committed_version_3_rows=${survived}"
        echo "final_status=${final_status}"
        echo "final_state=${final_state}"
        echo "final_applied_seq=${final_applied_seq}"
        echo "final_target_seq=${final_target_seq}"
        echo "final_terminal_prefix_seq=${final_terminal_prefix_seq}"
        echo "final_blocked_seq=${final_blocked_seq}"
        echo "final_tree_lag=${final_tree_lag}"
        echo "final_queue_rows=${final_queue_rows}"
        echo "final_verify=${final_verify}"
    } | tee "${ARTIFACT_DIR}/final_result.txt"
}

main() {
    stage "PRE-FLIGHT: CLEAN-BRANCH BUILD + DURABILITY TEST v5"

    [[ "$(id -u)" -ne 0 ]] || die "Do not run as root."
    require_exec "$(command -v git)"
    require_exec "$(command -v make)"
    require_file "${REPO}/.git/HEAD"

    mkdir -p "$ARTIFACT_DIR" "$SCRATCH_ROOT"

    info "User working tree:     ${REPO}"
    info "Source repository:     ${REPO}"
    info "Source ref:            ${SOURCE_REF}"
    info "Port:                  ${PORT}"
    info "Parallel build jobs:   ${JOBS}"
    info "Permanent artifacts:   ${ARTIFACT_DIR}"
    info "Temporary run root:    ${RUN_ROOT}"

    cat <<'TEXT'

This script does not trust the currently compiled src/backend/postgres binary.

It pins the local merkle/main ref (or SOURCE_REF) and checks out that commit in
a brand-new detached worktree. It performs a complete build inside that
disposable worktree. This avoids the current worktree's broken VPATH/out-of-tree path
for tracked generated files such as src/interfaces/libpq/exports.list. It then
installs into a private prefix and verifies /proc/<pid>/exe points to that
freshly installed postgres process.

Your current working tree and uncommitted files are not modified.
TEXT

    if [[ "${NONINTERACTIVE:-0}" != "1" ]]; then
        read -r -p "Continue with clean build and isolated crash test? [y/N] " answer
        [[ "$answer" == "y" || "$answer" == "Y" ]] || die "Cancelled."
    fi

    record_existing_tree_and_binary
    prepare_local_source_worktree
    validate_branch_contract
    clean_build_and_install
    free_port_safely
    initialize_isolated_cluster
    bootstrap_official_schema_only
    create_baseline
    clean_restart_control
    post_commit_crash_test

    stage "FINAL RESULT: PASS"

    cat <<TEXT
PASS

The test used a completely fresh build of:
  repository: ${REPO}
  source ref: ${SOURCE_REF}
  commit:     $(git -C "$WORKTREE" rev-parse HEAD)

It did not use the existing in-tree postgres binary.

Proved:
  - the disposable worktree began with zero compiled artifacts;
  - merkleapply.c and merkledelta.c were compiled during this run;
  - the running postmaster and runtime libpq came from the private install;
  - the official pinned-branch schema alone produced managed=true / READY;
  - clean restart preserved the verified Merkle root;
  - the post-COMMIT postmaster failpoint was reached;
  - PostgreSQL performed WAL crash recovery;
  - 10,000 committed rows survived;
  - the committed Merkle delta survived;
  - ordered catch-up reached READY with zero lag;
  - final merkle_verify() returned true.

Artifacts:
  ${ARTIFACT_DIR}
TEXT
}

main "$@"
