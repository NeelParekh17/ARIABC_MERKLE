#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: run_synced_remote_recovery_benchmark.sh [--host HOST] [options]

Default target is the EPYC machine ranking.cse.iitb.ac.in as protectdr.

Options:
  --profile NAME              default: dynamic-size-scaling-k75-c300
  --ssh-user USER             default: protectdr
  --ssh-port PORT
  --ssh-key PATH
  --remote-root PATH           default: /home/protectdr/merkle_recovery_runs
  --remote-python PATH         default: /usr/bin/python3
  --build-profile debug|release  default: release
  --experiment figure12|figure13
  --tuple-count N              default: 1000000,3000000,5000000
  --partitions N
  --bad-leaf-count K
  --leaves-per-partition N
  --fanout N
  --geometry-label LABEL
  --profiling off|light|deep    default: off
  --repetitions N               default: 1
  --artifact-mode summary|debug  default: summary
  --corruption-mode paper-update-only|update-only|delete-only|insert-only|mixed
                               default: paper-update-only
  --audit-mode full|skip       default: skip
  --fast-diagnostic           dynamic only: one repetition and audit skipped
  --run-dynamic-crash-gate    opt in to the destructive dynamic crash/lifecycle
                              gate before the recovery benchmark
  --leaf-fetch-batch-size N    default: 64 (0 = unbounded single SQL)
  --min-free-gib N             default: 40
  --ssh-timeout SECONDS        default: 15
  --keep-remote-archive         default: enabled
  --keep-failure-logs
USAGE
}

HOST="ranking.cse.iitb.ac.in"
SSH_USER="protectdr"
SSH_PORT="22"
SSH_KEY=""
SSH_PASSWORD="${SSH_PASSWORD:-}"
REMOTE_ROOT="/home/protectdr/merkle_recovery_runs"
REMOTE_PYTHON="/usr/bin/python3"
PROFILE="dynamic-size-scaling-k75-c300"
BUILD_PROFILE="release"
EXPERIMENT=""
TUPLE_COUNT="1000000,3000000,5000000"
PARTITIONS=""
BAD_LEAF_COUNT=""
LEAVES_PER_PARTITION=""
FANOUT=""
GEOMETRY_LABEL=""
PROFILING="off"
REPETITIONS="1"
ARTIFACT_MODE="summary"
CORRUPTION_MODE="paper-update-only"
AUDIT_MODE="skip"
LEAF_FETCH_BATCH_SIZE=64
MIN_FREE_GIB=40
SSH_TIMEOUT="${SSH_TIMEOUT:-15}"
KEEP_REMOTE_ARCHIVE=1
KEEP_FAILURE_LOGS=0
FAST_DIAGNOSTIC=0
RUN_DYNAMIC_CRASH_GATE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="${2:?}"; shift 2 ;;
    --ssh-user) SSH_USER="${2:?}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:?}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:?}"; shift 2 ;;
    --remote-root) REMOTE_ROOT="${2:?}"; shift 2 ;;
    --remote-python) REMOTE_PYTHON="${2:?}"; shift 2 ;;
    --build-profile) BUILD_PROFILE="${2:?}"; shift 2 ;;
    --profile) PROFILE="${2:?}"; shift 2 ;;
    --experiment) EXPERIMENT="${2:?}"; shift 2 ;;
    --tuple-count) TUPLE_COUNT="${2:?}"; shift 2 ;;
    --partitions) PARTITIONS="${2:?}"; shift 2 ;;
    --bad-leaf-count) BAD_LEAF_COUNT="${2:?}"; shift 2 ;;
    --leaves-per-partition) LEAVES_PER_PARTITION="${2:?}"; shift 2 ;;
    --fanout) FANOUT="${2:?}"; shift 2 ;;
    --geometry-label) GEOMETRY_LABEL="${2:?}"; shift 2 ;;
    --profiling) PROFILING="${2:?}"; shift 2 ;;
    --repetitions) REPETITIONS="${2:?}"; shift 2 ;;
    --artifact-mode) ARTIFACT_MODE="${2:?}"; shift 2 ;;
    --corruption-mode) CORRUPTION_MODE="${2:?}"; shift 2 ;;
    --audit-mode) AUDIT_MODE="${2:?}"; shift 2 ;;
    --fast-diagnostic) FAST_DIAGNOSTIC=1; shift ;;
    --run-dynamic-crash-gate) RUN_DYNAMIC_CRASH_GATE=1; shift ;;
    --leaf-fetch-batch-size) LEAF_FETCH_BATCH_SIZE="${2:?}"; shift 2 ;;
    --min-free-gib) MIN_FREE_GIB="${2:?}"; shift 2 ;;
    --ssh-timeout) SSH_TIMEOUT="${2:?}"; shift 2 ;;
    --keep-remote-archive) KEEP_REMOTE_ARCHIVE=1; shift ;;
    --keep-failure-logs) KEEP_FAILURE_LOGS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$FAST_DIAGNOSTIC" -eq 1 ]]; then
  if [[ "$PROFILE" != "dynamic-size-scaling-k75-c300" ]]; then
    echo "--fast-diagnostic requires --profile dynamic-size-scaling-k75-c300" >&2
    exit 2
  fi
  AUDIT_MODE="skip"
  if [[ -z "$REPETITIONS" ]]; then
    REPETITIONS=1
  fi
fi
case "$PROFILE" in
  smoke|preflight|paper|recovery-scaling-diagnosis|fanout-width-sweep|size-scaling-k75-c300|best-scaling-f32-l1024-k75-c300|dynamic-size-scaling-k75-c300) ;;
  *) echo "profile must be smoke, preflight, paper, recovery-scaling-diagnosis, fanout-width-sweep, size-scaling-k75-c300, best-scaling-f32-l1024-k75-c300, or dynamic-size-scaling-k75-c300" >&2; exit 2 ;;
esac
case "$ARTIFACT_MODE" in
  summary|debug) ;;
  *) echo "artifact-mode must be summary or debug" >&2; exit 2 ;;
esac
case "$CORRUPTION_MODE" in
  paper-update-only|update-only|delete-only|insert-only|mixed) ;;
  *) echo "corruption-mode must be paper-update-only, update-only, delete-only, insert-only, or mixed" >&2; exit 2 ;;
esac
case "$AUDIT_MODE" in
  full|skip) ;;
  *) echo "audit-mode must be full or skip" >&2; exit 2 ;;
esac
case "$PROFILING" in
  off|light|deep) ;;
  *) echo "profiling must be off, light, or deep" >&2; exit 2 ;;
esac
case "$EXPERIMENT" in
  ""|figure12|figure13) ;;
  *) echo "experiment must be figure12 or figure13" >&2; exit 2 ;;
esac
if [[ "$PROFILE" == "dynamic-size-scaling-k75-c300" ]]; then
  if [[ "$PROFILING" != "off" ]]; then
    echo "dynamic-size-scaling-k75-c300 requires --profiling off" >&2
    exit 2
  fi
  if [[ "$CORRUPTION_MODE" != "paper-update-only" && "$CORRUPTION_MODE" != "update-only" ]]; then
    echo "dynamic-size-scaling-k75-c300 requires update-only corruption" >&2
    exit 2
  fi
fi
case "$SSH_TIMEOUT" in
  ''|*[!0-9]*) echo "ssh-timeout must be a positive integer number of seconds" >&2; exit 2 ;;
  0) echo "ssh-timeout must be greater than 0" >&2; exit 2 ;;
esac

# The wrapper defaults to a release build; smoke remains explicitly overridable
# with --build-profile debug when a fast diagnostic is desired.
case "$BUILD_PROFILE" in
  debug|release) ;;
  *) echo "build-profile must be debug or release" >&2; exit 2 ;;
esac
if [[ "$PROFILE" != "smoke" && "$BUILD_PROFILE" != "release" ]]; then
  echo "$PROFILE requires --build-profile release (debug builds produce invalid performance numbers)" >&2
  exit 2
fi

resolve_host_ip() {
  case "$1" in
    admin123) printf '%s\n' "10.129.148.247" ;;
    user4) printf '%s\n' "10.129.148.246" ;;
    utkarsh) printf '%s\n' "10.129.148.248" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

progress() {
  printf '[%s] [local] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LOCAL_PYTHON="$ROOT/.venv/bin/python3"
if [[ ! -x "$LOCAL_PYTHON" ]]; then
  LOCAL_PYTHON="$(command -v python3)"
fi
if [[ ! -x "$LOCAL_PYTHON" ]]; then
  echo "python3 not available locally" >&2
  exit 1
fi
if [[ -n "$SSH_PASSWORD" ]]; then
  command -v sshpass >/dev/null 2>&1 ||
    { echo "SSH_PASSWORD was set but sshpass is unavailable" >&2; exit 1; }
fi
for tool in ssh rsync scp; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing local tool: $tool" >&2; exit 1; }
done

if [[ -z "$SSH_USER" ]]; then
  SSH_TARGET="$(resolve_host_ip "$HOST")"
else
  SSH_TARGET="$SSH_USER@$(resolve_host_ip "$HOST")"
fi

SSH_COMMON_OPTS=(
  -o StrictHostKeyChecking=accept-new
  -o ConnectTimeout="$SSH_TIMEOUT"
  -o ConnectionAttempts=1
  -o ServerAliveInterval="$SSH_TIMEOUT"
  -o ServerAliveCountMax=1
  -o NumberOfPasswordPrompts=1
  -o LogLevel=ERROR
)
if [[ -n "$SSH_PASSWORD" ]]; then
  SSH_OPTS=(-p "$SSH_PORT" "${SSH_COMMON_OPTS[@]}")
  SCP_OPTS=(-P "$SSH_PORT" "${SSH_COMMON_OPTS[@]}")
else
  SSH_OPTS=(-p "$SSH_PORT" -o BatchMode=yes "${SSH_COMMON_OPTS[@]}")
  SCP_OPTS=(-P "$SSH_PORT" -o BatchMode=yes "${SSH_COMMON_OPTS[@]}")
fi
if [[ -n "$SSH_KEY" ]]; then
  SSH_OPTS+=(-i "$SSH_KEY")
  SCP_OPTS+=(-i "$SSH_KEY")
fi
if [[ -n "$SSH_PASSWORD" ]]; then
  export SSHPASS="$SSH_PASSWORD"
  SSH_CMD=(sshpass -e ssh)
  SCP_CMD=(sshpass -e scp)
  RSYNC_SSH=(sshpass -e ssh)
else
  SSH_CMD=(ssh)
  SCP_CMD=(scp)
  RSYNC_SSH=(ssh)
fi
SSH_CMD+=("${SSH_OPTS[@]}")
SCP_CMD+=("${SCP_OPTS[@]}")
if [[ -n "$SSH_PASSWORD" ]]; then
  RSYNC_SSH+=(-p "$SSH_PORT" "${SSH_COMMON_OPTS[@]}")
else
  RSYNC_SSH+=(-p "$SSH_PORT" -o BatchMode=yes "${SSH_COMMON_OPTS[@]}")
fi
if [[ -n "$SSH_KEY" ]]; then
  RSYNC_SSH+=(-i "$SSH_KEY")
fi
RSYNC_RSH="$(printf '%q ' "${RSYNC_SSH[@]}")"

remote_ssh_cmd() {
  "${SSH_CMD[@]}" "$SSH_TARGET" "$@"
}

remote_ssh_cmd_stdinless() {
  remote_ssh_cmd "$@" </dev/null
}

remote_ssh_step() {
  local label="$1"
  shift
  progress "$label"
  if remote_ssh_cmd_stdinless "$@"; then
    progress "$label: done"
  else
    local rc=$?
    progress "$label: failed with rc=$rc"
    return "$rc"
  fi
}

rsync_remote() {
  rsync -aL --delete -e "$RSYNC_RSH" "$@"
}

RUN_ID="ariabc-recovery-${PROFILE}-$(date -u +%Y%m%dT%H%M%SZ)-$(printf '%06x' "$((RANDOM << 1 ^ RANDOM))")"
REMOTE_RUNS_ROOT="$REMOTE_ROOT/runs"
REMOTE_ARTIFACTS_ROOT="$REMOTE_ROOT/artifacts"
REMOTE_FAILURES_ROOT="$REMOTE_ROOT/failures"
REMOTE_LOCK_DIR="$REMOTE_ROOT/lock"
REMOTE_RUN_DIR="$REMOTE_RUNS_ROOT/$RUN_ID"
REMOTE_SRC_DIR="$REMOTE_RUN_DIR/src"
REMOTE_INSTALL_DIR="$REMOTE_RUN_DIR/install"
REMOTE_PGDATA="$REMOTE_RUN_DIR/pgdata"
REMOTE_SCRATCH_DIR="$REMOTE_RUN_DIR/scratch"
REMOTE_RESULTS_DIR="$REMOTE_RUN_DIR/results"
REMOTE_LOG_DIR="$REMOTE_RUN_DIR/logs"
LOCAL_MANIFEST_DIR="/tmp/ariabc-recovery-manifests/$RUN_ID"
LOCAL_MANIFEST="$LOCAL_MANIFEST_DIR/source_snapshot.json"
ROOTS_FILE="$ROOT/scripts/benchmark/recovery/sync_source_roots.txt"

progress "starting recovery benchmark run $RUN_ID on $SSH_TARGET (profile=$PROFILE build=$BUILD_PROFILE artifact_mode=$ARTIFACT_MODE)"
progress "checking SSH connectivity to $SSH_TARGET with timeout ${SSH_TIMEOUT}s"
if remote_ssh_cmd_stdinless "printf '%s\n' ssh-ok" >/dev/null; then
  progress "SSH connectivity check passed"
else
  rc=$?
  progress "SSH connectivity check failed with rc=$rc"
  cat >&2 <<EOF
Remote SSH did not complete within the configured timeout.
Target: $SSH_TARGET
Port: $SSH_PORT
Timeout: ${SSH_TIMEOUT}s

If this is a slow or tarpitted SSH daemon, retry with a larger --ssh-timeout.
If password auth is required, run with SSH_PASSWORD set in the environment.
EOF
  exit "$rc"
fi
progress "creating local source snapshot manifest"
mkdir -p "$LOCAL_MANIFEST_DIR"
"$LOCAL_PYTHON" "$ROOT/scripts/benchmark/recovery/create_source_snapshot.py" \
  --repo-root "$ROOT" \
  --roots-file "$ROOTS_FILE" \
  --output "$LOCAL_MANIFEST" \
  --run-id "$RUN_ID" >/dev/null
progress "created local source snapshot manifest: $LOCAL_MANIFEST"

remote_ssh_step "creating remote run directories under $REMOTE_RUN_DIR" \
  "mkdir -p '$REMOTE_RUN_DIR' '$REMOTE_ARTIFACTS_ROOT' '$REMOTE_FAILURES_ROOT' '$REMOTE_LOCK_DIR' '$REMOTE_LOG_DIR' '$REMOTE_SCRATCH_DIR' '$REMOTE_RESULTS_DIR'"

LOCAL_OWNS_RUN_DIR=1
REMOTE_PAYLOAD_STARTED=0
local_cleanup() {
  local rc=$?
  if [[ "$LOCAL_OWNS_RUN_DIR" -eq 1 ]]; then
    progress "cleaning remote run directory after setup failure: $REMOTE_RUN_DIR"
    remote_ssh_cmd_stdinless "rm -rf '$REMOTE_RUN_DIR'" >/dev/null 2>&1 || true
  elif [[ "$REMOTE_PAYLOAD_STARTED" -eq 1 ]]; then
    progress "remote payload owns cleanup; preserving remote diagnostics for $RUN_ID"
  fi
  return "$rc"
}
trap local_cleanup EXIT

verify_remote_env() {
  local env_prefix
  env_prefix=$(printf 'REMOTE_ROOT=%q REMOTE_PYTHON=%q MIN_FREE_GIB=%q' \
    "$REMOTE_ROOT" "$REMOTE_PYTHON" "$MIN_FREE_GIB")
  remote_ssh_cmd "env $env_prefix bash -s" <<'REMOTE_ENV'
set -Eeuo pipefail

fail() {
  printf 'remote environment check failed: %s\n' "$*" >&2
  exit 1
}

[[ -d "$REMOTE_ROOT" ]] || fail "remote root does not exist: $REMOTE_ROOT"
[[ -w "$REMOTE_ROOT" ]] || fail "remote root is not writable: $REMOTE_ROOT"

free_kib="$(df -Pk "$REMOTE_ROOT" | awk "NR==2 {print \$4}")"
need_kib="$((MIN_FREE_GIB * 1024 * 1024))"
[[ -n "$free_kib" ]] || fail "could not read free space for $REMOTE_ROOT"
(( free_kib >= need_kib )) ||
  fail "insufficient free space under $REMOTE_ROOT: have $((free_kib / 1024 / 1024)) GiB, need ${MIN_FREE_GIB} GiB"

for tool in rsync gcc make flock; do
  command -v "$tool" >/dev/null 2>&1 || fail "missing remote tool: $tool"
done

[[ -x "$REMOTE_PYTHON" ]] || fail "remote python is not executable: $REMOTE_PYTHON"
"$REMOTE_PYTHON" -c 'import psycopg' >/dev/null 2>&1 ||
  fail "remote python cannot import psycopg: $REMOTE_PYTHON"

printf 'remote environment ok: %s GiB free under %s\n' "$((free_kib / 1024 / 1024))" "$REMOTE_ROOT" >&2
REMOTE_ENV
}

sync_root() {
  local rel="$1"
  local src="$ROOT/$rel"
  local dest="$REMOTE_SRC_DIR/$rel"
  progress "syncing source root: $rel"
  if [[ -d "$src" && ! -L "$src" ]]; then
    remote_ssh_cmd_stdinless "mkdir -p '$(dirname "$dest")' '$dest'"
    rsync_remote \
      --exclude 'results/' --exclude 'fetched/' --exclude '__pycache__/' \
      --exclude '*.pyc' --exclude '*.pyo' \
      --exclude '*.o' --exclude '*.a' --exclude '*.so' --exclude '*.so.*' \
      --exclude '*.d' --exclude '*.gcda' --exclude '*.gcno' \
      --exclude '*.copybin' --exclude '*.tar' --exclude '*.tar.gz' --exclude '*.zip' \
      --exclude '*.pc' --exclude '*.list' \
      --exclude '*_d.h' --exclude '*_d.dat' \
      --exclude 'schemapg.h' --exclude 'errcodes.h' \
      --exclude 'fmgroids.h' --exclude 'fmgrprotos.h' \
      --exclude 'lwlocknames.h' --exclude 'lwlocknames.c' \
      --exclude 'probes.h' --exclude 'plerrcodes.h' \
      --exclude 'pg_config.h' --exclude 'pg_config_ext.h' \
      --exclude 'pg_config_os.h' --exclude 'ecpg_config.h' \
      --exclude 'pg_config_paths.h' \
      --exclude 'objfiles.txt' --exclude 'exports.list' \
      --exclude 'snowball_create.sql' \
      --exclude '*.stamp' --exclude 'bki-stamp' --exclude 'header-stamp' \
      --exclude '*/postgres' --exclude 'bin/initdb/postgres' \
      --exclude 'bin/pg_ctl/postgres' --exclude 'backend/postgres' \
      --exclude 'bin/initdb/initdb' --exclude 'bin/pg_ctl/pg_ctl' \
      --exclude 'bin/psql/psql' --exclude 'bin/pg_config/pg_config' \
      --exclude 'bin/pg_dump/pg_dump' --exclude 'bin/pg_dump/pg_dumpall' \
      --exclude 'bin/pg_dump/pg_restore' --exclude 'bin/pgbench/pgbench' \
      --exclude 'test/regress/pg_regress' --exclude 'test/isolation/pg_isolation_regress' \
      --exclude 'test/isolation/isolationtester' --exclude 'test/isolation/pg_regress.o' \
      "$src/" "$SSH_TARGET:$dest/"
  else
    remote_ssh_cmd_stdinless "mkdir -p '$(dirname "$dest")'"
    rsync_remote "$src" "$SSH_TARGET:$dest"
  fi
  progress "synced source root: $rel"
}

progress "checking remote environment and free space"
verify_remote_env
progress "remote environment check passed"

while IFS= read -r rel; do
  [[ -z "$rel" || "$rel" == \#* ]] && continue
  sync_root "$rel"
done < "$ROOTS_FILE"
progress "all source roots synced"

if [[ -d "/usr/include/openssl" ]]; then
  progress "uploading local OpenSSL headers to remote include directory for compatibility"
  local_tmp_headers="/tmp/openssl_compat_headers_$$"
  mkdir -p "$local_tmp_headers"
  cp -rL /usr/include/openssl/* "$local_tmp_headers/"
  if [[ -d "/usr/include/x86_64-linux-gnu/openssl" ]]; then
    cp -rL /usr/include/x86_64-linux-gnu/openssl/* "$local_tmp_headers/"
  fi
  remote_ssh_cmd_stdinless "mkdir -p '$REMOTE_SRC_DIR/src/include/openssl'"
  rsync_remote "$local_tmp_headers/" "$SSH_TARGET:$REMOTE_SRC_DIR/src/include/openssl/"
  rm -rf "$local_tmp_headers"
fi

# Upload manifest and verify remote source — LOCAL_RUNDIR_GUARD stays 1 until
# the remote benchmark process takes ownership after this sequence.
progress "uploading source snapshot manifest"
"${SCP_CMD[@]}" "$LOCAL_MANIFEST" "$SSH_TARGET:$REMOTE_RUN_DIR/source_snapshot.json"
remote_ssh_step "verifying remote source snapshot" \
  "'$REMOTE_PYTHON' '$REMOTE_RUN_DIR/src/scripts/benchmark/recovery/verify_source_snapshot.py' --repo-root '$REMOTE_RUN_DIR/src' --manifest '$REMOTE_RUN_DIR/source_snapshot.json'"
remote_ssh_step "verifying remote Python benchmark environment" \
  "'$REMOTE_PYTHON' '$REMOTE_RUN_DIR/src/scripts/benchmark/recovery/verify_recovery_python_env.py' --contract '$REMOTE_RUN_DIR/src/scripts/benchmark/recovery/python_requirements_contract.json'"
progress "remote source and Python environment verified"

remote_env_prefix=$(printf 'RUN_ID=%q REMOTE_ROOT=%q REMOTE_RUNS_ROOT=%q REMOTE_ARTIFACTS_ROOT=%q REMOTE_FAILURES_ROOT=%q REMOTE_LOCK_DIR=%q REMOTE_RUN_DIR=%q REMOTE_SRC_DIR=%q REMOTE_INSTALL_DIR=%q REMOTE_PGDATA=%q REMOTE_SCRATCH_DIR=%q REMOTE_RESULTS_DIR=%q REMOTE_LOG_DIR=%q REMOTE_PYTHON=%q BENCH_PROFILE=%q BUILD_PROFILE=%q EXPERIMENT=%q TUPLE_COUNT=%q PARTITIONS=%q BAD_LEAF_COUNT=%q LEAVES_PER_PARTITION=%q FANOUT=%q GEOMETRY_LABEL=%q PROFILING=%q REPETITIONS=%q ARTIFACT_MODE=%q CORRUPTION_MODE=%q AUDIT_MODE=%q LEAF_FETCH_BATCH_SIZE=%q MIN_FREE_GIB=%q KEEP_FAILURE_LOGS=%q FAST_DIAGNOSTIC=%q RUN_DYNAMIC_CRASH_GATE=%q' \
  "$RUN_ID" "$REMOTE_ROOT" "$REMOTE_RUNS_ROOT" "$REMOTE_ARTIFACTS_ROOT" "$REMOTE_FAILURES_ROOT" "$REMOTE_LOCK_DIR" "$REMOTE_RUN_DIR" "$REMOTE_SRC_DIR" "$REMOTE_INSTALL_DIR" "$REMOTE_PGDATA" "$REMOTE_SCRATCH_DIR" "$REMOTE_RESULTS_DIR" "$REMOTE_LOG_DIR" "$REMOTE_PYTHON" "$PROFILE" "$BUILD_PROFILE" "$EXPERIMENT" "$TUPLE_COUNT" "$PARTITIONS" "$BAD_LEAF_COUNT" "$LEAVES_PER_PARTITION" "$FANOUT" "$GEOMETRY_LABEL" "$PROFILING" "${REPETITIONS:-}" "$ARTIFACT_MODE" "$CORRUPTION_MODE" "$AUDIT_MODE" "$LEAF_FETCH_BATCH_SIZE" "$MIN_FREE_GIB" "$KEEP_FAILURE_LOGS" "$FAST_DIAGNOSTIC" "$RUN_DYNAMIC_CRASH_GATE")

remote_archive="$REMOTE_ARTIFACTS_ROOT/$RUN_ID.tar.gz"

progress "starting remote benchmark payload; remote logs will be under $REMOTE_LOG_DIR"
REMOTE_PAYLOAD_STARTED=1
LOCAL_OWNS_RUN_DIR=0
if ! remote_ssh_cmd "env $remote_env_prefix bash -s" <<'REMOTE'
set -Eeuo pipefail

remote_progress() {
  local remote_host
  remote_host="$(hostname -s 2>/dev/null || hostname 2>/dev/null || printf unknown)"
  printf '[%s] [remote:%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$remote_host" "$*" >&2
}

tail_log() {
  local log_file="$1"
  if [[ -s "$log_file" ]]; then
    remote_progress "last 40 lines from $log_file:"
    tail -n 40 "$log_file" >&2 || true
  elif [[ -e "$log_file" ]]; then
    remote_progress "$log_file exists but is empty"
  else
    remote_progress "$log_file was not created"
  fi
}

fail_with_log() {
  failure_reason="$1"
  if [[ $# -ge 2 && -n "$2" ]]; then
    tail_log "$2"
  fi
  exit 1
}

mkdir -p "$REMOTE_ROOT"/{runs,artifacts,failures,lock}
remote_progress "waiting for recovery benchmark lock"
exec 9>"$REMOTE_LOCK_DIR/recovery_benchmark.lock"
flock 9
remote_progress "acquired recovery benchmark lock for $RUN_ID"

cleanup_success=0
failure_reason="unknown"
postgres_started=0
memory_monitor_pid=0
benchmark_completed=0
REMOTE_SOCKET_DIR=""

monitor_private_postgres_memory() {
  local postmaster_pid process_pid metrics
  local process_count rss_sum_kib pss_anon_sum_kib pss_anon_max_kib pss_shmem_sum_kib
  local rss_kib anon_kib shmem_kib

  printf '%s\n' \
    'timestamp_utc,process_count,rss_sum_kib,pss_anon_sum_kib,pss_anon_max_kib,pss_shmem_sum_kib'
  while [[ -r "$REMOTE_PGDATA/postmaster.pid" ]]; do
    postmaster_pid="$(sed -n '1p' "$REMOTE_PGDATA/postmaster.pid" 2>/dev/null || true)"
    if [[ -z "$postmaster_pid" ]] || ! kill -0 "$postmaster_pid" 2>/dev/null; then
      break
    fi
    process_count=0
    rss_sum_kib=0
    pss_anon_sum_kib=0
    pss_anon_max_kib=0
    pss_shmem_sum_kib=0
    while read -r process_pid; do
      [[ -n "$process_pid" && -r "/proc/$process_pid/smaps_rollup" ]] || continue
      metrics="$(awk '
        /^Rss:/ { rss = $2 }
        /^Pss_Anon:/ { anon = $2 }
        /^Pss_Shmem:/ { shmem = $2 }
        END { printf "%d %d %d", rss, anon, shmem }
      ' "/proc/$process_pid/smaps_rollup" 2>/dev/null || true)"
      [[ -n "$metrics" ]] || continue
      read -r rss_kib anon_kib shmem_kib <<<"$metrics"
      process_count=$((process_count + 1))
      rss_sum_kib=$((rss_sum_kib + rss_kib))
      pss_anon_sum_kib=$((pss_anon_sum_kib + anon_kib))
      pss_shmem_sum_kib=$((pss_shmem_sum_kib + shmem_kib))
      if (( anon_kib > pss_anon_max_kib )); then
        pss_anon_max_kib="$anon_kib"
      fi
    done < <(ps -o pid= -p "$postmaster_pid" --ppid "$postmaster_pid" 2>/dev/null || true)
    printf '%s,%d,%d,%d,%d,%d\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$process_count" "$rss_sum_kib" "$pss_anon_sum_kib" \
      "$pss_anon_max_kib" "$pss_shmem_sum_kib"
    sleep 5
  done
}

stop_memory_monitor() {
  if [[ "$memory_monitor_pid" -gt 0 ]]; then
    kill "$memory_monitor_pid" >/dev/null 2>&1 || true
    wait "$memory_monitor_pid" >/dev/null 2>&1 || true
    memory_monitor_pid=0
  fi
}

stop_postgres() {
  if [[ "$postgres_started" -eq 1 && -x "$REMOTE_INSTALL_DIR/bin/pg_ctl" && -d "$REMOTE_PGDATA" ]]; then
    "$REMOTE_INSTALL_DIR/bin/pg_ctl" -D "$REMOTE_PGDATA" stop -m fast >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local rc=$?
  if [[ $rc -eq 0 && $cleanup_success -eq 1 ]]; then
    remote_progress "run completed successfully; cleaning remote work directory"
  else
    remote_progress "run exiting with rc=$rc reason=$failure_reason"
  fi
  stop_memory_monitor
  stop_postgres
  # Always remove the short socket directory (lives in /tmp, outside RUN_DIR).
  if [[ -n "${REMOTE_SOCKET_DIR:-}" ]]; then
    remote_progress "removing temporary socket directory: $REMOTE_SOCKET_DIR"
    rm -rf -- "$REMOTE_SOCKET_DIR"
    REMOTE_SOCKET_DIR=""
  fi
  if [[ $rc -eq 0 && $cleanup_success -eq 1 ]]; then
    rm -rf "$REMOTE_RUN_DIR"
    return 0
  fi

  mkdir -p "$REMOTE_FAILURES_ROOT/$RUN_ID"
  remote_progress "writing compact failure artifact: $REMOTE_FAILURES_ROOT/$RUN_ID"
  "$REMOTE_PYTHON" "$REMOTE_SRC_DIR/scripts/benchmark/recovery/write_failure_json.py" \
    "$REMOTE_FAILURES_ROOT/$RUN_ID/failure.json" \
    "$RUN_ID" "$BENCH_PROFILE" "$ARTIFACT_MODE" "$rc" "$failure_reason" "$KEEP_FAILURE_LOGS"
  cp -f "$REMOTE_RUN_DIR/source_snapshot.json" "$REMOTE_FAILURES_ROOT/$RUN_ID/source_snapshot.json" 2>/dev/null || true
  for name in configure.log generated_headers.log build.log install.log initdb.log \
              pg_ctl_start.log pg_isready.log postgres.log \
              postgres_memory.csv dynamic_crash_gate.log benchmark.stdout benchmark.stderr package.log; do
    if [[ -f "$REMOTE_LOG_DIR/$name" ]]; then
      cp -f "$REMOTE_LOG_DIR/$name" "$REMOTE_FAILURES_ROOT/$RUN_ID/$name"
    fi
  done
  if [[ -d "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate" ]]; then
    cp -a "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate" \
      "$REMOTE_FAILURES_ROOT/$RUN_ID/dynamic_merkle_crash_gate"
  fi
  if [[ "$benchmark_completed" -eq 1 && -d "$REMOTE_RESULTS_DIR" ]]; then
    # A post-benchmark failure (for example packaging) must not discard a
    # complete, expensive acceptance result.  Moving within the same remote
    # filesystem is atomic and avoids duplicating a large result tree.
    mv "$REMOTE_RESULTS_DIR" \
      "$REMOTE_FAILURES_ROOT/$RUN_ID/completed_results"
  elif [[ -d "$REMOTE_RESULTS_DIR" ]]; then
    mkdir -p "$REMOTE_FAILURES_ROOT/$RUN_ID/partial_results"
    find "$REMOTE_RESULTS_DIR" -maxdepth 2 -type f \
      \( -name 'config.json' \
         -o -name 'progress.json' \
         -o -name 'progress.jsonl' \
         -o -name '*.partial.csv' \
         -o -name 'runs.csv' \
         -o -name 'phase_timings.csv' \
         -o -name 'timing_contract.csv' \
         -o -name 'stderr.log' \
         -o -name 'stdout.log' \) \
      -exec cp --parents {} "$REMOTE_FAILURES_ROOT/$RUN_ID/partial_results/" \; \
      2>/dev/null || true
  fi
  # Compact failure logs are always retained — they are small and required for diagnosis.
  echo "$REMOTE_FAILURES_ROOT/$RUN_ID" >&2
  rm -rf "$REMOTE_SRC_DIR" "$REMOTE_INSTALL_DIR" "$REMOTE_PGDATA" "$REMOTE_SCRATCH_DIR" "$REMOTE_RESULTS_DIR"
  rm -rf "$REMOTE_RUN_DIR"
}
on_interrupt() {
  failure_reason="interrupted"
  exit 130
}

trap on_interrupt INT TERM HUP
trap cleanup EXIT

free_kib="$(df -Pk "$REMOTE_ROOT" | awk "NR==2 {print \$4}")"
need_kib="$((MIN_FREE_GIB * 1024 * 1024))"
if (( free_kib < need_kib )); then
  failure_reason="insufficient free space"
  exit 1
fi
remote_progress "free-space check passed: $((free_kib / 1024 / 1024)) GiB available, need ${MIN_FREE_GIB} GiB"

for tool in rsync gcc make flock; do
  command -v "$tool" >/dev/null 2>&1 || { failure_reason="missing remote tool: $tool"; exit 1; }
done
[[ -x "$REMOTE_PYTHON" ]] || { failure_reason="missing remote python"; exit 1; }
remote_progress "remote tool check passed"

if [[ -n "$REPETITIONS" ]]; then
  BENCH_REPETITIONS=(--repetitions "$REPETITIONS")
else
  BENCH_REPETITIONS=()
fi
BENCH_SELECTORS=()
if [[ -n "$EXPERIMENT" ]]; then
  BENCH_SELECTORS+=(--experiment "$EXPERIMENT")
fi
if [[ -n "$TUPLE_COUNT" ]]; then
  BENCH_SELECTORS+=(--tuple-count "$TUPLE_COUNT")
fi
if [[ -n "$PARTITIONS" ]]; then
  BENCH_SELECTORS+=(--partitions "$PARTITIONS")
fi
if [[ -n "$BAD_LEAF_COUNT" ]]; then
  BENCH_SELECTORS+=(--bad-leaf-count "$BAD_LEAF_COUNT")
fi
if [[ -n "$LEAVES_PER_PARTITION" ]]; then
  BENCH_SELECTORS+=(--leaves-per-partition "$LEAVES_PER_PARTITION")
fi
if [[ -n "$FANOUT" ]]; then
  BENCH_SELECTORS+=(--fanout "$FANOUT")
fi
if [[ -n "$GEOMETRY_LABEL" ]]; then
  BENCH_SELECTORS+=(--geometry-label "$GEOMETRY_LABEL")
fi

# Build profile: CFLAGS must be an env assignment, not a positional configure arg.
# Generated files (*_d.h, fmgroids.h, etc.) are excluded by the source
# snapshot manifest and rsync filter, so no clean_synced_tree() needed.
if [[ "$BUILD_PROFILE" == "release" ]]; then
  CONFIGURE_ARGS=(--prefix="$REMOTE_INSTALL_DIR" --without-readline)
  BUILD_CFLAGS="-O2 -g"
else
  CONFIGURE_ARGS=(--prefix="$REMOTE_INSTALL_DIR" --enable-debug --enable-cassert --without-readline)
  BUILD_CFLAGS="-O0 -g3"
fi
CONFIGURE_DESCRIPTION="CFLAGS=$BUILD_CFLAGS ${CONFIGURE_ARGS[*]}"

cd "$REMOTE_SRC_DIR"

# Set up local library directory for OpenSSL compatibility
local_lib_dir="$REMOTE_SRC_DIR/openssl_compat_libs"
mkdir -p "$local_lib_dir"
ssl_so=$(find /usr/lib /lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu -name "libssl.so.*" -print -quit 2>/dev/null)
crypto_so=$(find /usr/lib /lib /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu -name "libcrypto.so.*" -print -quit 2>/dev/null)
if [[ -n "$ssl_so" ]]; then
  ln -sf "$ssl_so" "$local_lib_dir/libssl.so"
fi
if [[ -n "$crypto_so" ]]; then
  ln -sf "$crypto_so" "$local_lib_dir/libcrypto.so"
fi

remote_progress "configure started ($CONFIGURE_DESCRIPTION); log: $REMOTE_LOG_DIR/configure.log"
if ac_cv_exeext= CPPFLAGS="${CPPFLAGS:-}" LDFLAGS="-L$local_lib_dir ${LDFLAGS:-}" \
    CFLAGS="$BUILD_CFLAGS" \
    ./configure "${CONFIGURE_ARGS[@]}" \
    >"$REMOTE_LOG_DIR/configure.log" 2>&1; then
  remote_progress "configure completed"
else
  fail_with_log "configure failed" "$REMOTE_LOG_DIR/configure.log"
fi
# Delete any stale build stamps and generated headers that may have been
# left over from the local tree (rsync excludes them, but *.stamp matching
# may have missed bki-stamp / header-stamp if present). This forces make
# to regenerate catalog headers before the full parallel build.
remote_progress "removing stale generated header stamps"
rm -f \
  src/backend/catalog/bki-stamp \
  src/backend/catalog/header-stamp \
  src/include/catalog/header-stamp \
  src/backend/utils/header-stamp \
  src/include/catalog/*_d.h \
  src/backend/catalog/*_d.h
remote_progress "generated-headers build started; log: $REMOTE_LOG_DIR/generated_headers.log"
if make -B -C src/backend generated-headers >"$REMOTE_LOG_DIR/generated_headers.log" 2>&1; then
  remote_progress "generated-headers build completed"
else
  fail_with_log "generated headers failed" "$REMOTE_LOG_DIR/generated_headers.log"
fi
remote_progress "full build started with $(nproc) jobs; log: $REMOTE_LOG_DIR/build.log"
if make -j"$(nproc)" >"$REMOTE_LOG_DIR/build.log" 2>&1; then
  remote_progress "full build completed"
else
  fail_with_log "build failed" "$REMOTE_LOG_DIR/build.log"
fi
remote_progress "install started; log: $REMOTE_LOG_DIR/install.log"
if make install prefix="$REMOTE_INSTALL_DIR" >"$REMOTE_LOG_DIR/install.log" 2>&1; then
  remote_progress "install completed at $REMOTE_INSTALL_DIR"
else
  fail_with_log "install failed" "$REMOTE_LOG_DIR/install.log"
fi
export LD_LIBRARY_PATH="$REMOTE_INSTALL_DIR/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
remote_progress "using installed shared libraries from $REMOTE_INSTALL_DIR/lib"

# The destructive durability gate is deliberately opt-in.  Recovery campaigns
# own a private fresh cluster but should not run crash/lifecycle tests unless
# the caller explicitly requests that separate proof tier.
if [[ "$BENCH_PROFILE" == "dynamic-size-scaling-k75-c300" && "$RUN_DYNAMIC_CRASH_GATE" -eq 1 ]]; then
  DYNAMIC_GATE_DIR="$REMOTE_LOG_DIR/dynamic_merkle_crash_gate"
  remote_progress "dynamic Merkle crash/lifecycle gate started; log: $REMOTE_LOG_DIR/dynamic_crash_gate.log"
  if PG_BIN="$REMOTE_INSTALL_DIR/bin" \
      "$REMOTE_SRC_DIR/scripts/test/merkle_crash_atomicity/run_dynamic_smoke.sh" \
      --result-root "$DYNAMIC_GATE_DIR" \
      >"$REMOTE_LOG_DIR/dynamic_crash_gate.log" 2>&1; then
    remote_progress "dynamic Merkle crash/lifecycle gate PASSED"
  else
    fail_with_log "dynamic Merkle crash/lifecycle gate failed" "$REMOTE_LOG_DIR/dynamic_crash_gate.log"
  fi
fi

remote_progress "initdb started; log: $REMOTE_LOG_DIR/initdb.log"
if "$REMOTE_INSTALL_DIR/bin/initdb" -D "$REMOTE_PGDATA" >"$REMOTE_LOG_DIR/initdb.log" 2>&1; then
  remote_progress "initdb completed"
else
  fail_with_log "initdb failed" "$REMOTE_LOG_DIR/initdb.log"
fi

# Create a short-path socket directory in /tmp to avoid the 107-byte Unix
# socket path limit that would be exceeded by the long run-ID path.
remote_progress "creating temporary PostgreSQL socket directory"
SOCKET_TEMPLATE="${TMPDIR:-/tmp}/ariabc-pg.XXXXXX"
REMOTE_SOCKET_DIR="$(mktemp -d "$SOCKET_TEMPLATE")" ||
  { failure_reason="could not create short PostgreSQL socket directory"; exit 1; }
chmod 700 "$REMOTE_SOCKET_DIR"
SOCKET_FILE="$REMOTE_SOCKET_DIR/.s.PGSQL.55432"
if (( ${#SOCKET_FILE} > 107 )); then
  failure_reason="PostgreSQL socket path is too long (${#SOCKET_FILE} bytes): $SOCKET_FILE"
  exit 1
fi
remote_progress "temporary socket directory ready: $REMOTE_SOCKET_DIR"
remote_progress "PostgreSQL start requested; logs: $REMOTE_LOG_DIR/pg_ctl_start.log and $REMOTE_LOG_DIR/postgres.log"
if "$REMOTE_INSTALL_DIR/bin/pg_ctl" -D "$REMOTE_PGDATA" -l "$REMOTE_LOG_DIR/postgres.log" \
    -o "-k $REMOTE_SOCKET_DIR -p 55432 -c listen_addresses='' -c max_wal_size=16GB -c min_wal_size=2GB -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9" \
    -w start >"$REMOTE_LOG_DIR/pg_ctl_start.log" 2>&1; then
  remote_progress "PostgreSQL started"
else
  tail_log "$REMOTE_LOG_DIR/postgres.log"
  fail_with_log "postgres start failed" "$REMOTE_LOG_DIR/pg_ctl_start.log"
fi
# Mark postgres started immediately so the cleanup trap can stop it even if
# pg_isready fails (e.g. socket not yet visible but process is running).
postgres_started=1
remote_progress "checking PostgreSQL readiness; log: $REMOTE_LOG_DIR/pg_isready.log"
if "$REMOTE_INSTALL_DIR/bin/pg_isready" -h "$REMOTE_SOCKET_DIR" -p 55432 -d postgres -U "$(id -un)" >"$REMOTE_LOG_DIR/pg_isready.log" 2>&1; then
  remote_progress "PostgreSQL readiness check passed"
else
  tail_log "$REMOTE_LOG_DIR/postgres.log"
  fail_with_log "pg_isready failed" "$REMOTE_LOG_DIR/pg_isready.log"
fi
monitor_private_postgres_memory >"$REMOTE_LOG_DIR/postgres_memory.csv" &
memory_monitor_pid=$!
remote_progress "private PostgreSQL memory monitor started; log: $REMOTE_LOG_DIR/postgres_memory.csv"

# The benchmark uses the crash-safe Merkle DDL guards, so the fresh temporary
# cluster must have the current Raft/Merkle ledger schema before any table or
# Merkle index is created.  Do this after PostgreSQL starts and before the
# benchmark Python process opens its connection.
LEDGER_SCHEMA_SQL="$REMOTE_SRC_DIR/scripts/distributed/sql/raft_apply_ledger_schema.sql"
if [[ ! -f "$LEDGER_SCHEMA_SQL" ]]; then
  failure_reason="missing Raft/Merkle ledger schema SQL"
  fail_with_log "ledger schema file missing" "$REMOTE_LOG_DIR/ledger_schema.log"
fi
remote_progress "bootstrapping Raft/Merkle ledger schema; log: $REMOTE_LOG_DIR/ledger_schema.log"
if "$REMOTE_INSTALL_DIR/bin/psql" -X -v ON_ERROR_STOP=1 \
    -h "$REMOTE_SOCKET_DIR" -p 55432 -d postgres -U "$(id -un)" \
    -f "$LEDGER_SCHEMA_SQL" >"$REMOTE_LOG_DIR/ledger_schema.log" 2>&1; then
  remote_progress "Raft/Merkle ledger schema bootstrap completed"
else
  tail_log "$REMOTE_LOG_DIR/ledger_schema.log"
  fail_with_log "Raft/Merkle ledger schema bootstrap failed" "$REMOTE_LOG_DIR/ledger_schema.log"
fi

BENCH_DSN="host=$REMOTE_SOCKET_DIR port=55432 dbname=postgres user=$(id -un)"
BENCH_ARGS=(
  "$REMOTE_PYTHON"
  "$REMOTE_SRC_DIR/scripts/benchmark/recovery/run_merkle_recovery_benchmark.py"
  --dsn "$BENCH_DSN"
  --profile "$BENCH_PROFILE"
  --result-dir "$REMOTE_RESULTS_DIR"
  --scratch-dir "$REMOTE_SCRATCH_DIR"
  --artifact-mode "$ARTIFACT_MODE"
  --corruption-mode "$CORRUPTION_MODE"
  --audit-mode "$AUDIT_MODE"
  --leaf-fetch-batch-size "$LEAF_FETCH_BATCH_SIZE"
  --profiling "$PROFILING"
)
if [[ "$BENCH_PROFILE" == "dynamic-size-scaling-k75-c300" ]]; then
  BENCH_ARGS+=(--merkle-mode dynamic)
fi
BENCH_ARGS+=("${BENCH_REPETITIONS[@]}")
BENCH_ARGS+=("${BENCH_SELECTORS[@]}")

remote_progress "benchmark started (profile=$BENCH_PROFILE artifact_mode=$ARTIFACT_MODE repetitions=${REPETITIONS:-default}); logs: $REMOTE_LOG_DIR/benchmark.stdout and $REMOTE_LOG_DIR/benchmark.stderr"
set +e
# This runner owns a fresh, per-run PGDATA and database instance.  Scope the
# destructive-reset acknowledgement to the benchmark process only; never
# export it into the caller or the wider remote session.
ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 \
ARIABC_NATIVE_PROFILE_LOG="$REMOTE_LOG_DIR/postgres.log" \
PGOPTIONS='-c merkle_native_profile_enabled=on' \
PYTHONUNBUFFERED=1 "${BENCH_ARGS[@]}" \
  2> >(tee "$REMOTE_LOG_DIR/benchmark.stderr" >&2) |
  tee "$REMOTE_LOG_DIR/benchmark.stdout"
bench_rc=${PIPESTATUS[0]}
set -e
if [[ $bench_rc -ne 0 ]]; then
  if [[ $bench_rc -eq 130 ]]; then
    failure_reason="interrupted"
  else
    failure_reason="benchmark failed"
  fi
  tail_log "$REMOTE_LOG_DIR/benchmark.stdout"
  tail_log "$REMOTE_LOG_DIR/benchmark.stderr"
  exit "$bench_rc"
fi
remote_progress "benchmark completed"
stop_memory_monitor

RESULT_DIR="$(tail -n 1 "$REMOTE_LOG_DIR/benchmark.stdout")"
if [[ ! -d "$RESULT_DIR" ]]; then
  failure_reason="benchmark result dir missing"
  exit 1
fi
benchmark_completed=1
remote_progress "benchmark result directory: $RESULT_DIR"

remote_progress "writing host info and source snapshot into result directory"
"$REMOTE_PYTHON" "$REMOTE_SRC_DIR/scripts/benchmark/recovery/write_host_info.py" --output "$RESULT_DIR/host_info.json" --filesystem-path "$REMOTE_RUN_DIR" >/dev/null
cp "$REMOTE_RUN_DIR/source_snapshot.json" "$RESULT_DIR/source_snapshot.json"
if [[ -f "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate/summary.json" ]]; then
  cp "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate/summary.json" \
     "$RESULT_DIR/dynamic_crash_gate_summary.json"
fi
if [[ -f "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate/campaign.env" ]]; then
  cp "$REMOTE_LOG_DIR/dynamic_merkle_crash_gate/campaign.env" \
     "$RESULT_DIR/dynamic_crash_gate_campaign.env"
fi
if [[ -f "$REMOTE_LOG_DIR/postgres_memory.csv" ]]; then
  cp "$REMOTE_LOG_DIR/postgres_memory.csv" "$RESULT_DIR/postgres_memory.csv"
fi
if [[ -f "$REMOTE_LOG_DIR/postgres.log" ]]; then
  cp "$REMOTE_LOG_DIR/postgres.log" "$RESULT_DIR/postgres.log"
fi

# Append build provenance to config.json
remote_progress "patching config.json with build provenance"
"$REMOTE_PYTHON" "$REMOTE_SRC_DIR/scripts/benchmark/recovery/patch_config_json.py" \
  "$RESULT_DIR/config.json" "$BUILD_PROFILE" "$CONFIGURE_DESCRIPTION"

remote_progress "packaging artifacts; log: $REMOTE_LOG_DIR/package.log"
if "$REMOTE_PYTHON" "$REMOTE_SRC_DIR/scripts/benchmark/recovery/package_recovery_artifacts.py" "$RESULT_DIR" --output "$REMOTE_ARTIFACTS_ROOT/$RUN_ID.tar.gz" --artifact-mode "$ARTIFACT_MODE" >"$REMOTE_LOG_DIR/package.log" 2>&1; then
  remote_progress "packaged remote artifact: $REMOTE_ARTIFACTS_ROOT/$RUN_ID.tar.gz"
else
  fail_with_log "packaging failed" "$REMOTE_LOG_DIR/package.log"
fi

cleanup_success=1
REMOTE
then
  progress "remote benchmark payload failed for $RUN_ID; see remote failure output above"
  exit 1
fi
progress "remote benchmark payload completed"
REMOTE_PAYLOAD_STARTED=0

FETCH_SCRIPT="$SCRIPT_DIR/fetch_synced_remote_recovery_results.sh"
fetch_args=(
  --host "$HOST"
  --ssh-port "$SSH_PORT"
  --remote-root "$REMOTE_ROOT"
  --run-id "$RUN_ID"
)
if [[ -n "$SSH_USER" ]]; then
  fetch_args+=(--ssh-user "$SSH_USER")
fi
if [[ -n "$SSH_KEY" ]]; then
  fetch_args+=(--ssh-key "$SSH_KEY")
fi
if [[ "$KEEP_REMOTE_ARCHIVE" -eq 1 ]]; then
  fetch_args+=(--keep-remote-archive)
fi
progress "fetching remote artifact for $RUN_ID"
fetched_dir="$("$FETCH_SCRIPT" "${fetch_args[@]}")"
progress "fetched result directory: $fetched_dir"
printf '%s\n' "$fetched_dir"
