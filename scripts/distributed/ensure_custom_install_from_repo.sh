#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=""
INSTALL_DIR=""
CLEAN_WHEN_REBUILD=0
FORCE_REBUILD=0
TRUST_INSTALL=0
EXTRA_INCLUDE_ROOT=""
EXTRA_LIB_ROOT=""

usage() {
  cat <<'EOF'
Usage:
  ensure_custom_install_from_repo.sh \
    --repo-root </path/to/ariabc_cluster> \
    --install-dir </path/to/ariabc_install> \
    [--clean-when-rebuild] [--force-rebuild] [--trust-install]

Ensures the custom BCDB PostgreSQL install tree is runnable. If the install tree
is missing or not runnable on the current host, rebuilds it from the synced repo
and installs it into the requested install directory.

--trust-install: caller guarantees the install was just synced from a known-good
  build (e.g. via rsync from the orchestrator). Skips the source-mtime staleness
  check and the make/gcc toolchain requirement; only verify_install runs. Use on
  execution-only hosts that have no compiler.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) REPO_ROOT="${2:-}"; shift 2 ;;
    --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
    --clean-when-rebuild) CLEAN_WHEN_REBUILD=1; shift 1 ;;
    --force-rebuild) FORCE_REBUILD=1; shift 1 ;;
    --trust-install) TRUST_INSTALL=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$REPO_ROOT" || -z "$INSTALL_DIR" ]]; then
  usage
  echo "ERROR: --repo-root and --install-dir are required" >&2
  exit 2
fi

verify_install() {
  local dir="$1"
  [[ -x "$dir/bin/postgres" ]] || return 1
  [[ -x "$dir/bin/initdb" ]] || return 1
  [[ -x "$dir/bin/pg_ctl" ]] || return 1
  [[ -x "$dir/bin/psql" ]] || return 1
  LD_LIBRARY_PATH="$dir/lib:${LD_LIBRARY_PATH:-}" "$dir/bin/postgres" --version >/dev/null 2>&1 || return 1
  LD_LIBRARY_PATH="$dir/lib:${LD_LIBRARY_PATH:-}" "$dir/bin/initdb" --version >/dev/null 2>&1 || return 1
  LD_LIBRARY_PATH="$dir/lib:${LD_LIBRARY_PATH:-}" "$dir/bin/psql" --version >/dev/null 2>&1 || return 1
  return 0
}

install_is_stale() {
  local dir="$1"
  local postgres_bin="$dir/bin/postgres"
  local stale_path=""

  [[ -x "$postgres_bin" ]] || return 0

  stale_path="$(
    find \
      "$REPO_ROOT/GNUmakefile" \
      "$REPO_ROOT/Makefile" \
      "$REPO_ROOT/configure" \
      "$REPO_ROOT/src" \
      "$REPO_ROOT/contrib" \
      -type f \
      \( -name '*.c' -o -name '*.h' -o -name '*.l' -o -name '*.y' -o -name 'Makefile' -o -name 'GNUmakefile' \) \
      -newer "$postgres_bin" \
      -print -quit 2>/dev/null || true
  )"

  [[ -n "$stale_path" ]]
}

if [[ "$FORCE_REBUILD" != "1" ]] && verify_install "$INSTALL_DIR"; then
  if [[ "$TRUST_INSTALL" == "1" ]] || ! install_is_stale "$INSTALL_DIR"; then
    echo "INSTALL_READY=1"
    echo "INSTALL_DIR=$INSTALL_DIR"
    [[ "$TRUST_INSTALL" == "1" ]] && echo "TRUST_INSTALL=1"
    exit 0
  fi
  FORCE_REBUILD=1
fi

if [[ "$TRUST_INSTALL" == "1" ]]; then
  echo "ERROR: --trust-install passed but install at $INSTALL_DIR did not verify" >&2
  echo "       (binaries missing, not executable, or fail --version on this host)" >&2
  exit 1
fi

if [[ ! -x "$REPO_ROOT/configure" ]]; then
  echo "ERROR: configure not found or not executable under repo root: $REPO_ROOT" >&2
  exit 1
fi

for tool in make gcc; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required build tool missing on host: $tool" >&2
    exit 1
  fi
done

mkdir -p "$INSTALL_DIR" "$REPO_ROOT/.bench_tmp"

# configure/distclean mutate the source tree, so keep concurrent benchmark
# runners from rebuilding the same checkout at the same time.
build_lock="$REPO_ROOT/.bench_tmp/build_custom_install.lock"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$build_lock"
  flock 9
fi

build_log="$REPO_ROOT/.bench_tmp/build_custom_install_$(date +%Y%m%d_%H%M%S).log"
jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)"
if [[ -d "$REPO_ROOT/.bench_tmp/deps/include" ]]; then
  EXTRA_INCLUDE_ROOT="$REPO_ROOT/.bench_tmp/deps/include"
fi
if [[ -d "$REPO_ROOT/.bench_tmp/deps/lib" ]]; then
  EXTRA_LIB_ROOT="$REPO_ROOT/.bench_tmp/deps/lib"
fi
combined_cppflags="-D_GNU_SOURCE${CPPFLAGS:+ ${CPPFLAGS}}"
if [[ -n "$EXTRA_INCLUDE_ROOT" ]]; then
  combined_cppflags="-I$EXTRA_INCLUDE_ROOT${combined_cppflags:+ $combined_cppflags}"
fi
combined_ldflags="${LDFLAGS:-}"
if [[ -n "$EXTRA_LIB_ROOT" ]]; then
  combined_ldflags="-L$EXTRA_LIB_ROOT${combined_ldflags:+ $combined_ldflags}"
fi
USE_EXISTING_CONFIG=0
if [[ -x "$REPO_ROOT/config.status" ]]; then
  USE_EXISTING_CONFIG=1
  if [[ -L "$REPO_ROOT/src/include/utils/errcodes.h" ]]; then
    errcodes_target="$(readlink "$REPO_ROOT/src/include/utils/errcodes.h")"
    case "$errcodes_target" in
      "$REPO_ROOT"/*|../*) ;;
      *) USE_EXISTING_CONFIG=0 ;;
    esac
  fi
fi
if [[ "$FORCE_REBUILD" == "1" && "$CLEAN_WHEN_REBUILD" == "1" ]]; then
  USE_EXISTING_CONFIG=0
fi

{
  echo "[INFO] repo_root=$REPO_ROOT"
  echo "[INFO] install_dir=$INSTALL_DIR"
  echo "[INFO] jobs=$jobs"
  echo "[INFO] clean_when_rebuild=$CLEAN_WHEN_REBUILD"
  echo "[INFO] force_rebuild=$FORCE_REBUILD"
  echo "[INFO] reconfigure=1"
  echo "[INFO] extra_include_root=${EXTRA_INCLUDE_ROOT:-none}"
  echo "[INFO] extra_lib_root=${EXTRA_LIB_ROOT:-none}"
  cd "$REPO_ROOT"
  if [[ "$USE_EXISTING_CONFIG" == "1" ]]; then
    echo "[INFO] configured tree found; refreshing generated files via config.status"
    ./config.status
  elif [[ "$CLEAN_WHEN_REBUILD" == "1" ]]; then
    echo "[INFO] pre-configure clean=distclean"
    make -C "$REPO_ROOT" distclean || make -C "$REPO_ROOT" clean || true
    ac_cv_exeext= CPPFLAGS="$combined_cppflags" LDFLAGS="$combined_ldflags" ./configure --prefix="$INSTALL_DIR" --enable-debug --enable-cassert CFLAGS="-O0 -g3"
  else
    ac_cv_exeext= CPPFLAGS="$combined_cppflags" LDFLAGS="$combined_ldflags" ./configure --prefix="$INSTALL_DIR" --enable-debug --enable-cassert CFLAGS="-O0 -g3"
  fi
  make -C "$REPO_ROOT" -j"$jobs" install prefix="$INSTALL_DIR"
} >"$build_log" 2>&1

if ! verify_install "$INSTALL_DIR"; then
  echo "ERROR: custom install is still not runnable after rebuild" >&2
  echo "BUILD_LOG=$build_log" >&2
  exit 1
fi

echo "INSTALL_READY=1"
echo "INSTALL_DIR=$INSTALL_DIR"
echo "BUILD_LOG=$build_log"
