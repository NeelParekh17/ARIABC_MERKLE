#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=""
INSTALL_DIR=""
CLEAN_WHEN_REBUILD=0
FORCE_REBUILD=0
EXTRA_INCLUDE_ROOT=""
EXTRA_LIB_ROOT=""

usage() {
  cat <<'EOF'
Usage:
  ensure_custom_install_from_repo.sh \
    --repo-root </path/to/ariabc_cluster> \
    --install-dir </path/to/ariabc_install> \
    [--clean-when-rebuild] [--force-rebuild]

Ensures the custom BCDB PostgreSQL install tree is runnable. If the install tree
is missing or not runnable on the current host, rebuilds it from the synced repo
and installs it into the requested install directory.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) REPO_ROOT="${2:-}"; shift 2 ;;
    --install-dir) INSTALL_DIR="${2:-}"; shift 2 ;;
    --clean-when-rebuild) CLEAN_WHEN_REBUILD=1; shift 1 ;;
    --force-rebuild) FORCE_REBUILD=1; shift 1 ;;
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

if [[ "$FORCE_REBUILD" != "1" ]] && verify_install "$INSTALL_DIR"; then
  echo "INSTALL_READY=1"
  echo "INSTALL_DIR=$INSTALL_DIR"
  exit 0
fi

if [[ ! -f "$REPO_ROOT/GNUmakefile" ]]; then
  echo "ERROR: GNUmakefile not found under repo root: $REPO_ROOT" >&2
  exit 1
fi

for tool in make gcc; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required build tool missing on host: $tool" >&2
    exit 1
  fi
done

mkdir -p "$INSTALL_DIR" "$REPO_ROOT/.bench_tmp"
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
  if [[ "$CLEAN_WHEN_REBUILD" == "1" ]]; then
    echo "[INFO] pre-configure clean=distclean"
    make -C "$REPO_ROOT" distclean || make -C "$REPO_ROOT" clean || true
  fi
  ac_cv_exeext= CPPFLAGS="$combined_cppflags" LDFLAGS="$combined_ldflags" ./configure --prefix="$INSTALL_DIR" --enable-debug --enable-cassert CFLAGS="-O0 -g3"
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
