#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=""
INSTALL_DIR=""
CLEAN_WHEN_REBUILD=0
FORCE_REBUILD=0
TRUST_INSTALL=0
EXTRA_INCLUDE_ROOT=""
EXTRA_LIB_ROOT=""
build_log=""

cleanup() {
  local rc=$?
  if [[ $rc -ne 0 && -n "${build_log:-}" && -f "$build_log" ]]; then
    echo "ERROR: custom install build failed (exit code $rc). Last 20 lines of build log:" >&2
    tail -n 20 "$build_log" >&2
  fi
}
trap cleanup EXIT

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

if [[ "$FORCE_REBUILD" != "1" ]]; then
  if verify_install "$INSTALL_DIR"; then
    if [[ "$TRUST_INSTALL" == "1" ]] || ! install_is_stale "$INSTALL_DIR"; then
      echo "INSTALL_READY=1"
      echo "INSTALL_DIR=$INSTALL_DIR"
      [[ "$TRUST_INSTALL" == "1" ]] && echo "TRUST_INSTALL=1"
      exit 0
    fi
  fi
  # If we reach here, either the install didn't verify, or it's stale
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
build_dir="$REPO_ROOT/.bench_tmp/pg_build"
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
  echo "[INFO] build_dir=$build_dir"
  echo "[INFO] extra_include_root=${EXTRA_INCLUDE_ROOT:-none}"
  echo "[INFO] extra_lib_root=${EXTRA_LIB_ROOT:-none}"
  cd "$REPO_ROOT"
  if [[ "$USE_EXISTING_CONFIG" == "1" ]]; then
    echo "[INFO] configured tree found; refreshing generated files via config.status"
    ./config.status
    make -C "$REPO_ROOT" -j"$jobs" install prefix="$INSTALL_DIR"
  else
    echo "[INFO] cleaning in-tree compiled objects, generated files, and binaries from source tree to avoid VPATH build issues"
    # 1. Delete compiled binaries and executables (excluding tracked scripts)
    find "$REPO_ROOT/src" -type f -executable \
      ! -path "*/src/tools/*" \
      ! -name "*.sh" ! -name "*.py" ! -name "*.pl" \
      ! -name "runall" ! -name "unused_oids" ! -name "duplicate_oids" \
      -delete
      
    # 2. Delete object files, libraries, generated headers, stamps, and objfiles/export lists.
    #    IMPORTANT: do NOT include any .c filenames that exist in the git source tree
    #    (e.g. localtime.c, encnames.c, wchar.c, xlogreader.c, fmgrtab.c, *desc.c).
    #    Those are real tracked source files; deleting them breaks the out-of-tree build
    #    with errors like "No rule to make target 'localtime.o'".
    #    Only lwlocknames.c is truly generated (from lwlocknames.txt) and safe to remove.
    #
    #    Also IMPORTANT: do NOT delete *_d.h / schemapg.h / fmgroids.h / fmgrprotos.h
    #    from src/include/catalog/ — the out-of-tree 'make install' copies them from
    #    the source tree (not from the build dir), so they must be present there.
    #    They are synced from the local build machine and are safe to keep as-is.
    #    NOTE: we only exclude src/include/catalog/ — NOT all of src/include/ — because
    #    pg_config.h / pg_config_ext.h in src/include/ are host-specific generated files
    #    that must be regenerated by configure for the target host's OS/glibc.
    find "$REPO_ROOT" -type f \( \
      -name "*.o" -o -name "*.a" -o -name "*.so" -o -name "*.so.*" -o \
      -name "*stamp" -o -name "stamp-*" -o -name "*_d.h" -o -name "schemapg.h" -o \
      -name "errcodes.h" -o -name "fmgroids.h" -o -name "fmgrprotos.h" -o \
      -name "lwlocknames.h" -o -name "lwlocknames.c" -o -name "probes.h" -o \
      -name "plerrcodes.h" -o -name "pg_config.h" -o -name "pg_config_ext.h" -o \
      -name "pg_config_os.h" -o -name "ecpg_config.h" -o -name "pg_config_paths.h" -o \
      -name "objfiles.txt" -o -name "exports.list" -o -name "*.list" -o \
      -name "*.pc" -o -name "snowball_create.sql" \
    \) \
      ! -path "*/src/tools/*" \
      ! -path "*/src/include/catalog/*" \
      -delete

    # _repair_src_include_symlinks: fix dangling symlinks in src/include/ before 'make install'.
    # Context: the build machine creates symlinks like:
    #   src/include/catalog/schemapg.h -> /work/ARIABC/AriaBC/src/backend/catalog/schemapg.h
    #   src/include/parser/gram.h      -> /work/ARIABC/AriaBC/src/backend/parser/gram.h
    # When rsync copies these to a remote host, the symlinks are dangling (target path doesn't
    # exist on the remote). 'make install' then fails with "cp: cannot stat <file>".
    # Strategy: for each dangling symlink, derive the relative portion of the target path
    # (stripping any /work/.../src/ prefix) and look for the real file in:
    #   (a) REPO_ROOT/src/<relative> — the synced source tree
    #   (b) build_dir/src/backend/<dir>/<basename> — the out-of-tree build output
    # If found, replace the dangling symlink with a real copy.
    _repair_src_include_symlinks() {
      local src_include="$REPO_ROOT/src/include"
      echo "[INFO] repairing dangling symlinks in src/include/ (stale absolute paths from build machine)"
      find "$src_include" -type l | while IFS= read -r lnk; do
        local target
        target="$(readlink "$lnk")"
        # Skip if symlink still works
        [[ -e "$lnk" ]] && continue
        local bname
        bname="$(basename "$lnk")"
        # Derive a relative path from the symlink target: strip everything up to and including /src/
        # e.g. /work/ARIABC/AriaBC/src/backend/catalog/foo.h  =>  backend/catalog/foo.h
        local rel_from_src
        rel_from_src="$(echo "$target" | sed 's|.*/src/||')"
        # Try to find the real file in several locations
        local real_file=""
        # 1. Direct match in REPO_ROOT/src/
        if [[ -f "$REPO_ROOT/src/$rel_from_src" ]]; then
          real_file="$REPO_ROOT/src/$rel_from_src"
        # 2. In the out-of-tree build backend dir (generated files)
        elif [[ -f "$build_dir/src/backend/catalog/$bname" ]]; then
          real_file="$build_dir/src/backend/catalog/$bname"
        elif [[ -f "$build_dir/src/backend/parser/$bname" ]]; then
          real_file="$build_dir/src/backend/parser/$bname"
        elif [[ -f "$build_dir/src/backend/utils/$bname" ]]; then
          real_file="$build_dir/src/backend/utils/$bname"
        else
          # Search build dir broadly (slower, fallback)
          local found
          found="$(find "$build_dir/src" -maxdepth 4 -name "$bname" -type f 2>/dev/null | head -1)"
          [[ -n "$found" ]] && real_file="$found"
        fi
        if [[ -n "$real_file" ]]; then
          rm -f "$lnk"
          cp -f "$real_file" "$lnk"
          echo "[INFO]   fixed: $(basename "$lnk")  <-  $real_file"
        else
          echo "[WARN]   could not resolve dangling symlink: $lnk -> $target"
        fi
      done
    }

    # _stage_vpath_generated_headers: copy bison/gperf/perl-generated headers into
    # the build-dir's src/include/ subdirs BEFORE 'make install'.
    #
    # Context: in a VPATH (out-of-tree) build, the src/include/Makefile install rule
    # contains the block:
    #   ifeq ($(vpath_build),yes)
    #     for file in catalog/schemapg.h catalog/pg_*_d.h parser/gram.h \
    #                 storage/lwlocknames.h utils/probes.h; do \
    #       cp $$file '$(DESTDIR)$(includedir_server)'/$$file || exit; done
    #   endif
    # This cp runs with CWD = $build_dir/src/include/ and expects those generated
    # files to exist there.  Most (lwlocknames.h, probes.h, *_d.h) are produced
    # directly in $build_dir/src/include/ by make.  But parser/gram.h is only
    # produced in $build_dir/src/backend/parser/ (never mirrored into the include
    # vpath subdir), so the install step fails with:
    #   cp: cannot stat 'parser/gram.h': No such file or directory
    # We also handle schemapg.h which is generated in src/backend/catalog/ and
    # may not be present in src/include/catalog/ on the build machine after a clean.
    _stage_vpath_generated_headers() {
      local inc_vpath="$build_dir/src/include"
      echo "[INFO] staging vpath-generated headers into $inc_vpath"

      # --- parser/gram.h ---
      # Bison generates this in $build_dir/src/backend/parser/.  The source tree
      # also tracks it at $REPO_ROOT/src/backend/parser/gram.h (and a symlink at
      # $REPO_ROOT/src/include/parser/gram.h -> ../backend/parser/gram.h).
      local gram_dst="$inc_vpath/parser/gram.h"
      if [[ ! -f "$gram_dst" ]]; then
        mkdir -p "$inc_vpath/parser"
        local gram_src=""
        # Prefer the freshly built copy in the build dir
        if [[ -f "$build_dir/src/backend/parser/gram.h" ]]; then
          gram_src="$build_dir/src/backend/parser/gram.h"
        # Fall back to the source tree copy (may be from the last full build)
        elif [[ -f "$REPO_ROOT/src/backend/parser/gram.h" ]]; then
          gram_src="$REPO_ROOT/src/backend/parser/gram.h"
        else
          # Last resort: generate gram.h by running bison on gram.y
          echo "[INFO]   gram.h not found in build or source tree; generating via bison"
          if command -v bison >/dev/null 2>&1; then
            bison -d -o /dev/null "$REPO_ROOT/src/backend/parser/gram.y" 2>/dev/null || true
            # bison -d writes <stem>.tab.h; try the build dir output
            if [[ -f "$build_dir/src/backend/parser/gram.h" ]]; then
              gram_src="$build_dir/src/backend/parser/gram.h"
            fi
          fi
        fi
        if [[ -n "$gram_src" ]]; then
          cp -f "$gram_src" "$gram_dst"
          echo "[INFO]   staged: parser/gram.h  <-  $gram_src"
        else
          echo "[WARN]   could not locate parser/gram.h — make install may fail"
        fi
      else
        echo "[INFO]   parser/gram.h already present in vpath include dir"
      fi

      # --- catalog/schemapg.h ---
      local schemapg_dst="$inc_vpath/catalog/schemapg.h"
      if [[ ! -f "$schemapg_dst" ]]; then
        mkdir -p "$inc_vpath/catalog"
        local schemapg_src=""
        if [[ -f "$build_dir/src/backend/catalog/schemapg.h" ]]; then
          schemapg_src="$build_dir/src/backend/catalog/schemapg.h"
        elif [[ -f "$REPO_ROOT/src/include/catalog/schemapg.h" && ! -L "$REPO_ROOT/src/include/catalog/schemapg.h" ]]; then
          schemapg_src="$REPO_ROOT/src/include/catalog/schemapg.h"
        fi
        if [[ -n "$schemapg_src" ]]; then
          cp -f "$schemapg_src" "$schemapg_dst"
          echo "[INFO]   staged: catalog/schemapg.h  <-  $schemapg_src"
        fi
      fi

      # --- catalog/pg_*_d.h (OID definition headers generated by genbki.pl) ---
      # These are normally produced directly in $build_dir/src/include/catalog/
      # so usually nothing to do; but sync from src/backend/catalog/ if missing.
      local d_missing=0
      if ! ls "$inc_vpath/catalog/pg_"*"_d.h" >/dev/null 2>&1; then
        mkdir -p "$inc_vpath/catalog"
        for f in "$build_dir/src/backend/catalog/"pg_*_d.h; do
          [[ -f "$f" ]] || continue
          cp -f "$f" "$inc_vpath/catalog/"
          d_missing=1
        done
        [[ "$d_missing" -eq 1 ]] && echo "[INFO]   staged missing pg_*_d.h from backend/catalog/"
      fi

      # --- storage/lwlocknames.h and utils/probes.h ---
      # These should already be in $build_dir/src/include/{storage,utils}/ after make;
      # but copy from backend dirs as a safety net if absent.
      for pair in "storage/lwlocknames.h:$build_dir/src/backend/storage/lmgr/lwlocknames.h" \
                  "utils/probes.h:$build_dir/src/backend/utils/probes.h"; do
        local rel_dst="${pair%%:*}"
        local fallback_src="${pair##*:}"
        local dst_file="$inc_vpath/$rel_dst"
        if [[ ! -f "$dst_file" && -f "$fallback_src" ]]; then
          mkdir -p "$(dirname "$dst_file")"
          cp -f "$fallback_src" "$dst_file"
          echo "[INFO]   staged: $rel_dst  <-  $fallback_src"
        fi
      done
    }

    if [[ "$CLEAN_WHEN_REBUILD" == "1" ]]; then
      echo "[INFO] clean out-of-tree build dir"
      rm -rf "$build_dir"
      mkdir -p "$build_dir"
      rm -f "$REPO_ROOT"/conftest "$REPO_ROOT"/conftest.* "$REPO_ROOT"/confdefs.h "$REPO_ROOT"/a.out "$REPO_ROOT"/b.out
      cd "$build_dir"
      ac_cv_exeext= CPPFLAGS="$combined_cppflags" LDFLAGS="$combined_ldflags" "$REPO_ROOT/configure" --prefix="$INSTALL_DIR" --enable-debug --enable-cassert CFLAGS="-O0 -g3"
      make -C "$build_dir" -j"$jobs"
      _repair_src_include_symlinks
      _stage_vpath_generated_headers
      make -C "$build_dir" install prefix="$INSTALL_DIR"
    else
      mkdir -p "$build_dir"
      rm -f "$REPO_ROOT"/conftest "$REPO_ROOT"/conftest.* "$REPO_ROOT"/confdefs.h "$REPO_ROOT"/a.out "$REPO_ROOT"/b.out
      cd "$build_dir"
      ac_cv_exeext= CPPFLAGS="$combined_cppflags" LDFLAGS="$combined_ldflags" "$REPO_ROOT/configure" --prefix="$INSTALL_DIR" --enable-debug --enable-cassert CFLAGS="-O0 -g3"
      make -C "$build_dir" -j"$jobs"
      _repair_src_include_symlinks
      _stage_vpath_generated_headers
      make -C "$build_dir" install prefix="$INSTALL_DIR"
    fi
  fi
} >"$build_log" 2>&1

if ! verify_install "$INSTALL_DIR"; then
  echo "ERROR: custom install is still not runnable after rebuild" >&2
  echo "BUILD_LOG=$build_log" >&2
  exit 1
fi

echo "INSTALL_READY=1"
echo "INSTALL_DIR=$INSTALL_DIR"
echo "BUILD_LOG=$build_log"
