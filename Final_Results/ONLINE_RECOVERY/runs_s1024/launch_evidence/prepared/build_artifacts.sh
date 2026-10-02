#!/usr/bin/env bash
# Orchestrator only: execute on .247 (U22 build inside a host-network container).
set -euo pipefail
RECOVERY_BASE=${1:?usage: build_artifacts.sh /home/neel/Desktop/recovery_s1024_TAG u24_or_u22}
RECOVERY_ABI=${2:?missing ABI}
[[ "$RECOVERY_BASE" =~ ^/home/neel/Desktop/recovery_s1024_[a-zA-Z0-9_]+$ ]] || exit 2
[[ "$RECOVERY_ABI" == u24 || "$RECOVERY_ABI" == u22 ]] || exit 2
case " $(hostname -I) " in *' 10.129.148.247 '*) ;; *) echo 'Build only on lab .247' >&2; exit 2;; esac
RECOVERY_REPO="$RECOVERY_BASE/repo"
RECOVERY_INSTALL="$RECOVERY_BASE/install_$RECOVERY_ABI"
RECOVERY_ARTIFACT="$RECOVERY_BASE/artifacts/$RECOVERY_ABI"
[[ ! -e "$RECOVERY_ARTIFACT" && ! -e "$RECOVERY_INSTALL" ]] || { echo 'Use a fresh artifact path' >&2; exit 2; }
mkdir -p "$RECOVERY_ARTIFACT"
RECOVERY_RING=$(sed -n -E 's/^#define[[:space:]]+BCDB_RESULT_RING_CAPACITY[[:space:]]+([0-9]+).*/\1/p' "$RECOVERY_REPO/src/include/bcdb/globals.h")
[[ "$RECOVERY_RING" =~ ^[0-9]+$ ]] || exit 2
RECOVERY_FP=$(python3 "$RECOVERY_REPO/scripts/distributed/source_fingerprint.py" --repo "$RECOVERY_REPO" --ring-capacity "$RECOVERY_RING")
bash "$RECOVERY_REPO/scripts/distributed/ensure_custom_install_from_repo.sh" \
  --repo-root "$RECOVERY_REPO" --install-dir "$RECOVERY_INSTALL" --force-rebuild --clean-when-rebuild
# PostgreSQL generation may normalize generated files; use the built identity.
RECOVERY_FP=$(python3 "$RECOVERY_REPO/scripts/distributed/source_fingerprint.py" --repo "$RECOVERY_REPO" --ring-capacity "$RECOVERY_RING")
[[ -f "$RECOVERY_BASE/rdkafka/lib/librdkafka.so" ]] || { echo 'Stage librdkafka 2.3.0 first' >&2; exit 2; }
cmake -S "$RECOVERY_REPO/ariabc_pg" -B "$RECOVERY_REPO/ariabc_pg/build_$RECOVERY_ABI" \
  -DCMAKE_BUILD_TYPE=Release \
  -DRDKAFKA_INCLUDE_DIR="$RECOVERY_BASE/rdkafka/include" \
  -DRDKAFKA_LIBRARY="$RECOVERY_BASE/rdkafka/lib/librdkafka.so" \
  -DLIBPQ_INCLUDE_DIR="$RECOVERY_INSTALL/include" \
  -DPOSTGRES_INCLUDE_DIR="$RECOVERY_INSTALL/include/postgresql/server" \
  -DLIBPQ_LIBRARY="$RECOVERY_INSTALL/lib/libpq.so"
cmake --build "$RECOVERY_REPO/ariabc_pg/build_$RECOVERY_ABI" \
  --target ariabc_pg_gateway ariabc_pg_server -j"${RECOVERY_BUILD_JOBS:-2}"
mkdir -p "$RECOVERY_ARTIFACT/bin"
cp -a "$RECOVERY_REPO/ariabc_pg/build_$RECOVERY_ABI/bin/ariabc_pg_gateway" \
      "$RECOVERY_REPO/ariabc_pg/build_$RECOVERY_ABI/bin/ariabc_pg_server" "$RECOVERY_ARTIFACT/bin/"
for RECOVERY_BINARY in "$RECOVERY_INSTALL/bin/postgres" "$RECOVERY_ARTIFACT/bin/ariabc_pg_gateway" "$RECOVERY_ARTIFACT/bin/ariabc_pg_server"; do
  [[ ! -e "$RECOVERY_BINARY.manifest" ]] || exit 2
  {
    printf 'binary_name=%s\n' "$(basename "$RECOVERY_BINARY")"
    printf 'binary_sha256=%s\n' "$(sha256sum "$RECOVERY_BINARY" | awk '{print $1}')"
    printf 'source_fingerprint=%s\n' "$RECOVERY_FP"
    printf 'build_time=%s\n' "$(date -u +%FT%TZ)"
  } > "$RECOVERY_BINARY.manifest"
done
printf 'source_fingerprint=%s\nring_capacity=%s\nabi=%s\n' "$RECOVERY_FP" "$RECOVERY_RING" "$RECOVERY_ABI" > "$RECOVERY_ARTIFACT/build.env"
[[ "$RECOVERY_FP" == "$(python3 "$RECOVERY_REPO/scripts/distributed/source_fingerprint.py" --repo "$RECOVERY_REPO" --ring-capacity "$RECOVERY_RING")" ]] || { echo 'Source changed during build' >&2; exit 1; }
