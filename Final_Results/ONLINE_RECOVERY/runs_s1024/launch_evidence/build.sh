#!/usr/bin/env bash
set -euo pipefail
B=${1:?}; ABI=${2:?}
[[ "$B" == /home/neel/Desktop/recovery_s1024_20261001T060514ZJ ]]
case "$(hostname -I) $ABI" in
  *10.129.27.111*u24|*10.129.148.246*u22) ;;
  *) exit 2;;
esac
export LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
if [[ $ABI == u22 ]]; then
  cp -a /tmp/cmake-3.28.3-linux-x86_64 "$B/cmake"
  export PATH="$B/cmake/bin:$PATH"
fi
RING=$(sed -n -E 's/^#define[[:space:]]+BCDB_RESULT_RING_CAPACITY[[:space:]]+([0-9]+).*/\1/p' "$B/repo/src/include/bcdb/globals.h")
python3 "$B/repo/scripts/distributed/source_fingerprint.py" --repo "$B/repo" --ring-capacity "$RING" > "$B/source_before.txt"
bash "$B/repo/scripts/distributed/ensure_custom_install_from_repo.sh" --repo-root "$B/repo" --install-dir "$B/install" --force-rebuild --clean-when-rebuild
python3 "$B/repo/scripts/distributed/source_fingerprint.py" --repo "$B/repo" --ring-capacity "$RING" > "$B/source_after.txt"
cmp "$B/source_before.txt" "$B/source_after.txt"
cmake -S "$B/repo/ariabc_pg" -B "$B/repo/ariabc_pg/build" -DCMAKE_BUILD_TYPE=Release -DRDKAFKA_INCLUDE_DIR="$B/rdkafka/include" -DRDKAFKA_LIBRARY="$B/rdkafka/lib/librdkafka.so" -DLIBPQ_INCLUDE_DIR="$B/install/include" -DPOSTGRES_INCLUDE_DIR="$B/install/include/postgresql/server" -DLIBPQ_LIBRARY="$B/install/lib/libpq.so"
cmake --build "$B/repo/ariabc_pg/build" --target ariabc_pg_gateway ariabc_pg_server -j2
mkdir -p "$B/artifacts/$ABI/bin"
cp -a "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server" "$B/artifacts/$ABI/bin/"
if [[ $ABI == u22 ]]; then mkdir -p "$B/u22_build/bin"; cp -a "$B/artifacts/$ABI/bin/." "$B/u22_build/bin/"; fi
FP=$(cat "$B/source_after.txt")
for BIN in "$B/install/bin/postgres" "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server"; do
  printf 'binary_name=%s\nbinary_sha256=%s\nsource_fingerprint=%s\nbuild_time=%s\n' "$(basename "$BIN")" "$(sha256sum "$BIN" | awk '{print $1}')" "$FP" "$(date -u +%FT%TZ)" > "$BIN.manifest"
done
cp -a "$B/repo/ariabc_pg/build/bin/"*.manifest "$B/artifacts/$ABI/bin/"
if [[ $ABI == u22 ]]; then cp -a "$B/repo/ariabc_pg/build/bin/"*.manifest "$B/u22_build/bin/"; fi
printf 'source_fingerprint=%s\nring_capacity=%s\nabi=%s\n' "$FP" "$RING" "$ABI" > "$B/artifacts/$ABI/build.env"
for BIN in "$B/install/bin/postgres" "$B/artifacts/$ABI/bin/ariabc_pg_gateway" "$B/artifacts/$ABI/bin/ariabc_pg_server"; do ldd "$BIN"; done
echo BUILD_DONE
