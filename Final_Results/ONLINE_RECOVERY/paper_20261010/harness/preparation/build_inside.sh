#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/ariabc_data/paper_20261010_recov
export TMPDIR="$B/scratch" PATH="$B/tools/cmake-portable/bin:$PATH" LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
mkdir -p "$TMPDIR" "$B/pgbuild" "$B/provenance"
trap 'rc=$?; printf "build_exit=%s at=%s\n" "$rc" "$(date -u +%FT%TZ)" >> "$B/build_status.txt"' EXIT
cd "$B/repo"
git rev-parse HEAD > "$B/provenance/git_head.txt"
git status --porcelain > "$B/provenance/git_status_before.txt"
test "$(cat "$B/provenance/git_head.txt")" = 16577072ecf888fbbbb5d80d0e90fc340c8ca342
python3 scripts/distributed/source_fingerprint.py --repo . --ring-capacity 2048 > "$B/provenance/source_before.txt"
cd "$B/pgbuild"
"$B/repo/configure" --prefix="$B/install" --enable-depend --without-readline CFLAGS=-O2 > "$B/pg_configure.log" 2>&1
cp "$B/repo/src/interfaces/libpq/exports.list" "$B/pgbuild/src/interfaces/libpq/exports.list"
make -j8 > "$B/pg_build.log" 2>&1
make install > "$B/pg_install.log" 2>&1
make -C contrib/pg_prewarm -j4 > "$B/prewarm_build.log" 2>&1
make -C contrib/pg_prewarm install > "$B/prewarm_install.log" 2>&1
cmake -S "$B/repo/ariabc_pg" -B "$B/cppbuild" -DCMAKE_BUILD_TYPE=Release \
 -DLIBPQ_INCLUDE_DIR="$B/install/include" -DPOSTGRES_INCLUDE_DIR="$B/install/include" \
 -DLIBPQ_LIBRARY="$B/install/lib/libpq.so" \
 -DRDKAFKA_INCLUDE_DIR="$B/rdkafka/include" -DRDKAFKA_LIBRARY="$B/rdkafka/lib/librdkafka.so" > "$B/cpp_configure.log" 2>&1
cmake --build "$B/cppbuild" --target ariabc_pg_server ariabc_pg_gateway -j4 > "$B/cpp_build.log" 2>&1
mkdir -p "$B/repo/ariabc_pg/build/bin" "$B/u22_build/bin"
cp -a "$B/cppbuild/bin/ariabc_pg_server" "$B/cppbuild/bin/ariabc_pg_gateway" "$B/repo/ariabc_pg/build/bin/"
FP=$(cat "$B/provenance/source_before.txt")
for BIN in "$B/install/bin/postgres" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server" "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway"; do
 printf 'binary_name=%s\nbinary_sha256=%s\nsource_fingerprint=%s\nbuild_time=%s\n' "$(basename "$BIN")" "$(sha256sum "$BIN" | awk '{print $1}')" "$FP" "$(date -u +%FT%TZ)" > "$BIN.manifest"
done
cp -a "$B/repo/ariabc_pg/build/bin/." "$B/u22_build/bin/"
sha256sum "$B/install"/bin/* "$B/install/lib/libpq.so" "$B/install/lib/postgresql/pg_prewarm.so" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server" "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway" "$B/rdkafka/lib/librdkafka.so" > "$B/provenance/BINARIES.sha256"
cd "$B/repo"
python3 scripts/distributed/source_fingerprint.py --repo . --ring-capacity 2048 > "$B/provenance/source_after.txt"
cmp "$B/provenance/source_before.txt" "$B/provenance/source_after.txt"
git status --porcelain > "$B/provenance/git_status_after.txt"
printf 'BUILD_READY %s\n' "$(date -u +%FT%TZ)" >> "$B/build_status.txt"
