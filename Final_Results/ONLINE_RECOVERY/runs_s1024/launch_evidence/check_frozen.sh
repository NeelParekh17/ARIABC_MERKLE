set -euo pipefail
B='@RBASE@'
FP=$(python3 "$B/repo/scripts/distributed/source_fingerprint.py" --repo "$B/repo" --ring-capacity 2048)
test "$FP" = 60cae0f48ccd4cb4797c671757e6af640c0512ec827598fca234a338c6390490
echo "full_frozen_source_fingerprint=$FP"
# The omitted file was src/benchmark/requirements.txt; it has no compiled inputs.
# Recheck the existing C++ targets after restoring all fingerprint inputs.
if [[ $(hostname -I) == *10.129.148.246* ]]; then export PATH="$B/cmake/bin:$PATH"; ABI=u22; else ABI=u24; fi
cmake --build "$B/repo/ariabc_pg/build" --target ariabc_pg_gateway ariabc_pg_server -j2
for BIN in "$B/install/bin/postgres" "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server"; do
 printf 'binary_name=%s\nbinary_sha256=%s\nsource_fingerprint=%s\nbuild_time=%s\n' "$(basename "$BIN")" "$(sha256sum "$BIN" | awk '{print $1}')" "$FP" "$(date -u +%FT%TZ)" > "$BIN.manifest"
done
cp -a "$B/repo/ariabc_pg/build/bin/"*.manifest "$B/artifacts/$ABI/bin/"
if [[ $ABI == u22 ]]; then cp -a "$B/repo/ariabc_pg/build/bin/"*.manifest "$B/u22_build/bin/"; fi
printf 'source_fingerprint=%s\nring_capacity=2048\nabi=%s\n' "$FP" "$ABI" > "$B/artifacts/$ABI/build.env"
