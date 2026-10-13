#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/ariabc_data/paper_20261010_recov
trap 'rc=$?; printf "outer_exit=%s at=%s\n" "$rc" "$(date -u +%FT%TZ)" >> "$B/build_status.txt"' EXIT
printf 'BUILD_START %s\n' "$(date -u +%FT%TZ)" > "$B/build_status.txt"
tar -xzf "$B/preparation/frozen_source.tar.gz" -C "$B/repo"
cp "$B/preparation/Dockerfile" "$B/image/"
docker build -t ariabc-recov-paper-20261010:u22 "$B/image" > "$B/image_build.log" 2>&1
docker image inspect ariabc-recov-paper-20261010:u22 > "$B/provenance/container_image.json"
test -f "$B/rdkafka/lib/librdkafka.so"
test -x "$B/tools/cmake-portable/bin/cmake"
chown -R 1003:1003 "$B"
docker run --rm --network host --user 1003:1003 -v "$B:$B" -w "$B/repo" \
 -e TMPDIR="$B/scratch" ariabc-recov-paper-20261010:u22 \
 bash "$B/preparation/build_inside.sh" > "$B/build_container.log" 2>&1
