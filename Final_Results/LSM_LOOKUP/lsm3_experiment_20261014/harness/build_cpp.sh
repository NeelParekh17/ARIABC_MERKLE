#!/bin/bash
set -euo pipefail
root=/home/neel/claude_ctl/lsm_lookup_20261014
export TMPDIR="$root/scratch"
trap 'echo BUILD_FAILED > "$root/build.status"' ERR
echo BUILDING > "$root/build.status"
rsync -a neel@10.129.148.247:claude_checks/lsm_lookup_20261014/install/ "$root/pg_install/"
cmake -S "$root/src/ariabc_pg" -B "$root/build" -DCMAKE_BUILD_TYPE=Release -DLIBPQ_INCLUDE_DIR="$root/pg_install/include" -DPOSTGRES_INCLUDE_DIR="$root/pg_install/include" -DLIBPQ_LIBRARY="$root/pg_install/lib/libpq.so" > "$root/configure.log" 2>&1
cmake --build "$root/build" --target ariabc_pg_server ariabc_pg_gateway -j4 > "$root/build.log" 2>&1
sha256sum "$root/build/bin/ariabc_pg_server" "$root/build/bin/ariabc_pg_gateway" "$root/pg_install/lib/libpq.so" > "$root/evidence/cpp_binaries.sha256"
echo BUILD_OK > "$root/build.status"
