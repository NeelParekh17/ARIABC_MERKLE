#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_ctl/ycsb_matrix_20261020
export TMPDIR=$root/scratch
trap 'echo FAIL > "$root/evidence/cpp_build.status"' ERR
rsync -a -e 'ssh -o ControlMaster=no' neel@10.129.148.247:claude_checks/ycsb_matrix_20261020/install/ "$root/pg_install/"
cmake -S "$root/src/ariabc_pg" -B "$root/src/ariabc_pg/build" -DCMAKE_BUILD_TYPE=Release -DLIBPQ_INCLUDE_DIR="$root/pg_install/include" -DPOSTGRES_INCLUDE_DIR="$root/pg_install/include" -DLIBPQ_LIBRARY="$root/pg_install/lib/libpq.so" > "$root/evidence/cpp_configure.log" 2>&1
cmake --build "$root/src/ariabc_pg/build" --target ariabc_pg_server ariabc_pg_gateway -j2 > "$root/evidence/cpp_build.log" 2>&1
sha256sum "$root/src/ariabc_pg/build/bin/ariabc_pg_server" "$root/src/ariabc_pg/build/bin/ariabc_pg_gateway" > "$root/evidence/cpp_binaries.sha256"
rsync -a -e 'ssh -o ControlMaster=no' "$root/src/ariabc_pg/build/bin/ariabc_pg_server" neel@10.129.148.247:claude_checks/ycsb_matrix_20261020/cluster/ariabc_pg/build/bin/
echo OK > "$root/evidence/cpp_build.status"
