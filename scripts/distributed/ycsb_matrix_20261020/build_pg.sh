#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_checks/ycsb_matrix_20261020
export TMPDIR=$root/scratch
export LD_LIBRARY_PATH=$root/install/lib:/home/neel/Desktop/rdkafka_local/lib:${LD_LIBRARY_PATH:-}
trap 'echo FAIL > "$root/evidence/pg_build.status"' ERR
cd "$root/src"
./configure --prefix="$root/install" --enable-depend --without-readline CFLAGS=-O2 CPPFLAGS=-D_GNU_SOURCE > "$root/evidence/pg_configure.log" 2>&1
make -j4 > "$root/evidence/pg_build.log" 2>&1
make install > "$root/evidence/pg_install.log" 2>&1
make -C contrib/merkle_lsm -j4 >> "$root/evidence/pg_build.log" 2>&1
make -C contrib/merkle_lsm install >> "$root/evidence/pg_install.log" 2>&1
make -C contrib/pgcrypto -j4 >> "$root/evidence/pg_build.log" 2>&1
make -C contrib/pgcrypto install >> "$root/evidence/pg_install.log" 2>&1
sha256sum "$root/install/bin/postgres" "$("$root/install/bin/pg_config" --pkglibdir)/merkle_lsm.so" "$root/install/lib/libpq.so" > "$root/evidence/pg_binaries.sha256"
echo OK > "$root/evidence/pg_build.status"
