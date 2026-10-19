#!/bin/bash
set -euo pipefail
root=/home/neel/claude_checks/lsm_lookup_20261014
export TMPDIR="$root/scratch"
trap 'echo BUILD_FAILED > "$root/build.status"' ERR
echo BUILDING > "$root/build.status"
cd "$root/src"
./configure --prefix="$root/install" --enable-depend --without-readline CFLAGS=-O2 CPPFLAGS=-D_GNU_SOURCE > "$root/configure.log" 2>&1
make -j8 > "$root/build.log" 2>&1
make install > "$root/install.log" 2>&1
make -C contrib/pgcrypto install >> "$root/install.log" 2>&1
sha256sum "$root"/install/bin/{postgres,psql,pg_ctl,initdb,pg_config} "$root/install/lib/libpq.so" > "$root/evidence/pg_binaries.sha256"
echo BUILD_OK > "$root/build.status"
echo LSM3_BUILDING > "$root/build.status"
make -C contrib/lsm3 USE_PGXS=1 PG_CONFIG="$root/install/bin/pg_config" PG_CPPFLAGS="-DARIABC_PRE13 -I$root/src/src/include" > "$root/lsm3_build.log" 2>&1
make -C contrib/lsm3 USE_PGXS=1 PG_CONFIG="$root/install/bin/pg_config" PG_CPPFLAGS="-DARIABC_PRE13 -I$root/src/src/include" install >> "$root/lsm3_build.log" 2>&1
sha256sum "$root/install/lib/postgresql/lsm3.so" >> "$root/evidence/pg_binaries.sha256"
echo PG_AND_LSM3_BUILD_OK > "$root/build.status"
