#!/usr/bin/env bash
# usage: build.sh <variant>   (source must already be rsynced into ~/claude_checks/detopt_20261007/<variant>/src)
# First call configures; later calls are incremental. Logs in <variant>/build_*.log
set -euo pipefail
V=$1; D=$HOME/claude_checks/detopt_20261007/$V; SRC=$D/src; INST=$D/install; C=$HOME/Desktop
[[ -d $SRC ]] || { echo "no src at $SRC"; exit 2; }
export PATH=$HOME/.local/lib/python3.12/site-packages/cmake/data/bin:$PATH
export LD_LIBRARY_PATH=$INST/lib:$C/compat_lib:$C/rdkafka_local/lib
cd $SRC
if [[ ! -f $SRC/config.status ]]; then
  ./configure --prefix="$INST" --without-readline CFLAGS=-O2 CPPFLAGS=-I$C/compat_include LDFLAGS=-L$C/compat_lib > $D/build_configure.log 2>&1
fi
make -j24 > $D/build_pg.log 2>&1 || { tail -40 $D/build_pg.log; exit 1; }
make install > $D/build_install.log 2>&1
make -C contrib/pg_prewarm > $D/build_prewarm.log 2>&1 && make -C contrib/pg_prewarm install >> $D/build_prewarm.log 2>&1
if [[ ! -f $SRC/ariabc_pg/build/CMakeCache.txt ]]; then
cmake -S "$SRC/ariabc_pg" -B "$SRC/ariabc_pg/build" -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$INST" \
  -DLIBPQ_INCLUDE_DIR="$INST/include" -DPOSTGRES_INCLUDE_DIR="$INST/include" -DLIBPQ_LIBRARY="$INST/lib/libpq.so" \
  -DOPENSSL_INCLUDE_DIR=$C/compat_include -DOPENSSL_CRYPTO_LIBRARY=$C/compat_lib/libcrypto.so -DOPENSSL_SSL_LIBRARY=$C/compat_lib/libssl.so \
  -DRDKAFKA_INCLUDE_DIR=$C/rdkafka_local/include -DRDKAFKA_LIBRARY=$C/rdkafka_local/lib/librdkafka.so > $D/build_cmake.log 2>&1
fi
cmake --build "$SRC/ariabc_pg/build" --target ariabc_pg_server ariabc_pg_gateway -j24 > $D/build_cpp.log 2>&1 || { tail -40 $D/build_cpp.log; exit 1; }
sha256sum $INST/bin/postgres $SRC/ariabc_pg/build/bin/ariabc_pg_server $SRC/ariabc_pg/build/bin/ariabc_pg_gateway > $D/BINARIES.txt
echo BUILD_OK $V
