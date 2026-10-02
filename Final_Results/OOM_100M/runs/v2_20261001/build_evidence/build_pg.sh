#!/bin/bash
# Runs under nohup ON .247. Only writes the dedicated v2 paths.
set -euo pipefail
export LC_ALL=C
V2_EVIDENCE=/home/neel/claude_opt/oom_v2_evidence
V2_SRC=/home/neel/claude_opt/source_v2
V2_BUILD=/home/neel/claude_opt/pg_build_v2_inplace
V2_INSTALL=/home/neel/claude_opt/install_v2
mkdir -p "$V2_EVIDENCE"
exec > >(tee -a "$V2_EVIDENCE/build_pg.log") 2>&1
trap 'rc=$?; printf "FAILED rc=%s line=%s\n" "$rc" "$LINENO" > "$V2_EVIDENCE/BUILD_STATUS"; exit "$rc"' ERR
printf 'RUNNING pid=%s\n' "$$" > "$V2_EVIDENCE/BUILD_STATUS"
cd "$V2_EVIDENCE"
sha256sum -c source.tar.gz.sha256
if [[ ! -f "$V2_SRC/.snapshot_sha256" ]]; then
  test ! -e "$V2_SRC"
  mkdir "$V2_SRC"
  tar -xzf source.tar.gz -C "$V2_SRC"
  cp source.tar.gz.sha256 "$V2_SRC/.snapshot_sha256"
fi
cmp source.tar.gz.sha256 "$V2_SRC/.snapshot_sha256"
test ! -e "$V2_SRC/config.status"
if [[ ! -f "$V2_BUILD/.snapshot_sha256" ]]; then
  test ! -e "$V2_BUILD"
  mkdir "$V2_BUILD"
  tar -xzf source.tar.gz -C "$V2_BUILD"
  cp source.tar.gz.sha256 "$V2_BUILD/.snapshot_sha256"
fi
cmp source.tar.gz.sha256 "$V2_BUILD/.snapshot_sha256"
cd "$V2_BUILD"
# Configure a dedicated build COPY in place, like the canonical workflow.
# The source_v2 copy and source archive remain separate from generated files.
if [[ ! -f config.status ]]; then
  ./configure --prefix="$V2_INSTALL" --without-readline CFLAGS=-O2 CPPFLAGS=-D_GNU_SOURCE
fi
# This fork's common objects include backend headers before the backend build.
make -C src/backend generated-headers
make -j4
make install
make -C contrib/pg_prewarm -j2
make -C contrib/pg_prewarm install
export LD_LIBRARY_PATH="$V2_INSTALL/lib:${LD_LIBRARY_PATH:-}"
"$V2_INSTALL/bin/postgres" --version
"$V2_INSTALL/bin/pg_config" --configure
sha256sum "$V2_INSTALL/bin/postgres" "$V2_INSTALL/lib/libpq.so.5" > "$V2_EVIDENCE/postgres.sha256"
printf 'OK\n' > "$V2_EVIDENCE/BUILD_STATUS"
