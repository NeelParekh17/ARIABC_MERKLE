#!/usr/bin/env bash
# Run on ranking in tmux/nohup. Never rerun into an existing output root.
set -Eeuo pipefail
C=$HOME/claude_checks
SRC=$C/src_v2
INST=$C/install_v2
ROOT=${1:?usage: master.sh ~/claude_checks/tpcc_v2_DATE}
[[ $ROOT == "$C"/tpcc_v2_* && -d $ROOT/provenance && ! -e $ROOT/status.txt ]] || exit 2
exec 9>"$C/tpcc_v2.lock"
flock -n 9 || { echo 'Another v2 campaign owns the lock' >&2; exit 2; }
HERE=$SRC/scripts/distributed/tpcc_v2
STAGE=preflight
status() {
  printf '%s %s\n' "$(date -Is)" "$*" > "$ROOT/status.txt.tmp"
  mv "$ROOT/status.txt.tmp" "$ROOT/status.txt"
  printf '%s %s\n' "$(date -Is)" "$*" >> "$ROOT/status_history.txt"
}
failed() {
  rc=$?
  trap - ERR
  status "FAILED stage=$STAGE exit=$rc line=$1"
  python3 "$HERE/summary.py" summarize "$ROOT" > "$ROOT/partial_summary.log" 2>&1 || true
  exit "$rc"
}
trap 'failed "$LINENO"' ERR
# Record every master command for unattended auditing.
exec 8>>"$ROOT/commands.log"
export BASH_XTRACEFD=8
PS4='+ $(date -Is) ${BASH_SOURCE}:${LINENO}: '
set -x
status 'RUNNING preflight'
python3 - <<'PY'
import socket
for port in (55439,18100,19100):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1',port))
PY
[[ $(df --output=avail -k "$C" | tail -1) -gt 80000000 ]]
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
python3 "$SRC/scripts/distributed/source_fingerprint.py" --repo "$SRC" --ring-capacity 2048 --list > "$ROOT/provenance/remote_source_before.txt"
cmp "$ROOT/provenance/local_source.txt" "$ROOT/provenance/remote_source_before.txt"
STAGE=build
status 'RUNNING build (PostgreSQL -j64)'
cd "$SRC"
make distclean > "$ROOT/distclean.log" 2>&1
./configure --prefix="$INST" --without-readline CFLAGS=-O2 CPPFLAGS=-I/home/protectdr/Desktop/compat_include LDFLAGS=-L/home/protectdr/Desktop/compat_lib > "$ROOT/configure.log" 2>&1
make -j64 > "$ROOT/build.log" 2>&1
make install > "$ROOT/install.log" 2>&1
make -C contrib/pg_prewarm -j64 > "$ROOT/prewarm_build.log" 2>&1
make -C contrib/pg_prewarm install > "$ROOT/prewarm_install.log" 2>&1
cp config.log "$ROOT/provenance/config.log"
python3 "$SRC/scripts/distributed/source_fingerprint.py" --repo "$SRC" --ring-capacity 2048 --list > "$ROOT/provenance/remote_source_after.txt"
cmp "$ROOT/provenance/local_source.txt" "$ROOT/provenance/remote_source_after.txt"
if [[ -s $ROOT/provenance/ariabc_pg.diff ]]; then
  STAGE=build_cpp
  status 'RUNNING build_cpp'
  cmake -S "$SRC/ariabc_pg" -B "$SRC/ariabc_pg/build_v2" -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$INST" > "$ROOT/cmake.log" 2>&1
  cmake --build "$SRC/ariabc_pg/build_v2" --target ariabc_pg_server ariabc_pg_gateway -j64 > "$ROOT/cpp_build.log" 2>&1
  BIN=$SRC/ariabc_pg/build_v2/bin
else
  BIN=$C/src/ariabc_pg/build/bin
fi
sha256sum "$INST/bin/postgres" "$INST/bin/psql" "$INST/bin/pg_waldump" "$("$INST/bin/pg_config" --pkglibdir)/pg_prewarm.so" "$BIN/ariabc_pg_server" "$BIN/ariabc_pg_gateway" > "$ROOT/provenance/binaries.sha256"
export INST SRCDIR=$SRC BINDIR=$BIN RUNROOT=$ROOT WORKLOAD_ROOT=$ROOT/workloads
mkdir "$ROOT/workloads"
# Copy existing published workload as read-only input, never write C/runs.
if [[ -f $C/runs/tpcc-20000-w100-seed42.txt ]]; then
  cp "$C/runs/tpcc-20000-w100-seed42.txt" "$ROOT/workloads/"
fi
REFERENCE=
run_case() {
  local config=$1 trial=$2 workers=$3 tx=$4 group=$5
  local mode=merkle split=1024 merge=256 ff= label run
  case $config in
    C1) mode=det ;;
    C2) split=32; merge=8 ;;
    C3) ;;
    C4) mode=det; ff=90 ;;
    C5) ff=90 ;;
    *) return 2 ;;
  esac
  label=${group}_${config}_t${trial}_e${workers}
  run=$ROOT/${mode}_${label}_w100
  STAGE=$label
  status "RUNNING $label tx=$tx mode=$mode split=$split merge=$merge fillfactor=${ff:-default}"
  FILLFACTOR=$ff SPLIT=$split MERGE=$merge bash "$HERE/tpcc_v2_run.sh" "$label" 100 "$tx" "$workers" 16384 1 16 "$mode" > "$ROOT/${label}.log" 2>&1
  local compare=()
  if [[ $group != smoke && -n $REFERENCE ]]; then compare=(--reference "$REFERENCE"); fi
  python3 "$HERE/summary.py" accept --run "$run" --src "$SRC" --config "$config" --trial "$trial" --group "$group" "${compare[@]}" > "$run/acceptance.log" 2>&1
  if [[ $group != smoke && -z $REFERENCE ]]; then REFERENCE=$run/state.hash; fi
  sha256sum "$WORKLOAD_ROOT/tpcc-$tx-w100-seed42.txt" > "$run/workload.sha256"
  status "PASS $label"
  # All retained evidence is outside pgdata. Leave failed PGDATA for diagnosis.
  [[ -f $run/accepted.json && -f $run/pgdata/PG_VERSION && ! -e $run/pgdata/postmaster.pid ]]
  rm -rf -- "$run/pgdata"
  printf '%s accepted stopped PGDATA removed\n' "$(date -Is)" > "$run/pgdata_removed.txt"
}
run_case C5 0 32 2000 smoke
cp "$ROOT/merkle_smoke_C5_t0_e32_w100/accepted.json" "$ROOT/smoke.json"
status 'PASS smoke; RUNNING headline matrix'
for trial in 1 2 3; do
  for config in C1 C2 C3 C4 C5; do run_case "$config" "$trial" 32 20000 headline; done
done
status 'PASS headline; RUNNING appended worker sweep'
# Small sweep appended to the same detached job after the complete headline.
for trial in 1 2; do
  for workers in 16 64; do
    for config in C1 C3 C5; do run_case "$config" "$trial" "$workers" 20000 sweep; done
  done
done
STAGE=summary
status 'RUNNING summary'
python3 "$HERE/summary.py" summarize "$ROOT"
status 'COMPLETE headline=15 sweep=12 smoke=1 summary.md ready'
