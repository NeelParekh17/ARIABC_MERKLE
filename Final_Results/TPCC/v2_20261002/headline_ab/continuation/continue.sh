#!/usr/bin/env bash
# Continuation of tpcc_v2_20261002_051210 after headline_C4_t2 failed only at pg_ctl stop (60 s default timeout < 60.8 s shutdown checkpoint).
# Runs C4 t2, C5 t2, and C1 t3 (C1 t2 hit ranking's slow-window stall: 888 vs 2573 TPS); skips the worker sweep.
set -Eeuo pipefail
C=$HOME/claude_checks; SRC=$C/src_v2; INST=$C/install_v2; ROOT=$C/tpcc_v2_20261002_051210
HERE=$SRC/scripts/distributed/tpcc_v2; CONT=$ROOT/continuation
exec 9>"$C/tpcc_v2.lock"; flock -n 9 || { echo 'lock busy' >&2; exit 2; }
status() { printf '%s %s\n' "$(date -Is)" "$*" > "$ROOT/status.txt.tmp"; mv "$ROOT/status.txt.tmp" "$ROOT/status.txt"; printf '%s %s\n' "$(date -Is)" "$*" >> "$ROOT/status_history.txt"; }
STAGE=continuation_preflight
trap 'rc=$?; status "FAILED stage=$STAGE exit=$rc line=$LINENO"; python3 "$HERE/summary.py" summarize "$ROOT" > "$ROOT/partial_summary.log" 2>&1 || true; exit $rc' ERR
exec 8>>"$ROOT/commands.log"; export BASH_XTRACEFD=8; PS4='+ $(date -Is) ${BASH_SOURCE}:${LINENO}: '; set -x
status 'RUNNING continuation preflight'
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
sha256sum -c "$ROOT/provenance/binaries.sha256" > "$CONT/binaries_check.txt"
BIN=$C/src/ariabc_pg/build/bin
export INST SRCDIR=$SRC BINDIR=$BIN RUNROOT=$ROOT WORKLOAD_ROOT=$ROOT/workloads
REFERENCE=$ROOT/det_headline_C1_t1_e32_w100/state.hash
test -f "$REFERENCE"
if [[ -d $ROOT/det_headline_C4_t2_e32_w100 ]]; then mv "$ROOT/det_headline_C4_t2_e32_w100" "$ROOT/det_headline_C4_t2_e32_w100.failed_stop_timeout"; mv "$ROOT/headline_C4_t2_e32.log" "$ROOT/headline_C4_t2_e32.failed_stop_timeout.log"; fi
run_case() {
  local config=$1 trial=$2 workers=$3 tx=$4 group=$5
  local mode=merkle split=1024 merge=256 ff= label run
  case $config in C1) mode=det ;; C2) split=32; merge=8 ;; C3) ;; C4) mode=det; ff=90 ;; C5) ff=90 ;; *) return 2 ;; esac
  label=${group}_${config}_t${trial}_e${workers}; run=$ROOT/${mode}_${label}_w100; STAGE=$label
  status "RUNNING $label tx=$tx mode=$mode split=$split merge=$merge fillfactor=${ff:-default} (continuation)"
  FILLFACTOR=$ff SPLIT=$split MERGE=$merge bash "$CONT/tpcc_v2_run.sh" "$label" 100 "$tx" "$workers" 16384 1 16 "$mode" > "$ROOT/${label}.log" 2>&1
  python3 "$HERE/summary.py" accept --run "$run" --src "$SRC" --config "$config" --trial "$trial" --group "$group" --reference "$REFERENCE" > "$run/acceptance.log" 2>&1
  sha256sum "$WORKLOAD_ROOT/tpcc-$tx-w100-seed42.txt" > "$run/workload.sha256"
  status "PASS $label"
  [[ -f $run/accepted.json && -f $run/pgdata/PG_VERSION && ! -e $run/pgdata/postmaster.pid ]]
  rm -rf -- "$run/pgdata"; printf '%s accepted stopped PGDATA removed\n' "$(date -Is)" > "$run/pgdata_removed.txt"
}
run_case C4 2 32 20000 headline
run_case C5 2 32 20000 headline
run_case C1 3 32 20000 headline
STAGE=summary; status 'RUNNING summary'
python3 "$HERE/summary.py" summarize "$ROOT"
status 'COMPLETE headline (2 trials C2-C5, 3 for C1; C4_t2 rerun after stop-timeout fix) summary.md ready'
