#!/usr/bin/env bash
# Final controlled A/B. usage: ab.sh <cfg> <W> <trial>
# cfg: base | off | ev | dd | nr | all   (all but base use the combined binary with env switches)
set -uo pipefail
CFG=$1 W=$2 T=$3 K=${K:-32}
C=$HOME/claude_checks; D=$C/detopt_20261007
case $CFG in
  base) V=base; SW="" ;;
  final) V=final; SW="" ;;
  settle) V=settle; SW="" ;;
  settleoff) V=settle; SW="BCDB_DT_POST_PUBLISH_SETTLE=0" ;;
  off)  V=combined; SW="BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0 BCDB_DT_EARLY_ROTATION=0" ;;
  ev)   V=combined; SW="BCDB_DT_EARLY_VALIDATE=1 BCDB_DT_TAG_DEDUP=0 BCDB_DT_EARLY_ROTATION=0" ;;
  dd)   V=combined; SW="BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=1 BCDB_DT_EARLY_ROTATION=0" ;;
  nr)   V=combined; SW="BCDB_DT_EARLY_VALIDATE=1 BCDB_DT_TAG_DEDUP=1 BCDB_DT_EARLY_ROTATION=0" ;;
  all)  V=combined; SW="BCDB_DT_EARLY_VALIDATE=1 BCDB_DT_TAG_DEDUP=1 BCDB_DT_EARLY_ROTATION=1" ;;
  *) echo bad cfg; exit 2 ;;
esac
export INST=$D/$V/install BINDIR=$D/$V/src/ariabc_pg/build/bin SRCDIR=$C/verify_20261006/src
export RUNROOT=$C/tpcc_sweep_v2_dx/$CFG WORKLOAD_ROOT=$C/tpcc_sweep_v2_verify_20261006/workloads
export FILLFACTOR=90 SPLIT=1024 MERGE=256 SYNC_BEFORE_MEASURE=1 PTRACE=off DET_WINDOW=65536
mkdir -p "$RUNROOT"
label=ab_k${K}_t$T
exec 8>"$D/bench.lock"; flock 8
echo "$(date -Is) RUN ab $CFG W=$W k=$K t=$T [$SW]" >> "$D/ab_status.txt"
iostat -x -t 1 > "$RUNROOT/iostat_${label}_w$W.txt" 2>&1 & IOP=$!
env $SW bash $D/harness/sweep_run.sh "$label" "$W" 20000 $K 16384 1 16 det > "$RUNROOT/det_${label}_w$W.log" 2>&1
rc=$?
kill $IOP 2>/dev/null; wait $IOP 2>/dev/null
run=$RUNROOT/det_${label}_w$W
res=$(grep -Eo "completed_tps=[0-9.]+|divergence_count=[0-9]+|permanent_failures=[0-9]+|dirty_kb_at_start=[0-9]+ sync_wait_s=[0-9]+" "$run/result.txt" 2>/dev/null | tr "\n" " ")
io=$(python3 $D/harness/run_io.py "$run/gateway.log" "$RUNROOT/iostat_${label}_w$W.txt" 2>/dev/null)
sh=$(sha256sum < "$run/state.hash" 2>/dev/null | cut -c1-16)
echo "$(date -Is) DONE ab $CFG W=$W k=$K t=$T rc=$rc $res $io state=$sh" >> "$D/ab_status.txt"
[[ $rc == 0 ]] && rm -rf "$run/pgdata"
exit $rc
