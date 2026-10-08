#!/usr/bin/env bash
# Noise diagnosis: base W<W> repeated, alternating pinned/unpinned, with iostat/vmstat/mpstat capture.
set -uo pipefail
W=${W:-5}; N=${N:-6}
C=$HOME/claude_checks; D=$C/detopt_20261007
export INST=$D/base/install BINDIR=$D/base/src/ariabc_pg/build/bin SRCDIR=$C/verify_20261006/src
export WORKLOAD_ROOT=$C/tpcc_sweep_v2_verify_20261006/workloads
export FILLFACTOR=90 SPLIT=1024 MERGE=256 SYNC_BEFORE_MEASURE=1 PTRACE=off DET_WINDOW=65536
for i in $(seq 1 $N); do
  if (( i % 2 )); then P="taskset -c 32-95"; tag=pin; else P=""; tag=nopin; fi
  export PIN="$P" RUNROOT=$C/tpcc_sweep_v2_dx/diag_$tag
  mkdir -p $RUNROOT; label=d_t$i; run=$RUNROOT/det_${label}_w$W
  exec 8>"$D/bench.lock"; flock 8
  ( iostat -x -t 1 > $RUNROOT/iostat_$i.txt 2>&1 & echo $! > $RUNROOT/iostat_$i.pid; vmstat -t 1 > $RUNROOT/vmstat_$i.txt 2>&1 & echo $! > $RUNROOT/vmstat_$i.pid; mpstat -P ALL 5 > $RUNROOT/mpstat_$i.txt 2>&1 & echo $! > $RUNROOT/mpstat_$i.pid )
  bash $D/harness/sweep_run.sh "$label" "$W" 20000 32 16384 1 16 det > $RUNROOT/det_${label}_w$W.log 2>&1; rc=$?
  kill $(cat $RUNROOT/iostat_$i.pid $RUNROOT/vmstat_$i.pid $RUNROOT/mpstat_$i.pid) 2>/dev/null
  exec 8>&-
  echo "$(date -Is) DIAG W=$W i=$i $tag rc=$rc $(grep -Eo "completed_tps=[0-9.]+" $run/result.txt) state=$(sha256sum < $run/state.hash | cut -c1-16)" >> $D/diag_status.txt
  [[ $rc == 0 ]] && rm -rf $run/pgdata
done
echo DIAG_DONE >> $D/diag_status.txt
