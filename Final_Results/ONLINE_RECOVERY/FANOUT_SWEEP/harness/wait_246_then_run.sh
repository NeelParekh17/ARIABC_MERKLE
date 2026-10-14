#!/usr/bin/env bash
# Start the fanout 2/4/8 extension once .246 has >= 9.5 GB available for 3 consecutive minutes.
set -uo pipefail
S=/home/neel/ariabc_data/fanout_sweep_20261014
export PATH=$S/tools:$PATH
LOG=$S/results/wait_246.log
need_kb=9961472; ok=0
while (( ok < 3 )); do
  avail=$(timeout 60 ssh -o ConnectTimeout=5 neel@10.129.148.246 "awk '/MemAvailable/ {print \$2}' /proc/meminfo" 2>/dev/null || echo 0)
  if (( ${avail:-0} >= need_kb )); then ok=$((ok+1)); else ok=0; fi
  echo "$(date -u +%FT%TZ) avail_kb=${avail:-?} consecutive_ok=$ok" >> $LOG
  (( ok < 3 )) && sleep 60
done
echo "$(date -u +%FT%TZ) START sweep (fanout 2/4/8 extension)" >> $LOG
rm -f $S/results/supervisor_exit.env $S/results/artifact_status.json
env RESUME=--resume bash $S/sweep_supervise.sh
