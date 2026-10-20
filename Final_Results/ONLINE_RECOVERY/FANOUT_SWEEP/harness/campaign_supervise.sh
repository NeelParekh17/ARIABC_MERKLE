#!/usr/bin/env bash
# 2026-10-15 campaign: smoke (f1024, f512 M_mixed online repair) -> 60-run matrix -> analyze -> report + plots.
# Resume after a reviewed stop:  env RESUME=--resume bash campaign_supervise.sh
set -uo pipefail
S=/home/neel/ariabc_data/fanout_sweep_20261014
export PATH=$S/tools:$PATH
cd $S
rc=0
if ! grep -q "\"state\": \"DONE\"" results/smoke_status.json 2>/dev/null; then
  python3 $S/controller.py --base $S --phase smoke ${RESUME:-}; rc=$?
  cp results/status.json results/smoke_status.json
fi
if [[ $rc == 0 ]]; then
  r2=${RESUME:-}; [[ -f results/matrix_attempts.jsonl ]] && r2=--resume
  python3 $S/controller.py --base $S --phase matrix $r2; rc=$?
fi
if [[ $rc == 0 ]]; then
  python3 $S/repo/scripts/distributed/recovery_fanout/analyze.py --base $S; rc=$?
fi
printf "supervisor_exit_code=%s\nfinished_utc=%s\n" "$rc" "$(date -u +%FT%TZ)" > $S/results/supervisor_exit.env
[[ $rc == 0 ]] && python3 $S/repo/scripts/distributed/recovery_fanout/finish.py --base $S > $S/results/finish.log 2>&1
