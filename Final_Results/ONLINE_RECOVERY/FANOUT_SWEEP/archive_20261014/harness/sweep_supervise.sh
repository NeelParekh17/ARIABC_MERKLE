#!/usr/bin/env bash
# Reduced fanout sweep: matrix only (A + M_mixed, 4M rows), then report + plots.
set -uo pipefail
S=/home/neel/ariabc_data/fanout_sweep_20261014
export PATH=$S/tools:$PATH
cd $S
python3 $S/controller.py --base $S --phase matrix ${RESUME:-}; rc=$?
if [[ $rc == 0 ]]; then
  python3 $S/repo/scripts/distributed/recovery_fanout/analyze.py --base $S; rc=$?
fi
printf 'supervisor_exit_code=%s\nfinished_utc=%s\n' "$rc" "$(date -u +%FT%TZ)" > $S/results/supervisor_exit.env
[[ $rc == 0 ]] && python3 $S/repo/scripts/distributed/recovery_fanout/finish.py --base $S > $S/results/finish.log 2>&1
