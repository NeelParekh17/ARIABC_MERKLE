#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_ctl/lsm_lookup_20261014
export TMPDIR=$root/scratch MPLCONFIGDIR=$root/scratch/mplconfig PYTHONDONTWRITEBYTECODE=1
exec 9> "$root/results/analyze_after_ff90.lock"
flock -n 9 || exit 1
printf '%s WAITING_FF90_AUDIT\n' "$(date -u +%FT%TZ)" >> "$root/results/analyze_after_ff90.status"
until test -f "$root/results/audit_after_ff90.status" && grep -q FF90_AUDIT_PASS_75_CASES_1500000_TERMINALS "$root/results/audit_after_ff90.status"; do sleep 60; done
python3 "$root/analyze.py" --root "$root/results" --out "$root/analysis" > "$root/results/final_analysis.log" 2>&1
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["complete"]' "$root/analysis/analysis_status.json"
printf '%s FF90_ANALYSIS_COMPLETE\n' "$(date -u +%FT%TZ)" >> "$root/results/analyze_after_ff90.status"
