#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_ctl/lsm_lookup_20261014
export TMPDIR=$root/scratch MPLCONFIGDIR=$root/scratch/mplconfig PYTHONDONTWRITEBYTECODE=1
exec 9> "$root/results/audit_after_ff90.lock"
flock -n 9 || exit 1
printf '%s WAITING_FF90_ONLY
' "$(date -u +%FT%TZ)" >> "$root/results/audit_after_ff90.status"
until test -f "$root/results/stages/ff90.OK"; do sleep 60; done
python3 "$root/audit.py" --root "$root/results" --out "$root/results/final_audit.json" > "$root/results/final_audit.log" 2>&1
python3 -c 'import json,sys; a=json.load(open(sys.argv[1])); assert a["complete"] and a["accepted"]==75 and a["terminals"]==1500000' "$root/results/final_audit.json"
printf '%s FF90_AUDIT_PASS_75_CASES_1500000_TERMINALS
' "$(date -u +%FT%TZ)" >> "$root/results/audit_after_ff90.status"
