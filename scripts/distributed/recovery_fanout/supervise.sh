#!/usr/bin/env bash
set -euo pipefail
CAMPAIGN=${1:?usage: supervise.sh /home/neel/ariabc_data/fanout_TAG}
RESUME=${2:-}
[[ -z "$RESUME" || "$RESUME" == --resume ]]
[[ "$CAMPAIGN" =~ ^/home/neel/ariabc_data/fanout_[a-zA-Z0-9_]+$ ]]
[[ " $(hostname -I) " == *' 10.129.148.247 '* ]]
test -f "$CAMPAIGN/DEPLOY_READY"
test -f "$CAMPAIGN/provenance/collector_contract_checks.json"
export PATH="$CAMPAIGN/tools:$PATH"
LAUNCH_UTC=$(date -u +%Y%m%dT%H%M%SZ)
trap 'rc=$?; printf "supervisor_exit_code=%s\nfinished_utc=%s\n" "$rc" "$(date -u +%FT%TZ)" > "$CAMPAIGN/results/supervisor_exit_$LAUNCH_UTC.env"; cp "$CAMPAIGN/results/supervisor_exit_$LAUNCH_UTC.env" "$CAMPAIGN/results/supervisor_exit.env"' EXIT
resume_args=()
[[ -z "$RESUME" ]] || resume_args=(--resume)
python3 "$CAMPAIGN/controller.py" --base "$CAMPAIGN" --phase smoke "${resume_args[@]}"
python3 "$CAMPAIGN/controller.py" --base "$CAMPAIGN" --phase matrix "${resume_args[@]}"
python3 "$CAMPAIGN/repo/scripts/distributed/recovery_fanout/analyze.py" --base "$CAMPAIGN"
python3 "$CAMPAIGN/repo/scripts/distributed/recovery_fanout/curate.py" --base "$CAMPAIGN"
