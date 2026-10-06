#!/usr/bin/env bash
# Point syntax W:workers:N; modes and trial count are fixed for the whole campaign.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
[[ $(id -un) == protectdr && " $(hostname -I) " == *' 10.129.7.57 '* ]] || exit 2
RUNROOT=${RUNROOT:?set fresh ~/claude_checks/v3/B_* root}
[[ $RUNROOT == "$HOME"/claude_checks/v3/B_* && ! -e $RUNROOT ]] || { echo 'Refusing existing/non-B campaign root' >&2; exit 2; }
TRIALS=${TRIALS:-3} MODES=${MODES:-'pg det merkle'}
[[ $TRIALS =~ ^[1-9][0-9]*$ && $# -gt 0 ]] || exit 2
read -r -a MODE_LIST <<< "$MODES"
for mode in "${MODE_LIST[@]}"; do case $mode in pg|det|merkle) ;; *) exit 2;; esac; done
mkdir -p "$HOME/claude_checks/v3"
exec 9>"$HOME/claude_checks/v3/B_campaign.lock"
flock -n 9 || { echo 'Another B campaign owns the lock' >&2; exit 2; }
mkdir "$RUNROOT"
export RUNROOT CAMPAIGN_LOCK_HELD=1
python3 - "$RUNROOT" "$TRIALS" "$MODES" "$@" <<'PY'
import json, sys
from pathlib import Path
points=[list(map(int,p.split(':'))) for p in sys.argv[4:]]
if any(len(p)!=3 or min(p)<=0 for p in points): raise SystemExit('Points must be W:workers:N, positive')
modes=sys.argv[3].split()
if len(set(modes)) != len(modes) or len(set(map(tuple,points))) != len(points): raise SystemExit('Duplicate mode/point')
Path(sys.argv[1],'campaign.json').write_text(json.dumps(dict(trials=int(sys.argv[2]),modes=modes,points=points),indent=2)+'\n')
PY
python3 "$HERE/provenance.py" "$RUNROOT"
FAILED=0
# Trial-outer ordering; no replacement attempts and no best-of selection.
for ((trial=1; trial<=TRIALS; trial++)); do
  for point in "$@"; do
    IFS=: read -r warehouses workers count <<< "$point"
    for mode in "${MODE_LIST[@]}"; do
      "$HERE/run.sh" "$mode" "$warehouses" "$count" "$workers" "$trial" || FAILED=1
      python3 "$HERE/summary.py" "$RUNROOT"
    done
  done
done
python3 "$HERE/summary.py" "$RUNROOT"
# Final acceptance also includes matched det/Merkle state hashes and complete trial count.
if [[ ${SMOKE:-0} != 1 ]]; then
  python3 - "$RUNROOT/campaign_acceptance.json" <<'PY' || FAILED=1
import json,sys
sys.exit(0 if json.load(open(sys.argv[1]))['accepted'] else 1)
PY
fi
exit "$FAILED"
