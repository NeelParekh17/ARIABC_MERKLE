#!/bin/bash
set -euo pipefail
root=$HOME/claude_checks/lsm_lookup_20261014
export TMPDIR=$root/scratch
until grep -q FF90_BASELINES_PASS_STOPPED "$root/baselines/status.txt"; do
 test ! -f "$root/baselines/FAILED.txt"
 sleep 60
done
python3 -u "$root/normalize_ff90.py"
