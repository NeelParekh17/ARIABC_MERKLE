#!/usr/bin/env bash
H=$HOME/claude_checks/detopt_20261007/harness
cfgs=(base ev dd nr all)
for t in 11 12 13 14; do
  # rotate config order each trial to spread drift
  rot=$(( (t-1) % ${#cfgs[@]} )); order=("${cfgs[@]:$rot}" "${cfgs[@]:0:$rot}")
  [[ $t == 11 ]] && order+=(off)
  for w in 5 30 100; do for c in "${order[@]}"; do $H/ab.sh $c $w $t; done; done
done
echo AB_QUEUE_DONE >> $HOME/claude_checks/detopt_20261007/ab_status.txt
