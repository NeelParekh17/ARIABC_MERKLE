#!/usr/bin/env bash
H=$HOME/claude_checks/detopt_20261007/harness
for t in 41 42 43; do for w in 5 30 100; do for c in settle final; do $H/ab.sh $c $w $t; done; done; done
for t in 41 42; do for c in settle final; do K=48 $H/ab.sh $c 100 $t; done; done
echo AB_SETTLE_DONE >> $HOME/claude_checks/detopt_20261007/ab_status.txt
