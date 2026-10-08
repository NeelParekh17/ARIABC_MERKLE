#!/usr/bin/env bash
H=$HOME/claude_checks/detopt_20261007/harness
for t in 31 32; do for w in 5 30 100; do $H/ab.sh final $w $t; done; K=48 $H/ab.sh final 100 $t; done
echo AB_FINAL_DONE >> $HOME/claude_checks/detopt_20261007/ab_status.txt
