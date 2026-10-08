#!/usr/bin/env bash
H=$HOME/claude_checks/detopt_20261007/harness
for t in 21 22 23; do for k in 64 48; do for c in base nr; do K=$k $H/ab.sh $c 100 $t; done; done; done
echo AB_K_DONE >> $HOME/claude_checks/detopt_20261007/ab_status.txt
