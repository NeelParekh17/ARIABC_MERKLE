#!/usr/bin/env bash
H=$HOME/claude_checks/detopt_20261007/harness
for t in 51 52 53 54; do for c in final settle settleoff; do $H/ab.sh $c 100 $t; done; done
echo AB_SETTLE2_DONE >> $HOME/claude_checks/detopt_20261007/ab_status.txt
