#!/usr/bin/env bash
# Worker-scaling sweep on ranking at W=100: pg, det, merkle (current), merkle (warehouse routing).
C=$HOME/claude_checks
W=100
for T in 1 2 3; do
  for WK in 8 16 24 32 48 64; do
    for cfg in "pg cur 200 0 1" "det cur 200 0 1" "merkle cur 200 0 1" "merkle wh16 16384 1 16"; do
      set -- $cfg; MODE=$1; LAYOUT=$2
      label=${LAYOUT}_k${WK}_t$T
      echo "=== START $MODE $LAYOUT workers=$WK T=$T $(date +%T)"
      $C/merkle_run.sh $label $W 20000 $WK $3 $4 $5 $MODE >/dev/null 2>&1
      grep -E "completed_tps|failures|merkle_verify|restarts" $C/chart/${MODE}_${label}_w$W/result.txt | tr '\n' ' '; echo
      echo "=== END $MODE $LAYOUT workers=$WK T=$T $(date +%T)"
      rm -rf $C/chart/${MODE}_${label}_w$W/pgdata
    done
    d=$C/chart/det_cur_k${WK}_t${T}_w$W/state.hash
    for m in merkle_cur merkle_wh16; do
      cmp -s $d $C/chart/${m}_k${WK}_t${T}_w$W/state.hash && echo "STATE_MATCH det=$m workers=$WK T=$T yes" || echo "STATE_MATCH det=$m workers=$WK T=$T NO"
    done
  done
  echo "TRIAL_DONE $T"
done
echo ALL_DONE
