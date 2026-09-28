#!/usr/bin/env bash
# Chart sweep on ranking: pg, det, merkle (current), merkle (warehouse routing).
C=$HOME/claude_checks
for T in 1 2 3; do
  for W in 5 10 20 30 50 75 100; do
    for cfg in "pg cur 200 0 1" "det cur 200 0 1" "merkle cur 200 0 1" "merkle wh16 16384 1 16"; do
      set -- $cfg; MODE=$1; LAYOUT=$2
      label=${LAYOUT}_t$T
      echo "=== START $MODE $LAYOUT W=$W T=$T $(date +%T)"
      $C/merkle_run.sh $label $W 20000 32 $3 $4 $5 $MODE 2>&1 | grep -v Killed | grep -E "completed_tps|failures|merkle_verify|restarts|gateway_rc|rror" | tr '\n' ' '
      echo
      echo "=== END $MODE $LAYOUT W=$W T=$T $(date +%T)"
      rm -rf $C/chart/${MODE}_${label}_w$W/pgdata
    done
    d=$C/chart/det_cur_t${T}_w$W/state.hash
    for m in merkle_cur merkle_wh16; do
      cmp -s $d $C/chart/${m}_t${T}_w$W/state.hash && echo "STATE_MATCH det=$m W=$W T=$T yes" || echo "STATE_MATCH det=$m W=$W T=$T NO"
    done
  done
  echo "TRIAL_DONE $T"
done
echo ALL_DONE
