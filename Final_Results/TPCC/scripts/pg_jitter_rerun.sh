#!/usr/bin/env bash
# pg rerun after the retry-jitter fix (2026-09-29): same merkle_run.sh flags as the
# original sweeps, new labels (jit_*) so the original run directories are untouched.
C=$HOME/claude_checks
note() {  # $1 = run dir
  echo "serialization_failures=$(grep -c 'could not serialize' $1/postgres.log)" >> $1/result.txt
  echo "server_sha256=$(sha256sum $C/src/ariabc_pg/build/bin/ariabc_pg_server | cut -c1-16) retry_jitter=default_on" >> $1/result.txt
}
run() {  # label W workers mode
  echo "=== START $4 $1 W=$2 workers=$3 $(date +%T)"
  $C/merkle_run.sh $1 $2 20000 $3 200 0 1 $4 >/dev/null 2>&1
  d=$C/chart/${4}_${1}_w$2; note $d
  grep -E "completed_tps|failures|serialization_failures|restarts|gateway_rc" $d/result.txt | tr '\n' ' '; echo
  echo "=== END $4 $1 W=$2 workers=$3 $(date +%T)"
  rm -rf $d/pgdata
}
echo "PHASE det_verify $(date +%T)"
for T in 1 2 3; do run jitv_k32_t$T 100 32 det; run jitv_t$T 5 32 det; done
echo "PHASE pg_warehouses $(date +%T)"
for T in 1 2 3; do for W in 5 10 20 30 50 75 100; do run jit_t$T $W 32 pg; done; echo "TRIAL_DONE warehouses $T"; done
echo "PHASE pg_workers $(date +%T)"
for T in 1 2 3; do for WK in 8 16 24 32 48 64; do run jit_k${WK}_t$T 100 $WK pg; done; echo "TRIAL_DONE workers $T"; done
echo ALL_DONE
