#!/usr/bin/env bash
# Extra trials (t4-t6) for worker-sweep points where 2 of 3 trials hit ranking's stall.
C=$HOME/claude_checks
for T in 4 5 6; do for WK in 8 48; do
  L=jit_k${WK}_t$T; d=$C/chart/pg_${L}_w100
  echo "=== START pg $L $(date +%T)"
  $C/merkle_run.sh $L 100 20000 $WK 200 0 1 pg >/dev/null 2>&1
  echo "serialization_failures=$(grep -c 'could not serialize' $d/postgres.log)" >> $d/result.txt
  echo "server_sha256=$(sha256sum $C/src/ariabc_pg/build/bin/ariabc_pg_server | cut -c1-16) retry_jitter=default_on" >> $d/result.txt
  grep -E "completed_tps|failures|serialization_failures|gateway_rc" $d/result.txt | tr '\n' ' '; echo
  rm -rf $d/pgdata
done; done
echo ALL_DONE
