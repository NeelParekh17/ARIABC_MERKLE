#!/usr/bin/env bash
C=$HOME/claude_checks
( while true; do echo "$(date +%T) $(awk '/^(Dirty|Writeback):/{printf "%s=%s ", $1, $2}' /proc/meminfo)"; sleep 1; done ) > $C/rerun_k48_meminfo.log &
SAMPLER=$!
for T in s1 s2 s3; do
  echo "=== START merkle wh16 workers=48 $T $(date +%T)"
  SYNC_BEFORE_MEASURE=1 $C/merkle_run.sh wh16_k48_$T 100 20000 48 16384 1 16 merkle >/dev/null 2>&1
  R=$C/chart/merkle_wh16_k48_${T}_w100
  cat $R/result.txt | tr '\n' ' '; echo
  grep -oE "elapsed_s=[0-9.]+ total=[0-9]+ sent=[0-9]+ accepted=[0-9]+ completed=[0-9]+" $R/gateway.log | awk '{split($1,a,"=");split($5,b,"=");printf "%s:%s ",a[2],b[2]}'; echo
  rm -rf $R/pgdata
done
kill $SAMPLER
echo ALL_DONE
