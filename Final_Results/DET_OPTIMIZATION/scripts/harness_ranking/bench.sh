#!/usr/bin/env bash
# usage: bench.sh <variant> <W> <workers> <trial> [ptrace=on|off]
# Runs one v2 TPC-C det attempt with the build in ~/claude_checks/detopt_20261007/<variant>.
# Serialised by a blocking flock so only one DB run uses the host at a time.
# Env: PORT/CLIENT_PORT/RAFT_PORT (defaults 55439/18100/19100), DET_WINDOW (default 65536), MODE (default det).
set -uo pipefail
V=$1 W=$2 K=$3 T=$4 PT=${5:-off}
C=$HOME/claude_checks; D=$C/detopt_20261007
MODE=${MODE:-det}
export INST=$D/$V/install BINDIR=$D/$V/src/ariabc_pg/build/bin SRCDIR=$C/verify_20261006/src
export RUNROOT=$C/tpcc_sweep_v2_detopt_20261007/$V WORKLOAD_ROOT=$C/tpcc_sweep_v2_verify_20261006/workloads
export FILLFACTOR=90 SPLIT=1024 MERGE=256 SYNC_BEFORE_MEASURE=${SYNC_BEFORE_MEASURE:-0} PTRACE=$PT DET_WINDOW=${DET_WINDOW:-65536}
[[ -x $INST/bin/postgres && -x $BINDIR/ariabc_pg_server ]] || { echo "no build for $V"; exit 2; }
mkdir -p "$RUNROOT"
label=p${PT}_k${K}_t${T}
exec 8>"$D/bench.lock"; flock 8
echo "$(date -Is) RUN $V $MODE W=$W k=$K t=$T ptrace=$PT window=$DET_WINDOW" >> "$D/status.txt"
bash $D/harness/sweep_run.sh "$label" "$W" 20000 "$K" 16384 1 16 "$MODE" > "$RUNROOT/${MODE}_${label}_w$W.log" 2>&1
rc=$?
run=$RUNROOT/${MODE}_${label}_w$W
res=$(grep -Eo "completed_tps=[0-9.]+|divergence_count=[0-9]+|permanent_failures=[0-9]+|total_restarts=[0-9]+" "$run/result.txt" 2>/dev/null | tr "\n" " ")
sh=$(sha256sum < "$run/state.hash" 2>/dev/null | cut -c1-16)
echo "$(date -Is) DONE $V $MODE W=$W k=$K t=$T ptrace=$PT rc=$rc $res state=$sh" >> "$D/status.txt"
[[ $rc == 0 ]] && rm -rf "$run/pgdata"
echo "$V W=$W k=$K t=$T ptrace=$PT rc=$rc $res state=$sh"
exit $rc
