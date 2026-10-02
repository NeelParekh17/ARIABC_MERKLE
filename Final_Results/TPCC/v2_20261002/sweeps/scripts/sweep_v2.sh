#!/usr/bin/env bash
# Ranking only. Launch this wrapper with nohup inside tmux.
set -Eeuo pipefail
ROOT=${1:?usage: sweep_v2.sh ROOT smoke|matrix|pass2|extra}
ACTION=${2:?}
C=$HOME/claude_checks
[[ $ROOT == "$C"/tpcc_sweep_v2_* && -d $ROOT/provenance ]] || exit 2
[[ $ACTION == smoke || $ACTION == matrix || $ACTION == pass2 || $ACTION == extra ]] || exit 2
exec 9>"$C/tpcc_v2.lock"
flock -n 9 || { echo 'Campaign lock occupied'; exit 2; }
exec 8>>"$ROOT/commands.log"
BASH_XTRACEFD=8
PS4='+ ${EPOCHREALTIME} ${BASH_SOURCE}:${LINENO}: '
set -x
failed() {
  local rc=$?
  trap - ERR
  printf '%s HARD_FAILURE wrapper action=%s exit=%s line=%s\n' "$(date -Is)" "$ACTION" "$rc" "$1" > "$ROOT/status.txt"
  cat "$ROOT/status.txt" >> "$ROOT/status_history.txt"
  exit "$rc"
}
trap 'failed "$LINENO"' ERR
export INST=$C/install_v2 SRCDIR=$C/src_v2 BINDIR=$C/src/ariabc_pg/build/bin
export RUNROOT=$ROOT WORKLOAD_ROOT=$ROOT/workloads FILLFACTOR=90 SPLIT=1024 MERGE=256
export SYNC_BEFORE_MEASURE=0
sha256sum -c "$ROOT/provenance/binaries.sha256" > "$ROOT/provenance/binaries_${ACTION}_check.txt"
sha256sum -c "$ROOT/provenance/inputs.sha256" > "$ROOT/provenance/inputs_${ACTION}_check.txt"
# The published pg jitter build is also recorded in the headline manifest.
[[ $(sha256sum "$BINDIR/ariabc_pg_server" | cut -d' ' -f1) == cc02e6df017f5d1dbbb181ac39ecf2b8a29b971a4c53c72d17cdf52695ca2a17 ]]
[[ $(df --output=avail -k "$ROOT" | tail -1) -gt 80000000 ]]
python3 - <<'PY'
import socket
for port in (55439,18100,19100):
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        sock.bind(('127.0.0.1',port))
PY
HERE=$(cd "$(dirname "$0")" && pwd)
bash -n "$HERE/sweep_v2.sh" "$HERE/sweep_run.sh"
PYTHONPYCACHEPREFIX=$ROOT/pycache python3 -m py_compile "$HERE/sweep_summary.py" "$HERE/summary.py"
printf '%s\n' "$$" > "$ROOT/${ACTION}.pid"
python3 "$HERE/sweep_summary.py" "$ACTION" "$ROOT"
