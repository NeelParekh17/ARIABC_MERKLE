#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/Desktop/recovery_s1024_20261001T060514ZJ
F="$B/pids/ariabc_server.pid"
[[ -f "$F" ]] || exit 0
PID=$(cat "$F")
[[ $PID =~ ^[0-9]+$ ]] || exit 2
if ! kill -0 "$PID" 2>/dev/null; then rm "$F"; exit 0; fi
EXE=$(readlink "/proc/$PID/exe")
case "$EXE" in "$B/repo/ariabc_pg/build/bin/ariabc_pg_server"|"$B/u22_build/bin/ariabc_pg_server") ;; *) echo "Refuse PID $PID: $EXE" >&2; exit 2;; esac
kill -TERM "$PID"
for i in {1..100}; do
  if ! kill -0 "$PID" 2>/dev/null || [[ $(awk '{print $3}' "/proc/$PID/stat" 2>/dev/null) == Z ]]; then rm "$F"; exit 0; fi
  sleep 0.1
done
echo "Own server $PID did not stop" >&2
exit 1
