set -e
B='@RBASE@'
free -m
cat /proc/loadavg
ss -ltnp | awk '$4 ~ /:(5448|9018|8018|8019)$/ {print}'
if test -f "$B/pids/ariabc_server.pid"; then cat "$B/pids/ariabc_server.pid"; exit 2; fi
if test -f "$B/repo/.bench_tmp/recovery_pgdata/postmaster.pid"; then cat "$B/repo/.bench_tmp/recovery_pgdata/postmaster.pid"; exit 2; fi
