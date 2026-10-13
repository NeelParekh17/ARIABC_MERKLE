#!/usr/bin/env bash
set -u
B=/home/neel/ariabc_data/paper_20261010_recov
printf 'time=%s host=%s\n' "$(date -u +%FT%TZ)" "$(hostname)"
free -m
cat /proc/loadavg
cat /proc/sys/kernel/random/boot_id
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
awk '/pswpin|pswpout|pgmajfault/ {print}' /proc/vmstat
ps -eo user,pid,comm,rss --sort=-rss | head -15
if [[ -x "$B/install/bin/psql" ]]; then
 LD_LIBRARY_PATH="$B/install/lib" timeout 3 "$B/install/bin/psql" -X -q -h 127.0.0.1 -p 5448 -U postgres postgres -At \
  -c 'SELECT bcdb_last_committed_txid();' 2>/dev/null || true
fi
