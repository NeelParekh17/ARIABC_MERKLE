#!/usr/bin/env bash
set -euo pipefail

for diag_ip in 10.129.148.247 10.129.148.246 10.129.148.248; do
  printf 'NODE %s\n' "$diag_ip"
  ssh -o BatchMode=yes -o ConnectTimeout=8 "neel@$diag_ip" 'bash -s' <<'REMOTE'
set -euo pipefail
export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib
pgdata=/home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata
pg_ctl=/home/neel/Desktop/ariabc_install/bin/pg_ctl
psql=/home/neel/Desktop/ariabc_install/bin/psql
if "$pg_ctl" -D "$pgdata" status >/dev/null 2>&1; then
  echo unexpected_postgres_running
  exit 9
fi
"$pg_ctl" -D "$pgdata" -l /tmp/ycsb_cluster_diag.log -w -t 120 start >/dev/null
trap '"$pg_ctl" -D "$pgdata" -m fast -w -t 30 stop >/dev/null 2>&1 || true' EXIT
"$psql" -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5438 -U postgres -d postgres -At -c "SELECT count(*), min(YCSB_KEY), max(YCSB_KEY), merkle_root_hash('usertable_small'), merkle_verify('usertable_small') FROM usertable_small"
REMOTE
done
