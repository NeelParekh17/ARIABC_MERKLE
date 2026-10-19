#!/bin/bash
set -euo pipefail
root=$HOME/claude_checks/lsm_lookup_20261014
data=$HOME/ariabc_data/lsm_lookup_20261014/small
export TMPDIR=$root/scratch
export LD_LIBRARY_PATH=$root/install/lib:$HOME/Desktop/rdkafka_local/lib
mkdir -p "$root/small"
test ! -e "$data"
"$root/install/bin/initdb" -D "$data" -U postgres --no-locale -E UTF8 > "$root/small/initdb.log"
cat >> "$data/postgresql.conf" <<CONF
port=5488
listen_addresses='127.0.0.1'
unix_socket_directories='$root/scratch'
shared_buffers='32MB'
maintenance_work_mem='128MB'
shared_preload_libraries='lsm3'
lsm3.top_index_size=1024
max_worker_processes=32
default_transaction_isolation='serializable'
enable_merkle_index=on
merkle_apply_synchronous_direct=on
bcdb_worker_count=8
autovacuum=off
CONF
free -g > "$root/small/free_before_start.log"
test "$(df -B1 --output=avail "$data" | tail -1)" -ge 42949672960
"$root/install/bin/pg_ctl" -D "$data" -l "$root/small/postgres.log" -w start
"$root/install/bin/psql" -X -h 127.0.0.1 -p 5488 -U postgres -d postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION lsm3;'
