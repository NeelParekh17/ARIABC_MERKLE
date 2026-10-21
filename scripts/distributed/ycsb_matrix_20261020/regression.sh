#!/bin/bash
set -Eeuo pipefail
root=/home/neel/claude_checks/ycsb_matrix_20261020
export TMPDIR=$root/scratch LD_LIBRARY_PATH=$root/install/lib
export PGHOST=$root/socket PGPORT=5494 PGUSER=neel
trap 'echo FAIL > "$root/evidence/regression.status"; "$root/install/bin/pg_ctl" -D "$root/regression_data" -m fast -w stop || true' ERR
"$root/install/bin/initdb" -D "$root/regression_data" -A trust --no-locale > "$root/evidence/initdb.log" 2>&1
cat >> "$root/regression_data/postgresql.conf" <<EOF
port=5494
unix_socket_directories='$root/socket'
shared_buffers='32MB'
maintenance_work_mem='128MB'
shared_preload_libraries='merkle_lsm'
max_worker_processes=32
default_transaction_isolation='serializable'
EOF
"$root/install/bin/pg_ctl" -D "$root/regression_data" -l "$root/evidence/regression_postgres.log" -w start
make -C "$root/src/contrib/merkle_lsm" installcheck > "$root/evidence/installcheck.log" 2>&1
"$root/install/bin/pg_ctl" -D "$root/regression_data" -m fast -w stop
echo OK > "$root/evidence/regression.status"
