#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/ariabc_data/paper_20261010_recov
export TMPDIR="$B/scratch" LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
mkdir -p "$B/provenance" "$B/pids" "$B/logs" "$B/scratch"
for bin in "$B/install/bin/postgres" "$B/repo/ariabc_pg/build/bin/ariabc_pg_server" "$B/repo/ariabc_pg/build/bin/ariabc_pg_gateway"; do
 ldd "$bin"
 if ldd "$bin" | grep -q 'not found'; then exit 2; fi
 sha256sum "$bin"
done
"$B/install/bin/postgres" --version
"$B/install/bin/pg_config" --configure
git -C "$B/repo" rev-parse HEAD
git -C "$B/repo" status --porcelain
python3 "$B/repo/scripts/distributed/source_fingerprint.py" --repo "$B/repo" --ring-capacity 2048
sha256sum -c "$B/provenance/BINARIES.sha256"
DATA="$B/repo/.bench_tmp/recovery_pgdata"
test ! -e "$DATA"
mkdir -p "$B/repo/.bench_tmp"
"$B/install/bin/initdb" -D "$DATA" -U postgres -A trust
cat >> "$DATA/postgresql.conf" <<'CONF'
port = 5448
listen_addresses = '*'
max_connections = 256
shared_buffers = '32MB'
default_transaction_isolation = 'serializable'
bcdb_worker_count = 8
merkle_apply_synchronous_direct = on
synchronous_commit = on
fsync = on
full_page_writes = on
CONF
printf "unix_socket_directories = '%s'\n" "$B" >> "$DATA/postgresql.conf"
printf 'host all all 10.129.0.0/16 trust\n' >> "$DATA/pg_hba.conf"
