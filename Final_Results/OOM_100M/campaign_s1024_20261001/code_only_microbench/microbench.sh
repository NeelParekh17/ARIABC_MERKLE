#!/bin/bash
# Merkle node-path CPU microbench on .247: base vs opt install, split 32 vs 1024.
set -euo pipefail
R=$HOME/claude_opt/micro; mkdir -p $R; PORT=55481
cat > $R/tx.pgbench <<'PGB'
\set w random(1, 100)
\set i1 random(1, 20000)
\set i2 random(1, 20000)
\set i3 random(1, 20000)
\set i4 random(1, 20000)
\set i5 random(1, 20000)
\set i6 random(1, 20000)
\set i7 random(1, 20000)
\set i8 random(1, 20000)
\set i9 random(1, 20000)
\set i10 random(1, 20000)
BEGIN;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i1;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i2;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i3;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i4;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i5;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i6;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i7;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i8;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i9;
UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = :w AND i = :i10;
COMMIT;
PGB
setup() { # $1=build $2=split
  local I=$HOME/claude_opt/install_$1 D=$R/pg_$1_$2
  export LD_LIBRARY_PATH=$I/lib
  [ -d $D ] && return 0
  $I/bin/initdb -D $D -U postgres >/dev/null
  cat >> $D/postgresql.conf <<CONF
port = $PORT
listen_addresses = ''
unix_socket_directories = '$R'
shared_buffers = 512MB
maintenance_work_mem = 128MB
max_parallel_maintenance_workers = 0
synchronous_commit = off
autovacuum = off
checkpoint_timeout = 30min
max_wal_size = 8GB
enable_merkle_index = on
merkle_apply_synchronous_direct = on
CONF
  $I/bin/pg_ctl -D $D -l $D/log -w start >/dev/null
  $I/bin/psql -h $R -p $PORT -U postgres -qX -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE stock (w int NOT NULL, i int NOT NULL, qty int NOT NULL, payload text, PRIMARY KEY (w, i));
INSERT INTO stock SELECT w, i, 0, md5((w*100000+i)::text) FROM generate_series(1,100) w, generate_series(1,20000) i;
CREATE INDEX stock_merkle ON stock USING merkle (w, i) WITH (fanout=32, split_threshold=$2, merge_threshold=$(( $2 / 4 )), partitions=8192, partition_key_columns=1, subpartitions=16);
VACUUM ANALYZE stock;
SQL
  $I/bin/psql -h $R -p $PORT -U postgres -AtX -c "select merkle_tree_stats('stock'::regclass)" > $D/geometry.json
  $I/bin/pg_ctl -D $D -m fast -w stop >/dev/null
}
run() { # $1=build $2=split $3=round
  local I=$HOME/claude_opt/install_$1 D=$R/pg_$1_$2
  export LD_LIBRARY_PATH=$I/lib
  $I/bin/pg_ctl -D $D -l $D/log -w start >/dev/null
  local P="$I/bin/psql -h $R -p $PORT -U postgres -AtX"
  $P -c "CREATE EXTENSION IF NOT EXISTS pg_prewarm" >/dev/null 2>&1 || true
  $P -c "select count(*) from stock" >/dev/null; $P -c "select count(*) from ariabc_internal.merkle_node_stock" >/dev/null
  $P -c "CREATE OR REPLACE FUNCTION stock_tx(pw int, pids int[]) RETURNS void LANGUAGE plpgsql AS \$\$ DECLARE x int; BEGIN FOREACH x IN ARRAY pids LOOP UPDATE stock SET qty = qty + 1, payload = md5(qty::text) WHERE w = pw AND i = x; END LOOP; END \$\$" >/dev/null
  python3 $HOME/claude_opt/microdrv.py $R $PORT 8 15 99 >/dev/null  # warmup
  $P -c CHECKPOINT >/dev/null
  for c in 1 8; do
    L0=$($P -c "select pg_current_wal_lsn()")
    out=$(python3 $HOME/claude_opt/microdrv.py $R $PORT $c 45 $3)
    L1=$($P -c "select pg_current_wal_lsn()")
    tps=$(echo "$out" | grep -oP "tps=\K[0-9.]+"); lat=$(echo "$out" | grep -oP "retries=\K[0-9]+"); iso=$(echo "$out" | grep -oP "isolation=\K\S+")
    tx=$(echo "$out" | grep -oP "committed=\K[0-9]+")
    wal=$($P -c "select pg_wal_lsn_diff('$L1','$L0')")
    echo "$1,$2,$3,$c,$tps,$lat,$tx,$wal,$iso" | tee -a $R/results.csv
  done
  $I/bin/pg_ctl -D $D -m fast -w stop >/dev/null
}
[ -f $R/results.csv ] || echo "build,split,round,clients,tps,retries,tx,wal_bytes,isolation" > $R/results.csv
for b in base opt; do for s in 32 1024; do setup $b $s; done; done
for r in 1 2 3; do for s in 32 1024; do for b in base opt; do run $b $s $r; done; done; done
echo MICRO_DONE
