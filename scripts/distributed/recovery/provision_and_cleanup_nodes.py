#!/usr/bin/env python3
import subprocess
import sys

NODES = [
    ('admin123', '10.129.148.247'),
    ('user4', '10.129.148.246'),
    ('utkarsh', '10.129.148.248')
]

SETUP_SQL = """
DROP TABLE IF EXISTS customer, orders, stock, item, warehouse CASCADE;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_node_upper_bound' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.merkle_node_upper_bound(node_id bytea, prefix_len integer)
    RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_node_upper_bound_sql';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_partition_for_hash' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.merkle_partition_for_hash(key_hash bytea, partitions integer)
    RETURNS smallint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_partition_for_hash';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_key_hash' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.merkle_key_hash(anyelement)
    RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_sql';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_find_spurious_key' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.merkle_find_spurious_key(lower_bound bytea, upper_bound bytea, partition_id integer, partitions integer, base_offset bigint, max_attempts integer)
    RETURNS bigint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_find_spurious_key_sql';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'bcdb_cut_snapshot_export' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.bcdb_cut_snapshot_export(integer, integer)
    RETURNS text LANGUAGE internal VOLATILE AS 'bcdb_cut_snapshot_export';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'bcdb_recovery_rebase' AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.bcdb_recovery_rebase(integer)
    RETURNS boolean LANGUAGE internal VOLATILE AS 'bcdb_recovery_rebase';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION public.bcdb_cut_snapshot_export(integer, integer)
RETURNS text LANGUAGE internal VOLATILE AS 'bcdb_cut_snapshot_export';

CREATE OR REPLACE FUNCTION public.bcdb_recovery_rebase(integer)
RETURNS boolean LANGUAGE internal VOLATILE AS 'bcdb_recovery_rebase';

CREATE OR REPLACE FUNCTION public.merkle_node_upper_bound(node_id bytea, prefix_len integer)
RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_node_upper_bound_sql';

CREATE OR REPLACE FUNCTION public.merkle_partition_for_hash(key_hash bytea, partitions integer)
RETURNS smallint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_partition_for_hash';

CREATE OR REPLACE FUNCTION public.merkle_key_hash(anyelement)
RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_sql';

CREATE OR REPLACE FUNCTION public.merkle_find_spurious_key(lower_bound bytea, upper_bound bytea, partition_id integer, partitions integer, base_offset bigint, max_attempts integer)
RETURNS bigint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_find_spurious_key_sql';

SELECT encode(merkle_node_upper_bound(decode('0000000000000000', 'hex'), 0), 'hex') AS upper_bound,
       merkle_partition_for_hash(decode('0000000000000001', 'hex'), 200) AS partition,
       encode(merkle_key_hash(1::bigint), 'hex') AS key_hash;
"""

for name, ip in NODES:
    print(f"=== Provisioning & Cleaning Node {name} ({ip}) ===")
    remote_script = f"""
export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:${{LD_LIBRARY_PATH:-}}
export PATH=/home/neel/Desktop/ariabc_install/bin:${{PATH:-}}
fuser -k 9000/tcp 8000/tcp 8001/tcp 2>/dev/null || true
pkill -9 -f ariabc_pg 2>/dev/null || true
pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -w -t 30 start 2>&1 || true
psql -h 127.0.0.1 -p 5438 -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
{SETUP_SQL}
SQL
echo "Tables in public:"
psql -h 127.0.0.1 -p 5438 -U postgres -d postgres -tAc "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY relname;"
pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -m fast stop 2>&1 || true
"""
    cmd = [
        "sshpass", "-p", "clusterinfolab123",
        "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
        f"neel@{ip}", "bash -s"
    ]
    res = subprocess.run(cmd, input=remote_script, capture_output=True, text=True)
    print("STDOUT:")
    print(res.stdout)
    if res.stderr:
        print("STDERR:")
        print(res.stderr)
    if res.returncode != 0:
        print(f"FAILED on {name} with code {res.returncode}")
        sys.exit(1)

print("ALL 3 NODES CLEANED AND PROVISIONED SUCCESSFULLY!")
