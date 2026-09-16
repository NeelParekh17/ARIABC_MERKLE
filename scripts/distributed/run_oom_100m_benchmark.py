#!/usr/bin/env python3
"""Cold-start large-database YCSB sweep, with measured block-device I/O.

32MB shared_buffers does not cap the Linux page cache. Each case restores a
clean baseline, validates it, stops PostgreSQL, drops caches, and restarts.
See OOM_100M_BENCHMARK_REVIEW.md for comparison limits and run instructions.
"""
import argparse
import csv
import datetime
import getpass
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path

from benchmark_validation import count_workload_queries, parse_gateway_result
from large_zipf import LargeZipfGenerator

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from generate_ycsb_workloads import generate_workload_statements

DEFAULT_REMOTE_HOST = "10.129.148.247"
DEFAULT_REMOTE_USER = "neel"
DEFAULT_DB_PORT = 5438
DEFAULT_SERVER_PORT = 8000
DEFAULT_REMOTE_DIR = "/tmp/ariabc_oom_100m"
DEFAULT_INSTALL_DIR = "/home/neel/Desktop/ariabc_install"
DEFAULT_CLUSTER_DIR = "/home/neel/Desktop/ariabc_cluster"
DEFAULT_GATEWAY_HOST = "10.129.27.111"
DEFAULT_GATEWAY_USER = "neel"
DEFAULT_GATEWAY_REPO = "/home/neel/ARIABC/AriaBC"
SUPPORTED_WORKLOADS = ("a", "b", "c", "d", "f", "all_update", "all_insert", "all_delete")
_SUDO_PASSWORD = None


def run_remote(host, user, cmd, timeout=600, check=True):
    res = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                          f"{user}@{host}", "bash -s"],
                         input="set -euo pipefail\nexport LC_ALL=C\n" + cmd,
                         capture_output=True, text=True, timeout=timeout)
    if check and res.returncode:
        raise RuntimeError(f"Remote command failed on {host} ({res.returncode}):\n{res.stdout}\n{res.stderr}")
    return res


def db_shell(args):
    return f"export LD_LIBRARY_PATH={args.install_dir}/lib:/home/neel/Desktop/rdkafka_local/lib:${{LD_LIBRARY_PATH:-}}\n"


def sudo_command(command):
    if _SUDO_PASSWORD is None:
        return "sudo -n sh -c " + shlex.quote(command)
    # Password travels only through SSH stdin; never an argv, manifest or log.
    return ("printf '%s\\n' " + shlex.quote(_SUDO_PASSWORD) +
            " | sudo -S -p '' sh -c " + shlex.quote(command))


def load_sudo_password():
    global _SUDO_PASSWORD
    if _SUDO_PASSWORD:
        return _SUDO_PASSWORD
    _SUDO_PASSWORD = os.environ.get("ARIABC_BENCH_SUDO_PASSWORD")
    if _SUDO_PASSWORD:
        return _SUDO_PASSWORD
    for path in (Path.home() / ".env", REPO_ROOT / ".bench_tmp/.env", REPO_ROOT / ".env"):
        if path.is_file():
            for line in path.read_text().splitlines():
                line = line.strip()
                if line.startswith("ARIABC_BENCH_SUDO_PASSWORD="):
                    _SUDO_PASSWORD = line.split("=", 1)[1].strip().strip('"').strip("'")
                    return _SUDO_PASSWORD
    return None


def ensure_cache_drop_access(args):
    global _SUDO_PASSWORD
    probe = "test -w /proc/sys/vm/drop_caches"
    result = run_remote(args.remote_host, args.remote_user, sudo_command(probe), check=False)
    if result.returncode:
        _SUDO_PASSWORD = load_sudo_password()
        if _SUDO_PASSWORD is None and sys.stdin.isatty():
            _SUDO_PASSWORD = getpass.getpass(f"sudo password for {args.remote_user}@{args.remote_host} (cache clearing): ")
        if _SUDO_PASSWORD is None:
            raise RuntimeError("Cache clearing requires sudo. Set ARIABC_BENCH_SUDO_PASSWORD in ~/.env, environment, or run interactively.")
        run_remote(args.remote_host, args.remote_user, sudo_command(probe))


def sql(args, statement, timeout=600):
    command = (db_shell(args) + f"{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 "
               f"-h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -At "
               f"-c {shlex.quote(statement)}")
    return run_remote(args.remote_host, args.remote_user, command, timeout=timeout).stdout.strip()


def check_remote_db_exists(args):
    result = run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
if [ -f {args.remote_dir}/pgdata_base/PG_VERSION ]; then
    {args.install_dir}/bin/pg_controldata {args.remote_dir}/pgdata_base
elif [ -e {args.remote_dir}/pgdata_base ]; then
    echo 'Incomplete pgdata_base; repair it before benchmarking' >&2
    exit 1
else
    echo MISSING
fi
""")
    if result.stdout.strip() == "MISSING":
        return False
    if not re.search(r"Database cluster state:\s+shut down\s*$", result.stdout, re.M):
        raise RuntimeError("Golden database is not cleanly shut down")
    return True


def generate_remote_100m_database(args):
    """
    Creates the requested keyspace with 16 streaming COPY workers.
    """
    print("=" * 80)
    print(f"[OOM-100M] Database not found on {args.remote_host}:{args.remote_dir}.")
    print(f"[OOM-100M] Starting high-speed 16-worker parallel generation of {args.db_rows:,} rows...")
    print("=" * 80)

    start_total = time.time()

    schema = (REPO_ROOT / "scripts/distributed/sql/raft_apply_ledger_schema.sql").read_text()
    run_remote(args.remote_host, args.remote_user,
               f"mkdir -p {args.remote_dir}/scripts; printf %s {shlex.quote(schema)} > {args.remote_dir}/scripts/ledger.sql")
    # Step 1: Create 16-worker parallel generator helper script
    helper_script = f"""#!/usr/bin/env python3
import os, sys, time, string, random, subprocess, multiprocessing as mp

LETTERS = string.ascii_letters + string.digits
def make_pool(size=1000):
    rng = random.Random(42)
    return [''.join(rng.choices(LETTERS, k=20)) for _ in range(size)]

def copy_worker(w_id, start_k, count, psql_bin, db_port):
    pool = make_pool(1000)
    pool_len = len(pool)
    batch_size = 25000
    
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = "{args.install_dir}/lib:/home/neel/Desktop/rdkafka_local/lib:" + env.get("LD_LIBRARY_PATH", "")
    
    psql_cmd = [
        psql_bin, '-h', '127.0.0.1', '-p', str(db_port), '-U', 'postgres', '-d', 'postgres',
        '-X', '-v', 'ON_ERROR_STOP=1', '-c', 'COPY usertable (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) FROM stdin;'
    ]
    p = subprocess.Popen(psql_cmd, stdin=subprocess.PIPE, text=True, bufsize=2097152, env=env)
    
    lines = []
    end_k = start_k + count
    for i in range(start_k, end_k):
        idx = i % (pool_len - 10)
        f = pool[idx:idx+10]
        lines.append(f"{{i}}\\t{{f[0]}}\\t{{f[1]}}\\t{{f[2]}}\\t{{f[3]}}\\t{{f[4]}}\\t{{f[5]}}\\t{{f[6]}}\\t{{f[7]}}\\t{{f[8]}}\\t{{f[9]}}\\n")
        
        if len(lines) >= batch_size:
            p.stdin.write(''.join(lines))
            lines.clear()
            
    if lines:
        p.stdin.write(''.join(lines))
    p.stdin.close()
    p.wait()
    if p.returncode != 0:
        raise RuntimeError(f"Worker {{w_id}} psql COPY failed with exit code {{p.returncode}}")

def main():
    total_rows = {args.db_rows}
    num_workers = 16
    chunk = total_rows // num_workers
    psql_bin = '{args.install_dir}/bin/psql'
    db_port = {args.db_port}
    
    print(f"  [STREAM-16W] Launching 16 parallel COPY workers for {{total_rows:,}} rows...", flush=True)
    t0 = time.time()
    
    tasks = []
    for w in range(num_workers):
        start_k = 1 + w * chunk
        cnt = chunk if w < num_workers - 1 else (total_rows - start_k + 1)
        tasks.append((w, start_k, cnt, psql_bin, db_port))
        
    with mp.Pool(num_workers) as pool:
        pool.starmap(copy_worker, tasks)
        
    elapsed = time.time() - t0
    rate = total_rows / elapsed if elapsed > 0 else 0
    print(f"  [STREAM-16W] Successfully ingested {{total_rows:,}} rows in {{elapsed:.1f}}s ({{rate:,.0f}} rows/sec)!", flush=True)

if __name__ == '__main__':
    main()
"""

    remote_setup_cmd = f"""
    export LD_LIBRARY_PATH=/home/neel/Desktop/rdkafka_local/lib:{args.install_dir}/lib:${{LD_LIBRARY_PATH:-}}
    mkdir -p {args.remote_dir}/golden {args.remote_dir}/scripts
    test ! -e {args.remote_dir}/pgdata
    test ! -e {args.remote_dir}/pgdata_base

    # 1. Initialize clean PostgreSQL cluster with postgres superuser
    echo '  [1/6] Running initdb...'
    {args.install_dir}/bin/initdb -U postgres -A trust -D {args.remote_dir}/pgdata -E UTF8 --locale=C >/dev/null
    
    # 2. Configure high-performance bulk loading parameters
    cat << 'EOF' >> {args.remote_dir}/pgdata/postgresql.conf
port = {args.db_port}
shared_buffers = 4GB
maintenance_work_mem = 6GB
max_parallel_maintenance_workers = 16
max_parallel_workers = 16
max_parallel_workers_per_gather = 8
max_connections = 100
wal_level = minimal
max_wal_senders = 0
checkpoint_timeout = 60min
max_wal_size = 2GB
synchronous_commit = off
fsync = off
full_page_writes = off
enable_merkle_index = on
bcdb_worker_count = 1
listen_addresses = '*'
EOF

    cat << 'EOF' >> {args.remote_dir}/pgdata/pg_hba.conf
host all all 0.0.0.0/0 trust
local all all trust
EOF

    # 3. Start PostgreSQL
    echo '  [2/6] Starting PostgreSQL for bulk load...'
    {args.install_dir}/bin/pg_ctl -D {args.remote_dir}/pgdata -l {args.remote_dir}/postgres_init.log -w start
    
    {args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -f {args.remote_dir}/scripts/ledger.sql
    # 4. Create usertable and ariabc_internal catalog schema
    echo '  [3/6] Creating usertable and ariabc_internal catalog schema...'
    {args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "
        DROP TABLE IF EXISTS usertable CASCADE;
        CREATE TABLE usertable (
            ycsb_key integer NOT NULL,
            field1 text, field2 text, field3 text, field4 text, field5 text,
            field6 text, field7 text, field8 text, field9 text, field10 text
        );
    " >/dev/null
    """

    print("Executing remote environment initialization...")
    run_remote(args.remote_host, args.remote_user, remote_setup_cmd, timeout=300)

    # Write and execute streaming helper
    put_helper_cmd = f"cat << 'EOF' > {args.remote_dir}/scripts/stream_100m.py\n{helper_script}\nEOF\nchmod +x {args.remote_dir}/scripts/stream_100m.py"
    run_remote(args.remote_host, args.remote_user, put_helper_cmd)

    print(f"Streaming {args.db_rows:,} rows directly into PostgreSQL...")
    stream_cmd = f"export LD_LIBRARY_PATH=/home/neel/Desktop/rdkafka_local/lib:{args.install_dir}/lib:${{LD_LIBRARY_PATH:-}} && python3 {args.remote_dir}/scripts/stream_100m.py"
    stream_res = run_remote(args.remote_host, args.remote_user, stream_cmd, timeout=3600)
    for line in stream_res.stdout.splitlines():
        print(line)

    # Step 5: Indexing and vacuum
    print("Building Primary Key and Merkle covering indexes (using 16 parallel maintenance workers)...")
    index_cmd = f"""
    export LD_LIBRARY_PATH=/home/neel/Desktop/rdkafka_local/lib:{args.install_dir}/lib:${{LD_LIBRARY_PATH:-}}
    echo '  [4/6] Building Primary Key B-Tree index...'
    {args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "
        SET max_parallel_maintenance_workers = 16;
        SET maintenance_work_mem = '6GB';
        ALTER TABLE usertable ADD CONSTRAINT usertable_pkey1 PRIMARY KEY (ycsb_key);
    "
    
    echo '  [5/6] Building Merkle Covering Lookup Index...'
    {args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "
        SET max_parallel_maintenance_workers = 16;
        SET maintenance_work_mem = '6GB';
        DROP INDEX IF EXISTS usertable_merkle_lookup_idx;
        CREATE INDEX usertable_merkle_lookup_idx ON usertable (
            merkle_partition_for_hash(merkle_key_hash(ycsb_key), 200),
            merkle_key_hash(ycsb_key),
            ycsb_key
        );
        CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key)
        WITH (partitions = 200, fanout = 4, split_threshold = 32, merge_threshold = 8);
    "
    
    echo '  [6/6] Analyzing table statistics...'
    {args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "ANALYZE usertable;"
    
    # Restore standard safety settings before creating golden backup
    sed -i "s/fsync = off/fsync = on/g" {args.remote_dir}/pgdata/postgresql.conf
    sed -i "s/full_page_writes = off/full_page_writes = on/g" {args.remote_dir}/pgdata/postgresql.conf
    
    # Stop PostgreSQL cleanly
    {args.install_dir}/bin/pg_ctl -D {args.remote_dir}/pgdata stop -m fast
    
    # Keep PostgreSQL-managed WAL intact. Never delete WAL files by hand.
    sync

    # Create pristine uncompressed pgdata_base directory for clean cp -a resets
    echo 'Creating pristine pgdata_base directory ({args.remote_dir}/pgdata_base)...'
    test ! -e {args.remote_dir}/pgdata_base.tmp
    cp -a --reflink=never {args.remote_dir}/pgdata {args.remote_dir}/pgdata_base.tmp
    mv {args.remote_dir}/pgdata_base.tmp {args.remote_dir}/pgdata_base

    # Create pigz compressed golden tarball as persistent archive
    echo 'Packing golden archive ({args.remote_dir}/golden/pgdata_golden.tar.gz)...'
    test ! -e {args.remote_dir}/golden/pgdata_golden.tar.gz
    tar -cf - -C {args.remote_dir} pgdata | pigz -1 -p 16 > {args.remote_dir}/golden/pgdata_golden.tar.gz.tmp
    mv {args.remote_dir}/golden/pgdata_golden.tar.gz.tmp {args.remote_dir}/golden/pgdata_golden.tar.gz
    touch {args.remote_dir}/golden/done.flag
    echo 'GOLDEN_READY'
    """
    res = run_remote(args.remote_host, args.remote_user, index_cmd, timeout=3600)
    for line in res.stdout.splitlines():
        print(line)

    total_time = time.time() - start_total
    print("=" * 80)
    print(f"[OOM-100M] Generation and Golden Database Complete in {total_time/60:.2f} minutes!")
    print("=" * 80)


def generate_100m_ycsb_workload(out_path, wl_type, skew, txs=20000, db_size=100000000, seed=42):
    if wl_type not in SUPPORTED_WORKLOADS:
        raise ValueError(f"Unsupported large-keyspace workload: {wl_type}")
    statements = generate_workload_statements(
        wl_type, skew, num_tx=txs, num_keys=db_size, table_name="usertable",
        seed=seed + int(skew * 100), zipf_factory=LargeZipfGenerator)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(statements) + "\n")


def preflight(args):
    result = run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
command -v python3
command -v findmnt
command -v fuser
command -v flock
command -v timeout
test -x {args.install_dir}/bin/postgres
test -x {args.cluster_dir}/ariabc_pg/build/bin/ariabc_pg_server
mkdir -p {args.remote_dir}
for port in {args.db_port} {args.server_port} 9000; do
    if fuser "$port"/tcp >/dev/null 2>&1; then
        echo "Port $port is occupied; stop the existing benchmark first" >&2
        exit 1
    fi
done
# This probe does not clear caches. The actual cold reset uses the same sudo rule.
{sudo_command('test -w /proc/sys/vm/drop_caches')}
hostname
free -b
findmnt -T {args.remote_dir} -o SOURCE,FSTYPE,TARGET
findmnt -n -o MAJ:MIN -T {args.remote_dir}
df -B1 {args.remote_dir}
{args.install_dir}/bin/postgres --version
sha256sum {args.install_dir}/bin/postgres {args.cluster_dir}/ariabc_pg/build/bin/ariabc_pg_server
""", timeout=30)
    if args.gateway_host not in ("localhost", "127.0.0.1"):
        gw = run_remote(args.gateway_host, args.gateway_user,
                        f"test -x {args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway\n"
                        f"command -v timeout\nsha256sum {args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway",
                        timeout=30).stdout
    else:
        binary = REPO_ROOT / "ariabc_pg/build/bin/ariabc_pg_gateway"
        gw = f"{hashlib.sha256(binary.read_bytes()).hexdigest()} {binary}\n"
    if getattr(args, "reset_mode", "undo") == "undo":
        undo_script = REPO_ROOT / "scripts/distributed/ycsb_undo.py"
        if undo_script.exists():
            run_remote(args.remote_host, args.remote_user,
                       f"printf %s {shlex.quote(undo_script.read_text())} > {args.remote_dir}/ycsb_undo.py\n"
                       f"chmod +x {args.remote_dir}/ycsb_undo.py\n", timeout=30)
    return result.stdout + result.stderr + "\nGateway:\n" + gw


def stop_server(args):
    # Only terminate a server started by this runner. Never kill a port's owner.
    run_remote(args.remote_host, args.remote_user, f"""
if [ -f {args.remote_dir}/server.pid ]; then
    pid=$(cat {args.remote_dir}/server.pid)
    if kill -0 "$pid" 2>/dev/null; then
        tr '\\0' ' ' < /proc/"$pid"/cmdline | grep -F -- '{args.cluster_dir}/ariabc_pg/build/bin/ariabc_pg_server' >/dev/null
        kill -TERM "$pid"
        for i in $(seq 1 100); do
            if ! kill -0 "$pid" 2>/dev/null; then break; fi
            state=$(awk '{{print $3}}' /proc/"$pid"/stat 2>/dev/null || true)
            if [ "$state" = Z ]; then break; fi
            sleep 0.1
        done
        if fuser {args.server_port}/tcp >/dev/null 2>&1; then
            echo 'Server did not stop cleanly' >&2; exit 1
        fi
    fi
    rm -f {args.remote_dir}/server.pid
fi
""", timeout=30)


def stop_postgres(args):
    return run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
if {args.install_dir}/bin/pg_ctl -D {args.remote_dir}/pgdata status >/dev/null 2>&1; then
    {args.install_dir}/bin/pg_ctl -D {args.remote_dir}/pgdata -w -t 120 stop -m fast
fi
""", timeout=150)


def start_postgres(args):
    return run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
{args.install_dir}/bin/pg_ctl -D {args.remote_dir}/pgdata -l {args.remote_dir}/postgres.log -w -t 120 start
""", timeout=150)


def prepare_ledger_schema(args):
    # The old OOM generator created three placeholder tables instead of the
    # ledger schema. Repair only those empty placeholders in the disposable
    # copy. Never run the schema's destructive Merkle layout migration here.
    layout = sql(args, "SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) "
                       "FROM information_schema.columns WHERE table_schema='ariabc_internal' AND table_name='merkle_node';")
    if not layout.startswith('tuple_count:integer,index_oid:oid,partition_id:smallint,'):
        raise RuntimeError("Golden Merkle layout is incompatible; rebuild in a fresh --remote-dir")
    legacy = sql(args, "SELECT count(*) FROM information_schema.columns WHERE table_schema='ariabc_internal' "
                       "AND table_name='raft_apply_entry' AND column_name='test_marker';")
    if legacy == '1':
        nonempty = sql(args, "SELECT (SELECT count(*) FROM ariabc_internal.raft_apply_entry) + "
                            "(SELECT count(*) FROM ariabc_internal.raft_apply_entry_item) + "
                            "(SELECT count(*) FROM ariabc_internal.raft_apply_item);")
        if nonempty != '0':
            raise RuntimeError("Refusing to replace nonempty legacy ledger tables")
        sql(args, "DROP TABLE ariabc_internal.raft_apply_entry_item, "
                  "ariabc_internal.raft_apply_entry, ariabc_internal.raft_apply_item;")
    schema = (REPO_ROOT / 'scripts/distributed/sql/raft_apply_ledger_schema.sql').read_text()
    run_remote(args.remote_host, args.remote_user, db_shell(args) +
               f"printf %s {shlex.quote(schema)} > {args.remote_dir}/ledger.sql\n"
               f"{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} "
               f"-U postgres -d postgres -f {args.remote_dir}/ledger.sql", timeout=args.verify_timeout)
    state = sql(args, "SELECT c.next_seq, c.terminal_prefix_seq, s.applied_seq, s.state "
                      "FROM ariabc_internal.merkle_apply_counter c CROSS JOIN ariabc_internal.merkle_apply_state s;")
    if state != '0|0|0|0':
        raise RuntimeError(f"Golden baseline has nonzero apply state: {state}")


def get_remote_baseline_identity(args):
    """Inspect active pgdata_base to obtain fresh control, checksum and sizing metadata."""
    cmd = db_shell(args) + f"""
{args.install_dir}/bin/pg_controldata {args.remote_dir}/pgdata_base | grep -E 'Database system identifier|Latest checkpoint location|Database cluster state' | sed 's/.*: *//'
sha256sum {args.remote_dir}/pgdata_base/global/pg_control {args.remote_dir}/pgdata_base/PG_VERSION
find {args.remote_dir}/pgdata_base -type f | wc -l
du -sb {args.remote_dir}/pgdata_base | cut -f1
"""
    lines = run_remote(args.remote_host, args.remote_user, cmd, timeout=30).stdout.strip().splitlines()
    if len(lines) < 7:
        raise RuntimeError(f"Failed to inspect baseline identity; got {lines}")
    return dict(
        db_system_id=lines[0].strip(),
        cluster_state=lines[1].strip(),
        checkpoint_lsn=lines[2].strip(),
        pg_control_sha256=lines[3].split()[0],
        pg_version_sha256=lines[4].split()[0],
        file_count=int(lines[5].strip()),
        total_bytes=int(lines[6].strip()),
    )


def validate_golden_baseline(args):
    """Full-scan the golden baseline once per sweep and record a manifest outside pgdata_base.

    The manifest records exact row counts, key bounds, relation byte sizes, relpages,
    and block-device/control file hashes. Subsequent resets verify physical file copy
    fidelity before postmaster starts, followed by exact O(1) relation size and bound checks.
    """
    manifest_path = f"{args.remote_dir}/.ariabc_golden_manifest.json"
    meta = get_remote_baseline_identity(args)
    if meta["cluster_state"] != "shut down":
        raise RuntimeError(f"Golden database cluster state is not cleanly shut down: {meta['cluster_state']}")

    existing = run_remote(args.remote_host, args.remote_user,
                          f"cat {manifest_path} 2>/dev/null || echo MISSING",
                          timeout=15).stdout.strip()
    if existing != "MISSING":
        try:
            manifest = json.loads(existing)
        except Exception:
            manifest = {}
        if (manifest.get("version") == 2 and
                manifest.get("db_rows") == args.db_rows and
                manifest.get("keyspace") == f"{args.db_rows}|1|{args.db_rows}" and
                manifest.get("db_system_id") == meta["db_system_id"] and
                manifest.get("checkpoint_lsn") == meta["checkpoint_lsn"] and
                manifest.get("pg_control_sha256") == meta["pg_control_sha256"] and
                manifest.get("pg_version_sha256") == meta["pg_version_sha256"] and
                manifest.get("file_count") == meta["file_count"] and
                manifest.get("total_bytes") == meta["total_bytes"]):
            print(f"  Golden manifest verified (cached & fresh): {args.db_rows:,} rows, "
                  f"{manifest['file_count']} files, {manifest['total_bytes'] / 2**30:.2f} GiB, "
                  f"LSN {manifest['checkpoint_lsn']}")
            return manifest
        print(f"  Golden manifest is missing, stale, or baseline changed; re-validating golden baseline...")

    # Start PG against a temporary check copy to run the full verification scan.
    check_dir = f"{args.remote_dir}/pgdata_golden_check"
    stop_server(args)
    stop_postgres(args)
    try:
        run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
rm -rf {check_dir}
cp -a --reflink=never {args.remote_dir}/pgdata_base {check_dir}
cat > {check_dir}/postgresql.auto.conf <<'EOCONF'
port = {args.db_port}
listen_addresses = '*'
shared_buffers = '256MB'
enable_merkle_index = off
bcdb_worker_count = 1
bcdb_ledger_trace = off
max_parallel_workers_per_gather = 4
EOCONF
{args.install_dir}/bin/pg_ctl -D {check_dir} \
    -l {args.remote_dir}/golden_check.log -w -t 120 start
""", timeout=args.reset_timeout)

        def golden_sql(statement, timeout=600):
            command = (db_shell(args) + f"{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 "
                       f"-h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -At "
                       f"-c {shlex.quote(statement)}")
            return run_remote(args.remote_host, args.remote_user, command, timeout=timeout).stdout.strip()

        print(f"  Validating golden baseline: full-scanning {args.db_rows:,} rows...")
        identity = golden_sql("SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable;",
                              timeout=args.verify_timeout)
        expected = f"{args.db_rows}|1|{args.db_rows}"
        if identity != expected:
            raise RuntimeError(f"Golden baseline keyspace mismatch: {identity} != {expected}")

        # Exact byte sizes & page counts (avoid lossy float4 reltuples)
        table_stats = golden_sql("SELECT pg_relation_size('usertable'), "
                                 "pg_relation_size('usertable_pkey1'), "
                                 "pg_total_relation_size('usertable'), "
                                 "relpages FROM pg_class WHERE relname='usertable';")
        heap_bytes, index_bytes, total_rel_bytes, relpages = [int(x) for x in table_stats.split('|')]

        indexes = golden_sql("SELECT json_agg(row_to_json(i)) FROM "
                             "(SELECT indexname, indexdef FROM pg_indexes "
                             "WHERE tablename='usertable' ORDER BY indexname) i;")
    finally:
        # Guaranteed cleanup of the temporary check instance
        run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
if [ -f {check_dir}/postmaster.pid ]; then
    {args.install_dir}/bin/pg_ctl -D {check_dir} -w stop -m immediate || true
fi
rm -rf {check_dir}
""", timeout=150, check=False)

    manifest = dict(
        version=2,
        db_rows=args.db_rows,
        keyspace=identity,
        db_system_id=meta["db_system_id"],
        checkpoint_lsn=meta["checkpoint_lsn"],
        pg_control_sha256=meta["pg_control_sha256"],
        pg_version_sha256=meta["pg_version_sha256"],
        file_count=meta["file_count"],
        total_bytes=meta["total_bytes"],
        heap_bytes=heap_bytes,
        index_bytes=index_bytes,
        total_relation_bytes=total_rel_bytes,
        relpages=relpages,
        indexes=json.loads(indexes),
    )
    run_remote(args.remote_host, args.remote_user,
               f"printf %s {shlex.quote(json.dumps(manifest))} > {manifest_path}",
               timeout=15)
    print(f"  Golden manifest written: {args.db_rows:,} rows, "
          f"{manifest['file_count']} files, {manifest['total_bytes'] / 2**30:.2f} GiB, "
          f"heap {heap_bytes / 2**30:.2f} GiB ({relpages:,} pages)")
    return manifest


def prepare_workload_undo(args, workload_file):
    digest = hashlib.sha256(workload_file.read_bytes()).hexdigest()
    remote_wl = f"{args.remote_dir}/workload_{digest[:16]}.sql"
    undo_dir = f"{args.remote_dir}/undo_{digest[:16]}"
    undo_script = REPO_ROOT / "scripts/distributed/ycsb_undo.py"
    run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
printf %s {shlex.quote(undo_script.read_text())} > {args.remote_dir}/ycsb_undo.py
chmod +x {args.remote_dir}/ycsb_undo.py
if [ ! -f {remote_wl} ]; then
    printf %s {shlex.quote(workload_file.read_text())} > {remote_wl}
fi
export LD_LIBRARY_PATH={args.install_dir}/lib
if grep -q '::oid::regclass' {undo_dir}/restore.sql 2>/dev/null; then
    rm -rf {undo_dir}
fi
if [ ! -f {undo_dir}/restore.sql ]; then
    python3 {args.remote_dir}/ycsb_undo.py prepare --psql {args.install_dir}/bin/psql --port {args.db_port} --workload {remote_wl} --output-dir {undo_dir}
fi
""", timeout=600)
    return undo_dir


def apply_workload_undo(args, undo_dir):
    start = time.monotonic()
    res = run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
export LD_LIBRARY_PATH={args.install_dir}/lib
{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -f {undo_dir}/restore.sql
python3 {args.remote_dir}/ycsb_undo.py verify --psql {args.install_dir}/bin/psql --port {args.db_port} --output-dir {undo_dir}
""", timeout=args.reset_timeout, check=False)
    elapsed_ms = (time.monotonic() - start) * 1000
    if res.returncode != 0:
        print(f"  WARNING: Undo restore or verification failed ({res.stderr.strip()}); marking next reset for physical copy", flush=True)
        args._need_physical_reset = True
    return elapsed_ms


def reset_remote_pgdata(args, mode, workers):
    stop_server(args)
    stop_postgres(args)
    enable = "on" if mode == "bcdb_merkle" else "off"
    bcdb_workers = workers if mode != "pg" else 1
    # auto.conf is replaced on the disposable copy so old ALTER SYSTEM values
    # cannot silently override the campaign contract.
    config = f"""port = {args.db_port}
listen_addresses = '*'
shared_buffers = '{args.shared_buffers}'
enable_merkle_index = {enable}
bcdb_worker_count = {bcdb_workers}
synchronous_commit = on
fsync = on
full_page_writes = on
autovacuum = off
work_mem = '4MB'
maintenance_work_mem = '2GB'
effective_cache_size = '4GB'
max_parallel_maintenance_workers = 4
max_parallel_workers_per_gather = 0
checkpoint_timeout = '30min'
max_wal_size = '20GB'
max_connections = 256
track_counts = on
track_io_timing = on
log_checkpoints = on
bcdb_ledger_trace = off
bcdb_serial_gate_mode = 1
bcdb_serial_gate_source = 0
bcdb_dt_conflict_tracking = on
bcdb_result_ring_slots = 2048
bcdb_dt_completion_only_skip_reads = off
bcdb_dt_hashtab_switch_threshold = 1500
bcdb_gate_telemetry = off
bcdb_gate_snapshot_each_block = off
merkle_apply_synchronous_direct = on
"""
    manifest = getattr(args, '_golden_manifest', None)
    if manifest is None:
        raw = run_remote(args.remote_host, args.remote_user,
                         f"cat {args.remote_dir}/.ariabc_golden_manifest.json 2>/dev/null || echo MISSING",
                         check=False).stdout.strip()
        if raw != "MISSING":
            try:
                manifest = json.loads(raw)
                args._golden_manifest = manifest
            except Exception:
                manifest = None

    # Pre-startup copy integrity verification script.
    # Runs immediately after cp -a and BEFORE postmaster starts or touches any file.
    copy_verify_cmd = ""
    if manifest:
        copy_verify_cmd = f"""
test "$(find {args.remote_dir}/pgdata -type f | wc -l)" -eq "{manifest['file_count']}"
test "$(du -sb {args.remote_dir}/pgdata | cut -f1)" -eq "{manifest['total_bytes']}"
test "$(sha256sum {args.remote_dir}/pgdata/PG_VERSION | cut -d' ' -f1)" = "{manifest['pg_version_sha256']}"
test "$(sha256sum {args.remote_dir}/pgdata/global/pg_control | cut -d' ' -f1)" = "{manifest['pg_control_sha256']}"
"""

    do_physical_copy = False
    if getattr(args, 'reset_mode', 'undo') == 'cp':
        do_physical_copy = True
    elif getattr(args, '_need_physical_reset', False):
        do_physical_copy = True
    else:
        check_pg = run_remote(args.remote_host, args.remote_user,
                              f"test -f {args.remote_dir}/pgdata/PG_VERSION", check=False)
        check_dropped = run_remote(args.remote_host, args.remote_user,
                                   f"test -f {args.remote_dir}/pgdata/.merkle_index_dropped", check=False)
        if check_pg.returncode != 0:
            do_physical_copy = True
        elif mode == "bcdb_merkle" and (getattr(args, '_merkle_index_dropped', False) or check_dropped.returncode == 0):
            do_physical_copy = True

    if do_physical_copy:
        result = run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
test -f {args.remote_dir}/pgdata_base/PG_VERSION
test ! -L {args.remote_dir}/pgdata
test ! -L {args.remote_dir}/pgdata_base
# Require room for a full copy plus WAL growth; do not consume all free space.
needed=$(du -sb {args.remote_dir}/pgdata_base | cut -f1)
available=$(df -B1 --output=avail {args.remote_dir} | tail -1)
test "$available" -gt "$((needed + 21474836480))"
rm -rf -- {args.remote_dir}/pgdata
cp -a --reflink=never {args.remote_dir}/pgdata_base {args.remote_dir}/pgdata
{copy_verify_cmd}
sync
printf %s {shlex.quote(config)} > {args.remote_dir}/pgdata/postgresql.auto.conf
: > {args.remote_dir}/postgres.log
rm -f {args.remote_dir}/pgdata/.merkle_index_dropped
""", timeout=args.reset_timeout)
        args._need_physical_reset = False
        args._merkle_index_dropped = False
    else:
        result = run_remote(args.remote_host, args.remote_user, db_shell(args) + f"""
printf %s {shlex.quote(config)} > {args.remote_dir}/pgdata/postgresql.auto.conf
: > {args.remote_dir}/postgres.log
""")

    start_postgres(args)
    prepare_ledger_schema(args)
    if mode != "bcdb_merkle":
        sql(args, "DROP INDEX IF EXISTS usertable_merkle_idx; "
                  "DROP INDEX IF EXISTS usertable_merkle_lookup_idx;")
        args._merkle_index_dropped = True
        run_remote(args.remote_host, args.remote_user, f"touch {args.remote_dir}/pgdata/.merkle_index_dropped", check=False)
    else:
        args._merkle_index_dropped = False
        run_remote(args.remote_host, args.remote_user, f"rm -f {args.remote_dir}/pgdata/.merkle_index_dropped", check=False)
    # Verification phase:
    # If --verify-mode full is requested, run an exhaustive SELECT count(*) table scan.
    # Otherwise, rely on the validated immutable baseline and run fast O(1) sanity checks
    # (B-tree bounds, relation byte sizes, catalog block counts, and index definitions).
    if getattr(args, "verify_mode", "fast") == "full":
        identity = sql(args, "SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable;",
                       timeout=args.verify_timeout)
        expected = f"{args.db_rows}|1|{args.db_rows}"
        if identity != expected:
            raise RuntimeError(f"Wrong golden keyspace after restore: {identity} != {expected}")
        bounds = f"1|{args.db_rows}"
    else:
        bounds = sql(args, "SELECT min(ycsb_key), max(ycsb_key) FROM usertable;")
        if bounds != f"1|{args.db_rows}":
            raise RuntimeError(f"Post-copy key bounds mismatch: {bounds}")
    table_stats = sql(args, "SELECT pg_relation_size('usertable'), "
                            "pg_relation_size('usertable_pkey1'), "
                            "relpages FROM pg_class WHERE relname='usertable';")
    heap_bytes, index_bytes, relpages = [int(x) for x in table_stats.split('|')]
    if manifest:
        if getattr(args, "reset_mode", "undo") == "cp" or do_physical_copy:
            if heap_bytes != manifest['heap_bytes']:
                raise RuntimeError(f"Post-copy heap size mismatch: {heap_bytes} != {manifest['heap_bytes']}")
            if index_bytes != manifest['index_bytes']:
                raise RuntimeError(f"Post-copy index size mismatch: {index_bytes} != {manifest['index_bytes']}")
            if relpages != manifest['relpages']:
                raise RuntimeError(f"Post-copy relpages mismatch: {relpages} != {manifest['relpages']}")
        else:
            if heap_bytes > int(manifest['heap_bytes'] * 1.05):
                raise RuntimeError(f"Post-undo heap size excessive: {heap_bytes} > {int(manifest['heap_bytes'] * 1.05)}")
    indexes = sql(args, "SELECT json_agg(row_to_json(i)) FROM "
                       "(SELECT indexname, indexdef FROM pg_indexes WHERE tablename='usertable' ORDER BY indexname) i;")
    if ("USING merkle" in indexes) != (mode == "bcdb_merkle"):
        raise RuntimeError(f"Wrong Merkle index state: {indexes}")
    if ("merkle_lookup_idx" in indexes) != (mode == "bcdb_merkle"):
        raise RuntimeError("Merkle lookup index does not match mode")
    stop_postgres(args)
    # Cache dropping must happen AFTER the validation scan and BEFORE startup.
    cache = run_remote(args.remote_host, args.remote_user,
                       "sync\n" + sudo_command('echo 3 > /proc/sys/vm/drop_caches') + "\n"
                       "echo CACHES_DROPPED\ncat /proc/meminfo", timeout=120)
    start_postgres(args)

    settings = json.loads(sql(args, "SELECT json_object_agg(name, setting) FROM pg_settings;"))
    multiplier = {"kB": 1024, "MB": 1024**2, "GB": 1024**3}
    size = re.fullmatch(r"([0-9]+)(kB|MB|GB)", args.shared_buffers)
    expected = int(size[1]) * multiplier[size[2]]
    if int(settings["shared_buffers"]) * int(settings["block_size"]) != expected:
        raise RuntimeError("Effective shared_buffers differs from requested value")
    for key, value in {"bcdb_worker_count": str(bcdb_workers), "enable_merkle_index": enable,
                       "fsync": "on", "full_page_writes": "on", "synchronous_commit": "on",
                       "track_counts": "on", "track_io_timing": "on",
                       "log_checkpoints": "on", "bcdb_ledger_trace": "off"}.items():
        if settings[key] != value:
            raise RuntimeError(f"Unexpected effective setting {key}={settings[key]}")
    sizes = json.loads(sql(args, "SELECT json_build_object('heap_bytes', pg_relation_size('usertable'), "
                          "'table_total_bytes', pg_total_relation_size('usertable'), "
                          "'database_bytes', pg_database_size(current_database()));"))
    return dict(settings=settings, sizes=sizes,
                keyspace=f"{args.db_rows}|{bounds}", indexes=json.loads(indexes),
                reset_output=result.stdout, cache_drop_output=cache.stdout)


def start_remote_ariabc_server(args, mode, workers):
    command = [f"{args.cluster_dir}/ariabc_pg/build/bin/ariabc_pg_server",
               "--id", "1", "--raftEndpoint", "127.0.0.1:9000", "--clientPort", str(args.server_port),
               "--raftMembers", "1=127.0.0.1:9000", "--dbName", "postgres", "--dbHost", "127.0.0.1",
               "--dbPort", str(args.db_port), "--dbUser", "postgres", "--dbType", "0" if mode == "pg" else "1",
               "--safedb", "0" if mode == "pg" else "1", "--dbConnPoolSize", str(workers), "--bypassRaft", "1"]
    environment = {"ARIABC_PROFILE": "1"}
    if mode != "pg":
        command += ["--pgExecMode", "event", "--bcdbInitBlockSize", str(workers)]
        environment.update(BCDB_DECOUPLE_WORKERS="1", BCDB_DET_QUEUE_HIGH_WM="65536",
                           BCDB_DET_QUEUE_LOW_WM="32768", ARIABC_DET_BLOCK_PARALLEL="64",
                           ARIABC_DET_BLOCK_PIPELINE="4", ARIABC_DET_BLOCK_MAX="2048",
                           ARIABC_DET_ORDER_START_SEQ="0", ARIABC_DET_PREFIXED_DIRECT_PARALLEL="1")
    exports = "\n".join(f"export {key}={value}" for key, value in environment.items())
    run_remote(args.remote_host, args.remote_user, db_shell(args) + exports + f"""
if fuser {args.server_port}/tcp >/dev/null 2>&1; then exit 1; fi
cd {args.remote_dir}
nohup {shlex.join(command)} </dev/null >{args.remote_dir}/server.log 2>&1 &
pid=$!
echo "$pid" > {args.remote_dir}/server.pid
for i in $(seq 1 100); do
    kill -0 "$pid"
    if fuser {args.server_port}/tcp >/dev/null 2>&1; then exit 0; fi
    sleep 0.2
done
cat {args.remote_dir}/server.log
exit 1
""", timeout=30)
    return dict(command=command, environment=environment)


def get_pg_io_stats(args):
    # These are PostgreSQL buffer misses, not necessarily physical device reads.
    return json.loads(sql(args, "SELECT row_to_json(s) FROM (SELECT d.blks_read, d.blks_hit, "
        "d.blk_read_time, d.blk_write_time, b.buffers_clean, b.buffers_backend, "
        "b.checkpoint_write_time, b.checkpoint_sync_time, b.checkpoints_req, "
        "b.buffers_checkpoint, b.buffers_alloc FROM pg_stat_database d CROSS JOIN pg_stat_bgwriter b "
        "WHERE d.datname=current_database()) s;"))


def parse_checkpoint_log(output):
    matches = re.findall(
        r"checkpoint complete:.*?write=([0-9.]+) s, sync=([0-9.]+) s, total=([0-9.]+) s; "
        r"sync files=(\d+), longest=([0-9.]+) s, average=([0-9.]+) s", output)
    if not matches:
        raise RuntimeError("Missing checkpoint write/sync timing evidence")
    w, s, t, files, longest, average = matches[-1]
    return dict(write_ms=float(w)*1000, sync_ms=float(s)*1000, total_ms=float(t)*1000,
                sync_files=int(files), longest_sync_ms=float(longest)*1000,
                average_sync_ms=float(average)*1000)


def get_remote_nvme_stats(args):
    # Measure the filesystem's block device (often a partition), not an assumed
    # nvme0n1. Device counters include unrelated traffic on that device.
    output = run_remote(args.remote_host, args.remote_user, f"""
major_minor=$(findmnt -n -r -o MAJ:MIN -T {args.remote_dir}/pgdata | tr -d '[:space:]')
readlink -f /sys/dev/block/"$major_minor"
cat /sys/dev/block/"$major_minor"/stat
""", timeout=15).stdout.splitlines()
    if len(output) != 2:
        raise RuntimeError("Unable to resolve database block device; tmpfs is unsupported")
    values = [int(v) for v in output[1].split()]
    if len(values) < 11:
        raise RuntimeError("Incomplete block-device statistics")
    return dict(device=output[0], read_ios=values[0], read_sectors=values[2], read_ms=values[3],
                write_ios=values[4], write_sectors=values[6], write_ms=values[7], io_ms=values[9])


def run_local_gateway_benchmark(args, workload_file, mode, workers, log_path):
    remote = args.gateway_host not in ("localhost", "127.0.0.1")
    binary = (f"{args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway" if remote else
              str(REPO_ROOT / "ariabc_pg/build/bin/ariabc_pg_gateway"))
    remote_wl = f"/tmp/oom_{hashlib.sha256(workload_file.read_bytes()).hexdigest()}.sql"
    command = [binary, "--nodes", f"{args.remote_host}:{args.server_port}", "--queryFrom",
               remote_wl if remote else str(workload_file), "--dbType", "0" if mode == "pg" else "1"]
    if mode == "pg":
        command += ["--submitLimit", "512", "--nondetWindow", "8"]
    else:
        command += ["--detStartSeq", "0", "--reqIdOffset", "1", "--detWindow", "65536",
                    "--detBatchSize", "256", "--dbConnPoolSize", str(workers), "--detSubmitPipeline", "1",
                    "--detPipelineDepth", "1024", "--detClientMode", "event", "--detClientWorkers", "96",
                    "--detClientInflight", "16", "--clientId", "single-gateway-direct"]
    command += ["--numTerminals", "96", "--submitMode", "event", "--connFanout", "1",
                "--waitMajority", "0", "--completionPath", "direct", "--totalNodes", "1"]
    timed_command = ["timeout", "--signal=TERM", "--kill-after=15", str(args.gateway_timeout), *command]
    start = time.monotonic()
    try:
        if remote:
            result = run_remote(args.gateway_host, args.gateway_user, shlex.join(timed_command),
                                timeout=args.gateway_timeout + 30, check=False)
        else:
            result = subprocess.run(timed_command, capture_output=True, text=True,
                                    timeout=args.gateway_timeout + 30)
    except subprocess.TimeoutExpired as exc:
        def decode(data):
            return data.decode(errors="replace") if isinstance(data, bytes) else (data or "")
        log_path.write_text(decode(exc.stdout) + "\n" + decode(exc.stderr))
        raise
    output = result.stdout + "\n" + result.stderr
    log_path.write_text(output)
    metrics = parse_gateway_result(output, count_workload_queries(workload_file), result.returncode, mode=mode)
    metrics.update(gateway_process_wall_ms=(time.monotonic() - start) * 1000, command=command)
    return metrics


def delta(before, after, key):
    value = after[key] - before[key]
    if value < 0:
        raise RuntimeError(f"Counter {key} reset during measurement")
    return value


def collect_logs(args, case_dir):
    for name in ("server.log", "postgres.log"):
        result = run_remote(args.remote_host, args.remote_user,
                            f"cat {args.remote_dir}/{name}", timeout=60, check=False)
        (case_dir / name).write_text(result.stdout + result.stderr)


def run_case(args, workload, skew, mode, workers, trial, workload_file, case_dir):
    case_dir.mkdir()
    started = time.monotonic()
    try:
        setup = reset_remote_pgdata(args, mode, workers)
        setup["server"] = start_remote_ariabc_server(args, mode, workers)
        (case_dir / "setup.json").write_text(json.dumps(setup, indent=2) + "\n")
        reset_ms = (time.monotonic() - started) * 1000
        if args.gateway_host not in ("localhost", "127.0.0.1"):
            digest = hashlib.sha256(workload_file.read_bytes()).hexdigest()
            subprocess.run(["scp", "-o", "BatchMode=yes", str(workload_file),
                            f"{args.gateway_user}@{args.gateway_host}:/tmp/oom_{digest}.sql"],
                           check=True, timeout=60)
        undo_dir = None
        if getattr(args, "reset_mode", "undo") == "undo":
            undo_dir = prepare_workload_undo(args, workload_file)
        # PG13 collector publishes asynchronously. Allow startup counters to
        # settle; after the run disconnect the server before the final snapshot.
        time.sleep(1)
        pg_before = get_pg_io_stats(args)
        device_before = get_remote_nvme_stats(args)
        measurement_start = time.monotonic()
        metrics = run_local_gateway_benchmark(args, workload_file, mode, workers, case_dir / "gateway.log")
        device_after = get_remote_nvme_stats(args)
        device_window_ms = (time.monotonic() - measurement_start) * 1000
        stop_server(args)
        time.sleep(1)
        # CHECKPOINT is measured separately, outside the TPS interval. It makes
        # deferred dirty heap/index writeback visible instead of counting only
        # writes that happened to reach the device during the short workload.
        checkpoint_pg_before = get_pg_io_stats(args)
        memory_before = run_remote(args.remote_host, args.remote_user,
                                   "cat /proc/meminfo", timeout=15).stdout
        checkpoint_before = get_remote_nvme_stats(args)
        checkpoint_start = time.monotonic()
        sql(args, "CHECKPOINT;", timeout=args.verify_timeout)
        checkpoint_ms = (time.monotonic() - checkpoint_start) * 1000
        checkpoint_after = get_remote_nvme_stats(args)
        memory_after = run_remote(args.remote_host, args.remote_user,
                                  "cat /proc/meminfo", timeout=15).stdout
        checkpoint_log = run_remote(args.remote_host, args.remote_user,
            f"tail -n 30 {args.remote_dir}/postgres.log", timeout=15).stdout
        (case_dir / "checkpoint.log").write_text(checkpoint_log)
        checkpoint = parse_checkpoint_log(checkpoint_log)
        (case_dir / "checkpoint.json").write_text(json.dumps(dict(
            **checkpoint, memory_before=memory_before, memory_after=memory_after,
            pg_before=checkpoint_pg_before), indent=2) + "\n")
        # BCDB's long-lived workers do not call pgstat_report_stat during their
        # loop. Clean PostgreSQL shutdown invokes pgstat_beshutdown_hook(true),
        # persisting their counters. Restart preserves those cumulative totals.
        # Do this BEFORE the verification scan, which must not enter I/O deltas.
        stop_postgres(args)
        start_postgres(args)
        pg_after = get_pg_io_stats(args)
        snapshots = dict(pg_before=pg_before, pg_after=pg_after, device_before=device_before,
                         device_after=device_after, checkpoint_before=checkpoint_before,
                         checkpoint_after=checkpoint_after)
        (case_dir / "io.json").write_text(json.dumps(snapshots, indent=2) + "\n")
        merkle = "N/A"
        if mode == "bcdb_merkle":
            merkle = sql(args, "SELECT merkle_verify('usertable');", timeout=args.verify_timeout)
            (case_dir / "merkle_verify.txt").write_text(merkle + "\n")
            if merkle != "t":
                raise RuntimeError(f"Merkle verification failed: {merkle}")
            merkle = "PASS"
        undo_restore_ms = 0.0
        if getattr(args, "reset_mode", "undo") == "undo" and undo_dir:
            undo_restore_ms = apply_workload_undo(args, undo_dir)
            (case_dir / "undo_restore_ms.txt").write_text(f"{undo_restore_ms:.2f} ms\n")
        read = delta(pg_before, pg_after, "blks_read")
        hits = delta(pg_before, pg_after, "blks_hit")
        if device_before["device"] != device_after["device"]:
            raise RuntimeError("Database block device changed")
        row = dict(workload=workload, skew=skew, mode=mode, workers=workers, trial=trial,
                   total_queries=metrics["total_queries"], shared_buffers=args.shared_buffers,
                   db_rows=args.db_rows, reset_time_ms=reset_ms,
                   undo_restore_ms=undo_restore_ms,
                   wall_time_ms=metrics["wall_time_ms"], wall_including_drains_ms=metrics["wall_including_drains_ms"],
                   tps=metrics["tps"], completed_tps=metrics["completed_tps"],
                   device=device_before["device"], device_window_ms=device_window_ms,
                   device_read_mib=delta(device_before, device_after, "read_sectors") / 2048,
                   device_write_mib=delta(device_before, device_after, "write_sectors") / 2048,
                   device_read_ios=delta(device_before, device_after, "read_ios"),
                   device_write_ios=delta(device_before, device_after, "write_ios"),
                   checkpoint_ms=checkpoint_ms,
                   checkpoint_write_ms=checkpoint["write_ms"],
                   checkpoint_sync_ms=checkpoint["sync_ms"],
                   checkpoint_total_ms=checkpoint["total_ms"],
                   checkpoint_sync_files=checkpoint["sync_files"],
                   checkpoint_longest_sync_ms=checkpoint["longest_sync_ms"],
                   checkpoint_write_mib=delta(checkpoint_before, checkpoint_after, "write_sectors") / 2048,
                   blks_read=read, blks_hit=hits, hit_ratio_pct=100 * hits / (hits + read) if hits + read else "",
                   blk_read_time_ms=delta(pg_before, pg_after, "blk_read_time"),
                   blk_write_time_ms=delta(pg_before, pg_after, "blk_write_time"),
                   buffers_backend=delta(pg_before, pg_after, "buffers_backend"),
                   divergence_count=metrics["divergence_count"], permanent_failures=metrics["permanent_failures"],
                   validated_completed_queries=metrics["validated_completed_queries"], merkle_verify=merkle,
                   workload_sha256=hashlib.sha256(workload_file.read_bytes()).hexdigest(),
                   artifact_dir=str(case_dir))
        (case_dir / "result.json").write_text(json.dumps(dict(row=row, gateway=metrics), indent=2) + "\n")
        return row
    except BaseException as exc:
        (case_dir / "FAILED.txt").write_text(str(exc) + "\n")
        raise
    finally:
        if sys.exc_info()[0] is not None:
            args._need_physical_reset = True
        # Run all cleanup operations, preserving the original failure and logs.

        cleanup_errors = []
        for action in (lambda: stop_server(args), lambda: stop_postgres(args), lambda: collect_logs(args, case_dir)):
            try:
                action()
            except Exception as exc:
                cleanup_errors.append(str(exc))
        if cleanup_errors:
            (case_dir / "cleanup_errors.txt").write_text("\n".join(cleanup_errors))
            if sys.exc_info()[0] is None:
                raise RuntimeError("Cleanup failed; see cleanup_errors.txt")

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name, default in (("remote-host", DEFAULT_REMOTE_HOST), ("remote-user", DEFAULT_REMOTE_USER),
                          ("remote-dir", DEFAULT_REMOTE_DIR), ("install-dir", DEFAULT_INSTALL_DIR),
                          ("cluster-dir", DEFAULT_CLUSTER_DIR), ("gateway-host", DEFAULT_GATEWAY_HOST),
                          ("gateway-user", DEFAULT_GATEWAY_USER), ("gateway-repo", DEFAULT_GATEWAY_REPO)):
        parser.add_argument("--" + name, default=default)
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--server-port", type=int, default=DEFAULT_SERVER_PORT)
    parser.add_argument("--db-rows", type=int, default=100000000)
    parser.add_argument("--shared-buffers", default="32MB")
    parser.add_argument("--txs", type=int, default=20000, help="SQL statements per case, as in the in-memory suite")
    parser.add_argument("--seed", type=int, default=42, help="Suite base seed; adds int(skew * 100)")
    parser.add_argument("--trials", type=int, default=1, help="Independent cold restores per case")
    parser.add_argument("--modes", nargs="+", default=["pg", "bcdb_det", "bcdb_merkle"])
    parser.add_argument("--workers", nargs="+", default=["1", "2", "4", "8", "16"])
    parser.add_argument("--skews", nargs="+", default=["0.0", "0.99"])
    parser.add_argument("--workloads", nargs="+", default=["a"])
    parser.add_argument("--output-dir", "--out-dir", dest="out_dir",
                        default="scripts/bench_full_results/oom_100m_sweep")
    parser.add_argument("--gateway-timeout", type=int, default=1800)
    parser.add_argument("--reset-timeout", type=int, default=3600)
    parser.add_argument("--verify-timeout", type=int, default=1800)
    parser.add_argument("--skip-gen", action="store_true", help="Require an existing clean pgdata_base")
    parser.add_argument("--gen-only", action="store_true")
    parser.add_argument("--reset-mode", choices=["undo", "cp"], default="undo",
                        help="Reset strategy: 'undo' uses fast logical before-image restore + cold cache restart; 'cp' uses full physical file copy")
    parser.add_argument("--verify-mode", choices=["fast", "full"], default="fast",
                        help="Validation mode on per-case resets: 'fast' performs pre-start copy integrity and post-start catalog/bound sanity checks; 'full' runs an exhaustive SELECT count(*) scan on every reset.")
    parser.add_argument("--dry-run", action="store_true", help="Generate workloads and manifest locally; do not contact servers")
    parser.add_argument("--preflight-only", action="store_true", help="Probe binaries, ports, storage and sudo; do not start databases")
    args = parser.parse_args(argv)
    for name, convert in (("modes", str), ("workers", int), ("skews", float), ("workloads", str)):
        try:
            values = [convert(v) for item in getattr(args, name) for v in item.split(",") if v]
        except ValueError:
            parser.error(f"Invalid --{name}")
        if not values or len(values) != len(set(values)):
            parser.error(f"--{name} must be nonempty and contain no duplicates")
        setattr(args, name, values)
    if set(args.modes) - {"pg", "bcdb_det", "bcdb_merkle"}:
        parser.error("--modes supports pg, bcdb_det, bcdb_merkle")
    if set(args.workloads) - set(SUPPORTED_WORKLOADS):
        parser.error("Supported large-keyspace workloads: " + ", ".join(SUPPORTED_WORKLOADS) +
                     "; custom DML key-recycling semantics require a separate large-keyspace generator")
    if any(not 0 <= v <= 2 for v in args.skews):
        parser.error("--skews must be finite values between 0 and 2, inclusive")
    if any(v < 1 or v > 128 for v in args.workers):
        parser.error("--workers must be between 1 and 128")
    for name in ("txs", "db_rows", "trials", "gateway_timeout", "reset_timeout", "verify_timeout"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    max_key = args.db_rows + args.txs
    if "all_insert" in args.workloads:
        max_key = args.db_rows + max(10, args.txs // 32) * args.txs * 2 + args.txs
    if max_key > 2147483647:
        parser.error("Requested keyspace can overflow the integer primary key")
    if not re.fullmatch(r"[1-9][0-9]*(?:kB|MB|GB)", args.shared_buffers):
        parser.error("--shared-buffers must be an integer followed by kB, MB, or GB")
    for name in ("remote_dir", "install_dir", "cluster_dir", "gateway_repo"):
        value = getattr(args, name)
        if not re.fullmatch(r"/[a-zA-Z0-9_./-]+", value) or ".." in Path(value).parts or len(Path(value).parts) < 3:
            parser.error(f"--{name.replace('_', '-')} must be a specific absolute path without shell metacharacters")
        setattr(args, name, value.rstrip("/"))
    for name in ("remote_host", "remote_user", "gateway_host", "gateway_user"):
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", getattr(args, name)):
            parser.error(f"Invalid --{name.replace('_', '-')}")
    if args.remote_host == args.gateway_host:
        parser.error("Use a separate gateway host for comparison with the suite")
    if any(not 1024 <= p <= 65535 for p in (args.db_port, args.server_port)) or len({args.db_port, args.server_port, 9000}) != 3:
        parser.error("Database, server, and Raft ports must be distinct unprivileged ports")
    return args


def write_report(out_dir, rows):
    import statistics
    lines = ["# Large-database cold-start benchmark", "",
             "Only completed, successful cases appear below. Merkle cases require full verification.",
             "TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.",
             "32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.",
             "Checkpoint writeback is measured separately after the timed workload. Units are MiB.", "",
             "| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |",
             "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    groups = {}
    for row in rows:
        key = tuple(row[k] for k in ("workload", "skew", "mode", "workers"))
        groups.setdefault(key, []).append(row)
    for key, values in sorted(groups.items()):
        tps = [r["tps"] for r in values]
        lines.append("| " + " | ".join(map(str, key)) +
                     f" | {len(values)} | {statistics.median(tps):.2f} | {min(tps):.2f} | {max(tps):.2f}"
                     f" | {statistics.median(r['device_read_mib'] for r in values):.2f} |")
    (out_dir / "REPORT.md").write_text("\n".join(lines) + "\n")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    for wl, skew in sorted({(r["workload"], r["skew"]) for r in rows}):
        fig, ax = plt.subplots(figsize=(8, 5))
        for mode in ("pg", "bcdb_det", "bcdb_merkle"):
            keys = sorted(k for k in groups if k[:3] == (wl, skew, mode))
            if keys:
                ax.plot([k[3] for k in keys], [statistics.median(r["tps"] for r in groups[k]) for k in keys],
                        marker="o", label=mode)
        ax.set(title=f"Cold-start YCSB {wl.upper()}, skew={skew}, rows={rows[0]['db_rows']:,}",
               xlabel="Server workers / PG connection pool", ylabel="SQL statements per second")
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(out_dir / f"scaling_{wl}_{skew}.png", dpi=160)
        plt.close(fig)


def main(argv=None):
    args = parse_args(argv)
    # Never mix old generator/configuration results or overwrite user artifacts.
    out_root = REPO_ROOT / args.out_dir
    out_dir = out_root / (datetime.datetime.now().strftime("run_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8])
    out_dir.mkdir(parents=True)
    print(f"Artifacts: {out_dir}", flush=True)
    manifest = dict(version=2, arguments=vars(args), cache_policy="cold_start_os_cache_unbounded",
                    timing="gateway_overall_time_taken", workloads={})
    manifest["source_sha256"] = {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in (Path(__file__).resolve(), REPO_ROOT / "scripts/generate_ycsb_workloads.py",
                                               REPO_ROOT / "scripts/distributed/sql/raft_apply_ledger_schema.sql",
                                               Path(__file__).with_name("large_zipf.py"),
                                               Path(__file__).with_name("benchmark_validation.py"))}
    files = {}
    for wl in args.workloads:
        for skew in args.skews:
            path = out_dir / "workloads" / f"ycsb_{wl}_skew_{skew}_{args.txs}.sql"
            generate_100m_ycsb_workload(path, wl, skew, args.txs, args.db_rows, args.seed)
            if count_workload_queries(path) != args.txs:
                raise RuntimeError("Generated statement count differs from --txs")
            files[wl, skew] = path
            manifest["workloads"][path.name] = dict(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                                    queries=args.txs)
    (out_dir / "campaign.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if args.dry_run:
        print(f"Dry run: generated {len(files)} workloads; no remote commands executed.")
        return
    # A directory lock is deliberately persistent after an unclean controller
    # death. Inspect its owner.txt before manually removing a stale lock.
    run_remote(args.remote_host, args.remote_user,
               f"mkdir -p {args.remote_dir}\nmkdir {args.remote_dir}/benchmark.lock\n"
               f"printf '%s\\n' {shlex.quote(str(out_dir))} > {args.remote_dir}/benchmark.lock/owner.txt")
    generating = False
    try:
        ensure_cache_drop_access(args)
        (out_dir / "preflight.txt").write_text(preflight(args))
        if args.preflight_only:
            print("Preflight passed; no database started or cache cleared.")
            return
        exists = check_remote_db_exists(args)
        if not exists:
            if args.skip_gen:
                raise RuntimeError("--skip-gen requires an existing clean pgdata_base")
            generating = True
            generate_remote_100m_database(args)
            generating = False
        if args.gen_only:
            return
        golden = validate_golden_baseline(args)
        args._golden_manifest = golden
        rows = []
        cases = []
        if getattr(args, "reset_mode", "undo") == "undo":
            # Group by mode (putting bcdb_merkle first) so that the Merkle index
            # present in pgdata_base is preserved across all Merkle runs, and then
            # dropped once for bcdb_det and pg. This completely avoids physical cp -a copies.
            ordered_modes = sorted(args.modes, key=lambda m: (0 if m == "bcdb_merkle" else (1 if m == "bcdb_det" else 2)))
            for mode in ordered_modes:
                for wl in args.workloads:
                    for skew in args.skews:
                        for workers in args.workers:
                            for trial in range(1, args.trials + 1):
                                cases.append((wl, skew, mode, workers, trial))
        else:
            for wl in args.workloads:
                for skew in args.skews:
                    for workers in args.workers:
                        for trial in range(1, args.trials + 1):
                            shift = (trial - 1) % len(args.modes)
                            for mode in args.modes[shift:] + args.modes[:shift]:
                                cases.append((wl, skew, mode, workers, trial))

        for wl, skew, mode, workers, trial in cases:
            name = f"{wl}_s{skew}_{mode}_w{workers}_t{trial}"
            print(f"Running {name}: pristine restore, validation, then cold start", flush=True)
            row = run_case(args, wl, skew, mode, workers, trial, files[wl, skew], out_dir / name)
            rows.append(row)
            with (out_dir / "summary.csv").open("a", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(row))
                if len(rows) == 1:
                    writer.writeheader()
                writer.writerow(row)
            print(f"  PASS: {row['tps']:.2f} TPS, {row['device_read_mib']:.2f} MiB read, "
                  f"Merkle={row['merkle_verify']}", flush=True)
            write_report(out_dir, rows)
        print(f"Completed {len(rows)} accepted cases. Results: {out_dir / 'summary.csv'}")
    finally:
        try:
            if generating:
                stop_postgres(args)
        finally:
            run_remote(args.remote_host, args.remote_user,
                       f"rm -f {args.remote_dir}/benchmark.lock/owner.txt\nrmdir {args.remote_dir}/benchmark.lock")


if __name__ == "__main__":
    main()
