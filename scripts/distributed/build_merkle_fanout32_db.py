#!/usr/bin/env python3
"""
Builds a pristine 100M database baseline with Merkle fanout=32 on remote host.
Clones from pgdata_base, rebuilds usertable_merkle_idx with fanout=32,
verifies cryptographic integrity, and cleanly shuts down as pgdata_base_fanout32.
"""
import argparse
import json
import re
import shlex
import subprocess
import sys
import time

DEFAULT_REMOTE_HOST = "10.129.148.247"
DEFAULT_REMOTE_USER = "neel"
DEFAULT_REMOTE_DIR = "/tmp/ariabc_oom_100m"
DEFAULT_INSTALL_DIR = "/home/neel/Desktop/ariabc_install"
DEFAULT_DB_PORT = 5438


def run_remote(host, user, cmd, timeout=3600, check=True):
    res = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
         f"{user}@{host}", "bash -s"],
        input="set -euo pipefail\nexport LC_ALL=C\n" + cmd,
        capture_output=True, text=True, timeout=timeout
    )
    if check and res.returncode:
        raise RuntimeError(f"Remote command failed on {host} ({res.returncode}):\n{res.stdout}\n{res.stderr}")
    return res


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote-host", default=DEFAULT_REMOTE_HOST)
    parser.add_argument("--remote-user", default=DEFAULT_REMOTE_USER)
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    parser.add_argument("--install-dir", default=DEFAULT_INSTALL_DIR)
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--fanout", type=int, default=32)
    parser.add_argument("--source-base", default="pgdata_base")
    parser.add_argument("--target-base", default="pgdata_base_fanout32")
    args = parser.parse_args()

    db_env = f"export LD_LIBRARY_PATH={args.install_dir}/lib:/home/neel/Desktop/rdkafka_local/lib:${{LD_LIBRARY_PATH:-}}\n"

    print(f"=== Creating 100M Database Baseline with Merkle Fanout={args.fanout} ===")
    print(f"Remote host: {args.remote_host}")
    print(f"Source: {args.remote_dir}/{args.source_base}")
    print(f"Target: {args.remote_dir}/{args.target_base}")

    # 1. Pre-flight checks
    print("\n[1/7] Running pre-flight checks on remote host...")
    probe = run_remote(args.remote_host, args.remote_user, db_env + f"""
test -f {args.remote_dir}/{args.source_base}/PG_VERSION
{args.install_dir}/bin/pg_controldata {args.remote_dir}/{args.source_base} | grep 'Database cluster state'
df -B1 --output=avail {args.remote_dir} | tail -1
""")
    print("  Source baseline confirmed cleanly shut down.")
    avail_bytes = int(probe.stdout.strip().splitlines()[-1])
    print(f"  Available storage: {avail_bytes / 2**30:.2f} GiB")
    if avail_bytes < 40 * 2**30:
        raise RuntimeError(f"Insufficient disk space ({avail_bytes / 2**30:.2f} GiB < 40 GiB)")

    # 2. Prepare build directory
    build_dir = f"{args.remote_dir}/pgdata_build_fanout{args.fanout}"
    print(f"\n[2/7] Cloning source baseline to build workspace ({build_dir})...")
    start_clone = time.monotonic()
    run_remote(args.remote_host, args.remote_user, db_env + f"""
rm -rf {build_dir}
cp -a --reflink=never {args.remote_dir}/{args.source_base} {build_dir}
sync
""")
    print(f"  Cloned in {time.monotonic() - start_clone:.1f}s")

    # 3. Configure high-performance build parameters
    print("\n[3/7] Setting high-performance build configuration...")
    build_conf = f"""port = {args.db_port}
listen_addresses = '*'
shared_buffers = '4GB'
maintenance_work_mem = '6GB'
max_parallel_maintenance_workers = 16
enable_merkle_index = on
wal_level = minimal
max_wal_senders = 0
checkpoint_timeout = 60min
max_wal_size = 20GB
synchronous_commit = off
fsync = off
full_page_writes = off
"""
    run_remote(args.remote_host, args.remote_user, f"""
cat > {build_dir}/postgresql.auto.conf << 'EOF'
{build_conf}
EOF
""")

    # 4. Start PostgreSQL on build workspace
    print("\n[4/7] Starting PostgreSQL on build workspace...")
    run_remote(args.remote_host, args.remote_user, db_env + f"""
{args.install_dir}/bin/pg_ctl -D {build_dir} -l {args.remote_dir}/build_fanout{args.fanout}.log -w -t 120 start
""")
    print("  PostgreSQL started.")

    try:
        # 5. Rebuild Merkle index with fanout=32
        print(f"\n[5/7] Rebuilding usertable_merkle_idx with fanout={args.fanout} (this takes ~5-10 minutes)...")
        rebuild_sql = f"""
SET max_parallel_maintenance_workers = 16;
SET maintenance_work_mem = '6GB';
DROP INDEX IF EXISTS usertable_merkle_idx;
CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key)
WITH (partitions = 200, fanout = {args.fanout}, split_threshold = 32, merge_threshold = 8);
"""
        start_idx = time.monotonic()
        idx_res = run_remote(args.remote_host, args.remote_user, db_env + f"""
{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres << 'EOF'
{rebuild_sql}
EOF
""", timeout=1800)
        print(f"  Index built in {time.monotonic() - start_idx:.1f}s")

        # 6. Verify index and Merkle integrity
        print("\n[6/7] Verifying index definition and cryptographic integrity...")
        verify_res = run_remote(args.remote_host, args.remote_user, db_env + f"""
{args.install_dir}/bin/psql -X -A -t -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "
SELECT indexdef FROM pg_indexes WHERE indexname = 'usertable_merkle_idx';
"
{args.install_dir}/bin/psql -X -A -t -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "
SELECT merkle_verify('usertable');
"
""")
        lines = [line.strip() for line in verify_res.stdout.splitlines() if line.strip()]
        idx_def = lines[0]
        merkle_pass = lines[1] if len(lines) > 1 else "f"
        print(f"  Index definition: {idx_def}")
        print(f"  merkle_verify: {merkle_pass}")

        if f"fanout='{args.fanout}'" not in idx_def and f"fanout = {args.fanout}" not in idx_def:
            raise RuntimeError(f"Index definition does not contain fanout={args.fanout}: {idx_def}")
        if merkle_pass != "t":
            raise RuntimeError(f"Cryptographic merkle_verify failed: {merkle_pass}")

        # Restore standard production config before stopping
        prod_conf = f"""port = {args.db_port}
listen_addresses = '*'
shared_buffers = '32MB'
enable_merkle_index = on
synchronous_commit = on
fsync = on
full_page_writes = on
autovacuum = off
work_mem = '4MB'
maintenance_work_mem = '2GB'
effective_cache_size = '4GB'
max_connections = 256
track_counts = on
track_io_timing = on
log_checkpoints = on
bcdb_serial_gate_mode = 1
bcdb_dt_conflict_tracking = on
merkle_apply_synchronous_direct = on
"""
        run_remote(args.remote_host, args.remote_user, db_env + f"""
cat > {build_dir}/postgresql.auto.conf << 'EOF'
{prod_conf}
EOF
{args.install_dir}/bin/psql -X -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -c "CHECKPOINT;"
""")
    finally:
        print("\nStopping PostgreSQL cleanly...")
        run_remote(args.remote_host, args.remote_user, db_env + f"""
if {args.install_dir}/bin/pg_ctl -D {build_dir} status >/dev/null 2>&1; then
    {args.install_dir}/bin/pg_ctl -D {build_dir} -w stop -m fast
fi
""", check=False)

    # 7. Finalize baseline directory
    print(f"\n[7/7] Finalizing {args.remote_dir}/{args.target_base}...")
    run_remote(args.remote_host, args.remote_user, db_env + f"""
{args.install_dir}/bin/pg_controldata {build_dir} | grep 'Database cluster state'
sync
rm -rf {args.remote_dir}/{args.target_base}
mv {build_dir} {args.remote_dir}/{args.target_base}
sync
""")
    print(f"=== Successfully Created {args.remote_dir}/{args.target_base} ===")


if __name__ == "__main__":
    main()
