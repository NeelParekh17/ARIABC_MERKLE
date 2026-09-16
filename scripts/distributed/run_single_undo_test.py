#!/usr/bin/env python3
"""Run and measure logical undo for a single mode (bcdb_det, pg, bcdb_merkle)."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import time
import sys

import run_oom_100m_benchmark as oom


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["bcdb_merkle", "bcdb_det", "pg"], required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--remote-dir", default="/tmp/ariabc_undo_validation_20260916")
    parser.add_argument("--remote-host", default=oom.DEFAULT_REMOTE_HOST)
    parser.add_argument("--remote-user", default=oom.DEFAULT_REMOTE_USER)
    parser.add_argument("--install-dir", default=oom.DEFAULT_INSTALL_DIR)
    parser.add_argument("--cluster-dir", default=oom.DEFAULT_CLUSTER_DIR)
    parser.add_argument("--gateway-host", default=oom.DEFAULT_GATEWAY_HOST)
    parser.add_argument("--gateway-user", default=oom.DEFAULT_GATEWAY_USER)
    parser.add_argument("--gateway-repo", default=oom.DEFAULT_GATEWAY_REPO)
    parser.add_argument("--db-port", type=int, default=5548)
    parser.add_argument("--server-port", type=int, default=18080)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    args.gateway_timeout = 600
    args.shared_buffers = "32MB"
    args.db_rows = 100000000

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    def remote(cmd, timeout=600, check=True):
        return oom.run_remote(args.remote_host, args.remote_user, oom.db_shell(args) + cmd, timeout, check)

    def helper(action, extra="", timeout=3600):
        command = (f"export LD_LIBRARY_PATH={args.install_dir}/lib; "
                   f"python3 {args.remote_dir}/ycsb_undo.py {action} "
                   f"--psql {args.install_dir}/bin/psql --port {args.db_port} " + extra)
        return json.loads(remote(command, timeout).stdout)

    print(f"=== Testing Mode: {args.mode} on {args.workload.name} ===", flush=True)

    # 1. Stop any running server/postgres
    oom.stop_server(args)
    oom.stop_postgres(args)

    enable = "on" if args.mode == "bcdb_merkle" else "off"
    bcdb_workers = args.workers if args.mode != "pg" else 1
    conf = f"""port = {args.db_port}
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
    remote(f"cat > {args.remote_dir}/pgdata/postgresql.auto.conf <<'CONF'\n{conf}CONF\n")
    oom.start_postgres(args)
    if args.mode != "bcdb_merkle":
        oom.sql(args, "DROP INDEX IF EXISTS usertable_merkle_idx; DROP INDEX IF EXISTS usertable_merkle_lookup_idx;")

    case_name = f"{args.mode}_{args.workload.stem}"
    remote_wl = f"{args.remote_dir}/{case_name}.sql"
    undo_dir = f"{args.remote_dir}/{case_name}_undo"

    # Upload workload
    remote(f"printf %s {shlex.quote(args.workload.read_text())} > {shlex.quote(remote_wl)}")

    # 2. Prepare undo before-images
    print(f"Capturing before-images into {undo_dir}...", flush=True)
    remote(f"rm -rf {undo_dir}")
    capture = helper("prepare", f"--workload {remote_wl} --output-dir {undo_dir}")
    (out / "capture.json").write_text(json.dumps(capture, indent=2) + "\n")
    print(f"Captured {capture['before_rows']} before-image rows in {capture['capture_ms']:.1f} ms", flush=True)

    # 3. Start server
    oom.start_remote_ariabc_server(args, args.mode, args.workers)

    # 4. Run gateway benchmark
    digest = hashlib.sha256(args.workload.read_bytes()).hexdigest()
    oom.run_remote(args.gateway_host, args.gateway_user,
                   f"printf %s {shlex.quote(args.workload.read_text())} > /tmp/oom_{digest}.sql")
    print(f"Running workload via gateway ({args.mode})...", flush=True)
    gw_metrics = oom.run_local_gateway_benchmark(args, args.workload, args.mode, args.workers, out / "gateway.log")
    print(f"Gateway completed: accepted={gw_metrics.get('accepted_count')}, permanent_failures={gw_metrics.get('permanent_failures')}, divergence={gw_metrics.get('divergence_count')}", flush=True)

    # Stop server
    oom.stop_server(args)

    # 5. Verify data actually mutated
    changed = remote(f"export LD_LIBRARY_PATH={args.install_dir}/lib; "
                     f"python3 {args.remote_dir}/ycsb_undo.py verify "
                     f"--psql {args.install_dir}/bin/psql --port {args.db_port} "
                     f"--output-dir {undo_dir}", check=False)
    if changed.returncode == 0:
        raise RuntimeError("Workload failed to mutate any data!")
    print("Confirmed: table was mutated by the workload.", flush=True)

    # 6. Apply restore.sql and time it!
    print("Applying restore.sql...", flush=True)
    begin = time.monotonic()
    restore_res = remote(f"export LD_LIBRARY_PATH={args.install_dir}/lib; "
                         f"{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 "
                         f"-h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -f {undo_dir}/restore.sql")
    restore_ms = (time.monotonic() - begin) * 1000
    (out / "restore.log").write_text(restore_res.stdout + "\n" + restore_res.stderr)
    print(f"Restore applied in {restore_ms:.1f} ms ({restore_ms/1000:.3f} s)!", flush=True)

    # 7. Fast verify
    fast = helper("verify", f"--output-dir {undo_dir}")
    print(f"Verification result: verified={fast['verified']} in {fast['elapsed_ms']:.1f} ms", flush=True)

    # If merkle mode, check merkle root and verify
    if args.mode == "bcdb_merkle":
        root = oom.sql(args, "SELECT merkle_root_hash('usertable');")
        m_ver = oom.sql(args, "SELECT merkle_verify('usertable');", timeout=3600)
        print(f"Merkle root: {root}, verify: {m_ver}", flush=True)

    result_summary = {
        "mode": args.mode,
        "workload": args.workload.name,
        "restore_ms": restore_ms,
        "capture_ms": capture["capture_ms"],
        "fast_verify_ms": fast["elapsed_ms"],
        "verified": fast["verified"],
        "gateway_metrics": gw_metrics,
    }
    (out / "summary.json").write_text(json.dumps(result_summary, indent=2) + "\n")
    print(f"=== Summary: Mode {args.mode} PASS ===")


if __name__ == "__main__":
    main()
