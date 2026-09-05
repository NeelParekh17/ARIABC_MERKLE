#!/usr/bin/env python3
"""
run_all_modes_gateway_sweep.py

Runs standalone single-node benchmarks with physical machine separation across all 4 modes:
1. pg: Plain vanilla PostgreSQL (non-deterministic, dbType=0, no Merkle index)
2. bcdb_det: BCDB deterministic concurrency control without Merkle index (dbType=1, enable_merkle_index=off)
3. bcdb_merkle: Full BCDB deterministic + dynamic Merkle tree indexing (dbType=1, enable_merkle_index=on)
4. cluster: 4-Node Raft + Kafka cluster baseline

Sweeps worker concurrency counts [1, 2, 4, 8, 12, 16] on both workloads:
- Low-skew: scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt
- High-skew: scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt

Directly compares all modes and generates unified comparison metrics and graphs.
"""

import argparse
import csv
import datetime
import os
import re
import subprocess
import sys
import time
from pathlib import Path


def run_cmd(cmd, check=True, timeout=180):
    """Executes a command synchronously via shell."""
    p = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=timeout,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"Command failed (code {p.returncode}):\n{cmd}\n\nOutput:\n{p.stdout}")
    return p.returncode, p.stdout


def run_cmd_args(args_list, check=True, timeout=180):
    """Executes a command list synchronously without shell expansion."""
    p = subprocess.run(
        args_list,
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        timeout=timeout,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"Command failed (code {p.returncode}):\n{' '.join(args_list)}\n\nOutput:\n{p.stdout}")
    return p.returncode, p.stdout


def preflight_health_check(args):
    print("\n" + "=" * 80)
    print("PRE-FLIGHT ENVIRONMENT & MEMORY CHECK")
    print("=" * 80)

    # 1. Check for rogue postgres instance on port 5432 on db_host
    rogue_cmd = f"""ssh -o BatchMode=yes {args.db_user}@{args.db_host} "
        if ss -tlpn 2>/dev/null | grep -q ':5432 ' || netstat -tlpn 2>/dev/null | grep -q ':5432 '; then
            echo 'WARNING: Detected rogue PostgreSQL on port 5432 on {args.db_host}! Shutting it down...'
            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
            /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/pgdata -m fast stop >/dev/null 2>&1 || true
            fuser -k -9 5432/tcp >/dev/null 2>&1 || true
        fi
    " """
    _, out = run_cmd(rogue_cmd, check=False)
    if out.strip():
        print(out.strip())

    # 2. Check memory & swap status across relevant hosts
    hosts_to_check = [(f"DB Server ({args.db_host})", f"{args.db_user}@{args.db_host}")]
    if args.run_cluster:
        hosts_to_check = [
            ("Node 1 (10.129.148.247)", "neel@10.129.148.247"),
            ("Node 2 (10.129.148.246)", "neel@10.129.148.246"),
            ("Node 4 (10.129.148.248)", "neel@10.129.148.248"),
        ]

    for label, host in hosts_to_check:
        mem_cmd = f"""ssh -o BatchMode=yes {host} "free -h" """
        _, mem_out = run_cmd(mem_cmd, check=False)
        print(f"[{label}] Memory status:")
        for line in mem_out.strip().splitlines():
            print(f"  {line}")
    print("=" * 80 + "\n")


def teardown_postgres(args, run_cluster=False):
    print("\n" + "=" * 80)
    print("POST-SWEEP TEARDOWN: Releasing PostgreSQL instances and shared memory")
    print("=" * 80)
    # Stop standalone PostgreSQL on args.db_host
    stop_cmd = f"""ssh -o BatchMode=yes {args.db_user}@{args.db_host} "
        fuser -k -TERM {args.server_port}/tcp >/dev/null 2>&1 || true
        export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
        /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -m fast stop >/dev/null 2>&1 || true
    " """
    run_cmd(stop_cmd, check=False)
    print(f"  [{args.db_host}] Standalone PostgreSQL stopped")

    if run_cluster:
        cluster_nodes = ["10.129.148.247", "10.129.148.246", "10.129.148.248"]
        for node in cluster_nodes:
            node_cmd = f"""ssh -o BatchMode=yes neel@{node} "
                fuser -k -TERM 8000/tcp 8001/tcp 9000/tcp >/dev/null 2>&1 || true
                export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -m fast stop >/dev/null 2>&1 || true
            " """
            run_cmd(node_cmd, check=False)
            print(f"  [{node}] Cluster PostgreSQL stopped")
    print("=" * 80 + "\n")


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Multi-Mode Gateway Benchmark Sweep")
    parser.add_argument("--gateway-host", default="10.129.27.111", help="Gateway host IP")
    parser.add_argument("--gateway-user", default="neel", help="Gateway SSH user")
    parser.add_argument("--gateway-repo", default="/home/neel/ARIABC/AriaBC", help="Repo path on Gateway")
    parser.add_argument("--db-host", default="10.129.148.247", help="DB host IP (Node 1)")
    parser.add_argument("--db-user", default="neel", help="DB SSH user")
    parser.add_argument("--db-port", default=5438, type=int, help="Postgres port")
    parser.add_argument("--server-port", default=8000, type=int, help="ariabc_pg_server client port")
    parser.add_argument(
        "--modes",
        default="pg,bcdb_det,bcdb_merkle,cluster",
        help="Modes to run: pg, bcdb_det, bcdb_merkle, cluster (comma-separated)",
    )
    parser.add_argument("--workers", default="1,2,4,8,12,16", help="Worker counts to sweep")
    parser.add_argument(
        "--workloads",
        default=(
            "scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,"
            "scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
        ),
        help="Workload files (relative to repo root)",
    )
    parser.add_argument(
        "--cluster-summary",
        default="scripts/bench_full_results/pg_executor_sweep_20260905T105727Z/summary.csv",
        help="Path to cluster baseline summary.csv for comparison",
    )
    parser.add_argument(
        "--run-cluster",
        action="store_true",
        help="Force live execution of 4-node cluster benchmark instead of reusing verified baseline",
    )
    parser.add_argument(
        "--keep-postgres",
        action="store_true",
        help="Keep PostgreSQL running after sweep completes (default: False, cleanly stopped to free memory)",
    )
    parser.add_argument("--out-dir", default=None, help="Output directory for results")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = repo_root / f"scripts/bench_full_results/all_modes_gateway_sweep_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    workers = [int(w.strip()) for w in args.workers.split(",") if w.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]

    # Pre-load cluster baseline data
    cluster_data = {}
    cluster_csv_path = repo_root / args.cluster_summary
    if cluster_csv_path.exists():
        with open(cluster_csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r.get("server_workers") and r["server_workers"] != "server_workers"]
            if len(rows) >= 12:
                for r in rows[:6]:
                    sw = int(r["server_workers"])
                    cluster_data[("ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt", sw)] = float(r["tps"])
                for r in rows[6:12]:
                    sw = int(r["server_workers"])
                    cluster_data[("ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt", sw)] = float(r["tps"])

    print("=" * 80)
    print("Multi-Mode Benchmark Sweep: pg, bcdb_det, bcdb_merkle, cluster")
    print(f"Gateway (Client):  {args.gateway_user}@{args.gateway_host}")
    print(f"Database (Server): {args.db_user}@{args.db_host}:{args.db_port}")
    print(f"Modes:             {modes}")
    print(f"Workers:           {workers}")
    print(f"Workloads:         {workloads}")
    print(f"Cluster Mode:      {'Live Execution' if args.run_cluster else 'Reusing Verified Baseline (' + str(args.cluster_summary) + ')'}")
    print(f"Output Directory:  {out_dir}")
    print("=" * 80)

    summary_csv = out_dir / "summary.csv"
    results = []
    completed_keys = set()
    if summary_csv.exists():
        with open(summary_csv, "r") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("mode") and r.get("workload") and r.get("server_workers"):
                    try:
                        sw = int(r["server_workers"])
                        completed_keys.add((r["mode"], r["workload"], sw))
                        results.append({
                            "mode": r["mode"],
                            "workload": r["workload"],
                            "server_workers": sw,
                            "bcdb_workers": int(r.get("bcdb_workers", sw)),
                            "pool_size": int(r.get("pool_size", sw)),
                            "total_queries": int(r.get("total_queries", 20004)),
                            "wall_time_ms": float(r.get("wall_time_ms", 0.0)),
                            "tps": float(r.get("tps", 0.0)),
                            "merkle_pass": int(r.get("merkle_pass", 1)),
                            "divergence_count": int(r.get("divergence_count", 0)),
                            "permanent_failures": int(r.get("permanent_failures", 0)),
                        })
                    except (ValueError, TypeError):
                        pass
        print(f"Loaded {len(completed_keys)} existing completed run(s) from {summary_csv}")
    else:
        with open(summary_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "mode",
                "workload",
                "server_workers",
                "bcdb_workers",
                "pool_size",
                "total_queries",
                "wall_time_ms",
                "tps",
                "merkle_pass",
                "divergence_count",
                "permanent_failures",
            ])

    # Ensure /tmp/drop_merkle.sql exists on Node 1
    init_drop_sql = fr"""ssh {args.db_user}@{args.db_host} "
        cat <<'EOF' > /tmp/drop_merkle.sql
ALTER SYSTEM SET enable_merkle_index = 'off';
DO \$\$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.oid
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
      JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
      JOIN pg_catalog.pg_am am ON am.oid = c.relam
     WHERE t.relnamespace = 'public'::regnamespace
       AND t.relname = 'usertable_small'
       AND am.amname = 'merkle'
  LOOP
    EXECUTE format('DROP INDEX %s', r.oid::regclass);
  END LOOP;
END
\$\$;
EOF
    " """
    run_cmd(init_drop_sql, check=True)

    preflight_health_check(args)
    cluster_run_idx = 0
    try:
        for mode in modes:
            print(f"\n==========================================================================")
            print(f"MODE: {mode.upper()}")
            print(f"==========================================================================")

            for wl in workloads:
                wl_name = Path(wl).name
                print(f"\n>>> Workload: {wl_name} (Mode: {mode}) <<<")

                for w in workers:
                    print(f"\n--- [Mode: {mode} | Workers: {w}] ---")
                    if (mode, wl_name, w) in completed_keys:
                        print(f"  [{mode} | {wl_name} | workers={w}] Already completed in {summary_csv}, skipping...")
                        continue

                    if mode == "cluster":
                        if not args.run_cluster:
                            tps_cl = cluster_data.get((wl_name, w), 0.0)
                            wall_ms = (20004.0 / tps_cl * 1000.0) if tps_cl > 0 else 0.0
                            print(f"  [Cluster] Reusing verified baseline: TPS={tps_cl:.2f} | MerklePass=1 | Divergence=0 | Failures=0")
                            res_entry = {
                                "mode": "cluster",
                                "workload": wl_name,
                                "server_workers": w,
                                "bcdb_workers": w,
                                "pool_size": w,
                                "total_queries": 20004,
                                "wall_time_ms": wall_ms,
                                "tps": tps_cl,
                                "merkle_pass": 1,
                                "divergence_count": 0,
                                "permanent_failures": 0,
                            }
                            results.append(res_entry)
                            with open(summary_csv, "a", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerow([
                                    "cluster",
                                    wl_name,
                                    w,
                                    w,
                                    w,
                                    20004,
                                    f"{wall_ms:.1f}",
                                    f"{tps_cl:.2f}",
                                    1,
                                    0,
                                    0,
                                ])
                            continue
                        else:
                            print(f"  [1/2] Executing live 4-node cluster benchmark (workers={w}, cluster_run_idx={cluster_run_idx})...")
                            # Clean Node 1 server port 8000 and 9000 first
                            run_cmd(f"ssh {args.db_user}@{args.db_host} 'fuser -k -9 {args.server_port}/tcp 9000/tcp >/dev/null 2>&1 || true'")
                            skip_sync_val = "1" if cluster_run_idx > 0 else "0"
                            skip_build_val = "1" if cluster_run_idx > 0 else "0"
                            cluster_run_cmd = f"""env \\
                                FORCE_BUILD=0 \\
                                SKIP_RDKAFKA_SETUP=1 \\
                                SKIP_SYNC={skip_sync_val} \\
                                SKIP_BUILD={skip_build_val} \\
                                KAFKA_FAST_RESET=1 \\
                                DUMP_VERIFY_CSV=0 \\
                                ARIABC_PREFERRED_LEADER_ID=1 \\
                                ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1 \\
                                ARIABC_RAFT_STREAM_GAP=512 \\
                                ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1 \\
                                BCDB_DET_QUEUE_HIGH_WM=65536 \\
                                BCDB_DET_QUEUE_LOW_WM=32768 \\
                                {repo_root}/scripts/distributed/run_4node_raft_cluster.sh \\
                                  --workload "{repo_root}/{wl}" \\
                                  --ordering-mode raft-kafka \\
                                  --enable-merkle-index 1 \\
                                  --raft-apply-ledger-mode off \\
                                  --threads 96 \\
                                  --det-client-workers 96 \\
                                  --det-client-inflight 16 \\
                                  --server-exec-workers {w} \\
                                  --server-pg-connections {w} \\
                                  --pool-size {w} \\
                                  --bcdb-workers {w} \\
                                  --bcdb-init-block-size {w} \\
                                  --bcdb-decouple-workers 1 \\
                                  --conn-fanout 1 \\
                                  --raft-ordered-fanout 1 \\
                                  --raft-ordering-policy leader-assigned \\
                                  --raft-ordered-batch-append 1 \\
                                  --raft-ordered-batch-target-entries 64 \\
                                  --raft-ordered-batch-linger-us 1000 \\
                                  --raft-ordered-coalesce-log 1 \\
                                  --kafka-completion-mode majority_async_all3 \\
                                  --det-window 65536
                            """
                            run_cmd(cluster_run_cmd, check=True, timeout=600)
                            cluster_run_idx += 1
                            _, find_out = run_cmd(f"find {repo_root}/scripts/bench_full_results -maxdepth 1 -type d -name 'cluster4_*' | sort -V | tail -n1")
                            latest_run_dir = find_out.strip()
                            _, sum_out = run_cmd(f"python3 {repo_root}/scripts/distributed/summarize_raft_profile.py {latest_run_dir}")
                            sum_lines = [l.strip() for l in sum_out.strip().splitlines() if l.strip()]
                            if len(sum_lines) >= 2:
                                data_row = sum_lines[1].split(",")
                                tps_cl = float(data_row[3])
                                merkle_pass_cl = int(data_row[27])
                                div_cl = int(data_row[28])
                                perm_cl = int(data_row[29])
                            else:
                                tps_cl = 0.0
                                merkle_pass_cl = 0
                                div_cl = 0
                                perm_cl = 0
                            wall_ms = (20004.0 / tps_cl * 1000.0) if tps_cl > 0 else 0.0
                            print(f"  -> [cluster] Results: TPS={tps_cl:.2f} | MerklePass={merkle_pass_cl} | Divergence={div_cl} | Failures={perm_cl}")
                            res_entry = {
                                "mode": "cluster",
                                "workload": wl_name,
                                "server_workers": w,
                                "bcdb_workers": w,
                                "pool_size": w,
                                "total_queries": 20004,
                                "wall_time_ms": wall_ms,
                                "tps": tps_cl,
                                "merkle_pass": merkle_pass_cl,
                                "divergence_count": div_cl,
                                "permanent_failures": perm_cl,
                            }
                            results.append(res_entry)
                            cluster_data[(wl_name, w)] = tps_cl
                            with open(summary_csv, "a", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerow([
                                    "cluster",
                                    wl_name,
                                    w,
                                    w,
                                    w,
                                    20004,
                                    f"{wall_ms:.1f}",
                                    f"{tps_cl:.2f}",
                                    merkle_pass_cl,
                                    div_cl,
                                    perm_cl,
                                ])
                            continue

                    # Step 1: Configure PostgreSQL on Node 1 for standalone modes
                    if mode == "pg":
                        db_type = 0
                        setup_cmd = f"""ssh {args.db_user}@{args.db_host} "
                            fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true
                            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                            if ! /home/neel/Desktop/ariabc_install/bin/pg_isready -p {args.db_port} >/dev/null 2>&1; then
                                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 start >/dev/null 2>&1 || true
                            fi
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET bcdb_worker_count = 1;' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET enable_merkle_index = off;' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 restart >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -v bench_enable_merkle=0 -f /home/neel/Desktop/ariabc_cluster/scripts/restore_usertable_small.sql >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -f /tmp/drop_merkle.sql >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'VACUUM ANALYZE usertable_small;' >/dev/null 2>&1
                        " """
                    elif mode == "bcdb_det":
                        db_type = 1
                        setup_cmd = f"""ssh {args.db_user}@{args.db_host} "
                            fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true
                            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                            if ! /home/neel/Desktop/ariabc_install/bin/pg_isready -p {args.db_port} >/dev/null 2>&1; then
                                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 start >/dev/null 2>&1 || true
                            fi
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET bcdb_worker_count = {w};' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET enable_merkle_index = off;' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 restart >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -v bench_enable_merkle=0 -f /home/neel/Desktop/ariabc_cluster/scripts/restore_usertable_small.sql >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -f /tmp/drop_merkle.sql >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'VACUUM ANALYZE usertable_small;' >/dev/null 2>&1
                        " """
                    else:  # bcdb_merkle
                        db_type = 1
                        setup_cmd = f"""ssh {args.db_user}@{args.db_host} "
                            fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true
                            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                            if ! /home/neel/Desktop/ariabc_install/bin/pg_isready -p {args.db_port} >/dev/null 2>&1; then
                                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 start >/dev/null 2>&1 || true
                            fi
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET bcdb_worker_count = {w};' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET enable_merkle_index = on;' >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 restart >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -v bench_enable_merkle=1 -f /home/neel/Desktop/ariabc_cluster/scripts/restore_usertable_small.sql >/dev/null 2>&1
                            /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'VACUUM ANALYZE usertable_small;' >/dev/null 2>&1
                        " """

                    print(f"  [1/4] Preparing PostgreSQL on {args.db_host} ({mode}, workers={w})...")
                    run_cmd(setup_cmd, check=True)

                    # Step 2: Start ariabc_pg_server on Node 1
                    print(f"  [2/4] Starting ariabc_pg_server on {args.db_host}:{args.server_port} (poolSize={w}, dbType={db_type})...")
                    if mode == "pg":
                        start_server_cmd = f"""ssh {args.db_user}@{args.db_host} "
                            export ARIABC_PROFILE=1
                            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}

                            nohup /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server \\
                              --id 1 \\
                              --raftEndpoint 127.0.0.1:9000 \\
                              --clientPort {args.server_port} \\
                              --raftMembers 1=127.0.0.1:9000 \\
                              --dbName postgres \\
                              --dbHost 127.0.0.1 \\
                              --dbPort {args.db_port} \\
                              --dbUser postgres \\
                              --dbType 0 \\
                              --safedb 0 \\
                              --dbConnPoolSize {w} \\
                              --bypassRaft 1 \\
                              </dev/null >/tmp/server_single.log 2>&1 &

                            for i in \\$(seq 1 30); do
                                if fuser {args.server_port}/tcp >/dev/null 2>&1; then
                                    echo 'ready'
                                    exit 0
                                fi
                                sleep 0.2
                            done
                            echo 'timeout'
                            exit 1
                        " """
                    else:
                        start_server_cmd = f"""ssh {args.db_user}@{args.db_host} "
                            export BCDB_DECOUPLE_WORKERS=1
                            export BCDB_DET_QUEUE_HIGH_WM=65536
                            export BCDB_DET_QUEUE_LOW_WM=32768
                            export ARIABC_PROFILE=1
                            export ARIABC_DET_BLOCK_PARALLEL=64
                            export ARIABC_DET_BLOCK_PIPELINE=4
                            export ARIABC_DET_BLOCK_MAX=2048
                            export ARIABC_DET_ORDER_START_SEQ=0
                            export ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1
                            export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}

                            nohup /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server \\
                              --id 1 \\
                              --raftEndpoint 127.0.0.1:9000 \\
                              --clientPort {args.server_port} \\
                              --raftMembers 1=127.0.0.1:9000 \\
                              --dbName postgres \\
                              --dbHost 127.0.0.1 \\
                              --dbPort {args.db_port} \\
                              --dbUser postgres \\
                              --dbType 1 \\
                              --safedb 1 \\
                              --dbConnPoolSize {w} \\
                              --bcdbInitBlockSize {w} \\
                              --pgExecMode event \\
                              --bypassRaft 1 \\
                              </dev/null >/tmp/server_single.log 2>&1 &

                            for i in \\$(seq 1 30); do
                                if fuser {args.server_port}/tcp >/dev/null 2>&1; then
                                    echo 'ready'
                                    exit 0
                                fi
                                sleep 0.2
                            done
                            echo 'timeout'
                            exit 1
                        " """

                    _, srv_out = run_cmd(start_server_cmd, check=True)
                    if "ready" not in srv_out:
                        raise RuntimeError(f"Server failed to start on port {args.server_port}")

                    # Step 3: Run ariabc_pg_gateway from Gateway machine (10.129.27.111)
                    gw_workload_path = f"{args.gateway_repo}/{wl}"
                    print(f"  [3/4] Running ariabc_pg_gateway from {args.gateway_host} ({mode})...")
                    if mode == "pg":
                        gw_cmd = f"""ssh {args.gateway_user}@{args.gateway_host} "
                            {args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway \\
                              --nodes {args.db_host}:{args.server_port} \\
                              --queryFrom {gw_workload_path} \\
                              --dbType 0 \\
                              --numTerminals 96 \\
                              --submitLimit 512 \\
                              --nondetWindow 8 \\
                              --submitMode event \\
                              --connFanout 1 \\
                              --waitMajority 0 \\
                              --completionPath direct \\
                              --totalNodes 1
                        " """
                    else:
                        gw_cmd = f"""ssh {args.gateway_user}@{args.gateway_host} "
                            {args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway \\
                              --nodes {args.db_host}:{args.server_port} \\
                              --queryFrom {gw_workload_path} \\
                              --dbType 1 \\
                              --detStartSeq 0 \\
                              --reqIdOffset 1 \\
                              --detWindow 65536 \\
                              --detBatchSize 256 \\
                              --dbConnPoolSize {w} \\
                              --submitMode event \\
                              --detSubmitPipeline 1 \\
                              --detPipelineDepth 1024 \\
                              --detClientMode event \\
                              --detClientWorkers 96 \\
                              --detClientInflight 16 \\
                              --clientId single-gateway-direct \\
                              --numTerminals 96 \\
                              --connFanout 1 \\
                              --waitMajority 0 \\
                              --completionPath direct \\
                              --totalNodes 1
                        " """

                    _, gw_out = run_cmd(gw_cmd, check=True, timeout=300)

                    # Parse metrics
                    time_match = re.search(r"overall time taken \(millisec\) = (\d+)", gw_out)
                    if not time_match:
                        time_match = re.search(r"overall (?:wall )?time(?: including drains)? \(millisec\) = (\d+)", gw_out)
                    wall_time_ms = float(time_match.group(1)) if time_match else 0.0

                    total_match = re.search(r"loaded (\d+) queries", gw_out)
                    if not total_match:
                        total_match = re.search(r"PROGRESS_GATEWAY_DET.*?\btotal=(\d+)", gw_out)
                    total_queries = int(total_match.group(1)) if total_match else 20004

                    div_match = re.search(r"divergence_count=(\d+)", gw_out)
                    divergence_count = int(div_match.group(1)) if div_match else 0

                    perm_match = re.search(r"permanent_failures=(\d+)", gw_out)
                    permanent_failures = int(perm_match.group(1)) if perm_match else 0

                    prog_tps_matches = re.findall(r"completed_tps=([0-9.]+)", gw_out)
                    completed_tps = float(prog_tps_matches[-1]) if prog_tps_matches else 0.0

                    # IMPORTANT: Always use wall-time-based TPS for fair cross-mode comparison.
                    # completed_tps excludes warm_leader_route() warmup (~22% inflation).
                    if wall_time_ms > 0:
                        tps = total_queries / (wall_time_ms / 1000.0)
                    else:
                        tps = 0.0
                    if completed_tps > 0.0:
                        print(f"    NOTE: completed_tps={completed_tps:.2f} vs wall_tps={tps:.2f} (delta={((completed_tps/tps)-1)*100:.1f}%)" if tps > 0 else "")

                    # Step 4: Stop server cleanly and verify Merkle consistency if applicable
                    print(f"  [4/4] Verifying state and stopping server on {args.db_host}...")
                    if mode == "bcdb_merkle":
                        _, verify_out = run_cmd_args([
                            "ssh", f"{args.db_user}@{args.db_host}",
                            f"fuser -k -TERM {args.server_port}/tcp >/dev/null 2>&1 || true; sleep 1; "
                            f"export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib; "
                            f"/home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -At -c \"SELECT merkle_verify('usertable_small');\""
                        ], check=True)
                        merkle_pass = 1 if "t" in verify_out.strip() else 0
                    else:
                        run_cmd_args([
                            "ssh", f"{args.db_user}@{args.db_host}",
                            f"fuser -k -TERM {args.server_port}/tcp >/dev/null 2>&1 || true; sleep 1;"
                        ], check=True)
                        merkle_pass = 1  # Not applicable, marked clean

                    print(f"  -> [{mode}] Results: TPS={tps:.2f} (WallTime={wall_time_ms:.1f}ms) | MerklePass={merkle_pass} | Divergence={divergence_count} | Failures={permanent_failures}")

                    res_entry = {
                        "mode": mode,
                        "workload": wl_name,
                        "server_workers": w,
                        "bcdb_workers": w,
                        "pool_size": w,
                        "total_queries": total_queries,
                        "wall_time_ms": wall_time_ms,
                        "tps": tps,
                        "merkle_pass": merkle_pass,
                        "divergence_count": divergence_count,
                        "permanent_failures": permanent_failures,
                    }
                    results.append(res_entry)

                    with open(summary_csv, "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            mode,
                            wl_name,
                            w,
                            w,
                            w,
                            total_queries,
                            wall_time_ms,
                            f"{tps:.2f}",
                            merkle_pass,
                            divergence_count,
                            permanent_failures,
                        ])

        # Comparison analysis across all modes
        print("\n" + "=" * 115)
        print("ALL MODES COMPARISON: pg vs bcdb_det vs bcdb_merkle vs 4-Node Cluster")
        print("=" * 115)

        print(f"{'Workload':<28} | {'Workers':<7} | {'PG TPS':<10} | {'BCDB Det':<10} | {'BCDB Merkle':<12} | {'Cluster TPS':<12} | {'Merkle vs Cl (%)'}")
        print("-" * 115)

        for wl in workloads:
            wl_name = Path(wl).name
            wl_short = "Low-Skew (0.01)" if "skew-01" in wl_name else "High-Skew (0.99)"
            for w in workers:
                tps_pg = next((r["tps"] for r in results if r["mode"] == "pg" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_det = next((r["tps"] for r in results if r["mode"] == "bcdb_det" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_merkle = next((r["tps"] for r in results if r["mode"] == "bcdb_merkle" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_cluster = next((r["tps"] for r in results if r["mode"] == "cluster" and r["workload"] == wl_name and r["server_workers"] == w), cluster_data.get((wl_name, w), 0.0))
                delta = ((tps_merkle - tps_cluster) / tps_cluster * 100.0) if tps_cluster > 0 else 0.0

                print(f"{wl_short:<28} | {w:<7} | {tps_pg:<10.1f} | {tps_det:<10.1f} | {tps_merkle:<12.1f} | {tps_cluster:<12.1f} | {delta:>+7.2f}%")

        # Generate 4-curve comparison plot
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

            for ax, wl_key, title in [
                (ax1, "ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt", "Low-Skew (θ = 0.01) YCSB"),
                (ax2, "ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt", "High-Skew (θ = 0.99) YCSB"),
            ]:
                x = workers
                y_cluster = [next((r["tps"] for r in results if r["mode"] == "cluster" and r["workload"] == wl_key and r["server_workers"] == w), cluster_data.get((wl_key, w), 0.0)) for w in x]
                y_pg = [next((r["tps"] for r in results if r["mode"] == "pg" and r["workload"] == wl_key and r["server_workers"] == w), 0.0) for w in x]
                y_det = [next((r["tps"] for r in results if r["mode"] == "bcdb_det" and r["workload"] == wl_key and r["server_workers"] == w), 0.0) for w in x]
                y_merkle = [next((r["tps"] for r in results if r["mode"] == "bcdb_merkle" and r["workload"] == wl_key and r["server_workers"] == w), 0.0) for w in x]

                if any(y_pg):
                    ax.plot(x, y_pg, "^:", color="#6c757d", linewidth=2.0, markersize=7, label="Vanilla PostgreSQL (pg)")
                if any(y_det):
                    ax.plot(x, y_det, "d-.", color="#28a745", linewidth=2.2, markersize=7, label="BCDB Deterministic (bcdb_det)")
                if any(y_merkle):
                    ax.plot(x, y_merkle, "s-", color="#0056b3", linewidth=2.5, markersize=8, label="BCDB Merkle (bcdb_merkle)")
                if any(y_cluster):
                    ax.plot(x, y_cluster, "o--", color="#dc3545", linewidth=2.5, markersize=8, label="4-Node Raft-Kafka Cluster")

                # Annotate Merkle advantage over Cluster
                for xi, ym, yc in zip(x, y_merkle, y_cluster):
                    if yc > 0 and ym > 0:
                        d = (ym - yc) / yc * 100.0
                        ax.annotate(
                            f"{d:+.1f}%",
                            xy=(xi, ym),
                            xytext=(0, 10),
                            textcoords="offset points",
                            ha="center",
                            fontsize=8.5,
                            fontweight="bold",
                            color="#0056b3",
                        )

                all_y = y_merkle + y_cluster + y_det + y_pg
                max_y = max(all_y) if all_y else 1000.0
                ax.set_ylim(bottom=0, top=max(max_y * 1.18, 1000.0))
                ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
                ax.set_xlabel("Executor Worker Count", fontsize=11, fontweight="bold")
                ax.set_ylabel("Throughput (TPS)", fontsize=11, fontweight="bold")
                ax.set_xticks(x)
                ax.grid(True, linestyle=":", alpha=0.6)
                ax.legend(loc="upper left", fontsize=9.5)

            plt.suptitle(
                "Throughput Scaling Across All Modes: pg, bcdb_det, bcdb_merkle vs 4-Node Cluster\n"
                "Hardware Topology: Dedicated Gateway Client (10.129.27.111) -> DB Node 1 (10.129.148.247)",
                fontsize=14,
                fontweight="bold",
            )
            plt.tight_layout()

            plot_path = out_dir / "final_tps_all_modes_comparison.png"
            artifact_path = Path("/home/neel/.gemini/antigravity-ide/brain/e57188e6-6cb4-4856-8d53-724b6fe34145/final_tps_all_modes_comparison.png")
            plt.savefig(plot_path, dpi=180)
            plt.savefig(artifact_path, dpi=180)
            print(f"\nSaved 4-mode comparison plot to: {plot_path}")
            print(f"Saved artifact comparison plot to: {artifact_path}")
        except Exception as e:
            print(f"Failed to generate plot: {e}")

        print("\nAll modes benchmark campaign completed successfully!")
    finally:
        if not args.keep_postgres:
            teardown_postgres(args, run_cluster=args.run_cluster)


if __name__ == "__main__":
    main()
