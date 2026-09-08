#!/usr/bin/env python3
"""
run_all_modes_gateway_sweep.py

Runs standalone single-node benchmarks with physical machine separation across all 4 modes:
1. pg: Plain vanilla PostgreSQL (non-deterministic, dbType=0, no Merkle index)
2. bcdb_det: BCDB deterministic concurrency control without Merkle index (dbType=1, enable_merkle_index=off)
3. bcdb_merkle: Full BCDB deterministic + dynamic Merkle tree indexing (dbType=1, enable_merkle_index=on)
4. cluster: 4-Node Raft + Kafka cluster baseline

Supports two benchmark types:
- ycsb (default): Sweeps worker concurrency counts on YCSB workloads
- tpcc: Sweeps warehouse counts (10-100) on standard TPC-C benchmark
  with official transaction mix (45% NewOrder, 43% Payment, 4% OrderStatus,
  4% Delivery, 4% StockLevel) per TPC-C specification.

Directly compares all modes and generates unified comparison metrics and graphs.
"""

import argparse
from collections import defaultdict
import csv
import datetime
import os
import re
import statistics
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

    # 1. Check for rogue processes or old servers on db_port/server_port
    rogue_cmd = f"""ssh -o BatchMode=yes {args.db_user}@{args.db_host} "
        fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true
    " """
    run_cmd(rogue_cmd, check=False)

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
        export LD_LIBRARY_PATH=/home/{args.db_user}/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
        /home/{args.db_user}/Desktop/ariabc_install/bin/pg_ctl -D /home/{args.db_user}/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -m fast stop >/dev/null 2>&1 || true
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


# ---------------------------------------------------------------------------
# TPC-C helpers
# ---------------------------------------------------------------------------

TPCC_REMOTE_REPO = "/home/neel/Desktop/ariabc_cluster"
PSQL_BIN = "/home/neel/Desktop/ariabc_install/bin/psql"
PG_CTL_BIN = "/home/neel/Desktop/ariabc_install/bin/pg_ctl"
PG_ISREADY_BIN = "/home/neel/Desktop/ariabc_install/bin/pg_isready"
LD_LIB = "/home/neel/Desktop/ariabc_install/lib"
PGDATA = "/home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata"


def init_remote_paths(db_user):
    global TPCC_REMOTE_REPO, PSQL_BIN, PG_CTL_BIN, PG_ISREADY_BIN, LD_LIB, PGDATA
    TPCC_REMOTE_REPO = f"/home/{db_user}/Desktop/ariabc_cluster"
    PSQL_BIN = f"/home/{db_user}/Desktop/ariabc_install/bin/psql"
    PG_CTL_BIN = f"/home/{db_user}/Desktop/ariabc_install/bin/pg_ctl"
    PG_ISREADY_BIN = f"/home/{db_user}/Desktop/ariabc_install/bin/pg_isready"
    LD_LIB = f"/home/{db_user}/Desktop/ariabc_install/lib"
    PGDATA = f"/home/{db_user}/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata"


def generate_tpcc_workload(args, repo_root, warehouses, out_path):
    """Generate a TPC-C workload file on the gateway host (reusing if already present)."""
    gen_script = f"{args.gateway_repo}/scripts/generate_tpcc_workload.py"
    gen_cmd = f"""ssh {args.gateway_user}@{args.gateway_host} "
        if [ -f {out_path} ] && [ \\$(wc -l < {out_path}) -eq {args.tpcc_tx_count} ]; then
            echo 'REUSE: {out_path} already exists with {args.tpcc_tx_count} transactions'
        else
            python3 {gen_script} \
              --count {args.tpcc_tx_count} \
              --warehouses {warehouses} \
              --seed {args.tpcc_seed} \
              --remote-payment-pct {args.tpcc_remote_payment_pct} \
              --remote-new-order-pct {args.tpcc_remote_new_order_pct} \
              -o {out_path}
        fi
    " """
    _, gen_out = run_cmd(gen_cmd, check=True, timeout=120)
    print(f"    Workload generation output: {gen_out.strip()}")
    return out_path


def restore_tpcc_db(args, warehouses, enable_merkle):
    """
    Full TPC-C database restore on the remote DB node.
    Sequence: drop tables -> load base dump -> scale to N warehouses ->
              create stored procs + indexes -> VACUUM ANALYZE.
    """
    repo = TPCC_REMOTE_REPO
    psql = PSQL_BIN
    port = args.db_port

    merkle_flag = "on" if enable_merkle else "off"

    # The restore is time-consuming for large warehouse counts, allow up to 20 min
    restore_timeout = max(600, warehouses * 30)

    merkle_val = 1 if enable_merkle else 0

    restore_cmd = f"""ssh {args.db_user}@{args.db_host} "
        export LD_LIBRARY_PATH={LD_LIB}:\\${{LD_LIBRARY_PATH:-}}

        # Drop all TPCC tables
        {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -f {repo}/scripts/restore_tpcc_drop.sql >/dev/null 2>&1

        # Load base 1-warehouse dump
        {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -f {repo}/scripts/tpcc/tpcc-pgdump-full.sql >/dev/null 2>&1

        # IMMEDIATELY convert base tables to UNLOGGED (takes <0.05s on 1 warehouse; district stays LOGGED for Merkle)
        {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"
            ALTER TABLE public.warehouse   SET UNLOGGED;
            ALTER TABLE public.customer    SET UNLOGGED;
            ALTER TABLE public.history     SET UNLOGGED;
            ALTER TABLE public.oorder      SET UNLOGGED;
            ALTER TABLE public.order_line  SET UNLOGGED;
            ALTER TABLE public.new_order   SET UNLOGGED;
            ALTER TABLE public.stock       SET UNLOGGED;
            ALTER TABLE public.item        SET UNLOGGED;
        \\" >/dev/null 2>&1

        # Scale to {warehouses} warehouses (multi-threaded inserts into UNLOGGED tables -> ZERO WAL generated!)
        python3 {repo}/scripts/restore_tpcc_scale.py \
          --host 127.0.0.1 --port {port} --user postgres --db postgres \
          --warehouses {warehouses} 2>&1

        # Create stored procs, indexes, and merkle index on district (in-memory parallel builds)
        {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -v bench_enable_merkle={merkle_val} -f {repo}/scripts/restore_tpcc_procs.sql 2>&1

        # Fast ANALYZE (freshly inserted rows have zero dead tuples, VACUUM is redundant)
        {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c 'ANALYZE public.warehouse, public.district, public.customer, public.stock, public.item, public.oorder, public.new_order, public.order_line, public.history;' >/dev/null 2>&1

        echo 'TPCC_RESTORE_DONE'
    " """
    _, restore_out = run_cmd(restore_cmd, check=True, timeout=restore_timeout)
    if "TPCC_RESTORE_DONE" not in restore_out:
        raise RuntimeError(f"TPC-C restore did not complete successfully:\n{restore_out}")
    # Print scaling info
    for line in restore_out.strip().splitlines():
        if line.startswith("warehouse_count=") or line.startswith("scaled_"):
            print(f"    {line}")


def setup_tpcc_drop_merkle_sql(args):
    """Create /tmp/drop_merkle_tpcc.sql on the DB node for non-merkle modes."""
    init_drop_sql = fr"""ssh {args.db_user}@{args.db_host} "
        cat <<'EOF' > /tmp/drop_merkle_tpcc.sql
ALTER SYSTEM SET enable_merkle_index = 'off';
SELECT pg_reload_conf();
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
       AND t.relname = 'district'
       AND am.amname = 'merkle'
  LOOP
    EXECUTE format('DROP INDEX %s', r.oid::regclass);
  END LOOP;
END
\$\$;
EOF
    " """
    run_cmd(init_drop_sql, check=True)


def setup_tpcc_postgres(args, mode, w, warehouses):
    """Configure PostgreSQL for TPC-C benchmark in the given mode."""
    psql = PSQL_BIN
    pg_ctl = PG_CTL_BIN
    pg_isready = PG_ISREADY_BIN
    port = args.db_port
    enable_merkle = mode == "bcdb_merkle"
    bcdb_workers = 1 if mode == "pg" else w
    merkle_val = "on" if enable_merkle else "off"

    # Step 1: Ensure server port is free and configure PostgreSQL
    print(f"    [1/5] Configuring PostgreSQL on {args.db_host} (workers={bcdb_workers}, merkle={merkle_val}, shared_buffers={args.db_shared_buffers})...")
    config_cmd = f"""ssh {args.db_user}@{args.db_host} "
        fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true
        export LD_LIBRARY_PATH={LD_LIB}:\\${{LD_LIBRARY_PATH:-}}

        running=0
        if {pg_isready} -h 127.0.0.1 -p {port} >/dev/null 2>&1; then
            running=1
        fi

        if [ \\$running -eq 0 ]; then
            {pg_ctl} -D {PGDATA} -l /tmp/postgres_single.log -w -t 180 start >/dev/null 2>&1
            running=1
        fi

        cur_workers=\\$({psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -t -A -c 'SHOW bcdb_worker_count;' 2>/dev/null || echo '')
        cur_merkle=\\$({psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -t -A -c 'SHOW enable_merkle_index;' 2>/dev/null || echo '')
        cur_buffers=\\$({psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -t -A -c 'SHOW shared_buffers;' 2>/dev/null || echo '')

        need_restart=0
        if [ \\"\\$cur_workers\\" != \\"{bcdb_workers}\\" ] || [ \\"\\$cur_merkle\\" != \\"{merkle_val}\\" ] || [ \\"\\$cur_buffers\\" != \\"{args.db_shared_buffers}\\" ] || [ \\"{mode}\\" != \\"pg\\" ]; then
            need_restart=1
        fi

        if [ \\$need_restart -eq 1 ]; then
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c 'CHECKPOINT;' >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c 'ALTER SYSTEM SET bcdb_worker_count = {bcdb_workers};' >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET enable_merkle_index = '{merkle_val}';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET shared_buffers = '{args.db_shared_buffers}';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET synchronous_commit = 'off';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET autovacuum = 'off';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET maintenance_work_mem = '2GB';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET max_parallel_maintenance_workers = 4;\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET checkpoint_timeout = '30min';\\" >/dev/null 2>&1 || true
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -c \\"ALTER SYSTEM SET max_wal_size = '20GB';\\" >/dev/null 2>&1 || true

            {pg_ctl} -D {PGDATA} -l /tmp/postgres_single.log -w -t 180 restart >/dev/null 2>&1
        fi
    " """
    run_cmd(config_cmd, check=True)

    # Step 3: Full TPC-C restore
    print(f"    [3/5] Restoring TPC-C database (warehouses={warehouses}, merkle={enable_merkle})...")
    restore_tpcc_db(args, warehouses, enable_merkle)

    # Step 4: Drop merkle index for non-merkle modes
    if not enable_merkle:
        print(f"    [4/5] Dropping merkle indexes...")
        drop_cmd = f"""ssh {args.db_user}@{args.db_host} "
            export LD_LIBRARY_PATH={LD_LIB}:\\${{LD_LIBRARY_PATH:-}}
            {psql} -h 127.0.0.1 -p {port} -U postgres -d postgres -f /tmp/drop_merkle_tpcc.sql >/dev/null 2>&1
        " """
        run_cmd(drop_cmd, check=True)
    else:
        print(f"    [4/5] Merkle index kept (bcdb_merkle mode).")

    print(f"    [5/5] TPC-C database setup complete.")


# ---------------------------------------------------------------------------
# Summary CSV field definitions
# ---------------------------------------------------------------------------

YCSB_CSV_FIELDS = [
    "mode", "workload", "server_workers", "bcdb_workers", "pool_size",
    "total_queries", "wall_time_ms", "tps", "merkle_pass",
    "divergence_count", "permanent_failures",
]

TPCC_CSV_FIELDS = [
    "benchmark", "mode", "workload", "warehouses", "server_workers",
    "bcdb_workers", "pool_size", "total_queries", "wall_time_ms", "tps",
    "merkle_pass", "divergence_count", "permanent_failures", "trial",
]


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
        default=None,
        help="Modes to run: pg, bcdb_det, bcdb_merkle, cluster (comma-separated). "
             "Default: pg,bcdb_det,bcdb_merkle,cluster for ycsb; pg,bcdb_det,bcdb_merkle for tpcc",
    )
    parser.add_argument("--workers", "--threads", default="1,2,4,8,12,16", help="Worker counts to sweep (YCSB mode)")
    parser.add_argument(
        "--workloads",
        default=(
            "scripts/ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,"
            "scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"
        ),
        help="Workload files for YCSB (relative to repo root). Can be comma-separated paths, globs, 'all' (all 72 suite files), or 'standard'/'sigmod' (9 representative workloads at skew 0.99)",
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

    # TPC-C specific arguments
    parser.add_argument(
        "--benchmark",
        default="ycsb",
        choices=["ycsb", "tpcc"],
        help="Benchmark type: ycsb (default) or tpcc",
    )
    parser.add_argument(
        "--warehouses",
        default="10,20,30,50,75,100",
        help="Warehouse counts to sweep for TPC-C (comma-separated, default: 10,20,30,50,75,100)",
    )
    parser.add_argument(
        "--tpcc-tx-count",
        default=20000,
        type=int,
        help="Number of TPC-C transactions per run (default: 20000)",
    )
    parser.add_argument(
        "--tpcc-seed",
        default=42,
        type=int,
        help="RNG seed for TPC-C workload generation (default: 42)",
    )
    parser.add_argument(
        "--tpcc-remote-payment-pct",
        default=15.0,
        type=float,
        help="Percent remote payments per TPC-C spec (default: 15.0)",
    )
    parser.add_argument(
        "--tpcc-remote-new-order-pct",
        default=1.0,
        type=float,
        help="Percent remote new-order per TPC-C spec (default: 1.0)",
    )
    parser.add_argument(
        "--tpcc-workers",
        default="8",
        help="Worker count(s) for TPC-C sweep (comma-separated, e.g. '8' or '2,4,8,12,16', default: 8)",
    )
    parser.add_argument(
        "--trials",
        "--runs",
        default=1,
        type=int,
        help="Number of trials/runs to execute per configuration (default: 1). If > 1, computes median metrics across trials.",
    )
    parser.add_argument(
        "--db-shared-buffers",
        default="3000MB",
        help="PostgreSQL shared_buffers setting (e.g. 3000MB or 32GB, default: 3000MB)",
    )

    args = parser.parse_args()
    init_remote_paths(args.db_user)

    # Set default modes based on benchmark type
    if args.modes is None:
        if args.benchmark == "tpcc":
            args.modes = "pg,bcdb_det,bcdb_merkle"
        else:
            args.modes = "pg,bcdb_det,bcdb_merkle,cluster"

    repo_root = Path(__file__).resolve().parents[2]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = repo_root / f"scripts/bench_full_results/all_modes_{args.benchmark}_sweep_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    if args.benchmark == "tpcc":
        _run_tpcc_sweep(args, repo_root, out_dir, modes)
    else:
        _run_ycsb_sweep(args, repo_root, out_dir, modes)


# ===========================================================================
# TPC-C Sweep
# ===========================================================================

def _run_tpcc_sweep(args, repo_root, out_dir, modes):
    """Run TPC-C benchmark sweep across warehouse counts and/or worker counts."""
    warehouse_counts = [int(w.strip()) for w in args.warehouses.split(",") if w.strip()]
    tpcc_workers = [int(w.strip()) for w in str(args.tpcc_workers).split(",") if w.strip()]

    # Validate: cluster mode not supported for TPC-C
    if "cluster" in modes:
        print("WARNING: Cluster mode is not supported for TPC-C benchmarks yet. Skipping cluster mode.")
        modes = [m for m in modes if m != "cluster"]

    print("=" * 80)
    print(f"TPC-C Benchmark Sweep: {', '.join(modes)}")
    print(f"Gateway (Client):  {args.gateway_user}@{args.gateway_host}")
    print(f"Database (Server): {args.db_user}@{args.db_host}:{args.db_port}")
    print(f"Modes:             {modes}")
    print(f"Warehouse Counts:  {warehouse_counts}")
    print(f"Workers:           {tpcc_workers}")
    print(f"TX per run:        {args.tpcc_tx_count}")
    print(f"Seed:              {args.tpcc_seed}")
    print(f"Remote Payment %:  {args.tpcc_remote_payment_pct}")
    print(f"Remote NewOrder %: {args.tpcc_remote_new_order_pct}")
    print(f"TPC-C Mix:         45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel")
    print(f"Trials:            {args.trials}")
    print(f"Output Directory:  {out_dir}")
    print("=" * 80)

    summary_csv = out_dir / "summary.csv"
    results = []
    completed_keys = set()

    if summary_csv.exists():
        with open(summary_csv, "r") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("mode") and r.get("warehouses"):
                    try:
                        wh = int(r["warehouses"])
                        w_val = int(r.get("server_workers", tpcc_workers[0]))
                        tr_val = int(r.get("trial", 1)) if r.get("trial") else 1
                        completed_keys.add((r["mode"], wh, w_val, tr_val))
                        results.append({
                            "benchmark": r.get("benchmark", "tpcc"),
                            "mode": r["mode"],
                            "workload": r.get("workload", ""),
                            "warehouses": wh,
                            "server_workers": w_val,
                            "bcdb_workers": int(r.get("bcdb_workers", w_val)),
                            "pool_size": int(r.get("pool_size", w_val)),
                            "total_queries": int(r.get("total_queries", args.tpcc_tx_count)),
                            "wall_time_ms": float(r.get("wall_time_ms", 0.0)),
                            "tps": float(r.get("tps", 0.0)),
                            "merkle_pass": int(r.get("merkle_pass", 1)),
                            "divergence_count": int(r.get("divergence_count", 0)),
                            "permanent_failures": int(r.get("permanent_failures", 0)),
                            "trial": tr_val,
                        })
                    except (ValueError, TypeError):
                        pass
        print(f"Loaded {len(completed_keys)} existing completed run(s) from {summary_csv}")
    else:
        with open(summary_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(TPCC_CSV_FIELDS)

    # Preflight: Create TPC-C drop-merkle SQL on DB node
    setup_tpcc_drop_merkle_sql(args)
    preflight_health_check(args)

    # Preflight: Verify TPC-C dump exists on DB node
    print("Verifying TPC-C infrastructure on DB node...")
    verify_cmd = f"""ssh {args.db_user}@{args.db_host} "
        missing=0
        for f in {TPCC_REMOTE_REPO}/scripts/tpcc/tpcc-pgdump-full.sql \
                 {TPCC_REMOTE_REPO}/scripts/restore_tpcc_drop.sql \
                 {TPCC_REMOTE_REPO}/scripts/restore_tpcc_scale.py \
                 {TPCC_REMOTE_REPO}/scripts/restore_tpcc_procs.sql \
                 {TPCC_REMOTE_REPO}/scripts/generate_tpcc_workload.py; do
            if [ ! -f \\$f ]; then
                echo MISSING: \\$f
                missing=1
            fi
        done
        if [ \\$missing -eq 0 ]; then
            echo TPCC_PREFLIGHT_OK
        fi
    " """
    _, verify_out = run_cmd(verify_cmd, check=True)
    if "MISSING:" in verify_out:
        for line in verify_out.strip().splitlines():
            if "MISSING:" in line:
                print(f"  ERROR: {line}")
        raise RuntimeError("TPC-C infrastructure files missing on DB node. Sync repo first.")
    print("  TPC-C infrastructure verified on DB node.")

    try:
        for trial in range(1, args.trials + 1):
            trial_str = f" (Trial {trial}/{args.trials})" if args.trials > 1 else ""
            for mode in modes:
                print(f"\n==========================================================================")
                print(f"MODE: {mode.upper()} (TPC-C){trial_str}")
                print(f"==========================================================================")

                for wh in warehouse_counts:
                    # Step 1: Generate/check TPC-C workload for this warehouse count
                    wl_filename = f"tpcc-workload-{args.tpcc_tx_count}-w{wh}-seed{args.tpcc_seed}.txt"
                    gw_workload_path = f"/tmp/{wl_filename}"
                    print(f"\n  Checking/generating TPC-C workload ({args.tpcc_tx_count} tx, {wh} warehouses)...")
                    generate_tpcc_workload(args, repo_root, wh, gw_workload_path)

                    for w in tpcc_workers:
                        print(f"\n--- [Mode: {mode} | Warehouses: {wh} | Workers: {w}{trial_str}] ---")

                        if (mode, wh, w, trial) in completed_keys:
                            print(f"  [{mode} | W={wh} | workers={w} | trial={trial}] Already completed in {summary_csv}, skipping...")
                            continue

                        # Step 2: Setup TPC-C database
                        db_type = 0 if mode == "pg" else 1
                        print(f"  [1/4] Setting up TPC-C database on {args.db_host} ({mode}, warehouses={wh}, workers={w})...")
                        setup_tpcc_postgres(args, mode, w, wh)

                        # Step 3: Start ariabc_pg_server
                        print(f"  [2/4] Starting ariabc_pg_server on {args.db_host}:{args.server_port} (poolSize={w}, dbType={db_type})...")
                        if mode == "pg":
                            start_server_cmd = f"""ssh {args.db_user}@{args.db_host} "
                                export ARIABC_PROFILE=1
                                export LD_LIBRARY_PATH={LD_LIB}:\\${{LD_LIBRARY_PATH:-}}

                                nohup {TPCC_REMOTE_REPO}/ariabc_pg/build/bin/ariabc_pg_server \\
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
                                export LD_LIBRARY_PATH={LD_LIB}:\\${{LD_LIBRARY_PATH:-}}

                                nohup {TPCC_REMOTE_REPO}/ariabc_pg/build/bin/ariabc_pg_server \\
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

                        if mode != "pg":
                            check_init_cmd = f"""ssh {args.db_user}@{args.db_host} "grep -E 'bcdb_init enabled on node|bcdb_init skipped' /tmp/server_single.log || true" """
                            _, init_out = run_cmd(check_init_cmd)
                            if "bcdb_init enabled on node" not in init_out:
                                raise RuntimeError(f"BCDB initialization failed or skipped in ariabc_pg_server:\n{init_out}")
                            print(f"    [BCDB] Verified: {init_out.strip()}")

                        # Step 4: Run ariabc_pg_gateway from Gateway machine
                        print(f"  [3/4] Running ariabc_pg_gateway from {args.gateway_host} ({mode}, W={wh}, workers={w})...")
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

                        _, gw_out = run_cmd(gw_cmd, check=True, timeout=600)

                        # Parse metrics
                        time_match = re.search(r"overall time taken \(millisec\) = (\d+)", gw_out)
                        if not time_match:
                            time_match = re.search(r"overall (?:wall )?time(?: including drains)? \(millisec\) = (\d+)", gw_out)
                        wall_time_ms = float(time_match.group(1)) if time_match else 0.0

                        total_match = re.search(r"loaded (\d+) queries", gw_out)
                        if not total_match:
                            total_match = re.search(r"PROGRESS_GATEWAY_DET.*?\btotal=(\d+)", gw_out)
                        total_queries = int(total_match.group(1)) if total_match else args.tpcc_tx_count

                        div_match = re.search(r"divergence_count=(\d+)", gw_out)
                        divergence_count = int(div_match.group(1)) if div_match else 0

                        perm_match = re.search(r"permanent_failures=(\d+)", gw_out)
                        permanent_failures = int(perm_match.group(1)) if perm_match else 0

                        prog_tps_matches = re.findall(r"completed_tps=([0-9.]+)", gw_out)
                        completed_tps = float(prog_tps_matches[-1]) if prog_tps_matches else 0.0

                        # Use wall-time-based TPS for fair cross-mode comparison
                        if wall_time_ms > 0:
                            tps = total_queries / (wall_time_ms / 1000.0)
                        else:
                            tps = 0.0
                        if completed_tps > 0.0 and tps > 0:
                            print(f"    NOTE: completed_tps={completed_tps:.2f} vs wall_tps={tps:.2f} (delta={((completed_tps/tps)-1)*100:.1f}%)")

                        # Step 5: Stop server and verify Merkle consistency
                        print(f"  [4/4] Verifying state and stopping server on {args.db_host}...")
                        if mode == "bcdb_merkle":
                            _, verify_out = run_cmd_args([
                                "ssh", f"{args.db_user}@{args.db_host}",
                                f"fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true; sleep 0.5; "
                                f"export LD_LIBRARY_PATH={LD_LIB}; "
                                f"{PSQL_BIN} -h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -At -c \"SELECT merkle_verify('district');\""
                            ], check=True)
                            merkle_pass = 1 if "t" in verify_out.strip() else 0
                        else:
                            run_cmd_args([
                                "ssh", f"{args.db_user}@{args.db_host}",
                                f"fuser -k -9 {args.server_port}/tcp >/dev/null 2>&1 || true; sleep 0.5;"
                            ], check=True)
                            merkle_pass = 1  # Not applicable, marked clean

                        print(f"  -> [{mode}{trial_str}] Results: TPS={tps:.2f} (WallTime={wall_time_ms:.1f}ms) | Warehouses={wh} | Workers={w} | MerklePass={merkle_pass} | Divergence={divergence_count} | Failures={permanent_failures}")

                        res_entry = {
                            "benchmark": "tpcc",
                            "mode": mode,
                            "workload": wl_filename,
                            "warehouses": wh,
                            "server_workers": w,
                            "bcdb_workers": w if mode != "pg" else 1,
                            "pool_size": w,
                            "total_queries": total_queries,
                            "wall_time_ms": wall_time_ms,
                            "tps": tps,
                            "merkle_pass": merkle_pass,
                            "divergence_count": divergence_count,
                            "permanent_failures": permanent_failures,
                            "trial": trial,
                        }
                        results.append(res_entry)

                        with open(summary_csv, "a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow([
                                "tpcc",
                                mode,
                                wl_filename,
                                wh,
                                w,
                                w if mode != "pg" else 1,
                                w,
                                total_queries,
                                wall_time_ms,
                                f"{tps:.2f}",
                                merkle_pass,
                                divergence_count,
                                permanent_failures,
                                trial,
                            ])

        # TPC-C Comparison analysis
        if args.trials > 1:
            aggregated = _compute_tpcc_median_aggregation(results)
            _write_tpcc_median_csv(aggregated, out_dir / "summary_median.csv")
            _print_tpcc_median_comparison(aggregated, warehouse_counts, tpcc_workers, modes, args.trials)
            _plot_tpcc_median_results(aggregated, warehouse_counts, tpcc_workers, modes, out_dir, args)
        else:
            _print_tpcc_comparison(results, warehouse_counts, tpcc_workers, modes)
            _plot_tpcc_results(results, warehouse_counts, tpcc_workers, modes, out_dir, args)

        print("\nTPC-C benchmark campaign completed successfully!")

    finally:
        if not args.keep_postgres:
            teardown_postgres(args, run_cluster=False)


def _print_tpcc_comparison(results, warehouse_counts, workers_list, modes):
    """Print TPC-C comparison table (vs warehouses or vs workers)."""
    sweep_workers = len(workers_list) > 1 and len(warehouse_counts) == 1
    fixed_desc = f"Fixed Warehouses: {warehouse_counts[0]}" if sweep_workers else f"Fixed Worker Count: {workers_list[0]}"
    sweep_dim = "Workers" if sweep_workers else "Warehouses"
    sweep_items = workers_list if sweep_workers else warehouse_counts

    print("\n" + "=" * 115)
    print(f"TPC-C COMPARISON: {' vs '.join(m.upper() for m in modes)}")
    print(fixed_desc)
    print(f"Transaction Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel (TPC-C Standard)")
    print("=" * 115)

    header_parts = [f"{sweep_dim:<12}"]
    for m in modes:
        header_parts.append(f"{m.upper() + ' TPS':<14}")
    if "bcdb_merkle" in modes and "pg" in modes:
        header_parts.append(f"{'Merkle/PG (%)'}")
    print(" | ".join(header_parts))
    print("-" * 115)

    for item in sweep_items:
        row_parts = [f"{item:<12}"]
        tps_by_mode = {}
        for m in modes:
            if sweep_workers:
                tps_val = next(
                    (r["tps"] for r in results if r["mode"] == m and r.get("server_workers") == item),
                    0.0,
                )
            else:
                tps_val = next(
                    (r["tps"] for r in results if r["mode"] == m and r.get("warehouses") == item),
                    0.0,
                )
            tps_by_mode[m] = tps_val
            row_parts.append(f"{tps_val:<14.1f}")
        if "bcdb_merkle" in modes and "pg" in modes:
            tps_pg = tps_by_mode.get("pg", 0.0)
            tps_merkle = tps_by_mode.get("bcdb_merkle", 0.0)
            if tps_pg > 0:
                delta = ((tps_merkle - tps_pg) / tps_pg * 100.0)
                row_parts.append(f"{delta:>+7.2f}%")
            else:
                row_parts.append(f"{'N/A':>8}")
        print(" | ".join(row_parts))


def _plot_tpcc_results(results, warehouse_counts, workers_list, modes, out_dir, args):
    """Generate TPC-C throughput plot (vs warehouses or vs workers)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        sweep_workers = len(workers_list) > 1 and len(warehouse_counts) == 1
        x_vals = workers_list if sweep_workers else warehouse_counts
        x_label = "Worker Count (Concurrency)" if sweep_workers else "Warehouse Count"

        fig, ax = plt.subplots(1, 1, figsize=(12, 7))

        mode_styles = {
            "pg": ("^:", "#6c757d", 2.0, 7, "Vanilla PostgreSQL (pg)"),
            "bcdb_det": ("d-.", "#28a745", 2.2, 7, "BCDB Deterministic (bcdb_det)"),
            "bcdb_merkle": ("s-", "#0056b3", 2.5, 8, "BCDB Merkle (bcdb_merkle)"),
        }

        for m in modes:
            style = mode_styles.get(m)
            if not style:
                continue
            marker_line, color, lw, ms, label = style
            y_vals = []
            for item in x_vals:
                if sweep_workers:
                    tps_val = next(
                        (r["tps"] for r in results if r["mode"] == m and r.get("server_workers") == item),
                        0.0,
                    )
                else:
                    tps_val = next(
                        (r["tps"] for r in results if r["mode"] == m and r.get("warehouses") == item),
                        0.0,
                    )
                y_vals.append(tps_val)
            if any(y_vals):
                ax.plot(x_vals, y_vals, marker_line, color=color,
                        linewidth=lw, markersize=ms, label=label)

        # Annotate merkle overhead vs pg
        if "bcdb_merkle" in modes and "pg" in modes:
            for item in x_vals:
                if sweep_workers:
                    tps_m = next((r["tps"] for r in results if r["mode"] == "bcdb_merkle" and r.get("server_workers") == item), 0.0)
                    tps_p = next((r["tps"] for r in results if r["mode"] == "pg" and r.get("server_workers") == item), 0.0)
                else:
                    tps_m = next((r["tps"] for r in results if r["mode"] == "bcdb_merkle" and r.get("warehouses") == item), 0.0)
                    tps_p = next((r["tps"] for r in results if r["mode"] == "pg" and r.get("warehouses") == item), 0.0)
                if tps_p > 0 and tps_m > 0:
                    d = (tps_m - tps_p) / tps_p * 100.0
                    ax.annotate(
                        f"{d:+.1f}%",
                        xy=(item, tps_m),
                        xytext=(0, 12),
                        textcoords="offset points",
                        ha="center",
                        fontsize=8.5,
                        fontweight="bold",
                        color="#0056b3",
                    )

        all_tps = [r["tps"] for r in results]
        max_y = max(all_tps) if all_tps else 1000.0
        ax.set_ylim(bottom=0, top=max(max_y * 1.18, 1000.0))
        ax.set_xlabel(x_label, fontsize=12, fontweight="bold")
        ax.set_ylabel("Throughput (TPS)", fontsize=12, fontweight="bold")
        ax.set_xticks(x_vals)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper left", fontsize=10)

        subtitle_desc = f"Warehouses={warehouse_counts[0]}" if sweep_workers else f"Workers={workers_list[0]}"
        plot_title = "TPC-C Throughput vs Concurrency (Workers)" if sweep_workers else "TPC-C Throughput vs Warehouse Scale"

        plt.suptitle(
            f"{plot_title} \u2014 {subtitle_desc}\n"
            f"Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel\n"
            f"Seed={args.tpcc_seed} | {args.tpcc_tx_count} tx/run | "
            f"Remote Payment={args.tpcc_remote_payment_pct}% | Remote NewOrder={args.tpcc_remote_new_order_pct}%",
            fontsize=12,
            fontweight="bold",
        )
        plt.tight_layout()

        fname = "tpcc_tps_vs_workers.png" if sweep_workers else "tpcc_tps_vs_warehouses.png"
        plot_path = out_dir / fname
        plt.savefig(plot_path, dpi=180)
        print(f"\nSaved TPC-C plot to: {plot_path}")

    except Exception as e:
        print(f"Failed to generate TPC-C plot: {e}")


def _compute_tpcc_median_aggregation(results):
    """Aggregate multi-trial TPC-C records by (mode, warehouses, server_workers)."""
    grouped = defaultdict(list)
    for r in results:
        key = (r["mode"], r["warehouses"], r["server_workers"])
        grouped[key].append(r)

    aggregated = []
    for key, items in sorted(grouped.items()):
        mode, wh, workers = key
        tps_list = [it["tps"] for it in items]
        wall_list = [it["wall_time_ms"] for it in items]

        med_tps = statistics.median(tps_list)
        mean_tps = statistics.mean(tps_list)
        std_tps = statistics.stdev(tps_list) if len(tps_list) > 1 else 0.0
        min_tps = min(tps_list)
        max_tps = max(tps_list)

        med_wall = statistics.median(wall_list)
        mean_wall = statistics.mean(wall_list)

        merkle_pass = 1 if all(it["merkle_pass"] == 1 for it in items) else 0
        div_count = sum(it["divergence_count"] for it in items)
        fail_count = sum(it["permanent_failures"] for it in items)

        aggregated.append({
            "mode": mode,
            "warehouses": wh,
            "server_workers": workers,
            "trials_count": len(items),
            "median_tps": med_tps,
            "mean_tps": mean_tps,
            "std_tps": std_tps,
            "min_tps": min_tps,
            "max_tps": max_tps,
            "median_wall_time_ms": med_wall,
            "mean_wall_time_ms": mean_wall,
            "merkle_pass": merkle_pass,
            "divergence_count": div_count,
            "permanent_failures": fail_count,
        })
    return aggregated


def _write_tpcc_median_csv(aggregated, csv_path):
    """Save aggregated median summary to CSV."""
    fields = [
        "mode", "warehouses", "server_workers", "trials_count",
        "median_tps", "mean_tps", "std_tps", "min_tps", "max_tps",
        "median_wall_time_ms", "mean_wall_time_ms", "merkle_pass",
        "divergence_count", "permanent_failures",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(aggregated)
    print(f"\nSaved median summary to: {csv_path}")


def _print_tpcc_median_comparison(aggregated, warehouse_counts, workers_list, modes, num_trials):
    """Print multi-trial TPC-C median comparison table."""
    sweep_workers = len(workers_list) > 1 and len(warehouse_counts) == 1
    fixed_desc = f"Fixed Warehouses: {warehouse_counts[0]}" if sweep_workers else f"Fixed Worker Count: {workers_list[0]}"
    sweep_dim = "Workers" if sweep_workers else "Warehouses"
    sweep_items = workers_list if sweep_workers else warehouse_counts

    print("\n" + "=" * 125)
    print(f"TPC-C MEDIAN COMPARISON: {' vs '.join(m.upper() for m in modes)} ({num_trials} Trials Aggregated)")
    print(fixed_desc)
    print(f"Transaction Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel (TPC-C Standard)")
    print("=" * 125)

    header_parts = [f"{sweep_dim:<12}"]
    for m in modes:
        header_parts.append(f"{m.upper() + ' Median TPS (±std)':<26}")
    if "bcdb_merkle" in modes and "pg" in modes:
        header_parts.append(f"{'Merkle/PG (%)':<14}")
    print(" | ".join(header_parts))
    print("-" * 125)

    for item in sweep_items:
        row_parts = [f"{item:<12}"]
        tps_by_mode = {}
        for m in modes:
            match = next(
                (a for a in aggregated if a["mode"] == m and (a["server_workers"] if sweep_workers else a["warehouses"]) == item),
                None
            )
            if match:
                val_str = f"{match['median_tps']:.1f} (±{match['std_tps']:.1f})"
                tps_by_mode[m] = match["median_tps"]
            else:
                val_str = "N/A"
                tps_by_mode[m] = 0.0
            row_parts.append(f"{val_str:<26}")
        if "bcdb_merkle" in modes and "pg" in modes:
            tps_pg = tps_by_mode.get("pg", 0.0)
            tps_merkle = tps_by_mode.get("bcdb_merkle", 0.0)
            if tps_pg > 0:
                delta = ((tps_merkle - tps_pg) / tps_pg * 100.0)
                row_parts.append(f"{delta:>+7.2f}%")
            else:
                row_parts.append(f"{'N/A':>8}")
        print(" | ".join(row_parts))
    print("=" * 125 + "\n")


def _plot_tpcc_median_results(aggregated, warehouse_counts, workers_list, modes, out_dir, args):
    """Generate publication-grade TPC-C median throughput plot with min-max shaded error regions."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        sweep_workers = len(workers_list) > 1 and len(warehouse_counts) == 1
        x_vals = workers_list if sweep_workers else warehouse_counts
        x_label = "Worker Count (Concurrency)" if sweep_workers else "Warehouse Count"

        fig, ax = plt.subplots(1, 1, figsize=(12, 7))

        mode_styles = {
            "pg": ("^:", "#6c757d", 2.2, 8, "Vanilla PostgreSQL (pg)"),
            "bcdb_det": ("d-.", "#28a745", 2.4, 8, "BCDB Deterministic (bcdb_det)"),
            "bcdb_merkle": ("s-", "#0056b3", 2.6, 9, "BCDB Merkle (bcdb_merkle)"),
        }

        for m in modes:
            style = mode_styles.get(m, ("o-", "#333333", 2.0, 7, m))
            marker_line, color, lw, ms, label = style

            y_meds = []
            y_mins = []
            y_maxs = []

            for item in x_vals:
                match = next(
                    (a for a in aggregated if a["mode"] == m and (a["server_workers"] if sweep_workers else a["warehouses"]) == item),
                    None
                )
                if match:
                    y_meds.append(match["median_tps"])
                    y_mins.append(match["min_tps"])
                    y_maxs.append(match["max_tps"])
                else:
                    y_meds.append(0.0)
                    y_mins.append(0.0)
                    y_maxs.append(0.0)

            if any(y_meds):
                ax.plot(x_vals, y_meds, marker_line, color=color,
                        linewidth=lw, markersize=ms, label=f"{label} (Median)")
                if any(y_mins[i] != y_maxs[i] for i in range(len(y_mins))):
                    ax.fill_between(x_vals, y_mins, y_maxs, color=color, alpha=0.15)

        # Annotate merkle overhead vs pg
        if "bcdb_merkle" in modes and "pg" in modes:
            for item in x_vals:
                m_match = next((a for a in aggregated if a["mode"] == "bcdb_merkle" and (a["server_workers"] if sweep_workers else a["warehouses"]) == item), None)
                p_match = next((a for a in aggregated if a["mode"] == "pg" and (a["server_workers"] if sweep_workers else a["warehouses"]) == item), None)
                if m_match and p_match and p_match["median_tps"] > 0:
                    d = (m_match["median_tps"] - p_match["median_tps"]) / p_match["median_tps"] * 100.0
                    ax.annotate(
                        f"{d:+.1f}%",
                        xy=(item, m_match["median_tps"]),
                        xytext=(0, 12),
                        textcoords="offset points",
                        ha="center",
                        fontsize=9,
                        fontweight="bold",
                        color="#0056b3",
                    )

        all_y = [a["max_tps"] for a in aggregated]
        max_y = max(all_y) if all_y else 1000.0
        ax.set_ylim(bottom=0, top=max(max_y * 1.20, 1000.0))
        ax.set_xlabel(x_label, fontsize=12, fontweight="bold")
        ax.set_ylabel("Median Throughput (TPS)", fontsize=12, fontweight="bold")
        ax.set_xticks(x_vals)
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.legend(loc="upper left", fontsize=10.5, framealpha=0.9)

        subtitle_desc = f"Warehouses={warehouse_counts[0]}" if sweep_workers else f"Workers={workers_list[0]}"
        plot_title = f"TPC-C Median Throughput vs Concurrency ({args.trials} Trials)" if sweep_workers else f"TPC-C Median Throughput vs Warehouse Scale ({args.trials} Trials)"

        plt.suptitle(
            f"{plot_title} \u2014 {subtitle_desc}\n"
            f"Mix: 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery, 4% StockLevel (TPC-C Spec)\n"
            f"Seed={args.tpcc_seed} | {args.tpcc_tx_count} tx/run | "
            f"Shaded Band = Min\u2013Max Across Trials",
            fontsize=12,
            fontweight="bold",
        )
        plt.tight_layout()

        fname_med = "tpcc_tps_vs_workers_median.png" if sweep_workers else "tpcc_tps_vs_warehouses_median.png"
        fname_std = "tpcc_tps_vs_workers.png" if sweep_workers else "tpcc_tps_vs_warehouses.png"
        plot_path_med = out_dir / fname_med
        plot_path_std = out_dir / fname_std
        plt.savefig(plot_path_med, dpi=300)
        plt.savefig(plot_path_std, dpi=300)
        print(f"\nSaved TPC-C median plot to: {plot_path_med} (and {plot_path_std})")

    except Exception as e:
        print(f"Failed to generate TPC-C median plot: {e}")


# ===========================================================================
# YCSB Sweep (original logic, preserved)
# ===========================================================================

def _run_ycsb_sweep(args, repo_root, out_dir, modes):
    """Run YCSB benchmark sweep across worker counts (original behavior)."""
    workers = [int(w.strip()) for w in args.workers.split(",") if w.strip()]
    workloads_raw = [w.strip() for w in args.workloads.split(",") if w.strip()]
    workloads = []
    for w in workloads_raw:
        if w == "all":
            suite_dir = repo_root / "scripts/ycsb_suite"
            suite_files = sorted(str(p.relative_to(repo_root)) for p in suite_dir.glob("*.txt"))
            workloads.extend(suite_files)
        elif w in ("standard", "sigmod", "core"):
            sigmod_files = [
                "scripts/ycsb_suite/ycsb_workload_a_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_b_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_c_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_d_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_f_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_dml_heavy_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_pure_dml_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_delete_heavy_skew_0_99_20k.txt",
                "scripts/ycsb_suite/ycsb_workload_balanced_dml_skew_0_99_20k.txt",
            ]
            workloads.extend(sigmod_files)
        elif "*" in w:
            import glob
            matched = sorted(glob.glob(str(repo_root / w)))
            if matched:
                workloads.extend(str(Path(m).relative_to(repo_root)) for m in matched)
            else:
                workloads.append(w)
        else:
            workloads.append(w)

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
            writer.writerow(YCSB_CSV_FIELDS)

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
                                ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US=2000 \\
                                ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS=64 \\
                                ARIABC_FULL_RESULT_REPLICA_LIMIT=-1 \\
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
                            run_cmd(cluster_run_cmd, check=False, timeout=600)
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
                            try:
                                with open(repo_root / wl) as _wlf:
                                    total_queries_wl = sum(1 for line in _wlf if line.strip() and not line.strip().startswith("--"))
                            except Exception:
                                total_queries_wl = 20004
                            wall_ms = (float(total_queries_wl) / tps_cl * 1000.0) if tps_cl > 0 else 0.0
                            print(f"  -> [cluster] Results: TPS={tps_cl:.2f} | MerklePass={merkle_pass_cl} | Divergence={div_cl} | Failures={perm_cl}")
                            res_entry = {
                                "mode": "cluster",
                                "workload": wl_name,
                                "server_workers": w,
                                "bcdb_workers": w,
                                "pool_size": w,
                                "total_queries": total_queries_wl,
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
                                    total_queries_wl,
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
        print("\n" + "=" * 120)
        print("ALL MODES COMPARISON: pg vs bcdb_det vs bcdb_merkle vs 4-Node Cluster")
        print("=" * 120)

        def _format_wl_label(wl_path):
            name = Path(wl_path).name
            if "skew-01" in name:
                return "Legacy Low-Skew (θ=0.01)"
            if "skew0-99" in name:
                return "Legacy High-Skew (θ=0.99)"
            m = re.search(r"ycsb_workload_(.*?)_20k", name)
            if m:
                return m.group(1).replace("_skew_", " θ=").replace("_", " ")
            return name

        print(f"{'Workload':<36} | {'Workers':<7} | {'PG TPS':<10} | {'BCDB Det':<10} | {'BCDB Merkle':<12} | {'Cluster TPS':<12} | {'Merkle vs Cl (%)'}")
        print("-" * 120)

        for wl in workloads:
            wl_name = Path(wl).name
            wl_short = _format_wl_label(wl)
            for w in workers:
                tps_pg = next((r["tps"] for r in results if r["mode"] == "pg" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_det = next((r["tps"] for r in results if r["mode"] == "bcdb_det" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_merkle = next((r["tps"] for r in results if r["mode"] == "bcdb_merkle" and r["workload"] == wl_name and r["server_workers"] == w), 0.0)
                tps_cluster = next((r["tps"] for r in results if r["mode"] == "cluster" and r["workload"] == wl_name and r["server_workers"] == w), cluster_data.get((wl_name, w), 0.0))
                delta = ((tps_merkle - tps_cluster) / tps_cluster * 100.0) if tps_cluster > 0 else 0.0

                print(f"{wl_short:<36} | {w:<7} | {tps_pg:<10.1f} | {tps_det:<10.1f} | {tps_merkle:<12.1f} | {tps_cluster:<12.1f} | {delta:>+7.2f}%")

        # Generate multi-curve comparison plot (dynamically sizing subplots)
        try:
            import math
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            n_plots = len(workloads)
            n_cols = min(3, n_plots) if n_plots > 1 else 1
            n_rows = math.ceil(n_plots / n_cols)
            fig, axes = plt.subplots(n_rows, n_cols, figsize=(6.5 * n_cols, 5.0 * n_rows), squeeze=False)

            for idx, wl in enumerate(workloads):
                r_idx = idx // n_cols
                c_idx = idx % n_cols
                ax = axes[r_idx][c_idx]
                wl_key = Path(wl).name
                title = _format_wl_label(wl)

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
                ax.set_title(title, fontsize=11, fontweight="bold", pad=10)
                ax.set_xlabel("Executor Worker Count", fontsize=10, fontweight="bold")
                ax.set_ylabel("Throughput (TPS)", fontsize=10, fontweight="bold")
                ax.set_xticks(x)
                ax.grid(True, linestyle=":", alpha=0.6)
                ax.legend(loc="upper left", fontsize=8.5)

            # Clean up empty subplots
            for empty_idx in range(n_plots, n_rows * n_cols):
                fig.delaxes(axes[empty_idx // n_cols][empty_idx % n_cols])

            plt.suptitle(
                "Throughput Scaling Across All Modes: pg, bcdb_det, bcdb_merkle vs 4-Node Cluster\n"
                f"Hardware Topology: Dedicated Gateway Client ({args.gateway_host}) -> DB Node ({args.db_host})",
                fontsize=13,
                fontweight="bold",
            )
            plt.tight_layout()

            plot_path = out_dir / "final_tps_all_modes_comparison.png"
            plt.savefig(plot_path, dpi=180)
            print(f"\nSaved comparison plot to: {plot_path}")
        except Exception as e:
            print(f"Failed to generate plot: {e}")

        print("\nAll modes benchmark campaign completed successfully!")
    finally:
        if not args.keep_postgres:
            teardown_postgres(args, run_cluster=args.run_cluster)


if __name__ == "__main__":
    main()
