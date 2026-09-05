#!/usr/bin/env python3
"""
run_singlenode_gateway_sweep.py

Runs standalone single-node BCDB Merkle benchmark with physical machine separation:
- Client: ariabc_pg_gateway running on Gateway machine (10.129.27.111)
- Server: ariabc_pg_server (--bypassRaft 1) + PostgreSQL running on Node 1 (10.129.148.247:5438)

Sweeps server executor worker counts [1, 2, 4, 8, 12, 16] across workloads:
1. Low-skew: ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt
2. High-skew: ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt

Directly compares throughput against the 4-node Raft-Kafka cluster baseline.
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


def run_cmd(cmd, check=True, timeout=120):
    """Executes a local command or SSH command synchronously via shell."""
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


def run_cmd_args(args_list, check=True, timeout=120):
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


def main():
    parser = argparse.ArgumentParser(description="Standalone Single-Node Gateway Sweep")
    parser.add_argument("--gateway-host", default="10.129.27.111", help="Gateway host IP")
    parser.add_argument("--gateway-user", default="neel", help="Gateway SSH user")
    parser.add_argument("--gateway-repo", default="/home/neel/ARIABC/AriaBC", help="Repo path on Gateway")
    parser.add_argument("--db-host", default="10.129.148.247", help="DB host IP (Node 1)")
    parser.add_argument("--db-user", default="neel", help="DB SSH user")
    parser.add_argument("--db-port", default=5438, type=int, help="Postgres port")
    parser.add_argument("--server-port", default=8000, type=int, help="ariabc_pg_server client port")
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
    parser.add_argument("--out-dir", default=None, help="Output directory for results")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = repo_root / f"scripts/bench_full_results/singlenode_gateway_sweep_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    workers = [int(w.strip()) for w in args.workers.split(",") if w.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]

    print("=" * 80)
    print("Standalone Single-Node BCDB Merkle Gateway Benchmark")
    print(f"Gateway (Client):  {args.gateway_user}@{args.gateway_host}")
    print(f"Database (Server): {args.db_user}@{args.db_host}:{args.db_port}")
    print(f"Workers:           {workers}")
    print(f"Workloads:         {workloads}")
    print(f"Output Directory:  {out_dir}")
    print("=" * 80)

    summary_csv = out_dir / "summary.csv"
    with open(summary_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
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

    results = []

    for wl in workloads:
        wl_name = Path(wl).name
        print(f"\n==========================================================================")
        print(f"Workload: {wl_name}")
        print(f"==========================================================================")

        for w in workers:
            print(f"\n--- [Worker Count: {w}] ---")

            # Step 1: Configure Postgres, restart, and restore usertable_small on Node 1
            print(f"  [1/4] Preparing PostgreSQL on {args.db_host} (bcdb_worker_count={w})...")
            setup_cmd = f"""ssh {args.db_user}@{args.db_host} "
                fuser -k -9 {args.server_port}/tcp 2>/dev/null || true
                export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -c 'ALTER SYSTEM SET bcdb_worker_count = {w};' >/dev/null 2>&1
                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 restart >/dev/null 2>&1
                /home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -f /home/neel/Desktop/ariabc_cluster/scripts/restore_usertable_small.sql >/dev/null 2>&1
            " """
            run_cmd(setup_cmd, check=True)

            # Step 2: Start ariabc_pg_server on Node 1
            print(f"  [2/4] Starting ariabc_pg_server on {args.db_host}:{args.server_port} (poolSize={w}, bypassRaft=1)...")
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
            _, out = run_cmd(start_server_cmd, check=True)
            if "ready" not in out:
                raise RuntimeError(f"Server failed to start on port {args.server_port}")

            # Step 3: Run ariabc_pg_gateway from Gateway machine (10.129.27.111)
            gw_workload_path = f"{args.gateway_repo}/{wl}"
            print(f"  [3/4] Running ariabc_pg_gateway from {args.gateway_host}...")
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

            # Parse metrics from gateway output
            time_match = re.search(r"overall time taken \(millisec\) = (\d+)", gw_out)
            if not time_match:
                time_match = re.search(r"overall (?:wall )?time(?: including drains)? \(millisec\) = (\d+)", gw_out)
            wall_time_ms = float(time_match.group(1)) if time_match else 0.0

            total_match = re.search(r"total=(\d+)", gw_out)
            total_queries = int(total_match.group(1)) if total_match else 20004

            div_match = re.search(r"divergence_count=(\d+)", gw_out)
            divergence_count = int(div_match.group(1)) if div_match else 0

            perm_match = re.search(r"permanent_failures=(\d+)", gw_out)
            permanent_failures = int(perm_match.group(1)) if perm_match else 0

            prog_tps_matches = re.findall(r"completed_tps=([0-9.]+)", gw_out)
            completed_tps = float(prog_tps_matches[-1]) if prog_tps_matches else 0.0

            # Prefer completed_tps if available, otherwise compute from wall_time_ms
            if completed_tps > 0.0:
                tps = completed_tps
            elif wall_time_ms > 0:
                tps = total_queries / (wall_time_ms / 1000.0)
            else:
                tps = 0.0

            # Step 4: Stop ariabc_pg_server cleanly and verify Merkle consistency
            print(f"  [4/4] Verifying Merkle consistency on {args.db_host}...")
            _, verify_out = run_cmd_args([
                "ssh", f"{args.db_user}@{args.db_host}",
                f"fuser -k -TERM {args.server_port}/tcp >/dev/null 2>&1 || true; sleep 1; "
                f"export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib; "
                f"/home/neel/Desktop/ariabc_install/bin/psql -p {args.db_port} -U postgres -d postgres -At -c \"SELECT merkle_verify('usertable_small');\""
            ], check=True)
            merkle_pass = 1 if "t" in verify_out.strip() else 0

            print(f"  -> Results: TPS={tps:.2f} (CompletedTPS={completed_tps:.2f}, WallTime={wall_time_ms:.1f}ms) | MerklePass={merkle_pass} | Divergence={divergence_count} | Failures={permanent_failures}")

            res_entry = {
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

    # Comparison analysis against cluster baseline
    print("\n" + "=" * 80)
    print("FINAL COMPARISON: 4-Node Cluster Baseline vs Standalone Single Node")
    print("=" * 80)

    cluster_data = {}
    cluster_csv_path = repo_root / args.cluster_summary
    if cluster_csv_path.exists():
        with open(cluster_csv_path, "r") as f:
            reader = csv.DictReader(f)
            # cluster summary alternates runs: first 6 are low skew (ycsbtx-skew-01), next 6 are high skew (ycsb-skew0-99)
            rows = [r for r in reader if r.get("server_workers") and r["server_workers"] != "server_workers"]
            if len(rows) >= 12:
                # First 6: ycsbtx-skew-01, second 6: ycsb-skew0-99
                for r in rows[:6]:
                    sw = int(r["server_workers"])
                    cluster_data[("ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt", sw)] = float(r["tps"])
                for r in rows[6:12]:
                    sw = int(r["server_workers"])
                    cluster_data[("ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt", sw)] = float(r["tps"])
            else:
                for r in rows:
                    sw = int(r["server_workers"])
                    cluster_data[("default", sw)] = float(r["tps"])

    comp_rows = []
    print(f"{'Workload':<32} | {'Workers':<7} | {'Cluster TPS':<12} | {'SingleNode TPS':<14} | {'Ratio':<8} | {'Delta (%)':<10} | {'Merkle'}")
    print("-" * 105)

    for r in results:
        wl_key = r["workload"]
        w = r["server_workers"]
        sn_tps = r["tps"]
        cl_tps = cluster_data.get((wl_key, w), 0.0)
        ratio = (sn_tps / cl_tps) if cl_tps > 0 else 0.0
        delta_pct = ((sn_tps - cl_tps) / cl_tps * 100.0) if cl_tps > 0 else 0.0
        m_status = "PASS" if r["merkle_pass"] == 1 else "FAIL"

        wl_short = "Low-Skew (0.01)" if "skew-01" in wl_key else "High-Skew (0.99)"
        print(f"{wl_short:<32} | {w:<7} | {cl_tps:<12.2f} | {sn_tps:<14.2f} | {ratio:<8.2f} | {delta_pct:>+7.2f}%   | {m_status}")
        comp_rows.append({
            "workload": wl_key,
            "workload_short": wl_short,
            "workers": w,
            "cluster_tps": cl_tps,
            "singlenode_tps": sn_tps,
            "ratio": ratio,
            "delta_pct": delta_pct,
            "merkle_pass": r["merkle_pass"],
        })

    # Generate plot using matplotlib
    try:
        import matplotlib.pyplot as plt

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        for ax, wl_substr, title in [
            (ax1, "skew-01", "Low-Skew (θ = 0.01) YCSB"),
            (ax2, "skew0-99", "High-Skew (θ = 0.99) YCSB"),
        ]:
            wl_comp = [c for c in comp_rows if wl_substr in c["workload"]]
            if not wl_comp:
                continue
            x = [c["workers"] for c in wl_comp]
            y_cl = [c["cluster_tps"] for c in wl_comp]
            y_sn = [c["singlenode_tps"] for c in wl_comp]

            ax.plot(x, y_cl, "o--", color="#d9534f", linewidth=2.5, markersize=8, label="4-Node Raft-Kafka Cluster")
            ax.plot(x, y_sn, "s-", color="#2e6da4", linewidth=2.5, markersize=8, label="Standalone Single-Node (BCDB Merkle)")

            for xi, y_s, y_c in zip(x, y_sn, y_cl):
                d = ((y_s - y_c) / y_c * 100.0) if y_c > 0 else 0
                ax.annotate(
                    f"{d:+.1f}%\n({y_s:.0f})",
                    xy=(xi, y_s),
                    xytext=(0, 10),
                    textcoords="offset points",
                    ha="center",
                    fontsize=9,
                    fontweight="bold",
                    color="#2e6da4",
                )

            ax.set_title(title, fontsize=13, fontweight="bold")
            ax.set_xlabel("Executor Worker Count", fontsize=11, fontweight="bold")
            ax.set_ylabel("Throughput (TPS)", fontsize=11, fontweight="bold")
            ax.set_xticks(x)
            ax.grid(True, linestyle=":", alpha=0.6)
            ax.legend(loc="lower right", fontsize=10)

        plt.suptitle(
            "Standalone Single-Node BCDB Merkle vs 4-Node Cluster Baseline\n"
            "Equal Hardware Architecture: Gateway Client (10.129.27.111) -> DB Node 1 (10.129.148.247)",
            fontsize=14,
            fontweight="bold",
        )
        plt.tight_layout()

        plot_path = out_dir / "final_tps_cluster_vs_singlenode.png"
        plt.savefig(plot_path, dpi=180)
        # Also copy to artifact path
        artifact_path = Path("/home/neel/.gemini/antigravity-ide/brain/e57188e6-6cb4-4856-8d53-724b6fe34145/final_tps_cluster_vs_singlenode.png")
        plt.savefig(artifact_path, dpi=180)
        print(f"\nSaved comparison plot to: {plot_path}")
        print(f"Saved artifact plot to:   {artifact_path}")
    except Exception as e:
        print(f"Failed to generate plot: {e}")

    print("\nBenchmark campaign completed successfully!")


if __name__ == "__main__":
    main()
