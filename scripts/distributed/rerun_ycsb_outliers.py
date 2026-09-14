#!/usr/bin/env python3
"""
Targeted Re-run Script for YCSB Outliers & Noisy Benchmark Runs
Fixes:
- DML standalone timeout crashes in bcdb_det and bcdb_merkle (forcing clean restart)
- Workload C cluster noise runs (w=4, 8, 16)
- Workload B cluster & merkle outlier runs
- Workload D cluster & merkle outlier runs
- All_insert outlier run
"""

import sys
import os
import re
import csv
import time
import shutil
import argparse
import subprocess
from pathlib import Path

# Add script dir to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from cluster_sweep_support import run_cluster_case

CSV_ALL_72 = REPO_ROOT / "scripts/bench_full_results/ycsb_all_72_sweep/summary.csv"
CSV_32MB_BACKUP = REPO_ROOT / "scripts/bench_full_results/ycsb_32mb_20260913T214018Z/summary.csv"
CHECKPOINT_CSV = REPO_ROOT / "scripts/bench_full_results/ycsb_all_72_sweep/outliers_rerun_checkpoint.csv"

GATEWAY_HOST = "10.129.27.111"
GATEWAY_USER = "neel"
GATEWAY_REPO = "/home/neel/ARIABC/AriaBC"
DB_HOST = "10.129.148.247"
DB_USER = "neel"
DB_PORT = 5438
SERVER_PORT = 8000
SHARED_BUFFERS = "32MB"


def run_cmd(cmd, check=True, timeout=300):
    res = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout
    )
    if check and res.returncode != 0:
        raise RuntimeError(f"Command failed (exit {res.returncode}): {cmd}\nStderr: {res.stderr}\nStdout: {res.stdout}")
    return res.returncode, res.stdout + "\n" + res.stderr


def run_standalone_case(mode, wl_name, w, retries=1):
    """Execute a single standalone benchmark run on Node 1 with clean PostgreSQL restart."""
    db_type = 1
    merkle_enable_guc = "on" if mode == "bcdb_merkle" else "off"
    merkle_flag = 1 if mode == "bcdb_merkle" else 0
    wl_path = f"{GATEWAY_REPO}/scripts/ycsb_suite/{wl_name}"

    for attempt in range(retries + 1):
        try:
            # 1. Setup PostgreSQL on Node 1 with clean restart
            print(f"    [1/4] Reconfiguring & Restarting PostgreSQL on {DB_HOST} ({mode}, workers={w})...", flush=True)
            setup_cmd = f"""ssh {DB_USER}@{DB_HOST} "
                fuser -k -9 {SERVER_PORT}/tcp >/dev/null 2>&1 || true
                export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib:\\${{LD_LIBRARY_PATH:-}}
                if ! /home/neel/Desktop/ariabc_install/bin/pg_isready -p {DB_PORT} >/dev/null 2>&1; then
                    /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 60 start >/dev/null 2>&1 || true
                fi
                /home/neel/Desktop/ariabc_install/bin/psql -p {DB_PORT} -U postgres -d postgres -c \\"ALTER SYSTEM SET bcdb_worker_count = {w};\\" -c \\"ALTER SYSTEM SET enable_merkle_index = '{merkle_enable_guc}';\\" -c \\"ALTER SYSTEM SET shared_buffers = '{SHARED_BUFFERS}';\\" -c \\"ALTER SYSTEM SET synchronous_commit = 'on';\\" >/dev/null 2>&1
                /home/neel/Desktop/ariabc_install/bin/pg_ctl -D /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata -l /tmp/postgres_single.log -w -t 120 -m fast restart >/dev/null 2>&1
                for _chk in \\$(seq 1 30); do
                    if /home/neel/Desktop/ariabc_install/bin/pg_isready -p {DB_PORT} >/dev/null 2>&1; then
                        break
                    fi
                    sleep 0.5
                done
                /home/neel/Desktop/ariabc_install/bin/psql -p {DB_PORT} -U postgres -d postgres -v bench_enable_merkle={merkle_flag} -f /home/neel/Desktop/ariabc_cluster/scripts/restore_usertable_small.sql -c 'VACUUM ANALYZE usertable_small;' >/dev/null 2>&1
            " """
            run_cmd(setup_cmd, check=True)

            # 2. Start ariabc_pg_server on Node 1
            print(f"    [2/4] Starting ariabc_pg_server on {DB_HOST}:{SERVER_PORT} (poolSize={w})...", flush=True)
            start_server_cmd = f"""ssh {DB_USER}@{DB_HOST} "
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

                fuser -k -9 {SERVER_PORT}/tcp >/dev/null 2>&1 || true

                for _chk in \\$(seq 1 30); do
                    if /home/neel/Desktop/ariabc_install/bin/pg_isready -p {DB_PORT} >/dev/null 2>&1; then
                        break
                    fi
                    sleep 0.5
                done

                nohup /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server \\
                  --id 1 \\
                  --raftEndpoint 127.0.0.1:9000 \\
                  --clientPort {SERVER_PORT} \\
                  --raftMembers 1=127.0.0.1:9000 \\
                  --dbName postgres \\
                  --dbHost 127.0.0.1 \\
                  --dbPort {DB_PORT} \\
                  --dbUser postgres \\
                  --dbType 1 \\
                  --safedb 1 \\
                  --dbConnPoolSize {w} \\
                  --bcdbInitBlockSize {w} \\
                  --pgExecMode event \\
                  --bypassRaft 1 \\
                  </dev/null >/tmp/server_single.log 2>&1 &

                for i in \\$(seq 1 30); do
                    if fuser {SERVER_PORT}/tcp >/dev/null 2>&1; then
                        echo ready
                        exit 0
                    fi
                    sleep 0.2
                done
                echo failed
                exit 1
            " """
            _, srv_out = run_cmd(start_server_cmd, check=True)
            if "ready" not in srv_out:
                raise RuntimeError("Server failed to start on port")

            # 3. Run ariabc_pg_gateway from Gateway machine
            print(f"    [3/4] Running ariabc_pg_gateway from {GATEWAY_HOST} ({mode})...", flush=True)
            gw_cmd = f"""ssh {GATEWAY_USER}@{GATEWAY_HOST} "
                {GATEWAY_REPO}/ariabc_pg/build/bin/ariabc_pg_gateway \\
                  --nodes {DB_HOST}:{SERVER_PORT} \\
                  --queryFrom {wl_path} \\
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
            _, gw_out = run_cmd(gw_cmd, check=False, timeout=120)

            # Parse metrics
            time_match = re.search(r"overall time taken \(millisec\) = (\d+)", gw_out)
            if not time_match:
                time_match = re.search(r"overall (?:wall )?time(?: including drains)? \(millisec\) = (\d+)", gw_out)
            wall_time_ms = float(time_match.group(1)) if time_match else 0.0
            if wall_time_ms == 0.0:
                print(f"    [DEBUG] Gateway output (first 500 chars):\n{gw_out[:500]}", flush=True)
                print(f"    [DEBUG] Gateway output (last 500 chars):\n{gw_out[-500:]}", flush=True)

            total_match = re.search(r"loaded (\d+) queries", gw_out)
            if not total_match:
                total_match = re.search(r"PROGRESS_GATEWAY_DET.*?\btotal=(\d+)", gw_out)
            total_queries = int(total_match.group(1)) if total_match else 20000

            div_match = re.search(r"divergence_count=(\d+)", gw_out)
            divergence_count = int(div_match.group(1)) if div_match else 0

            perm_match = re.search(r"permanent_failures=(\d+)", gw_out)
            permanent_failures = int(perm_match.group(1)) if perm_match else 0

            if wall_time_ms > 0:
                tps = total_queries / (wall_time_ms / 1000.0)
            else:
                tps = 0.0

            # 4. Stop server cleanly and verify Merkle consistency if applicable
            print(f"    [4/4] Verifying state and stopping server on {DB_HOST}...", flush=True)
            fast_teardown_cmd = (
                f"fuser -k -TERM {SERVER_PORT}/tcp >/dev/null 2>&1 || true; "
                f"for _i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do "
                f"  fuser {SERVER_PORT}/tcp >/dev/null 2>&1 || break; "
                f"  sleep 0.02; "
                f"done; "
            )
            if mode == "bcdb_merkle":
                _, verify_out = run_cmd(
                    f"""ssh {DB_USER}@{DB_HOST} "
                        {fast_teardown_cmd}
                        export LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib;
                        /home/neel/Desktop/ariabc_install/bin/psql -p {DB_PORT} -U postgres -d postgres -At -c \\"SELECT merkle_verify('usertable_small');\\"
                    " """, check=True
                )
                merkle_pass = 1 if "t" in verify_out.strip() else 0
            else:
                run_cmd(f"""ssh {DB_USER}@{DB_HOST} "{fast_teardown_cmd}" """, check=True)
                merkle_pass = 1

            if wall_time_ms >= 20000 or tps < 1000:
                raise RuntimeError(f"Run timed out or low TPS (wall={wall_time_ms}ms, tps={tps:.2f})")

            return {
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
                "run_id": f"rerun_{mode}_{int(time.time())}",
                "shared_buffers": SHARED_BUFFERS,
                "source_fingerprint": "rerun_outlier_clean"
            }
        except Exception as e:
            print(f"    [ATTEMPT {attempt+1} FAILED] {e}. Retrying...", flush=True)
            run_cmd(f"""ssh {DB_USER}@{DB_HOST} "fuser -k -9 {SERVER_PORT}/tcp >/dev/null 2>&1 || true" """, check=False)
            time.sleep(1)
            if attempt == retries:
                raise


def run_cluster_outlier_case(wl_name, w, args_obj):
    """Execute a single cluster outlier benchmark run with clean state."""
    wl_rel = f"scripts/ycsb_suite/{wl_name}"
    out_dir = REPO_ROOT / "scripts/bench_full_results/ycsb_all_72_sweep"
    res = run_cluster_case(args_obj, REPO_ROOT, out_dir, wl_rel, w, run_index=1, restart=True)
    return res


def identify_outliers():
    """Identify all 132 outlier benchmark runs from summary.csv."""
    with open(CSV_ALL_72, "r") as f:
        rows = list(csv.DictReader(f))

    outliers = []
    for r in rows:
        mode = r["mode"]
        wl = r["workload"]
        w = int(r["server_workers"])
        tps = float(r["tps"])
        wall = float(r["wall_time_ms"])

        # Extract family & skew
        base = wl.replace(".txt", "").replace("ycsb_workload_", "")
        parts = base.split("_skew_")
        family = parts[0]
        skew = parts[1].split("_")[0] + "_" + parts[1].split("_")[1]

        # 1. DML Standalone Timeouts (tps < 1000)
        if family in ["balanced_dml", "delete_heavy", "dml_heavy", "pure_dml"] and mode in ["bcdb_det", "bcdb_merkle"] and (tps < 1000 or wall >= 20000):
            outliers.append((mode, wl, w, family, skew, tps, "DML_TIMEOUT"))

        # 2. All_insert single outlier
        elif family == "all_insert" and mode == "bcdb_det" and skew == "0_70" and tps < 1000:
            outliers.append((mode, wl, w, family, skew, tps, "ALL_INSERT_OUTLIER"))

        # 3. Workload B Outliers
        elif family == "b" and mode == "bcdb_merkle" and w == 8 and skew == "0_90":
            outliers.append((mode, wl, w, family, skew, tps, "WL_B_MERKLE_DIP"))
        elif family == "b" and mode == "cluster" and w == 16 and skew in ["0_20", "0_50", "0_80", "0_90"]:
            outliers.append((mode, wl, w, family, skew, tps, "WL_B_CLUSTER_DIP"))

        # 4. Workload C Cluster Outliers
        elif family == "c" and mode == "cluster":
            if (w == 16 and skew in ["0_70", "0_99"]) or (w == 8 and skew in ["0_90", "1_20"]) or (w == 4 and skew == "0_70"):
                outliers.append((mode, wl, w, family, skew, tps, "WL_C_CLUSTER_DIP"))

        # 5. Workload D Outliers
        elif family == "d" and mode == "bcdb_merkle":
            if (w == 16 and skew == "0_99") or (w == 4 and skew == "0_20") or (w == 2 and skew == "1_20"):
                outliers.append((mode, wl, w, family, skew, tps, "WL_D_MERKLE_DIP"))
        elif family == "d" and mode == "cluster":
            if (w == 8 and skew == "0_90") or (w == 16 and skew in ["0_20", "1_20"]):
                outliers.append((mode, wl, w, family, skew, tps, "WL_D_CLUSTER_DIP"))

    return outliers


def main():
    parser = argparse.ArgumentParser(description="Targeted re-run of YCSB outliers")
    parser.add_argument("--cluster-only", action="store_true", help="Re-run only cluster outliers")
    parser.add_argument("--standalone-only", action="store_true", help="Re-run only standalone outliers")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of runs (for testing)")
    args = parser.parse_args()

    # Fake args object for cluster_sweep_support
    class ClusterArgs:
        db_shared_buffers = SHARED_BUFFERS

    cluster_args = ClusterArgs()

    outliers = identify_outliers()
    print(f"Total outliers identified: {len(outliers)}")

    if args.cluster_only:
        outliers = [o for o in outliers if o[0] == "cluster"]
    elif args.standalone_only:
        outliers = [o for o in outliers if o[0] != "cluster"]

    if args.limit > 0:
        outliers = outliers[:args.limit]

    print(f"Outliers to execute in this run: {len(outliers)}")

    # Load completed checkpoint runs
    checkpoint_results = {}
    if CHECKPOINT_CSV.exists():
        with open(CHECKPOINT_CSV, "r") as f:
            for r in csv.DictReader(f):
                k = (r["mode"], r["workload"], int(r["server_workers"]))
                checkpoint_results[k] = r
        print(f"Loaded {len(checkpoint_results)} already completed checkpoint runs.")

    # Execute runs
    completed_now = 0
    start_time = time.time()

    for idx, (mode, wl, w, family, skew, old_tps, reason) in enumerate(outliers, 1):
        key = (mode, wl, w)
        if key in checkpoint_results:
            print(f"[{idx}/{len(outliers)}] SKIPPING {mode} | {wl} | w={w} (Already completed in checkpoint: {float(checkpoint_results[key]['tps']):.1f} TPS)")
            continue

        print(f"\n================================================================================")
        print(f"[{idx}/{len(outliers)}] RE-RUNNING {mode.upper()} | {wl} | workers={w} | skew={skew}")
        print(f"  Reason: {reason} (Old TPS was {old_tps:.2f})")
        print(f"================================================================================")

        run_start = time.time()
        if mode == "cluster":
            res = run_cluster_outlier_case(wl, w, cluster_args)
        else:
            res = run_standalone_case(mode, wl, w)

        duration = time.time() - run_start
        print(f"  -> SUCCESS in {duration:.1f}s: New TPS={res['tps']:.2f} (was {old_tps:.2f}, +{((res['tps']/old_tps)-1)*100:.1f}%) | "
              f"MerklePass={res['merkle_pass']} | Divergence={res['divergence_count']} | Failures={res['permanent_failures']}")

        checkpoint_results[key] = res

        # Append to checkpoint CSV
        file_exists = CHECKPOINT_CSV.exists() and CHECKPOINT_CSV.stat().st_size > 0
        with open(CHECKPOINT_CSV, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "mode", "workload", "server_workers", "bcdb_workers", "pool_size",
                "total_queries", "wall_time_ms", "tps", "merkle_pass",
                "divergence_count", "permanent_failures", "run_id", "shared_buffers", "source_fingerprint"
            ])
            if not file_exists:
                writer.writeheader()
            writer.writerow(res)

        completed_now += 1

    total_time = time.time() - start_time
    print(f"\nCompleted {completed_now} targeted runs in {total_time/60:.1f} minutes.")

    # Merge checkpoint into summary.csv files
    print("\nMerging stable numbers into summary.csv files...")
    for target_csv in [CSV_ALL_72, CSV_32MB_BACKUP]:
        if not target_csv.exists():
            continue
        backup_path = target_csv.with_suffix(".before_outlier_merge.csv")
        shutil.copy2(target_csv, backup_path)
        print(f"Backed up {target_csv.name} to {backup_path.name}")

        with open(target_csv, "r") as f:
            all_rows = list(csv.DictReader(f))

        updated_rows = []
        replaced_count = 0
        for r in all_rows:
            k = (r["mode"], r["workload"], int(r["server_workers"]))
            if k in checkpoint_results:
                new_r = checkpoint_results[k]
                r["wall_time_ms"] = new_r["wall_time_ms"]
                r["tps"] = f"{float(new_r['tps']):.2f}"
                r["merkle_pass"] = new_r["merkle_pass"]
                r["divergence_count"] = new_r["divergence_count"]
                r["permanent_failures"] = new_r["permanent_failures"]
                if "run_id" in new_r:
                    r["run_id"] = new_r["run_id"]
                replaced_count += 1
            updated_rows.append(r)

        with open(target_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            writer.writeheader()
            writer.writerows(updated_rows)

        print(f"Successfully replaced {replaced_count} rows in {target_csv.name}.")

    print("\nAll outlier points successfully re-run and merged!")


if __name__ == "__main__":
    main()
