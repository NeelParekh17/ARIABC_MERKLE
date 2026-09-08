#!/usr/bin/env python3
"""
verify_workloads_bcd.py — Systematic verification of 4-Node Cluster on Workloads B, C, and D
Runs across worker thread counts [1, 2, 4, 8, 16] for both uniform (0.00) and Zipfian (0.99) skews.
Verifies throughput, Merkle consistency, zero divergence, zero permanent failures.
"""

import os
import sys
import subprocess
import re
import csv
import time
from datetime import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
OUTPUT_DIR = os.path.join(REPO_ROOT, "scripts/bench_full_results", f"verify_bcd_sweep_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
os.makedirs(OUTPUT_DIR, exist_ok=True)

WORKLOADS = [
    ("Workload C (100% Read, skew 0.00)", "scripts/ycsb_suite/ycsb_workload_c_skew_0_00_20k.txt"),
    ("Workload C (100% Read, skew 0.99)", "scripts/ycsb_suite/ycsb_workload_c_skew_0_99_20k.txt"),
    ("Workload B (95% Read, 5% Update, skew 0.00)", "scripts/ycsb_suite/ycsb_workload_b_skew_0_00_20k.txt"),
    ("Workload B (95% Read, 5% Update, skew 0.99)", "scripts/ycsb_suite/ycsb_workload_b_skew_0_99_20k.txt"),
    ("Workload D (95% Read, 5% Insert, skew 0.00)", "scripts/ycsb_suite/ycsb_workload_d_skew_0_00_20k.txt"),
    ("Workload D (95% Read, 5% Insert, skew 0.99)", "scripts/ycsb_suite/ycsb_workload_d_skew_0_99_20k.txt"),
]

WORKERS = [1, 2, 4, 8, 16]

csv_file = os.path.join(OUTPUT_DIR, "bcd_verification_results.csv")
results = []

print("================================================================================")
print("  Systematic Verification Campaign: Workloads B, C, D (4-Node Cluster)")
print(f"  Output Directory: {OUTPUT_DIR}")
print(f"  Kafka Host: 10.129.27.111 (Gateway Machine Offload)")
print(f"  Target Workloads: 6 | Worker Configs: {WORKERS}")
print("================================================================================")

for wl_name, wl_path in WORKLOADS:
    abs_wl = os.path.join(REPO_ROOT, wl_path)
    if not os.path.exists(abs_wl):
        print(f"ERROR: Workload file missing: {abs_wl}")
        continue

    print(f"\n>>> Running Suite: {wl_name}")
    print(f"    File: {wl_path}")
    print(f"    {'Workers':<8} {'TPS':<12} {'Merkle':<8} {'Divergence':<12} {'Perm Fail':<10} {'Wall Time (s)':<14} {'Status'}")
    print(f"    {'-'*8} {'-'*12} {'-'*8} {'-'*12} {'-'*10} {'-'*14} {'-'*8}")

    for w in WORKERS:
        t0 = time.time()
        cmd = [
            "env",
            "FORCE_BUILD=0",
            "SKIP_RDKAFKA_SETUP=1",
            "SKIP_SYNC=1",
            "SKIP_BUILD=1",
            "KAFKA_FAST_RESET=1",
            "DUMP_VERIFY_CSV=0",
            "ARIABC_PREFERRED_LEADER_ID=1",
            "ARIABC_RAFT_DURABLE_ASYNC_FLUSH=1",
            "ARIABC_RAFT_STREAM_GAP=512",
            "ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER=1",
            "ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US=0",
            "BCDB_DET_QUEUE_HIGH_WM=65536",
            "BCDB_DET_QUEUE_LOW_WM=32768",
            os.path.join(REPO_ROOT, "scripts/distributed/run_4node_raft_cluster.sh"),
            "--workload", abs_wl,
            "--ordering-mode", "raft-kafka",
            "--enable-merkle-index", "1",
            "--raft-apply-ledger-mode", "off",
            "--threads", "96",
            "--det-client-workers", "96",
            "--det-client-inflight", "16",
            "--server-exec-workers", str(w),
            "--server-pg-connections", str(w),
            "--pool-size", str(w),
            "--bcdb-workers", str(w),
            "--bcdb-init-block-size", str(w),
            "--bcdb-decouple-workers", "1",
            "--conn-fanout", "1",
            "--raft-ordered-fanout", "1",
            "--raft-ordering-policy", "leader-assigned",
            "--raft-ordered-batch-append", "1",
            "--raft-ordered-batch-target-entries", "64",
            "--raft-ordered-batch-linger-us", "1000",
            "--raft-ordered-coalesce-log", "1",
            "--kafka-completion-mode", "majority_async_all3",
            "--det-window", "65536",
        ]

        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        elapsed = time.time() - t0

        # Find latest cluster directory
        find_cmd = f"find {REPO_ROOT}/scripts/bench_full_results -maxdepth 1 -type d -name 'cluster4_*' | sort -V | tail -n1"
        latest_dir = subprocess.check_output(find_cmd, shell=True, text=True).strip()

        # Parse profile summary
        sum_cmd = f"python3 {REPO_ROOT}/scripts/distributed/summarize_raft_profile.py {latest_dir}"
        sum_out = subprocess.check_output(sum_cmd, shell=True, text=True).strip().splitlines()

        tps = 0.0
        merkle_pass = 0
        divergence = -1
        perm_fail = -1
        if len(sum_out) >= 2:
            data = sum_out[1].split(",")
            tps = float(data[3])
            merkle_pass = int(data[27])
            divergence = int(data[28])
            perm_fail = int(data[29])

        status = "PASS" if (proc.returncode == 0 and merkle_pass == 1 and divergence == 0 and perm_fail == 0) else "FAIL"
        print(f"    {w:<8} {tps:<12.2f} {merkle_pass:<8} {divergence:<12} {perm_fail:<10} {elapsed:<14.1f} {status}")

        record = {
            "workload": wl_name,
            "workload_file": wl_path,
            "workers": w,
            "tps": tps,
            "merkle_pass": merkle_pass,
            "divergence_count": divergence,
            "permanent_failures": perm_fail,
            "wall_time_s": elapsed,
            "run_dir": os.path.basename(latest_dir),
            "status": status,
        }
        results.append(record)

        # Write CSV progressively
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)

print("\n================================================================================")
print(f"Campaign complete! Results saved to: {csv_file}")
print("================================================================================")
