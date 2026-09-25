#!/usr/bin/env python3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts/distributed"))

from cluster_sweep_support import run_cluster_case

class Args:
    db_shared_buffers = "32MB"
    cold_runs = False
    gateway_host = "10.129.27.111"
    gateway_user = "neel"
    gateway_repo = "/home/neel/ARIABC/AriaBC"
    tx_sign = sys.argv[1] if len(sys.argv) > 1 else "0"

out_dir = REPO_ROOT / "scripts/bench_full_results/tx_sign_smoke_test"
out_dir.mkdir(parents=True, exist_ok=True)

workload = "scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt"
print(f"Running smoke test with tx_sign={Args.tx_sign} on {workload}...")
res = run_cluster_case(
    args=Args(),
    repo=REPO_ROOT,
    out=out_dir,
    workload=workload,
    workers=16,
    run_index=0,
    restart=True
)
print("SUCCESS:", res)
