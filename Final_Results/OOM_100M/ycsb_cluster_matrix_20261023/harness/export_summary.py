#!/usr/bin/env python3
import csv
import json
from pathlib import Path

BASE_DIR = Path("/work/ARIABC/AriaBC/Final_Results/OOM_100M/ycsb_cluster_matrix_20261023")
SINGLE_NODE_CSV = Path("/work/ARIABC/AriaBC/Final_Results/OOM_100M/ycsb_matrix_20261020/summary.csv")
RESULTS_DIR = BASE_DIR / "data" / "results"
OUT_CSV = BASE_DIR / "summary.csv"

# Load single node rows
rows = []
if SINGLE_NODE_CSV.exists():
    with open(SINGLE_NODE_CSV) as f:
        for r in csv.DictReader(f):
            rows.append({
                "workload": r["workload"].lower(),
                "skew": float(r["skew"]),
                "mode": r["mode"],
                "lookup_am": r.get("lookup_am", ""),
                "label": r["label"],
                "tps": float(r["tps"]),
                "runtime_s": float(r.get("gateway_s") or 0.0),
                "merkle_root": "",
                "status": "PASS"
            })

# Load cluster runs
WL_LIST = ["a", "b", "c", "d", "f"]
SK_LIST = [0.0, 0.5, 0.99, 1.2]

for wl in WL_LIST:
    for sk in SK_LIST:
        sk_tag = "s" + str(sk).replace(".", "")
        matching = list(RESULTS_DIR.glob(f"cluster4_lsm4m64_{wl}_{sk_tag}_*"))
        if not matching:
            print(f"Warning: missing cluster result for {wl} {sk}")
            continue
        acc_file = matching[0] / "acceptance.json"
        acc = json.loads(acc_file.read_text())
        
        all3_tps = float(acc["all3_tps"])
        maj_tps = float(acc["majority_tps"])
        all3_s = float(acc.get("all3_ms", 0)) / 1000.0
        maj_s = float(acc.get("majority_ms", 0)) / 1000.0
        root = acc.get("post_marker_root", "")
        status = acc.get("status", "PASS")

        # Add Cluster (All-3 Verified)
        rows.append({
            "workload": wl,
            "skew": sk,
            "mode": "cluster_raft_kafka",
            "lookup_am": "lsm4m64",
            "label": "cluster_merkle_lsm",
            "tps": all3_tps,
            "runtime_s": all3_s,
            "merkle_root": root,
            "status": status
        })

        # Add Cluster (Majority Quorum)
        rows.append({
            "workload": wl,
            "skew": sk,
            "mode": "cluster_raft_kafka",
            "lookup_am": "lsm4m64",
            "label": "cluster_merkle_lsm_maj",
            "tps": maj_tps,
            "runtime_s": maj_s,
            "merkle_root": root,
            "status": status
        })

fieldnames = ["workload", "skew", "mode", "lookup_am", "label", "tps", "runtime_s", "merkle_root", "status"]
with open(OUT_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print(f"Exported {len(rows)} rows to {OUT_CSV}")
