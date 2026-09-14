#!/usr/bin/env python3
"""
Compare 66-Run Benchmark Suite against Baseline in YCSB_72_WORKLOADS_DETAILED_ANALYSIS.md
"""
import csv
import sys
from pathlib import Path

def parse_summary(csv_path):
    data = {}
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = (r["mode"], r["workload"], int(r["bcdb_workers"]))
            data[key] = {
                "tps": float(r["tps"]),
                "wall_time_ms": float(r["wall_time_ms"]),
                "merkle_pass": int(r["merkle_pass"]),
                "divergence": int(r["divergence_count"]),
                "failures": int(r["permanent_failures"]),
            }
    return data

def main():
    repo_root = Path("/work/ARIABC/AriaBC")
    baseline_csv = repo_root / "scripts/bench_full_results/ycsb_all_72_sweep/summary.csv"
    new_csv = repo_root / "scripts/bench_full_results/ycsb_66_sweep_direct_root/summary.csv"

    if not new_csv.exists():
        print(f"New summary not found at: {new_csv}")
        return

    baseline_data = parse_summary(baseline_csv)
    new_data = parse_summary(new_csv)

    print(f"Loaded {len(baseline_data)} baseline records and {len(new_data)} new records.")

    workload_names = [
        ("ycsb_workload_a_skew_0_99_20k.txt", "Workload A (50% R, 50% U)"),
        ("ycsb_workload_b_skew_0_99_20k.txt", "Workload B (95% R, 5% U)"),
        ("ycsb_workload_c_skew_0_99_20k.txt", "Workload C (100% R)"),
        ("ycsb_workload_d_skew_0_99_20k.txt", "Workload D (95% R, 5% I)"),
        ("ycsb_workload_f_skew_0_99_20k.txt", "Workload F (67% R, 33% RMW)"),
        ("ycsb_workload_balanced_dml_skew_0_99_20k.txt", "Balanced DML (Full CRUD)"),
        ("ycsb_workload_delete_heavy_skew_0_99_20k.txt", "Delete Heavy (50% D)"),
        ("ycsb_workload_dml_heavy_skew_0_99_20k.txt", "DML Heavy (50% U, 21% I, 19% D)"),
        ("ycsb_workload_pure_dml_skew_0_99_20k.txt", "Pure DML (50% U, 25% I, 25% D)"),
        ("ycsb_workload_all_insert_skew_0_99_20k.txt", "ALL_INSERT (100% I)"),
        ("ycsb_workload_all_update_skew_0_99_20k.txt", "ALL_UPDATE (100% U)"),
    ]

    modes = ["pg", "bcdb_det", "bcdb_merkle"]
    workers_list = [8, 16]

    print("\n" + "=" * 110)
    print(f"{'Workload':<30} | {'Workers':<7} | {'Mode':<11} | {'Baseline TPS':<12} | {'New TPS':<10} | {'Delta (%)':<10} | {'Merkle':<6} | {'Div'}")
    print("=" * 110)

    rows = []
    for wl_file, wl_display in workload_names:
        for w in workers_list:
            for m in modes:
                key = (m, wl_file, w)
                if key in new_data:
                    n_info = new_data[key]
                    b_info = baseline_data.get(key, {"tps": 0.0, "divergence": 0})
                    b_tps = b_info["tps"]
                    n_tps = n_info["tps"]
                    delta_pct = ((n_tps - b_tps) / b_tps * 100.0) if b_tps > 0 else 0.0
                    m_pass = "PASS" if n_info["merkle_pass"] == 1 or m != "bcdb_merkle" else "FAIL"
                    div = n_info["divergence"]
                    rows.append({
                        "workload": wl_display,
                        "wl_file": wl_file,
                        "workers": w,
                        "mode": m,
                        "baseline_tps": b_tps,
                        "new_tps": n_tps,
                        "delta_pct": delta_pct,
                        "merkle_pass": m_pass,
                        "divergence": div
                    })
                    print(f"{wl_display:<30} | {w:<7} | {m:<11} | {b_tps:>12.2f} | {n_tps:>10.2f} | {delta_pct:>+9.2f}% | {m_pass:<6} | {div}")

    print("=" * 110)
    return rows

if __name__ == "__main__":
    main()
