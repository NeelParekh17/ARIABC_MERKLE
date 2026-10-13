#!/usr/bin/env python3
"""Online-recovery paper campaign (2026-10-12): tables and figures from curated run evidence.

Inputs: runs/cluster4_paper_20261010_recov_matrix_*/acceptance.json, server_node*.log, tps_timeline.csv,
fault_timeline.env. Outputs: all_attempts.csv, scenario_summary.csv, storage_flags.csv, figures/*.{pdf,png}.
Usage: python3 analyze.py   (run from this directory)
"""
import csv, json, re, statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
RUNS = HERE / "runs"
FIG = HERE / "figures"
FIG.mkdir(exist_ok=True)

SCENARIOS = ["A", "B", "C", "M_passive", "M_active", "M_mixed", "L", "L_mix", "C_deep"]
LABEL = {
    "A": "A: no recovery", "B": "B: recovery on, no fault", "C": "C: follower update",
    "M_passive": "M-passive", "M_active": "M-active", "M_mixed": "M-mixed: follower mixed",
    "L": "L: leader update", "L_mix": "L-mix: leader mixed", "C_deep": "C-deep: C + tracing",
}
NODES = ["node1_admin123", "node2_user4", "node4_utkarsh"]
FSYNC_RE = re.compile(r"PROFILE_RAFT_STORAGE .*?fdatasync_total_ms=([0-9.]+) fdatasync_max_ms=([0-9.]+)"
                      r" fdatasync_p50_ms=([0-9.]+) fdatasync_p95_ms=([0-9.]+) fdatasync_p99_ms=([0-9.]+)")
# Okabe-Ito, colorblind safe.
C_BASE, C_FAULT, C_LEADER, C_FLAG = "#0072B2", "#E69F00", "#009E73", "#D55E00"


def fsync(run_dir, node):
    last = None
    for line in (run_dir / f"server_{node}.log").read_text(errors="replace").splitlines():
        m = FSYNC_RE.search(line)
        if m:
            last = [float(x) for x in m.groups()]
    return last


def load():
    rows = []
    for d in sorted(RUNS.glob("cluster4_paper_20261010_recov_matrix_*")):
        acc = json.loads((d / "acceptance.json").read_text())
        ev = acc["events"][0] if acc["events"] else {}
        r = {
            "run_id": d.name, "scenario": acc["scenario"], "rep": int(acc["rep"]), "attempt": int(acc["attempt"]),
            "status": acc["status"], "tps": float(acc["tps_majority_visible"]),
            "cut_ms": acc["cut_ms"], "repair_ms": acc["repair_ms"], "catchup_ms": acc["catchup_ms"],
            "total_recovery_ms": acc["total_recovery_ms"], "mismatched_partitions": acc["mismatched_partitions"],
            "differing_leaves": acc["differing_leaves"], "candidate_rows": acc["candidate_rows"],
            "rows_deleted": acc["rows_deleted"], "rows_upserted": acc["rows_upserted"], "full_copies": acc["full_copies"],
            "empty_100ms_buckets": acc["empty_100ms_buckets"], "max_completion_gap_ms": acc["max_completion_gap_ms"],
            "quorum_completions": acc["quorum_completions"], "phase8_merkle_pass": acc["phase8_merkle_pass"],
            "phase8_root": acc["phase8_root"], "detected_by": ev.get("reason", ""), "target_node": ev.get("node", ""),
            "reference_node": ev.get("ref", ""), "recovery_at_det_seq": ev.get("B", ""),
            "failures": ";".join(acc["failures"]), "noise": ";".join(acc["noise"]),
        }
        for n in NODES:
            r[f"fsync_total_ms_{n}"], _, _, _, r[f"fsync_p99_ms_{n}"] = fsync(d, n)
        rows.append(r)
    return rows


def flag_storage(rows):
    """Post-hoc rule, applied uniformly: a run is storage-flagged if any node's total Raft-log fdatasync
    time exceeds 2x that node's median over all matrix attempts."""
    med = {n: statistics.median(r[f"fsync_total_ms_{n}"] for r in rows) for n in NODES}
    flags = []
    for r in rows:
        bad = [n for n in NODES if r[f"fsync_total_ms_{n}"] > 2 * med[n]]
        r["storage_flag"] = ",".join(bad)
        r["primary"] = r["status"] == "ACCEPTED" and not bad
        if bad:
            flags.append({"run_id": r["run_id"], "nodes": r["storage_flag"],
                          **{f"fsync_total_ms_{n}": r[f"fsync_total_ms_{n}"] for n in NODES},
                          **{f"node_median_fsync_total_ms_{n}": med[n] for n in NODES}})
    return flags


def write_csv(path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def summarize(rows):
    base = statistics.median(r["tps"] for r in rows if r["scenario"] == "A" and r["primary"])
    out = []
    for s in SCENARIOS:
        prim = [r for r in rows if r["scenario"] == s and r["primary"]]
        allacc = [r for r in rows if r["scenario"] == s and r["status"] == "ACCEPTED"]
        tps = [r["tps"] for r in prim]
        med = lambda k: statistics.median(float(r[k]) for r in prim)
        out.append({
            "scenario": s, "n_primary": len(prim), "n_accepted_all": len(allacc),
            "median_tps": statistics.median(tps), "min_tps": min(tps), "max_tps": max(tps),
            "median_delta_vs_A_pct": 100 * (statistics.median(tps) / base - 1),
            "median_tps_all_accepted": statistics.median(r["tps"] for r in allacc),
            "median_cut_ms": med("cut_ms"), "median_repair_ms": med("repair_ms"), "median_catchup_ms": med("catchup_ms"),
            "median_total_recovery_ms": med("total_recovery_ms"),
            "min_total_recovery_ms": min(float(r["total_recovery_ms"]) for r in prim),
            "max_total_recovery_ms": max(float(r["total_recovery_ms"]) for r in prim),
            "median_mismatched_partitions": med("mismatched_partitions"), "median_rows_upserted": med("rows_upserted"),
            "median_rows_deleted": med("rows_deleted"), "full_copies_total": sum(int(r["full_copies"] or 0) for r in prim),
            "max_empty_100ms_buckets": max(int(r["empty_100ms_buckets"] or 0) for r in prim),
            "median_max_gap_ms": med("max_completion_gap_ms"),
            "max_max_gap_ms": max(float(r["max_completion_gap_ms"]) for r in prim),
            "merkle_pass_all": all(r["phase8_merkle_pass"] == "PASS" for r in prim),
            "roots": ";".join(sorted({r["phase8_root"] for r in prim})),
        })
    return base, out


def style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.6)
    ax.set_axisbelow(True)


def save(fig, name):
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(FIG / f"{name}.{ext}", dpi=300)
    plt.close(fig)


def color(s):
    return C_BASE if s in ("A", "B") else (C_LEADER if s.startswith("L") else C_FAULT)


def fig_tps(rows, base, summ):
    fig, ax = plt.subplots(figsize=(3.35, 2.6))
    for i, s in enumerate(SCENARIOS):
        for r in rows:
            if r["scenario"] != s or r["status"] != "ACCEPTED":
                continue
            if r["primary"]:
                ax.plot(i, r["tps"] / 1000, "o", ms=3.2, color=color(s), alpha=0.85)
            else:
                ax.plot(i, r["tps"] / 1000, "x", ms=4, color=C_FLAG)
        m = next(x for x in summ if x["scenario"] == s)["median_tps"] / 1000
        ax.hlines(m, i - 0.3, i + 0.3, color="black", linewidth=1.2)
    ax.axhline(base / 1000, color=C_BASE, linestyle=":", linewidth=0.8)
    ax.set_xticks(range(len(SCENARIOS)), [s.replace("_", "-") for s in SCENARIOS], rotation=45, ha="right", fontsize=7)
    ax.set_ylabel("Majority-visible ktx/s", fontsize=8)
    ax.tick_params(axis="y", labelsize=7)
    ax.set_ylim(0, 10)
    style(ax)
    ax.text(0.99, 0.04, "bar = median of 3; x = storage-flagged run", transform=ax.transAxes,
            ha="right", fontsize=6, color="#555555")
    save(fig, "recovery_tps_by_scenario")


def fig_phases(rows, summ):
    """Each bar is the run with the median total recovery time in its scenario, so the stacked phases and the
    remainder (detect, quarantine, drain, rebase) sum exactly to that run's total; whiskers are min-max total."""
    fault = [x for x in summ if x["scenario"] not in ("A", "B")]
    fig, ax = plt.subplots(figsize=(3.35, 2.4))
    xs = range(len(fault))
    picked = []
    for x in fault:
        prim = sorted((r for r in rows if r["scenario"] == x["scenario"] and r["primary"]),
                      key=lambda r: float(r["total_recovery_ms"]))
        picked.append(prim[len(prim) // 2])
    cut = [float(r["cut_ms"]) / 1000 for r in picked]
    rep = [float(r["repair_ms"]) / 1000 for r in picked]
    cat = [float(r["catchup_ms"]) / 1000 for r in picked]
    tot = [float(r["total_recovery_ms"]) / 1000 for r in picked]
    other = [t - a - b - c for t, a, b, c in zip(tot, cut, rep, cat)]
    ax.bar(xs, cut, color="#56B4E9", label="snapshot cut")
    ax.bar(xs, rep, bottom=cut, color=C_FLAG, label="Merkle repair")
    b2 = [a + b for a, b in zip(cut, rep)]
    ax.bar(xs, cat, bottom=b2, color="#999999", label="log catch-up")
    ax.bar(xs, other, bottom=[a + c for a, c in zip(b2, cat)], color="#F0E442", label="other")
    lo = [t - x["min_total_recovery_ms"] / 1000 for t, x in zip(tot, fault)]
    hi = [x["max_total_recovery_ms"] / 1000 - t for t, x in zip(tot, fault)]
    ax.errorbar(xs, tot, yerr=[lo, hi], fmt="none", ecolor="black", elinewidth=0.8, capsize=2)
    ax.set_xticks(list(xs), [x["scenario"].replace("_", "-") for x in fault], rotation=45, ha="right", fontsize=7)
    ax.set_ylabel("Recovery time (s)", fontsize=8)
    ax.tick_params(axis="y", labelsize=7)
    ax.legend(fontsize=6, frameon=False, loc="lower center", ncol=4, bbox_to_anchor=(0.5, 1.0), handlelength=1.2, columnspacing=0.8)
    style(ax)
    save(fig, "recovery_phase_breakdown")


def timeline(run):
    d = RUNS / run
    t, v = [], []
    with (d / "tps_timeline.csv").open() as f:
        for r in csv.DictReader(f):
            t.append(int(r["t_ms"]) / 1000)
            v.append(float(r["tps"]) / 1000)
    env = dict(l.split("=", 1) for l in (d / "fault_timeline.env").read_text().split() if "=" in l) \
        if (d / "fault_timeline.env").exists() else {}
    fault = (int(env["fault_inject_start_epoch_ms"]) - int(env["gateway_start_epoch_ms"])) / 1000 \
        if "fault_inject_start_epoch_ms" in env else None
    return t, v, fault


def fig_timeline():
    runs = [("cluster4_paper_20261010_recov_matrix_A_r1_a1", "A: no recovery", C_BASE),
            ("cluster4_paper_20261010_recov_matrix_C_r1_a1", "C: follower update", C_FAULT),
            ("cluster4_paper_20261010_recov_matrix_L_mix_r1_a1", "L-mix: leader mixed", C_LEADER)]
    fig, ax = plt.subplots(figsize=(3.35, 2.2))
    fault = None
    for run, lab, c in runs:
        t, v, f = timeline(run)
        ax.plot(t, v, color=c, linewidth=0.8, label=lab)
        fault = f if f is not None else fault
    if fault is not None:
        ax.axvline(fault, color="black", linestyle="--", linewidth=0.7)
        ax.text(fault + 0.3, 0.6, "fault injected", fontsize=6)
    ax.set_xlabel("Time since gateway start (s)", fontsize=8)
    ax.set_ylabel("ktx/s (100 ms bins)", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.set_ylim(0, 12)
    ax.legend(fontsize=6, frameon=False, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.0))
    style(ax)
    save(fig, "recovery_tps_timeline")


def fig_flagged():
    fig, ax = plt.subplots(figsize=(3.35, 2.0))
    for run, lab, c in [("cluster4_paper_20261010_recov_matrix_M_active_r2_a1", "M-active r2 a1 (flagged)", C_FLAG),
                        ("cluster4_paper_20261010_recov_matrix_M_active_r2_a2", "M-active r2 a2 (extra)", C_FAULT)]:
        t, v, f = timeline(run)
        ax.plot(t, v, color=c, linewidth=0.8, label=lab)
    ax.set_xlabel("Time since gateway start (s)", fontsize=8)
    ax.set_ylabel("ktx/s (100 ms bins)", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.legend(fontsize=6, frameon=False)
    style(ax)
    save(fig, "recovery_flagged_storage_stall")


def main():
    rows = load()
    flags = flag_storage(rows)
    base, summ = summarize(rows)
    write_csv(HERE / "all_attempts.csv", rows)
    write_csv(HERE / "scenario_summary.csv", summ)
    if flags:
        write_csv(HERE / "storage_flags.csv", flags)
    fig_tps(rows, base, summ)
    fig_phases(rows, summ)
    fig_timeline()
    fig_flagged()
    print(f"baseline A median {base:.2f}; attempts {len(rows)}; primary {sum(r['primary'] for r in rows)}; "
          f"flagged {[f['run_id'] for f in flags]}")


if __name__ == "__main__":
    main()
