#!/usr/bin/env python3
"""Regenerate the curated local det campaign; never starts a DB or uses network.

Default input is the frozen data/raw and evidence snapshot. --refresh-local
explicitly refreshes those copies from .bench_tmp, without editing originals.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile

# Keep local-only regeneration usable when the user's home cache is read-only.
os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "ariabc_detopt_matplotlib"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
DATA, RAW, EVIDENCE, GRAPHS = (ROOT / p for p in
                             ("data", "data/raw", "evidence", "graphs"))
REF = {5: "fdb9545f1c588209", 30: "dcb895e5961ab8e8", 100: "e82921e1ebb1b7ab"}
COLORS = {"base": "#64748b", "dd": "#d69e2e", "ev": "#2563eb", "nr": "#059669",
          "all": "#8b5cf6", "off": "#9ca3af", "final": "#0891b2",
          "settle": "#e76f51", "settleoff": "#c08497"}
LABELS = {"base": "base", "dd": "dedup (dd)", "ev": "early validate (ev)",
          "nr": "EV + dedup (nr)", "all": "+ rotation (all)", "off": "all off",
          "final": "final (pre-settle)", "settle": "settle", "settleoff": "settle off"}
CFG_ORDER = list(COLORS)


def snapshot():
    det = REPO / ".bench_tmp/detopt_20261007"
    hazard = REPO / ".bench_tmp/apply_retry_hazard"
    review = REPO / ".bench_tmp/det_protocol_review_20261007"
    copies = {det / n: RAW / n for n in ("ab_status.txt", "status.txt", "diag_status.txt")}
    copies[hazard / "repro_matrix.txt"] = RAW / "repro_matrix.txt"
    for name in ("early_validate", "tag_dedup", "trace_rot", "commit_set", "FINAL"):
        copies[det / f"{name}_REPORT.md"] = EVIDENCE / f"{name}_REPORT.md"
    copies.update({
        hazard / "SETTLE_REPORT.md": EVIDENCE / "SETTLE_REPORT.md",
        review / "REPORT.md": EVIDENCE / "det_protocol_review_REPORT.md",
        review / "remote_phase_analysis.json": EVIDENCE / "remote_phase_analysis.json",
        REPO / ".bench_tmp/post_publish_restart_review_20261007_yzpcqgbi/REPORT.md":
            EVIDENCE / "post_publish_restart_review_REPORT.md",
        det / "prompts/common.md": EVIDENCE / "critical_path_source_brief.md",
    })
    for name in ("diag_io.py", "run_io.py"):
        copies[det / "tools" / name] = EVIDENCE / name
    for name in ("results.txt", "postpub_new_explainer.png", "postpub_old_explainer.png", "random_new_gate.png"):
        copies[hazard / "website_validation" / name] = EVIDENCE / "website_validation" / name
    for run in sorted((hazard / "repro_runs").glob("m2_*")):
        for name in ("result.txt", "serial.hash", "det.hash", "pg_env.txt"):
            copies[run / name] = RAW / "repro_runs" / run.name / name
    cluster = REPO / ".bench_tmp/detopt_cluster_20261008"
    for suffix in ("", "_2", "_3"):
        copies[cluster / f"cluster_results{suffix}.csv"] = RAW / "cluster" / f"cluster_results{suffix}.csv"
        copies[cluster / f"CLUSTER_REPORT{suffix}.md"] = EVIDENCE / "cluster" / f"CLUSTER_REPORT{suffix}.md"
    # Review every cluster input before copying any of them. Usernames such as
    # admin123 identify replicas; they are not authentication secrets.
    for src, dst in copies.items():
        if dst.parent.name == "cluster" and re.search(
                r"sshpass|password|PGPASSWORD|(?:passwd|pwd)\s*[:=]", src.read_text(), re.I):
            raise ValueError(f"Cluster input needs credential redaction: {src.name}")
    for src, dst in copies.items():
        if src.stat().st_size > 1024 * 1024:
            raise ValueError(f"Oversize evidence: {src}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
    manifest = {str(dst.relative_to(ROOT)): hashlib.sha256(dst.read_bytes()).hexdigest()
                for dst in copies.values()}
    (EVIDENCE / "snapshot_sha256.json").write_text(json.dumps(manifest, indent=2) + "\n")


def kv(text):
    return dict(re.findall(r"(\w+)=([^\s]+)", text))


def parse_ab():
    rows = []
    after_marker = False
    for lineno, line in enumerate((RAW / "ab_status.txt").read_text().splitlines(), 1):
        if line.startswith("===") and "restart with io capture" in line:
            after_marker = True
        match = re.search(r"\bDONE ab (\w+)\s", line)
        if not match:
            continue
        v = kv(line)
        w = int(v["W"])
        f = float(v.get("f_await", "nan"))
        rows.append(dict(cfg=match[1], W=w, workers=int(v.get("k", 32)),
                         trial=int(v["t"]), tps=float(v.get("completed_tps", "nan")),
                         f_await=f, w_await=float(v.get("w_await", "nan")),
                         fast_mode=after_marker and np.isfinite(f) and f < 2.1,
                         state_ok=v.get("state") == REF[w],
                         rc=int(v["rc"]), divergence_count=int(v.get("divergence_count", -1)),
                         permanent_failures=int(v.get("permanent_failures", -1)),
                         state=v.get("state", ""), after_io_restart=after_marker,
                         source_line=lineno))
    df = pd.DataFrame(rows)
    assert not df.duplicated(["cfg", "W", "workers", "trial"]).any(), "Duplicate run identifiers"
    df.to_csv(DATA / "tpcc_ab_runs.csv", index=False)
    valid = df.fast_mode & df.state_ok & df.rc.eq(0) & df.divergence_count.eq(0) & df.permanent_failures.eq(0)
    summary = df[valid].groupby(["cfg", "W", "workers"], as_index=False).tps.agg(
        median="median", n="size", min="min", max="max")
    summary["order"] = summary.cfg.map(CFG_ORDER.index)
    summary = summary.sort_values(["workers", "W", "order"]).drop(columns="order")
    summary.to_csv(DATA / "tpcc_ab_summary.csv", index=False)
    return df, summary


def repro_group(name):
    return re.sub(r"_t\d+$", "", name).removeprefix("m2_")


def parse_repro():
    rows = []
    blocks = re.split(r"^=== ", (RAW / "repro_matrix.txt").read_text(), flags=re.M)
    for block in blocks:
        if not block.startswith("m2_"):
            continue
        header = block.splitlines()[0].split()
        run, workers, wl = header[:3]
        v = kv(block)
        folder = RAW / "repro_runs" / run
        result = kv((folder / "result.txt").read_text())
        for key in ("state_equal", "serial_errors", "completed_tps", "traced_tx", "restarts",
                    "post_publish_settles", "terminal_unique", "invariant", "divergence_count", "permanent_failures"):
            assert result[key] == v[key], f"Matrix / result mismatch: {run} {key}"
        serial = (folder / "serial.hash").read_bytes()
        det = (folder / "det.hash").read_bytes()
        equal = serial == det
        assert equal == (v["state_equal"].lower() == "yes"), f"Hash mismatch: {run}"
        row = dict(run=run, cfg=repro_group(run), workers=int(workers), workload=Path(wl).name,
                   state_equal=equal, tps=float(v["completed_tps"]),
                   serial_hash=serial.decode().strip(), det_hash=det.decode().strip())
        for key in ("serial_errors", "traced_tx", "restarts", "post_publish_settles", "terminal_unique",
                    "invariant", "divergence_count", "permanent_failures", "invariant_warnings"):
            row[key] = int(v[key])
        rows.append(row)
    df = pd.DataFrame(rows)
    assert set(df.run) == {p.name for p in (RAW / "repro_runs").glob("m2_*")}
    df.to_csv(DATA / "repro_results.csv", index=False)
    return df


def save(fig, name):
    fig.savefig(GRAPHS / name, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def parse_cluster():
    columns = ["section", "workload", "workers", "trial", "config", "tps",
               "divergence", "permanent_failures", "merkle_pass", "run_id", "source_report"]
    frames = []
    for suffix in ("", "_2", "_3"):
        frame = pd.read_csv(RAW / "cluster" / f"cluster_results{suffix}.csv", dtype=str).fillna("")
        if "config" not in frame:
            # Untested entries have no observed configuration.
            frame["config"] = frame.section.map(
                lambda s: "" if s.endswith("_not_run") else "default")
        frame["source_report"] = f"CLUSTER_REPORT{suffix}.md"
        frames.append(frame[columns])
    runs = pd.concat(frames, ignore_index=True)
    ids = runs.run_id[runs.run_id.ne("")]
    assert not ids.duplicated().any(), "Duplicate cluster run ID"
    runs.to_csv(DATA / "cluster_runs.csv", index=False)
    # Re-read the normalised CSV so figures and numerical tables use that
    # published data product. Unknown terminal counters remain blank, never 0.
    runs = pd.read_csv(DATA / "cluster_runs.csv")
    measured = runs[runs.tps.notna()]
    assert measured.divergence.eq(0).all() and measured.permanent_failures.eq(0).all()
    assert measured.merkle_pass.eq(1).all(), "Incomplete measured cluster validation"
    matched = runs[(runs.source_report == "CLUSTER_REPORT_2.md") & (runs.section == "YCSB-A")]
    assert set(matched.config) == {"default", "nolookahead", "noev"}
    assert set(matched.workers) == {8, 16}
    assert not matched.duplicated(["workers", "trial", "config"]).any()
    ab = matched.groupby(["workers", "config"], as_index=False).tps.agg(
        median="median", n="size", min="min", max="max")
    assert ab.n.eq(3).all(), "Incomplete matched matrix"
    refs = ab[ab.config.eq("noev")].set_index("workers")["median"]
    ab["delta_vs_noev_pct"] = 100 * (ab["median"] / ab.workers.map(refs) - 1)
    ab.to_csv(DATA / "cluster_matched_summary.csv", index=False)

    # Historical reference values are the published TPS cells in report 1,
    # not today's repository defaults or a new measurement.
    report = (EVIDENCE / "cluster/CLUSTER_REPORT.md").read_text()
    a_text = report.split("## 1. YCSB-A:", 1)[1].split("## 2.", 1)[0]
    refs = []
    for line in a_text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 9 and cells[0].isdigit():
            refs.append(dict(workers=int(cells[0]), published_tps=float(cells[5]),
                             source_report="CLUSTER_REPORT.md", reference_day="2026-10-06"))
    reference = pd.DataFrame(refs)
    assert set(reference.workers) == {1, 4, 8, 16}
    reference.to_csv(DATA / "cluster_published_reference.csv", index=False)
    reference = pd.read_csv(DATA / "cluster_published_reference.csv")
    history = runs[(runs.source_report == "CLUSTER_REPORT.md") & (runs.section == "YCSB-A")]
    history = history.groupby("workers", as_index=False).tps.agg(
        median="median", n="size", min="min", max="max").merge(reference, on="workers", validate="one_to_one")
    assert history.n.eq(3).all()
    history["delta_vs_published_pct"] = 100 * (history["median"] / history.published_tps - 1)
    history.to_csv(DATA / "cluster_historical_summary.csv", index=False)
    return runs, ab, history


def cluster_figures():
    ab = pd.read_csv(DATA / "cluster_matched_summary.csv")
    history = pd.read_csv(DATA / "cluster_historical_summary.csv")
    fig, ax = plt.subplots(figsize=(11, 6.5))
    configs = [("default", "#2563eb"), ("nolookahead", "#d69e2e"), ("noev", "#64748b")]
    width = 0.24
    for j, (cfg, color) in enumerate(configs):
        for i, workers in enumerate((8, 16)):
            r = ab[(ab.config == cfg) & (ab.workers == workers)].iloc[0]
            x = i + (j - 1) * width
            ax.bar(x, r["median"], width, color=color, label=cfg if i == 0 else None)
            ax.errorbar(x, r["median"], yerr=[[r["median"] - r["min"]], [r["max"] - r["median"]]],
                        fmt="none", ecolor="#334155", capsize=4)
            ax.annotate(f'{r["median"]:,.2f}\n{r.delta_vs_noev_pct:+.2f}% vs noev\nn={int(r.n)}',
                        (x, r["max"]), xytext=(0, 10), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9)
    ax.set_xticks([0, 1], ["W8", "W16"])
    ax.set(xlabel="Executor workers", ylabel="Majority-visible transactions / second (TPS)",
           title="YCSB-A θ0 — same-day interleaved matched A/B (2026-10-08)", ylim=(0, ab["max"].max() * 1.32))
    ax.legend(loc="upper left", ncol=3, frameon=False)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    fig.text(0.5, 0.02, "Median bars; whiskers = observed min–max, not confidence intervals; settle stays on in all configs.",
             ha="center", fontsize=10)
    fig.subplots_adjust(bottom=0.15, top=0.86)
    save(fig, "cluster_matched_ab.png")

    fig, ax = plt.subplots(figsize=(11, 6.5))
    xs = np.arange(len(history))
    for offset, column, label, color in [(-0.18, "median", "Merged default — Oct 8 (n=3)", "#2563eb"),
                                         (0.18, "published_tps", "Published — Oct 6 (n=3)", "#64748b")]:
        values = history[column]
        ax.bar(xs + offset, values, 0.34, label=label, color=color)
        for i, value in enumerate(values):
            ax.annotate(f"{value:,.2f}", (i + offset, value), xytext=(0, 8), textcoords="offset points",
                        ha="center", fontsize=9, rotation=25)
    ax.set_xticks(xs, [f"W{w}" for w in history.workers])
    ax.set(xlabel="Executor workers", ylabel="Majority-visible transactions / second (TPS)",
           title="YCSB-A θ0 — merged default vs published medians (different day)",
           ylim=(0, history.published_tps.max() * 1.25))
    ax.legend(loc="upper left", frameon=False)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    fig.text(0.5, 0.02, "Different day and source fingerprint: historical context, not a matched causal A/B.", ha="center", fontsize=10)
    fig.subplots_adjust(bottom=0.15, top=0.86)
    save(fig, "cluster_ycsb_a_vs_published.png")


def bars(summary, points, configs, name, title, percentages=False):
    fig, ax = plt.subplots(figsize=(12, 6.5))
    width = 0.8 / len(configs)
    for j, cfg in enumerate(configs):
        labelled = False
        for i, (w, workers) in enumerate(points):
            row = summary[(summary.cfg == cfg) & (summary.W == w) & (summary.workers == workers)]
            if row.empty:
                continue  # No invented bars for unavailable combinations.
            r = row.iloc[0]
            x = i + (j - (len(configs) - 1) / 2) * width
            ax.bar(x, r["median"], width, color=COLORS[cfg], label=LABELS[cfg] if not labelled else None)
            labelled = True
            ax.errorbar(x, r["median"], yerr=[[r["median"] - r["min"]], [r["max"] - r["median"]]],
                        fmt="none", ecolor="#334155", capsize=3, linewidth=1)
            label = f'n={int(r["n"])}'
            if percentages and cfg != "base":
                b = summary[(summary.cfg == "base") & (summary.W == w) & (summary.workers == workers)].iloc[0]
                label = f'{100 * (r["median"] / b["median"] - 1):+.1f}%\n{label}'
            offset = 7 + (20 if percentages and len(configs) >= 4 and j % 2 == 0 else 0)
            ax.annotate(label, (x, r["max"]), xytext=(0, offset), textcoords="offset points",
                        ha="center", va="bottom", fontsize=9)
    ax.set_xticks(range(len(points)), [f"W{w} / {k} workers" for w, k in points])
    ax.set_ylabel("Completed transactions / second (TPS)")
    ax.set_xlabel("Warehouses / executor workers")
    ax.set_title(title, pad=18)
    ax.set_ylim(0, summary[summary.cfg.isin(configs)]["max"].max() * 1.25)
    ax.legend(loc="upper left", ncol=min(3, len(configs)), frameon=False, fontsize=9)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    fig.text(0.5, 0.015, "Fast mode: f_await < 2.1 ms; medians with min–max whiskers; n = accepted fast runs",
             ha="center", fontsize=10)
    fig.subplots_adjust(bottom=0.16, top=0.88)
    save(fig, name)


def figures(df, summary, repro):
    plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False})
    bars(summary, [(w, 32) for w in (5, 30, 100)], ["base", "dd", "ev", "nr", "all"],
         "tpcc_ab_by_warehouses.png", "TPC-C v2 det optimisation — 32 workers", True)
    bars(summary, [(100, k) for k in (32, 48, 64)], ["base", "nr", "final"],
         "tpcc_worker_scaling_w100.png", "TPC-C v2 worker scaling — 100 warehouses", True)
    bars(summary, [(5, 32), (30, 32), (100, 32), (100, 48)], ["final", "settle", "settleoff"],
         "settle_overhead.png", "Settle comparison — pooled fast runs (unequal n)")
    fig, ax = plt.subplots(figsize=(10, 6))
    measured = df[df.after_io_restart & df.f_await.notna()]
    for w, color in zip((5, 30, 100), ("#d69e2e", "#2563eb", "#059669")):
        d = measured[measured.W == w]
        ax.scatter(d.f_await, d.tps, color=color, alpha=0.75, edgecolors="white", s=45,
                   label=f"W{w} (n={len(d)})")
    ax.axvline(2.1, color="#dc2626", linestyle="--", label="Fast-mode cut: 2.1 ms")
    ax.set(xlabel="Mean NVMe flush latency f_await during measured window (ms)",
           ylabel="Completed transactions / second (TPS)",
           title="Ranking disk modes — all post-restart A/B runs")
    ax.legend(frameon=False)
    ax.grid(alpha=0.2)
    fig.text(0.5, 0.015, "Configs and worker counts are pooled within W; this is descriptive, not a matched causal comparison.",
             ha="center", fontsize=9)
    fig.subplots_adjust(bottom=0.17)
    save(fig, "disk_bimodality.png")

    trace = json.loads((EVIDENCE / "remote_phase_analysis.json").read_text())
    run = next(r for r in trace["runs"] if r["accepted"]["mode"] == "det"
               and r["accepted"]["W"] == 100 and r["accepted"]["workers"] == 32)
    total = 1e6 / run["accepted"]["tps"]
    # The shared brief estimates the retry share; it is not a direct phase timer.
    brief = (EVIDENCE / "critical_path_source_brief.md").read_text()
    retry_share = float(re.search(r"~(\d+)% is retries held at the turn", brief)[1]) / 100
    validation = sum(run["metrics"][m]["mean"] for m in
                     ("conflict_ws_us", "conflict_rs_us", "publish_ws_us"))
    values = [total * retry_share, validation, total * (1 - retry_share) - validation]
    components = ["Retry wait + re-execution", "Validation + publication", "Handoff + residual"]
    pd.DataFrame({"component": components, "estimated_us": values,
                  "share_pct": [100 * v / total for v in values]}).to_csv(DATA / "critical_path_estimate.csv", index=False)
    fig, ax = plt.subplots(figsize=(11, 4))
    left = 0
    for value, label, color in zip(values, components, ("#e76f51", "#2563eb", "#94a3b8")):
        ax.barh([0], [value], left=left, height=0.5, color=color, label=label)
        ax.text(left + value / 2, 0, f"{value:.1f} µs\n{100 * value / total:.1f}%", ha="center",
                va="center", color="white", fontweight="bold")
        left += value
    ax.set(yticks=[], xlabel="Estimated amortised serial-turn budget (µs / completed transaction)",
           xlim=(0, total), title=f"W100 / 32 workers: ≈{total:.0f} µs — estimate derived from v2 traces")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=3, frameon=False, fontsize=10)
    fig.text(0.5, 0.02, f'n={run["trace_rows"]:,} trace rows; total = 10⁶ / 2,814.97 TPS; retry share ≈40% from brief; residual is not timed.',
             ha="center", fontsize=9)
    fig.subplots_adjust(bottom=0.35)
    save(fig, "critical_path_w100_32.png")

    order = ["off_fp7", "offEV_fp7", "off_nofp", "on_fp7", "on_uq", "on_uq_fp7"]
    labels = ["Settle off, EV off + FP7", "Settle off, EV on + FP7", "Settle off, no failpoint",
              "Settle on + FP7", "Settle on, unique workload", "Settle on, unique workload + FP7"]
    fig, ax = plt.subplots(figsize=(11, 4.9))
    ax.axis("off")
    cells, colors = [], []
    for cfg, label in zip(order, labels):
        runs = repro[repro.cfg == cfg].sort_values("run")
        eq = ["YES" if r else "NO" for r in runs.state_equal]
        cells.append([label, str(len(runs))] + eq + ["—"] * (3 - len(eq)))
        colors.append(["#f1f5f9", "#f1f5f9"] + ["#bbf7d0" if r else "#fecaca" for r in runs.state_equal]
                      + ["#f8fafc"] * (3 - len(eq)))
    table = ax.table(cellText=cells, cellColours=colors, colLabels=["Configuration", "n", "Trial 1", "Trial 2", "Trial 3"],
                     colWidths=[0.56, 0.06, 0.126, 0.126, 0.126], loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.1)
    for row in range(1, len(cells) + 1):
        table[row, 0].set_text_props(ha="left")
    ax.set_title("Serial vs det final-state equality — valid m2 runs only", pad=15)
    fig.text(0.5, 0.035, "16 workers, 20,000 statements/run; FP7 injects a first-apply failure for tx_id % 7 = 0.",
             ha="center", fontsize=10)
    save(fig, "repro_state_equal.png")


def markdown_table(headers, rows):
    return "\n".join(["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
                     + ["| " + " | ".join(map(str, r)) + " |" for r in rows])


def update_readme(df, summary, repro, cluster, cluster_ab, cluster_history):
    path = ROOT / "README.md"
    if not path.exists():
        return
    def item(cfg, w, workers):
        d = summary[(summary.cfg == cfg) & (summary.W == w) & (summary.workers == workers)]
        return None if d.empty else d.iloc[0]
    def cell(cfg, w, workers, reference=None):
        r = item(cfg, w, workers)
        if r is None:
            return "—"
        value = f'{r["median"]:,.2f} ({int(r["n"])})'
        if reference:
            b = item(reference, w, workers)
            value += f'; {100 * (r["median"] / b["median"] - 1):+.2f}%'
        return value
    tables = {}
    tables["warehouse"] = markdown_table(["W", "base", "dd", "ev", "nr", "all"],
        [[w] + [cell(c, w, 32, "base" if c != "base" else None) for c in ["base", "dd", "ev", "nr", "all"]]
         for w in (5, 30, 100)])
    tables["workers"] = markdown_table(["Workers (W100)", "base", "nr", "final (pre-settle)"],
        [[k, cell("base", 100, k), cell("nr", 100, k, "base"), cell("final", 100, k, "base")]
         for k in (32, 48, 64)])
    tables["settle"] = markdown_table(["W / workers", "final", "settle", "settleoff", "settle vs final"],
        [[f"{w} / {k}", cell("final", w, k), cell("settle", w, k), cell("settleoff", w, k),
          f'{100 * (item("settle", w, k)["median"] / item("final", w, k)["median"] - 1):+.2f}%']
         for w, k in [(5, 32), (30, 32), (100, 32), (100, 48)]])
    tables["rotation"] = markdown_table(["W (32 workers)", "nr median TPS", "all median TPS", "Rotation change vs nr"],
        [[w, f'{item("nr", w, 32)["median"]:,.2f}', f'{item("all", w, 32)["median"]:,.2f}',
          f'{100 * (item("all", w, 32)["median"] / item("nr", w, 32)["median"] - 1):+.2f}%']
         for w in (5, 30, 100)])
    tables["all_summary"] = markdown_table(["cfg", "W", "workers", "n fast", "Median TPS", "Min TPS", "Max TPS"],
        [[r.cfg, r.W, r.workers, r.n, f'{r.median:,.2f}', f'{r.min:,.2f}', f'{r.max:,.2f}']
         for r in summary.itertuples(index=False)])
    tables["repro"] = markdown_table(["Configuration", "n", "state_equal (trial order)", "Settles / run",
                                         "Terminal 23505 / run", "Serial errors / run"],
        [[cfg, len(d), ", ".join("yes" if x else "NO" for x in d.state_equal),
          ", ".join(map(str, sorted(d.post_publish_settles.unique()))),
          ", ".join(map(str, sorted(d.terminal_unique.unique()))),
          ", ".join(map(str, sorted(d.serial_errors.unique())))]
         for cfg in ["off_fp7", "offEV_fp7", "off_nofp", "on_fp7", "on_uq", "on_uq_fp7"]
         for d in [repro[repro.cfg == cfg].sort_values("run")]])
    post = df[df.after_io_restart]
    tables["audit"] = (f'Parsed **{len(df)}** completed A/B runs: **{len(df) - len(post)}** pre-restart runs retained '
        f'in the run CSV but excluded from analysis; **{len(post)}** post-restart runs, '
        f'**{int(post.fast_mode.sum())}** fast and **{int((~post.fast_mode).sum())}** outside the fast cut. '
        f'All **{int(df.state_ok.sum())}/{len(df)}** recorded state prefixes match; every A/B line has rc=0, '
        'divergence_count=0 and permanent_failures=0. These fields are archived in the CSV for audit.')
    tables["cluster_matched"] = markdown_table(
        ["Workers", "Config", "n", "Median TPS", "Min–max TPS", "Δ vs noev"],
        [[w, cfg, int(r.n), f'{r["median"]:,.2f}', f'{r["min"]:,.2f}–{r["max"]:,.2f}',
          f'{r.delta_vs_noev_pct:+.2f}%']
         for w in (8, 16) for cfg in ("default", "nolookahead", "noev")
         for r in [cluster_ab[(cluster_ab.workers == w) & (cluster_ab.config == cfg)].iloc[0]]])
    tables["cluster_historical"] = markdown_table(
        ["Workers", "Oct 8 merged-default median TPS (n=3)", "Oct 6 published median TPS (n=3)", "Δ (different day)"],
        [[int(r.workers), f'{r.median:,.2f}', f'{r.published_tps:,.2f}', f'{r.delta_vs_published_pct:+.2f}%']
         for r in cluster_history.itertuples(index=False)])
    d = cluster[cluster.section.eq("YCSB-D")]
    report1 = (EVIDENCE / "cluster/CLUSTER_REPORT.md").read_text()
    d_text = report1.split("## 2. YCSB-D:", 1)[1].split("## Post-marker", 1)[0]
    published_d = next(float(cells[4]) for line in d_text.splitlines()
                       for cells in [[c.strip() for c in line.strip().strip("|").split("|")]]
                       if len(cells) == 8 and cells[0].isdigit())
    tables["cluster_d"] = markdown_table(
        ["Workload / workers", "Trial 1 TPS", "Trial 2 TPS", "Median TPS (n=2)", "Historical TPS (n=1)", "Δ (different day)"],
        [["YCSB-D θ0.99 / 16", f'{d.iloc[0].tps:,.2f}', f'{d.iloc[1].tps:,.2f}', f'{d.tps.median():,.2f}',
          f'{published_d:,.2f}', f'{100 * (d.tps.median() / published_d - 1):+.2f}%']])
    correctness = []
    for label, selected, verdict in [
        ("Initial YCSB-A θ0, W1/4/8/16", cluster[cluster.source_report.eq("CLUSTER_REPORT.md") & cluster.section.eq("YCSB-A")], "phase8"),
        ("YCSB-D θ0.99, W16", d, "phase8"),
        ("Matched YCSB-A θ0, W8/16, all configs", cluster[cluster.source_report.eq("CLUSTER_REPORT_2.md") & cluster.section.eq("YCSB-A")], "phase8"),
        ("Standalone Merkle", cluster[cluster.section.eq("standalone_merkle")], "standalone"),
        ("Recovery A (off), both reports", cluster[cluster.section.eq("recovery_A")], "phase8"),
        ("Recovery C, all attempts", cluster[cluster.section.eq("recovery_C")], "unknown"),
        ("Failed startup prerequisites", cluster[cluster.section.isin(["merkle_startup", "merkle_startup_failed"])], "unknown")]:
        if verdict == "phase8":
            assert selected.divergence.eq(0).all() and selected.permanent_failures.eq(0).all() and selected.merkle_pass.eq(1).all()
        correctness.append([label, len(selected), "0" if verdict == "phase8" else "UNKNOWN",
                            "0" if verdict == "phase8" else "UNKNOWN",
                            "PASS / PASS / PASS" if verdict != "unknown" else "NOT RUN",
                            "Equal" if verdict == "phase8" else ("Equal test roots; no Phase 8" if verdict == "standalone" else "NOT RUN")])
    tables["cluster_correctness"] = markdown_table(
        ["Campaign", "Runs", "Terminal divergence", "Terminal permanent failures", "Merkle admin123 / user4 / utkarsh", "Phase 8 roots"], correctness)
    # Recovery timings are report event fields, not inferred terminal metrics.
    events = {}
    for suffix in ("_2", "_3"):
        report = (EVIDENCE / "cluster" / f"CLUSTER_REPORT{suffix}.md").read_text()
        events[f"CLUSTER_REPORT{suffix}.md"] = [int(m) for m in re.findall(r"^RECOVERY_EVENT .*?\brepair_ms=(\d+)", report, re.M)]
    recovery_rows = []
    positions = {name: 0 for name in events}
    for r in cluster[cluster.section.isin(["recovery_A", "recovery_C"])].itertuples(index=False):
        is_a = r.section == "recovery_A"
        repair = "—"
        if not is_a:
            repair = str(events[r.source_report][positions[r.source_report]])
            positions[r.source_report] += 1
        recovery_rows.append(["A" if is_a else "C", r.source_report, r.config, int(r.trial),
                              f'{r.tps:,.2f}' if is_a else "UNKNOWN", repair,
                              "PASS" if is_a else "FAIL / exit 143",
                              "valid; 160000/160000" if is_a else "Not finalised",
                              "0 / 0" if is_a else "UNKNOWN / UNKNOWN",
                              "PASS / PASS / PASS; equal" if is_a else "NOT RUN"])
    tables["cluster_recovery"] = markdown_table(
        ["Case", "Source report", "Config", "Trial", "TPS", "repair_ms (live PASS)", "Outcome", "All-3 audit", "Terminal divergence / failures", "Phase 8 / roots"], recovery_rows)
    text = path.read_text()
    for marker, table in tables.items():
        pattern = rf"(<!-- BEGIN {marker} -->).*?(<!-- END {marker} -->)"
        text, count = re.subn(pattern, lambda m: m[1] + "\n\n" + table + "\n\n" + m[2], text, flags=re.S)
        assert count == 1, f"Missing / duplicate README marker: {marker}"
    path.write_text(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-local", action="store_true")
    args = parser.parse_args()
    for folder in (DATA, RAW, EVIDENCE, GRAPHS):
        folder.mkdir(parents=True, exist_ok=True)
    if args.refresh_local:
        snapshot()
    manifest = json.loads((EVIDENCE / "snapshot_sha256.json").read_text())
    for name, expected in manifest.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == expected, f"Snapshot changed: {name}"
    df, summary = parse_ab()
    repro = parse_repro()
    assert df.state_ok.all() and df.rc.eq(0).all() and df.divergence_count.eq(0).all() and df.permanent_failures.eq(0).all()
    figures(df, summary, repro)
    cluster, cluster_ab, cluster_history = parse_cluster()
    cluster_figures()
    update_readme(df, summary, repro, cluster, cluster_ab, cluster_history)
    print(f"Cluster: {len(cluster)} source rows; {len(cluster[cluster.tps.notna()])} measured PASS runs; "
          f"{len(cluster[cluster.section.eq('recovery_C')])} unfinalised recovery C attempts")
    print(f"A/B: {len(df)} runs, {int(df.fast_mode.sum())} fast; reproducer: {len(repro)} runs, {int(repro.state_equal.sum())} equal")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
