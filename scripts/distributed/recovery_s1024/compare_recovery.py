#!/usr/bin/env python3
"""Read-only recovery comparison; stdlib only, missing evidence stays UNKNOWN.

Uses the published generator's field names and signed TPS delta convention.
Prefer gateway events over their duplicates in runner.log. Multiple events
are summed, and their count is retained. Outputs never overwrite a file.
"""
import argparse
import csv
import math
from pathlib import Path
import re
import statistics
import sys

REPO = Path(__file__).resolve().parents[3]
PUBLISHED = REPO / "Final_Results/ONLINE_RECOVERY/summary.csv"
FIELDS = ["dataset", "run_id", "scenario", "group", "tps_majority_visible",
          "overhead_vs_baseline_pct", "published_run_id", "tps_change_vs_published_pct",
          "cut_ms", "repair_ms", "catchup_ms", "total_recovery_ms",
          "mismatched_partitions", "differing_leaves", "rows_upserted", "rows_deleted",
          "candidate_rows", "full_copies", "empty_100ms_buckets", "max_completion_gap_ms",
          "phase8_merkle_pass", "phase8_root", "all3_audit_valid", "divergence_count",
          "permanent_failures", "recovery_triggered_count", "recovery_success_count",
          "recovery_failure_count", "event_count", "event_failures", "event_live",
          "evidence_status", "warnings"]
EVENT_FIELDS = {"cut_ms": "cut_ms", "repair_ms": "repair_ms", "catchup_ms": "catchup_ms",
                "total_recovery_ms": "total_ms", "mismatched_partitions": "mismatched_partitions",
                "differing_leaves": "differing_leaves", "rows_upserted": "rows_upserted",
                "rows_deleted": "rows_deleted", "full_copies": "full_copies"}
MATCHES = {"A": "cluster4_final_A_baseline_180113", "B": "cluster4_final_B_nofault_180113",
           "C": "cluster4_final_C_fault_180113", "M_mixed": "cluster4_test_mixed_utkarsh_184953",
           "L_mix_prio": "cluster4_test_leader_mixed_192316"}


def read_text(path):
    return path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""


def env(path):
    out = {}
    for line in read_text(path).splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def kv(line):
    return dict(re.findall(r"(?:^|\s)(\w+)=([^\s]+)", line))


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def delta(value, base):
    v, b = number(value), number(base)
    return round(100 * (v / b - 1), 4) if v is not None and b and b > 0 else "UNKNOWN"


def aggregate(events, key):
    values = [number(e.get(key)) for e in events]
    if not values or any(v is None for v in values):
        return "UNKNOWN"
    n = sum(values)
    return int(n) if n.is_integer() else n


def scenario(path, meta):
    if meta.get("scenario"):
        return meta["scenario"]
    for key in ("L_mix_prio", "M_mixed", "A", "B", "C"):
        if re.search(r"(?:^|_)" + key + r"(?:_|$)", path.name):
            return key
    return "UNKNOWN"


def phase8(path, runner):
    # Three explicit post-marker readbacks are stronger than a wrapper PASS.
    vals = []
    for node in ("1_admin123", "2_user4", "4_utkarsh"):
        parts = read_text(path / ("post_verify_readback_node" + node + ".out")).strip().split("|")
        if len(parts) != 4 or not parts[0].isdigit() or not re.fullmatch(r"[0-9a-f]{64}", parts[1]):
            return "UNKNOWN", "UNKNOWN"
        vals.append(parts)
    ok = all(v[2] == "t" and v == vals[0] for v in vals)
    claim = re.findall(r"usertable_small consistency:\s+(\w+).*root=([0-9a-f]+)", runner)
    if claim and claim[-1] != ("PASS", vals[0][1]):
        ok = False
    return ("PASS" if ok else "FAIL"), vals[0][1]


def extract(path):
    record = dict.fromkeys(FIELDS, "UNKNOWN")
    summary = env(path / "run_summary.env")
    if not summary and (path / "run_summary.csv").is_file():
        with (path / "run_summary.csv").open(newline="") as f:
            summary = next(csv.DictReader(f), {})
    meta = env(path / "recovery_s1024.env")
    geometry = re.search(r"(?:^|_)(f\d+s\d+)(?:_|$)", path.name)
    record.update({"dataset": "new", "run_id": path.name,
                   "scenario": scenario(path, meta),
                   "group": meta.get("group", geometry.group(1) if geometry else "UNKNOWN")})
    for key in FIELDS:
        if key in summary:
            record[key] = summary[key]
    runner = read_text(path / "runner.log")
    gateway = read_text(path / "gateway_test.log")
    source = gateway if "RECOVERY_EVENT " in gateway else runner
    events = [kv(line) for line in source.splitlines() if line.startswith("RECOVERY_EVENT ")]
    record["event_count"] = len(events)
    record["event_failures"] = sum(e.get("result") != "PASS" for e in events)
    record["event_live"] = "yes" if events and all(e.get("live") == "1" for e in events) else "UNKNOWN"
    if events:
        for dest, key in EVENT_FIELDS.items():
            record[dest] = aggregate(events, key)
    elif (record["scenario"] in ("A", "B") and
          summary.get("recovery_triggered_count") == "0" and
          summary.get("recovery_failure_count") == "0"):
        for dest in EVENT_FIELDS:
            record[dest] = 0
        record["candidate_rows"] = 0
    ctrl = []
    for name in ("server_node1_admin123.log", "server_node2_user4.log", "server_node4_utkarsh.log"):
        ctrl.extend(kv(line) for line in read_text(path / name).splitlines()
                    if line.startswith("RECOVERY_CTRL ") and "verb=RECOVER " in line and "candidate_rows=" in line)
    if ctrl:
        record["candidate_rows"] = aggregate(ctrl, "candidate_rows")
    timeline = [kv(line) for line in runner.splitlines() if line.startswith("TPS_TIMELINE ")]
    if timeline:
        record["empty_100ms_buckets"] = timeline[-1].get("empty_buckets", "UNKNOWN")
        record["max_completion_gap_ms"] = timeline[-1].get("max_completion_gap_ms", "UNKNOWN")
    elif (path / "tx_latency.csv").is_file():
        # Same completion-origin buckets and 2% tail exclusion as tps_timeline.py.
        with (path / "tx_latency.csv").open(newline="") as f:
            times = sorted(float(r["finish_ms"]) for r in csv.DictReader(f))
        if times:
            counts = [0] * (int((times[-1] - times[0]) // 100) + 1)
            for t in times:
                counts[int((t - times[0]) // 100)] += 1
            lo, hi = int(len(counts) * .02), max(int(len(counts) * .02) + 1, int(len(counts) * .98))
            record["empty_100ms_buckets"] = counts[lo:hi].count(0)
            record["max_completion_gap_ms"] = round(max((b - a for a, b in zip(times, times[1:])), default=0), 2)
    record["phase8_merkle_pass"], record["phase8_root"] = phase8(path, runner)
    warnings = []
    if len(ctrl) != len(events):
        warnings.append("server/gateway recovery counts differ")
    expected_fault = record["scenario"] in ("C", "M_mixed", "L_mix_prio")
    audit_ok = (summary.get("all3_audit_valid") == "yes" and
                summary.get("divergence_count") == "0" and summary.get("permanent_failures") == "0" and
                summary.get("client_quorum_complete_count") == "160000" and
                summary.get("recovery_failure_count") == "0" and record["phase8_merkle_pass"] == "PASS")
    recovery_ok = (bool(events) and not record["event_failures"] and
                   record["event_live"] == "yes" and number(summary.get("recovery_success_count")) == len(events))
    if meta.get("exit_code") not in (None, "0"):
        audit_ok = False
        warnings.append("nonzero runner exit code")
    if meta.get("collection_exit_code") not in (None, "0"):
        audit_ok = False
        warnings.append("nonzero collection exit code")
    if record["scenario"] in ("A", "B") and (events or summary.get("recovery_triggered_count") != "0"):
        audit_ok = False
        warnings.append("unexpected recovery in no-fault scenario")
    record["evidence_status"] = "PASS" if audit_ok and (not expected_fault or recovery_ok) else "INCOMPLETE_OR_FAIL"
    if expected_fault and not events:
        warnings.append("fault scenario has no recovery event")
    if record["scenario"] == "UNKNOWN":
        warnings.append("supply recovery_s1024.env scenario")
    record["warnings"] = "; ".join(warnings)
    return record


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("runs", nargs="+", type=Path, help="Run directory or parent containing run directories")
    ap.add_argument("--published", type=Path, default=PUBLISHED)
    ap.add_argument("--csv", type=Path, help="New output file, never overwritten")
    ap.add_argument("--markdown", type=Path, help="New output file, never overwritten")
    ap.add_argument("--strict", action="store_true", help="Fail if any new run lacks acceptance evidence")
    args = ap.parse_args()
    for output in (args.csv, args.markdown):
        if output and output.exists():
            ap.error("refusing to overwrite " + str(output))
    with args.published.open(newline="") as f:
        published = list(csv.DictReader(f))
    old = {r["run_id"]: r for r in published}
    paths = []
    for p in args.runs:
        if not p.is_dir():
            ap.error("not a directory: " + str(p))
        paths.extend([p] if (p / "run_summary.env").exists() or (p / "run_summary.csv").exists()
                     else sorted(c for c in p.iterdir() if c.is_dir() and
                                 ((c / "run_summary.env").exists() or (c / "run_summary.csv").exists())))
    paths = list(dict.fromkeys(p.resolve() for p in paths))
    if not paths:
        ap.error("no run summaries found")
    rows = [extract(p) for p in paths]
    baselines = {}
    for r in rows:
        if (r["scenario"] == "A" and r["group"] != "UNKNOWN" and
                r["evidence_status"] == "PASS" and number(r["tps_majority_visible"])):
            baselines.setdefault(r["group"], []).append(number(r["tps_majority_visible"]))
    for r in rows:
        base = baselines.get(r["group"], [])
        r["overhead_vs_baseline_pct"] = delta(r["tps_majority_visible"], statistics.median(base) if base else None)
        old_id = MATCHES.get(r["scenario"])
        if old_id in old:
            r["published_run_id"] = old_id
            r["tps_change_vs_published_pct"] = delta(r["tps_majority_visible"], old[old_id]["tps_majority_visible"])
    output_rows = []
    for r in published:
        output_rows.append({**dict.fromkeys(FIELDS, "UNKNOWN"), **{k: v for k, v in r.items() if k in FIELDS},
                            "dataset": "published", "group": "historical", "evidence_status": "PUBLISHED"})
    output_rows.extend(rows)
    if args.csv:
        with args.csv.open("x", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            w.writeheader()
            w.writerows(output_rows)
    visible = ["dataset", "run_id", "scenario", "group", "tps_majority_visible", "overhead_vs_baseline_pct",
               "cut_ms", "repair_ms", "catchup_ms", "total_recovery_ms", "mismatched_partitions",
               "differing_leaves", "candidate_rows", "rows_upserted", "rows_deleted", "empty_100ms_buckets",
               "max_completion_gap_ms", "phase8_merkle_pass", "evidence_status"]
    lines = ["Signed overhead = 100 * (TPS / median valid baseline TPS in the same group - 1). Missing evidence is UNKNOWN.",
             "", "| " + " | ".join(visible) + " |", "| " + " | ".join("---" for _ in visible) + " |"]
    lines.extend("| " + " | ".join(str(r[k]).replace("|", "\\|") for k in visible) + " |" for r in output_rows)
    rendered = "\n".join(lines) + "\n"
    if args.markdown:
        with args.markdown.open("x") as f:
            f.write(rendered)
    print(rendered, end="")
    return 1 if args.strict and any(r["evidence_status"] != "PASS" for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
