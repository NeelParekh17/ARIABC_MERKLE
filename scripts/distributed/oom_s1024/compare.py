#!/usr/bin/env python3
"""Compare accepted S1024 campaigns with the published S32/det results.

Python stdlib only. Ratios use historical det; optional fresh det is reported
separately as a drift control. KB is decimal (1000 bytes), MiB is 2**20 bytes.
"""
import argparse
import csv
import io
import json
import math
import statistics
from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
METRICS = ("tps", "device_read_mib", "device_write_mib", "read_kb_per_statement",
           "write_kb_per_statement", "checkpoint_write_mib", "blks_read", "blks_hit")


def number(value):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"Invalid nonnegative metric: {value!r}")
    return result


def key(row):
    return row["workload"], number(row["skew"]), int(row["workers"])


def validate(row):
    queries = int(row["total_queries"])
    if (queries != 20000 or int(row["db_rows"]) != 100000000 or
            row["shared_buffers"] != "32MB" or row["reset_mode"] != "delta" or
            int(row["validated_completed_queries"]) != queries or
            int(row["divergence_count"]) != 0 or int(row["permanent_failures"]) != 0 or
            number(row["tps"]) <= 0):
        raise ValueError(f"Unaccepted or unmatched campaign contract: {key(row)}")
    if row["mode"] == "bcdb_merkle" and row["merkle_verify"] != "PASS":
        raise ValueError(f"Missing Merkle PASS: {key(row)}")
    for field in ("device_read_mib", "device_write_mib", "checkpoint_write_mib",
                  "blks_read", "blks_hit"):
        number(row[field])
    row["read_kb_per_statement"] = number(row["device_read_mib"]) * 2**20 / 1000 / queries
    row["write_kb_per_statement"] = number(row["device_write_mib"]) * 2**20 / 1000 / queries
    return row


def published(path):
    with path.open(newline="") as handle:
        return [validate(dict(r)) for r in csv.DictReader(handle)
                if r["mode"] in ("bcdb_det", "bcdb_merkle")]


def campaign_roots(paths):
    roots = set()
    for path in paths:
        path = Path(path).resolve()
        candidates = [path] if (path / "campaign.json").is_file() else [
            p.parent for p in path.rglob("campaign.json")]
        if not candidates:
            raise ValueError(f"No campaign.json under {path}")
        roots.update(candidates)
    return sorted(roots)


def campaigns(paths, mode):
    rows = []
    seen = set()
    for root in campaign_roots(paths):
        campaign = json.loads((root / "campaign.json").read_text())
        arguments = campaign["arguments"]
        if mode not in arguments["modes"]:
            raise ValueError(f"Campaign {root} does not include {mode}")
        if mode == "bcdb_det" and arguments["install_dir"] != "/home/neel/Desktop/ariabc_install":
            raise ValueError(f"Drift control must use canonical install: {root}")
        with (root / "summary.csv").open(newline="") as handle:
            summary = list(csv.DictReader(handle))
        for record in summary:
            if record["mode"] != mode:
                continue
            # Resolve relocated archives by their case basename first.
            case = root / Path(record["artifact_dir"]).name
            if not case.is_dir():
                case = Path(record["artifact_dir"])
            if case.resolve() in seen:
                continue
            seen.add(case.resolve())
            if any((case / name).exists() for name in ("FAILED.txt", "cleanup_errors.txt")):
                raise ValueError(f"Failure evidence in accepted case: {case}")
            result = json.loads((case / "result.json").read_text())
            row = validate(dict(result["row"]))
            if row["mode"] != mode or key(row) != key(record) or int(row["trial"]) != int(record["trial"]):
                raise ValueError(f"Summary/result identity mismatch: {case}")
            for field in METRICS:
                if field in record and not math.isclose(number(record[field]), number(row[field]), rel_tol=1e-9):
                    raise ValueError(f"Summary/result {field} mismatch: {case}")
            if result["gateway"]["gateway_returncode"] != 0:
                raise ValueError(f"Gateway failed: {case}")
            snapshots = json.loads((case / "io.json").read_text())
            for field, before, after, counter, scale in (
                ("device_read_mib", "device_before", "device_after", "read_sectors", 2048),
                ("device_write_mib", "device_before", "device_after", "write_sectors", 2048),
                ("checkpoint_write_mib", "checkpoint_before", "checkpoint_after", "write_sectors", 2048),
                ("blks_read", "pg_before", "pg_after", "blks_read", 1),
                ("blks_hit", "pg_before", "pg_after", "blks_hit", 1)):
                measured = (snapshots[after][counter] - snapshots[before][counter]) / scale
                if not math.isclose(measured, number(row[field]), rel_tol=1e-9, abs_tol=1e-9):
                    raise ValueError(f"I/O evidence mismatch ({field}): {case}")
            if mode == "bcdb_merkle":
                setup = json.loads((case / "setup.json").read_text())
                geometry = setup["merkle_stats"]
                if any(geometry[k] != v for k, v in dict(
                        fanout=32, partitions=200, split_threshold=1024, merge_threshold=256).items()):
                    raise ValueError(f"Not the requested S1024 geometry: {case}")
                if geometry["total_nodes"] <= 0 or (case / "merkle_verify.txt").read_text().strip() != "t":
                    raise ValueError(f"Missing node/verification evidence: {case}")
                if setup["provenance"]["install_dir"] != "/home/neel/claude_opt/install_opt":
                    raise ValueError(f"Not the optimized install: {case}")
            row["artifact_dir"] = str(case.resolve())
            rows.append(row)
    if not rows:
        raise ValueError(f"No accepted {mode} cases")
    return rows


def grouped(rows):
    groups = {}
    for row in rows:
        groups.setdefault(key(row), []).append(row)
    return groups


def aggregate(rows):
    if not rows:
        return {}
    return dict({m: statistics.median(number(r[m]) for r in rows) for m in METRICS},
                trials=len(rows), merkle_verify=",".join(sorted({r["merkle_verify"] for r in rows})),
                artifacts=";".join(r["artifact_dir"] for r in rows))


def table(fields, rows):
    def fmt(value):
        return f"{value:.3f}" if isinstance(value, float) else str(value)
    return "\n".join(["| " + " | ".join(fields) + " |",
                       "| " + " | ".join("---" for _ in fields) + " |"] + [
        "| " + " | ".join(fmt(r.get(f, "")) for f in fields) + " |" for r in rows])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new", nargs="+", required=True, help="Campaign directories or parents")
    parser.add_argument("--control", nargs="*", default=[], help="Optional canonical det drift campaigns")
    parser.add_argument("--old-summary", type=Path, default=REPO / "Final_Results/OOM_100M/summary.csv")
    parser.add_argument("--out-dir", type=Path, required=True, help="Fresh comparison output directory")
    args = parser.parse_args()
    old = published(args.old_summary)
    groups = dict(det=grouped([r for r in old if r["mode"] == "bcdb_det"]),
                  old=grouped([r for r in old if r["mode"] == "bcdb_merkle"]),
                  new=grouped(campaigns(args.new, "bcdb_merkle")),
                  control=grouped(campaigns(args.control, "bcdb_det")) if args.control else {})
    output, io_rows = [], []
    for identity in sorted(set(groups["new"]) | set(groups["control"])):
        if identity not in groups["det"] or identity not in groups["old"]:
            raise ValueError(f"No historical match: {identity}")
        all_rows = [r for group in groups.values() for r in group.get(identity, [])]
        if len({r["workload_sha256"] for r in all_rows}) != 1:
            raise ValueError(f"Workload bytes changed; comparison invalid: {identity}")
        row = dict(zip(("workload", "skew", "workers"), identity))
        for label, group in groups.items():
            values = aggregate(group.get(identity, []))
            row.update({f"{label}_{field}": value for field, value in values.items()})
            if values:
                io_rows.append(dict(workload=identity[0], skew=identity[1], workers=identity[2],
                                    series=label, **{m: values[m] for m in METRICS if m != "tps"},
                                    merkle_verify=values["merkle_verify"]))
        for name, numerator, denominator in (("new_old", "new", "old"), ("new_det", "new", "det"),
                                              ("old_det", "old", "det"), ("control_det", "control", "det")):
            if f"{numerator}_tps" in row:
                row[name] = row[f"{numerator}_tps"] / row[f"{denominator}_tps"]
        output.append(row)
    fields = ["workload", "skew", "workers"] + [f"{label}_{m}" for label in groups
              for m in (*METRICS, "trials", "merkle_verify", "artifacts")] + [
                  "new_old", "new_det", "old_det", "control_det"]
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    writer.writerows(output)
    markdown = ("# S32 versus S1024 + optimized code\n\n"
                "det/old are published 2026-09-29 results; new is S1024 + install_opt; "
                "control is fresh canonical det. Ratios use historical det. Missing points are blank. "
                "Repeated accepted trials use per-metric medians; ratios are ratios of medians. "
                "Single trials do not establish stable rankings.\n\n" + table(
                    ["workload", "skew", "workers", "det_tps", "old_tps", "new_tps", "new_old",
                     "new_det", "old_det", "control_tps", "control_det"], output) +
                "\n\nTimed device I/O excludes the separate checkpoint and verification. "
                "KB/statement uses 1000 bytes; MiB uses 2**20 bytes. Device counters include "
                "unrelated traffic. PostgreSQL blks_read is a buffer miss, not necessarily a device read.\n\n" +
                table(["workload", "skew", "workers", "series", *METRICS[1:], "merkle_verify"], io_rows) + "\n")
    args.out_dir.mkdir(parents=True, exist_ok=False)
    (args.out_dir / "comparison.csv").write_text(stream.getvalue())
    (args.out_dir / "comparison.md").write_text(markdown)
    print(stream.getvalue(), end="")
    print(markdown)


if __name__ == "__main__":
    main()
