#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional


DEFAULT_THREADS = [1] + list(range(4, 65, 4))
DEFAULT_WORKLOADS = [
    "ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt",
    "ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt",
]

MODE_NAME_TO_DB_TYPE = {
    "pg": 0,
    "postgres": 0,
    "det": 1,
    "safedb": 1,
    "nondet": 2,
    "aria": 2,
}


@dataclass(frozen=True)
class RunKey:
    mode: str
    workload: str
    threads: int
    rate: int
    run: int


@dataclass
class RunResult:
    ts_utc: str
    mode: str
    db_type: int
    workload: str
    threads: int
    run: int
    rate: int
    start_server_exit: int
    restore_exit: int
    py_exit: int
    wall_time_s: float
    workload_overall_ms: Optional[float]
    workload_wait_ms: Optional[float]
    duplicate_key_errors: Optional[int]
    retries_total: Optional[int]
    serialization_retries: Optional[int]
    reconnects: Optional[int]
    permanent_failures: Optional[int]
    db_row_count: Optional[int]
    db_root_hash: Optional[str]
    db_merkle_verify: Optional[str]
    workload_log: str
    start_server_log: str
    restore_log: str
    notes: str


OVERALL_MS_RE = re.compile(r"overall time taken \(millisec\)\s*=\s*([0-9.]+)")
WAIT_MS_RE = re.compile(r"total wait time \(ms\)\s*([0-9.]+)")
DUP_KEYS_RE = re.compile(r"duplicate_key_errors\s*=\s*([0-9]+)")
RETRY_SUMMARY_RE = re.compile(
    r"retry_summary:\s*retries_total=(\d+)\s+serialization_retries=(\d+)\s+reconnects=(\d+)\s+permanent_failures=(\d+)",
    re.IGNORECASE,
)


def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _split_csv_ints(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def _run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    stdin_path: Optional[Path] = None,
    timeout_s: Optional[int] = None,
) -> int:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
        stdin = None
        if stdin_path is not None:
            stdin = stdin_path.open("rb")
        try:
            proc = subprocess.run(
                argv,
                cwd=str(cwd),
                env=env,
                stdin=stdin,
                stdout=out,
                stderr=err,
                timeout=timeout_s,
                check=False,
            )
            return int(proc.returncode)
        except subprocess.TimeoutExpired:
            # Keep matrix execution alive when a single workload case hangs.
            # 124 mirrors GNU timeout's timeout exit code.
            return 124
        finally:
            if stdin is not None:
                stdin.close()


def _psql_value(psql: str, *, db: str, port: int, user: str, query: str, cwd: Path, env: dict[str, str]) -> Optional[str]:
    argv = [
        psql,
        "-X",
        "-q",
        "-p",
        str(port),
        "-U",
        user,
        "-d",
        db,
        "-At",
        "-c",
        query,
    ]
    try:
        proc = subprocess.run(argv, cwd=str(cwd), env=env, capture_output=True, text=True, check=False)
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""


def _capture_timeout_diagnostics(
    *,
    case_dir: Path,
    psql: str,
    db: str,
    port: int,
    user: str,
    cwd: Path,
    env: dict[str, str],
    server_log: Path,
) -> Optional[Path]:
    diag_path = case_dir / "timeout_diagnostics.txt"
    lines: list[str] = []
    lines.append(f"captured_utc={_now_utc_iso()}")
    lines.append(f"db={db} user={user} port={port}")
    lines.append("")

    queries: list[tuple[str, str]] = [
        (
            "pg_stat_activity (workload backends)",
            "select pid,state,wait_event_type,wait_event,"
            "extract(epoch from now()-query_start)::int as sec,left(query,140) "
            "from pg_stat_activity "
            "where datname = current_database() and usename = current_user "
            "and query not ilike '%pg_stat_activity%' "
            "order by query_start;",
        ),
        (
            "pg_locks (workload backends)",
            "select l.pid,l.locktype,l.mode,l.granted,l.relation::regclass,l.page,l.tuple,l.transactionid "
            "from pg_locks l "
            "join pg_stat_activity a on a.pid=l.pid "
            "where a.datname = current_database() and a.usename = current_user "
            "order by l.pid,l.granted,l.locktype;",
        ),
        (
            "blocking summary",
            "select pid, cardinality(pg_blocking_pids(pid)) as blockers, wait_event_type, wait_event, left(query,140) "
            "from pg_stat_activity "
            "where datname = current_database() and usename = current_user "
            "and query not ilike '%pg_stat_activity%' "
            "order by pid;",
        ),
    ]

    for title, query in queries:
        lines.append(f"=== {title} ===")
        argv = [
            psql,
            "-X",
            "-q",
            "-p",
            str(port),
            "-U",
            user,
            "-d",
            db,
            "-At",
            "-c",
            query,
        ]
        try:
            proc = subprocess.run(argv, cwd=str(cwd), env=env, capture_output=True, text=True, check=False)
            lines.append(f"returncode={proc.returncode}")
            if proc.stdout.strip():
                lines.extend(proc.stdout.rstrip("\n").splitlines())
            if proc.stderr.strip():
                lines.append("-- stderr --")
                lines.extend(proc.stderr.rstrip("\n").splitlines())
        except Exception as e:
            lines.append(f"query_failed={e}")
        lines.append("")

    lines.append("=== server.log tail (last 120 lines) ===")
    if server_log.exists():
        try:
            tail_lines = server_log.read_text(errors="replace").splitlines()[-120:]
            lines.extend(tail_lines)
        except Exception as e:
            lines.append(f"read_server_log_failed={e}")
    else:
        lines.append(f"missing_server_log={server_log}")

    try:
        diag_path.write_text("\n".join(lines) + "\n")
        return diag_path
    except Exception:
        return None


def _parse_workload_metrics(log_text: str) -> tuple[Optional[float], Optional[float], Optional[int]]:
    overall_ms = None
    wait_ms = None
    dup_keys = None

    m = OVERALL_MS_RE.search(log_text)
    if m:
        overall_ms = float(m.group(1))
    m = WAIT_MS_RE.search(log_text)
    if m:
        wait_ms = float(m.group(1))
    m = DUP_KEYS_RE.search(log_text)
    if m:
        dup_keys = int(m.group(1))

    return overall_ms, wait_ms, dup_keys


def _parse_retry_summary(log_text: str) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    m = RETRY_SUMMARY_RE.search(log_text)
    if not m:
        return None, None, None, None
    try:
        return int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    except Exception:
        return None, None, None, None


def _load_existing_keys(csv_path: Path) -> set[RunKey]:
    if not csv_path.exists():
        return set()
    keys: set[RunKey] = set()
    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rate_s = (row.get("rate") or "").strip()
                keys.add(
                    RunKey(
                        mode=row["mode"],
                        workload=row["workload"],
                        threads=int(row["threads"]),
                        rate=int(rate_s) if rate_s != "" else 0,
                        run=int(row["run"]),
                    )
                )
            except Exception:
                continue
    return keys


def _median(nums: list[float]) -> Optional[float]:
    if not nums:
        return None
    xs = sorted(nums)
    n = len(xs)
    mid = n // 2
    if n % 2 == 1:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _percentile(nums: list[float], p: float) -> Optional[float]:
    if not nums:
        return None
    if p <= 0:
        return min(nums)
    if p >= 100:
        return max(nums)
    xs = sorted(nums)
    # Linear interpolation between closest ranks.
    idx = (len(xs) - 1) * (p / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return xs[lo]
    frac = idx - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def _stddev_population(nums: list[float]) -> Optional[float]:
    if not nums:
        return None
    if len(nums) == 1:
        return 0.0
    mu = sum(nums) / len(nums)
    var = sum((x - mu) ** 2 for x in nums) / len(nums)
    return math.sqrt(var)


def _iqr_outlier_count(nums: list[float]) -> int:
    if len(nums) < 4:
        return 0
    q1 = _percentile(nums, 25.0)
    q3 = _percentile(nums, 75.0)
    if q1 is None or q3 is None:
        return 0
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    return sum(1 for x in nums if x < lo or x > hi)


def _write_summary(raw_csv: Path, summary_csv: Path) -> None:
    rows: list[dict[str, Any]] = []
    with raw_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    def as_float(v: str) -> Optional[float]:
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None

    def as_int(v: str) -> Optional[int]:
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return int(v)
        except Exception:
            return None

    groups: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
    for r in rows:
        key = (
            r.get("mode", ""),
            r.get("workload", ""),
            int(r.get("threads") or 0),
            int(r.get("rate") or 0),
        )
        groups.setdefault(key, []).append(r)

    def mean(nums: list[float]) -> Optional[float]:
        if not nums:
            return None
        return sum(nums) / len(nums)

    def cv_pct(nums: list[float]) -> Optional[float]:
        if not nums:
            return None
        mu = mean(nums)
        if mu is None or mu == 0:
            return None
        sd = _stddev_population(nums)
        if sd is None:
            return None
        return 100.0 * sd / mu

    def pass_rate(ys: list[bool]) -> float:
        if not ys:
            return 0.0
        return sum(1 for y in ys if y) / len(ys)

    out_rows: list[dict[str, Any]] = []
    for (mode, workload, threads, rate), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], x[0][3])):
        wall = [as_float(r.get("wall_time_s", "")) for r in rs]
        wall = [x for x in wall if x is not None]
        overall = [as_float(r.get("workload_overall_ms", "")) for r in rs]
        overall = [x for x in overall if x is not None]
        wait = [as_float(r.get("workload_wait_ms", "")) for r in rs]
        wait = [x for x in wait if x is not None]
        dups = [as_int(r.get("duplicate_key_errors", "")) for r in rs]
        dups = [x for x in dups if x is not None]
        retries_total = [as_int(r.get("retries_total", "")) for r in rs]
        retries_total = [x for x in retries_total if x is not None]
        serialization_retries = [as_int(r.get("serialization_retries", "")) for r in rs]
        serialization_retries = [x for x in serialization_retries if x is not None]
        reconnects = [as_int(r.get("reconnects", "")) for r in rs]
        reconnects = [x for x in reconnects if x is not None]
        permanent_failures = [as_int(r.get("permanent_failures", "")) for r in rs]
        permanent_failures = [x for x in permanent_failures if x is not None]

        throughput_tps = [1000.0 * 20000.0 / x for x in overall if x and x > 0]

        verify = [(r.get("db_merkle_verify", "") or "").strip().lower() == "t" for r in rs]

        def max_or_empty(nums: list[int]) -> str:
            return str(max(nums)) if nums else ""

        out_rows.append(
            {
                "mode": mode,
                "workload": workload,
                "threads": threads,
                "rate": rate,
                "runs": len(rs),
                "pass_rate_merkle_verify": f"{pass_rate(verify):.3f}",
                "mean_wall_time_s": f"{mean(wall):.6f}" if wall else "",
                "mean_workload_overall_ms": f"{mean(overall):.3f}" if overall else "",
                "median_workload_overall_ms": f"{_median(overall):.3f}" if overall else "",
                "p95_workload_overall_ms": f"{_percentile(overall, 95.0):.3f}" if overall else "",
                "stdev_workload_overall_ms": f"{_stddev_population(overall):.3f}" if overall else "",
                "cv_workload_overall_ms_pct": f"{cv_pct(overall):.3f}" if overall else "",
                "iqr_outliers_workload_overall_ms": str(_iqr_outlier_count(overall)) if overall else "0",
                "mean_workload_wait_ms": f"{mean(wait):.3f}" if wait else "",
                "median_workload_wait_ms": f"{_median(wait):.3f}" if wait else "",
                "mean_duplicate_key_errors": f"{mean([float(x) for x in dups]):.3f}" if dups else "",
                "mean_retries_total": f"{mean([float(x) for x in retries_total]):.3f}" if retries_total else "",
                "mean_serialization_retries": f"{mean([float(x) for x in serialization_retries]):.3f}" if serialization_retries else "",
                "mean_reconnects": f"{mean([float(x) for x in reconnects]):.3f}" if reconnects else "",
                "mean_permanent_failures": f"{mean([float(x) for x in permanent_failures]):.3f}" if permanent_failures else "",
                "mean_throughput_tps": f"{mean(throughput_tps):.3f}" if throughput_tps else "",
                "median_throughput_tps": f"{_median(throughput_tps):.3f}" if throughput_tps else "",
                "p95_throughput_tps": f"{_percentile(throughput_tps, 95.0):.3f}" if throughput_tps else "",
                "stdev_throughput_tps": f"{_stddev_population(throughput_tps):.3f}" if throughput_tps else "",
                "cv_throughput_tps_pct": f"{cv_pct(throughput_tps):.3f}" if throughput_tps else "",
                "max_retries_total": max_or_empty(retries_total),
                "max_serialization_retries": max_or_empty(serialization_retries),
                "max_reconnects": max_or_empty(reconnects),
                "max_permanent_failures": max_or_empty(permanent_failures),
            }
        )

    with summary_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()) if out_rows else [])
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)


def _print_scaling_check(summary_csv: Path) -> None:
    if not summary_csv.exists():
        return

    rows: list[dict[str, str]] = []
    with summary_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    def as_float(v: str) -> Optional[float]:
        v = (v or "").strip()
        if v == "":
            return None
        try:
            return float(v)
        except Exception:
            return None

    by_workload: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        by_workload.setdefault(r.get("workload", ""), []).append(r)

    print("")
    print("Scaling check (throughput should be non-decreasing with threads):")
    for workload in sorted(by_workload.keys()):
        rs = sorted(by_workload[workload], key=lambda x: int(x.get("threads") or 0))
        mean_tps = [as_float(r.get("mean_throughput_tps", "")) for r in rs]
        med_tps = [as_float(r.get("median_throughput_tps", "")) for r in rs]

        def is_nondecreasing(vals: list[Optional[float]]) -> bool:
            xs = [x for x in vals if x is not None]
            return all(xs[i] >= xs[i - 1] for i in range(1, len(xs)))

        mean_ok = is_nondecreasing(mean_tps)
        med_ok = is_nondecreasing(med_tps)

        pairs = []
        for r in rs:
            th = int(r.get("threads") or 0)
            tps = as_float(r.get("median_throughput_tps", ""))
            if tps is not None:
                pairs.append(f"{th}:{tps:.1f}")

        print(
            f"  workload={workload} median_non_decreasing={int(med_ok)} mean_non_decreasing={int(mean_ok)} median_tps_by_threads=[{' '.join(pairs)}]"
        )


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    if cleaned:
        return cleaned
    digest = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"workload-{digest}"


def _generate_tps_graphs(summary_csv: Path, out_dir: Path) -> list[Path]:
    """
    Generate one TPS-vs-threads graph per workload (and per rate when multiple
    rates are present) comparing pg/det/nondet series from summary.csv.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        raise RuntimeError("matplotlib is required for graph generation")

    rows: list[dict[str, str]] = []
    with summary_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    def as_int(v: str) -> Optional[int]:
        s = (v or "").strip()
        if s == "":
            return None
        try:
            return int(s)
        except Exception:
            return None

    def as_float(v: str) -> Optional[float]:
        s = (v or "").strip()
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None

    mode_norm = {
        "postgres": "pg",
        "pg": "pg",
        "safedb": "det",
        "det": "det",
        "aria": "nondet",
        "nondet": "nondet",
    }
    mode_order = ["pg", "det", "nondet"]
    mode_colors = {"pg": "#1f77b4", "det": "#2ca02c", "nondet": "#d62728"}

    # Group by workload and rate so each line compares modes under same workload/rate.
    # Older summary files may not have a `rate` column; treat those as rate=0.
    groups: dict[tuple[str, int], list[dict[str, str]]] = {}
    for r in rows:
        wl = (r.get("workload") or "").strip()
        rate = as_int(r.get("rate", ""))
        if rate is None:
            rate = 0
        if wl == "":
            continue
        groups.setdefault((wl, rate), []).append(r)

    if not groups:
        return []

    rates_present = sorted({rate for (_, rate) in groups.keys()})
    multiple_rates = len(rates_present) > 1

    out_paths: list[Path] = []
    for (workload, rate), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        series: dict[str, list[tuple[int, float]]] = {m: [] for m in mode_order}

        for r in rs:
            raw_mode = (r.get("mode") or "").strip().lower()
            mode = mode_norm.get(raw_mode, raw_mode)
            if mode not in series:
                continue

            th = as_int(r.get("threads", ""))
            # Preferred: median throughput (robust summary).
            tps = as_float(r.get("median_throughput_tps", ""))
            # Backward-compatible fallback: mean throughput from older summaries.
            if tps is None:
                tps = as_float(r.get("mean_throughput_tps", ""))
            # Oldest summaries may only have mean workload time.
            if tps is None:
                overall_ms = as_float(r.get("mean_workload_overall_ms", ""))
                if overall_ms is not None and overall_ms > 0:
                    tps = 1000.0 * 20000.0 / overall_ms
            if th is None or tps is None:
                continue
            series[mode].append((th, tps))

        # Skip graph if no data points at all for this workload/rate.
        if not any(series[m] for m in mode_order):
            continue

        fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=130)
        for mode in mode_order:
            points = sorted(series[mode], key=lambda x: x[0])
            if not points:
                continue
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=4.5, color=mode_colors[mode], label=mode)

        ax.set_xlabel("Threads")
        ax.set_ylabel("TPS (median throughput)")
        title = f"TPS vs Threads - {workload}"
        if multiple_rates:
            title += f" - rate={rate}"
        ax.set_title(title)
        ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.4)
        ax.legend()
        fig.tight_layout()

        stem = _safe_filename(workload)
        if multiple_rates:
            name = f"tps_vs_threads_{stem}_rate-{rate}.png"
        else:
            name = f"tps_vs_threads_{stem}.png"
        out_path = out_dir / name
        fig.savefig(out_path)
        plt.close(fig)
        out_paths.append(out_path)

    return out_paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark AriaBC across modes and thread counts.")
    parser.add_argument("--threads", default=",".join(str(x) for x in DEFAULT_THREADS), help="Comma-separated thread counts.")
    parser.add_argument("--runs", type=int, default=3, help="Runs per (mode, workload, threads).")
    parser.add_argument(
        "--workloads",
        default=",".join(DEFAULT_WORKLOADS),
        help="Comma-separated workload filenames under scripts/ to run.",
    )
    parser.add_argument(
        "--modes",
        default="pg,det,nondet",
        help="Comma-separated modes: pg/postgres, det/safedb, nondet/aria.",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=0,
        help="Qrate per worker thread passed to workload script; 0 means unthrottled.",
    )
    parser.add_argument(
        "--rates",
        default="",
        help="Comma-separated Qrates (per worker thread) to run; overrides --rate. Include 0 for unthrottled.",
    )
    parser.add_argument("--db", default="postgres", help="Database name.")
    parser.add_argument("--user", default="postgres", help="DB user.")
    parser.add_argument("--port", type=int, default=5438, help="DB port.")
    parser.add_argument("--out-dir", default="", help="Output directory. Default: scripts/bench_results/<timestamp>.")
    parser.add_argument("--no-resume", action="store_true", help="Do not skip runs already present in results.csv.")
    parser.add_argument(
        "--no-server-restart",
        action="store_true",
        help="Do not restart server before each run (ignored for det/safedb).",
    )
    parser.add_argument(
        "--timeout-workload-s",
        type=int,
        default=0,
        help="Per-case workload process timeout in seconds; 0 disables timeout.",
    )
    parser.add_argument(
        "--timeout-workload-det-s",
        type=int,
        default=1800,
        help="Per-case workload timeout for det/safedb mode; 0 falls back to --timeout-workload-s.",
    )

    args = parser.parse_args()

    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent

    start_server = scripts_dir / "start_server.sh"
    restore_sql = scripts_dir / "restore_usertable_small.sql"
    workload_py = scripts_dir / "generic-saicopg-traffic-load+logSkip-safedb+pg.py"

    if not start_server.exists():
        print(f"ERROR: missing {start_server}", file=sys.stderr)
        return 2
    if not restore_sql.exists():
        print(f"ERROR: missing {restore_sql}", file=sys.stderr)
        return 2
    if not workload_py.exists():
        print(f"ERROR: missing {workload_py}", file=sys.stderr)
        return 2

    threads = _split_csv_ints(args.threads)
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    if not workloads:
        print("ERROR: no workloads selected", file=sys.stderr)
        return 2
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for m in modes:
        if m not in MODE_NAME_TO_DB_TYPE:
            print(f"ERROR: unknown mode {m!r} (supported: {', '.join(MODE_NAME_TO_DB_TYPE)})", file=sys.stderr)
            return 2

    rates = _split_csv_ints(args.rates) if args.rates.strip() else [int(args.rate)]
    for r in rates:
        if r < 0:
            print(f"ERROR: invalid rate {r} (must be >= 0)", file=sys.stderr)
            return 2

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) if args.out_dir else (scripts_dir / "bench_results" / timestamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_csv = out_dir / "results.csv"
    summary_csv = out_dir / "summary.csv"
    meta_json = out_dir / "run_meta.json"

    psql_env = os.environ.get("ARIABC_PSQL", "").strip()
    if psql_env:
        psql_path = psql_env
    else:
        psql_src = repo_root / "src" / "bin" / "psql" / "psql"
        psql_path = str(psql_src) if psql_src.exists() else "/work/ARIABC/install/bin/psql"
    python_path = os.environ.get("ARIABC_PYTHON", sys.executable or "python3")
    server_log_path = repo_root / "server.log"

    env = dict(os.environ)
    env.setdefault("PGHOST", "localhost")
    env.setdefault("PGPORT", str(args.port))
    env.setdefault("PGUSER", args.user)
    env.setdefault("PGDATABASE", args.db)
    # generic-saicopg-traffic-load+logSkip-safedb+pg.py reads DB_* env vars,
    # not PG* vars; set both so remote/non-default socket layouts work.
    env.setdefault("DB_HOST", "localhost")
    env.setdefault("DB_PORT", str(args.port))
    env.setdefault("DB_USER", args.user)
    env.setdefault("DB_NAME", args.db)
    # Avoid client-side statement timeouts interrupting long deterministic runs.
    env.setdefault("STATEMENT_TIMEOUT", "0")

    meta = {
        "created_utc": _now_utc_iso(),
        "repo_root": str(repo_root),
        "scripts_dir": str(scripts_dir),
        "threads": threads,
        "workloads": workloads,
        "runs": args.runs,
        "modes": modes,
        "rate": args.rate,
        "rates": rates,
        "db": {"name": args.db, "user": args.user, "port": args.port},
        "timeouts": {
            "workload_s": args.timeout_workload_s,
            "workload_det_s": args.timeout_workload_det_s,
        },
        "commands": {
            "start_server": str(start_server),
            "restore_sql": str(restore_sql),
            "workload_py": str(workload_py),
            "psql": psql_path,
            "python": python_path,
        },
    }
    meta_json.write_text(json.dumps(meta, indent=2) + "\n")

    existing = _load_existing_keys(raw_csv) if not args.no_resume else set()
    if args.no_server_restart and any(MODE_NAME_TO_DB_TYPE[m] == 1 for m in modes):
        print(
            "NOTE: Forcing server restart for det/safedb runs because sequence numbers reset to 0 each run.",
            file=sys.stderr,
        )

    fieldnames = list(asdict(RunResult(
        ts_utc="",
        mode="",
        db_type=0,
        workload="",
        threads=0,
        run=0,
        rate=0,
        start_server_exit=0,
        restore_exit=0,
        py_exit=0,
        wall_time_s=0.0,
        workload_overall_ms=None,
        workload_wait_ms=None,
        duplicate_key_errors=None,
        retries_total=None,
        serialization_retries=None,
        reconnects=None,
        permanent_failures=None,
        db_row_count=None,
        db_root_hash=None,
        db_merkle_verify=None,
        workload_log="",
        start_server_log="",
        restore_log="",
        notes="",
    )).keys())

    write_header = not raw_csv.exists()
    with raw_csv.open("a", newline="") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        total = len(modes) * len(workloads) * len(threads) * len(rates) * args.runs
        done = 0

        for mode in modes:
            db_type = MODE_NAME_TO_DB_TYPE[mode]
            for workload in workloads:
                workload_path = scripts_dir / workload
                if not workload_path.exists():
                    print(f"WARNING: missing workload file {workload_path}, skipping", file=sys.stderr)
                    continue

                for th in threads:
                    for rate in rates:
                        for run_idx in range(1, args.runs + 1):
                            key = RunKey(mode=mode, workload=workload, threads=th, rate=rate, run=run_idx)
                            if key in existing:
                                done += 1
                                continue

                            case_dir = (
                                out_dir
                                / f"mode={mode}"
                                / f"workload={workload_path.stem}"
                                / f"rate={rate}"
                                / f"threads={th}"
                                / f"run={run_idx}"
                            )
                            case_dir.mkdir(parents=True, exist_ok=True)

                            start_log_out = case_dir / "start_server.out"
                            start_log_err = case_dir / "start_server.err"
                            restore_log_out = case_dir / "restore.out"
                            restore_log_err = case_dir / "restore.err"
                            workload_log_out = case_dir / "workload.out"
                            workload_log_err = case_dir / "workload.err"

                            t0 = time.monotonic()

                            restart_server = (not args.no_server_restart) or (db_type == 1)
                            start_exit = 0
                            if restart_server:
                                start_exit = _run(
                                    ["/usr/bin/bash", str(start_server)],
                                    cwd=scripts_dir,
                                    env=env,
                                    stdout_path=start_log_out,
                                    stderr_path=start_log_err,
                                )

                            restore_exit = _run(
                                [
                                    psql_path,
                                    "-X",
                                    "-q",
                                    "-p",
                                    str(args.port),
                                    "-U",
                                    args.user,
                                    "-d",
                                    args.db,
                                    "-f",
                                    str(restore_sql),
                                ],
                                cwd=scripts_dir,
                                env=env,
                                stdout_path=restore_log_out,
                                stderr_path=restore_log_err,
                            )

                            workload_exit = -1
                            if restore_exit == 0:
                                case_env = dict(env)
                                case_env.setdefault("PYTHONUNBUFFERED", "1")
                                if db_type == 1:
                                    # Deterministic runs against a freshly restarted server must start at txid 0.
                                    # Force a clean start even if the parent shell has DET_START_SEQ set.
                                    case_env["DET_START_SEQ"] = "0"
                                # Publish a pointer file so an external monitor can follow the
                                # active workload output (workload.out) for this run.
                                pointer_path = out_dir / "current_case.json"
                                try:
                                    pointer_payload = {
                                        "case_dir": str(case_dir),
                                        "mode": mode,
                                        "workload": workload,
                                        "threads": th,
                                        "rate": rate,
                                        "run": run_idx,
                                        "ts": _now_utc_iso(),
                                    }
                                    pointer_path.write_text(json.dumps(pointer_payload) + "\n")
                                except Exception as _e:
                                    print(f"WARNING: could not write current_case pointer: {_e}", file=sys.stderr)
                                workload_argv = [
                                    python_path,
                                    str(workload_py),
                                    args.db,
                                    str(workload_path),
                                    str(db_type),
                                    str(th),
                                ]
                                if rate > 0:
                                    workload_argv.append(str(rate))

                                timeout_workload_s = args.timeout_workload_s
                                if db_type == 1 and args.timeout_workload_det_s > 0:
                                    timeout_workload_s = args.timeout_workload_det_s

                                timeout_for_run: Optional[int] = None
                                if timeout_workload_s > 0:
                                    timeout_for_run = int(timeout_workload_s)

                                workload_exit = _run(
                                    workload_argv,
                                    cwd=scripts_dir,
                                    env=case_env,
                                    stdout_path=workload_log_out,
                                    stderr_path=workload_log_err,
                                    timeout_s=timeout_for_run,
                                )

                                # Remove the pointer after the workload process completes so
                                # the monitor knows we're idle.
                                try:
                                    if pointer_path.exists():
                                        pointer_path.unlink()
                                except Exception as _e:
                                    print(f"WARNING: could not remove current_case pointer: {_e}", file=sys.stderr)

                            t1 = time.monotonic()

                            log_text = ""
                            try:
                                log_text = workload_log_out.read_text(errors="replace") + "\n" + workload_log_err.read_text(errors="replace")
                            except Exception:
                                pass

                            overall_ms, wait_ms, dup_keys = _parse_workload_metrics(log_text)
                            retries_total, serialization_retries, reconnects, permanent_failures = _parse_retry_summary(log_text)

                            count_s = None
                            root_hash = None
                            verify = None
                            timeout_diag_path: Optional[Path] = None
                            if restore_exit == 0:
                                count_s = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select count(*) from usertable_small;", cwd=scripts_dir, env=env)
                                root_hash = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_root_hash('usertable_small');", cwd=scripts_dir, env=env)
                                verify = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_verify('usertable_small');", cwd=scripts_dir, env=env)
                                if workload_exit == 124:
                                    timeout_diag_path = _capture_timeout_diagnostics(
                                        case_dir=case_dir,
                                        psql=psql_path,
                                        db=args.db,
                                        port=args.port,
                                        user=args.user,
                                        cwd=scripts_dir,
                                        env=env,
                                        server_log=server_log_path,
                                    )

                            row_count = None
                            if count_s is not None and count_s.strip().isdigit():
                                row_count = int(count_s.strip())

                            notes_parts: list[str] = []
                            if restart_server and start_exit != 0:
                                notes_parts.append(f"start_server_exit={start_exit}")
                            if restore_exit != 0:
                                notes_parts.append(f"restore_exit={restore_exit}")
                                notes_parts.append("workload_skipped=1")
                            elif workload_exit != 0:
                                notes_parts.append(f"workload_exit={workload_exit}")
                                if workload_exit == 124:
                                    notes_parts.append("workload_timeout=1")
                                    if timeout_diag_path is not None:
                                        notes_parts.append(f"timeout_diag={timeout_diag_path.name}")
                            elif verify is not None and verify.strip().lower() not in ("t", "f", ""):
                                notes_parts.append(f"unexpected_verify={verify!r}")
                            notes = ";".join(notes_parts)

                            result = RunResult(
                                ts_utc=_now_utc_iso(),
                                mode=mode,
                                db_type=db_type,
                                workload=workload,
                                threads=th,
                                run=run_idx,
                                rate=rate,
                                start_server_exit=start_exit,
                                restore_exit=restore_exit,
                                py_exit=workload_exit,
                                wall_time_s=round(t1 - t0, 6),
                                workload_overall_ms=overall_ms,
                                workload_wait_ms=wait_ms,
                                duplicate_key_errors=dup_keys,
                                retries_total=retries_total,
                                serialization_retries=serialization_retries,
                                reconnects=reconnects,
                                permanent_failures=permanent_failures,
                                db_row_count=row_count,
                                db_root_hash=root_hash,
                                db_merkle_verify=verify,
                                workload_log=str(workload_log_out),
                                start_server_log=str(start_log_out),
                                restore_log=str(restore_log_out),
                                notes=notes,
                            )

                            writer.writerow(asdict(result))
                            fcsv.flush()

                            done += 1
                            print(f"[{done}/{total}] {mode} workload={workload} rate={rate} threads={th} run={run_idx} exit={workload_exit} verify={verify}")

    try:
        _write_summary(raw_csv, summary_csv)
    except Exception as e:
        print(f"WARNING: failed to write summary: {e}", file=sys.stderr)

    try:
        _print_scaling_check(summary_csv)
    except Exception as e:
        print(f"WARNING: failed to compute scaling check: {e}", file=sys.stderr)

    graph_paths: list[Path] = []
    try:
        graph_paths = _generate_tps_graphs(summary_csv, out_dir)
    except Exception as e:
        print(f"WARNING: failed to generate graphs: {e}", file=sys.stderr)

    print("")
    print(f"Wrote raw results: {raw_csv}")
    print(f"Wrote summary:     {summary_csv}")
    print(f"Wrote metadata:    {meta_json}")
    if graph_paths:
        print("Wrote graphs:")
        for p in graph_paths:
            print(f"  {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
