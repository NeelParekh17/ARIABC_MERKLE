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
    signing: int
    enforce_signatures: int
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
    signing: int
    enforce_signatures: int
    transaction_isolation: str
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
    dynamic_profile_splits: Optional[int]
    dynamic_profile_merges: Optional[int]
    dynamic_profile_max_leaf_items: Optional[int]
    workload_log: str
    start_server_log: str
    restore_log: str
    notes: str
    statement_count: Optional[int] = None


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


def _psql_exec(psql: str, *, db: str, port: int, user: str, query: str, cwd: Path, env: dict[str, str]) -> tuple[bool, str]:
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
        "-c",
        query,
    ]
    try:
        proc = subprocess.run(argv, cwd=str(cwd), env=env, capture_output=True, text=True, check=False)
    except Exception as e:
        return False, str(e)
    msg = "\n".join(part.strip() for part in (proc.stdout, proc.stderr) if part and part.strip())
    return proc.returncode == 0, msg


def _force_pgoption(env: dict[str, str], key: str, value: str) -> None:
    option = f"-c {key}={value}"
    existing = env.get("PGOPTIONS", "").strip()
    env["PGOPTIONS"] = f"{existing} {option}".strip() if existing else option


def _ensure_merkle_index_mode(
    psql: str,
    *,
    db: str,
    port: int,
    user: str,
    cwd: Path,
    env: dict[str, str],
    enabled: bool,
) -> tuple[bool, list[str]]:
    notes: list[str] = []
    effective_value = _psql_value(
        psql,
        db=db,
        port=port,
        user=user,
        query="SHOW enable_merkle_index;",
        cwd=cwd,
        env=env,
    )
    expected = "on" if enabled else "off"
    if (effective_value or "").strip().lower() != expected:
        notes.append(f"effective_enable_merkle_index={effective_value or 'unknown'}")
        return False, notes
    notes.append(f"benchmark_enable_merkle_index={expected}")
    return True, notes


def _prepare_plain_pg_baseline(
    psql: str,
    *,
    db: str,
    port: int,
    user: str,
    cwd: Path,
    env: dict[str, str],
) -> tuple[bool, str]:
    """Prove that the PG restore did not construct an AriaBC Merkle index."""
    return _psql_exec(
        psql,
        db=db,
        port=port,
        user=user,
        cwd=cwd,
        env=env,
        query=(
            "SET enable_merkle_index = off; "
            "DO $plain_pg$ BEGIN "
            "IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE n.nspname = 'public' "
            "AND c.relname = 'usertable_merkle_multikey_variable') THEN "
            "RAISE EXCEPTION 'plain PG restore unexpectedly created Merkle index'; "
            "END IF; END $plain_pg$; "
            "TRUNCATE ariabc_internal.merkle_local_delta; "
            "UPDATE ariabc_internal.merkle_apply_counter "
            "SET next_seq = 0, terminal_prefix_seq = 0 WHERE singleton; "
            "UPDATE ariabc_internal.merkle_apply_state "
            "SET applied_seq = 0, state = 0, error_text = NULL, "
            "updated_at = clock_timestamp() WHERE singleton;"
        ),
    )


def _canonical_path_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = value.strip()
    if s == "":
        return None
    return os.path.realpath(s)


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


def _is_skippable_workload_line(line: str) -> bool:
    s = line.strip()
    if s == "":
        return True
    if s.startswith("--") or s.startswith("/*") or s.startswith("\\"):
        return True
    return False


def _count_workload_statements(path: Path) -> Optional[int]:
    try:
        n = 0
        with path.open("r", errors="replace") as f:
            for line in f:
                if not _is_skippable_workload_line(line):
                    n += 1
        return n if n > 0 else None
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
                        signing=int((row.get("signing") or "0").strip() or 0),
                        enforce_signatures=int((row.get("enforce_signatures") or "0").strip() or 0),
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

    scripts_dir = Path(__file__).resolve().parent
    workload_stmt_cache: dict[str, Optional[int]] = {}

    def _resolve_stmt_count(workload_name: str) -> Optional[int]:
        if workload_name in workload_stmt_cache:
            return workload_stmt_cache[workload_name]
        count = _count_workload_statements(scripts_dir / workload_name)
        workload_stmt_cache[workload_name] = count
        return count

    groups: dict[tuple[str, str, int, int, int, int], list[dict[str, Any]]] = {}
    for r in rows:
        key = (
            r.get("mode", ""),
            r.get("workload", ""),
            int(r.get("threads") or 0),
            int(r.get("rate") or 0),
            int(r.get("signing") or 0),
            int(r.get("enforce_signatures") or 0),
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
    for (mode, workload, threads, rate, signing, enforce_signatures), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[0][5])):
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
        dynamic_splits = [as_int(r.get("dynamic_profile_splits", "")) for r in rs]
        dynamic_splits = [x for x in dynamic_splits if x is not None]
        dynamic_merges = [as_int(r.get("dynamic_profile_merges", "")) for r in rs]
        dynamic_merges = [x for x in dynamic_merges if x is not None]
        dynamic_max_leaf = [as_int(r.get("dynamic_profile_max_leaf_items", "")) for r in rs]
        dynamic_max_leaf = [x for x in dynamic_max_leaf if x is not None]

        # Per-row statement_count (from results.csv) takes precedence so a
        # different statement count per workload is reflected in TPS. Fall back
        # to counting the workload file on disk; final fallback is 20000.0 so
        # older results.csv files without the column still summarise without
        # crashing.
        per_row_counts: list[int] = []
        for r in rs:
            sc = as_int(r.get("statement_count", ""))
            if sc is None or sc <= 0:
                sc = _resolve_stmt_count(r.get("workload", ""))
            if sc and sc > 0:
                per_row_counts.append(sc)
        workload_stmt_count = (
            max(set(per_row_counts), key=per_row_counts.count) if per_row_counts else 20000
        )
        throughput_tps = [
            1000.0 * workload_stmt_count / x for x in overall if x and x > 0
        ]

        verify = [
            (r.get("db_merkle_verify", "") or "").strip().lower()
            in ({"not_applicable"} if mode == "pg" else {"t"})
            for r in rs
        ]

        def max_or_empty(nums: list[int]) -> str:
            return str(max(nums)) if nums else ""

        out_rows.append(
            {
                "mode": mode,
                "workload": workload,
                "threads": threads,
                "rate": rate,
                "signing": signing,
                "enforce_signatures": enforce_signatures,
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
                "dynamic_profile_splits": max_or_empty(dynamic_splits),
                "dynamic_profile_merges": max_or_empty(dynamic_merges),
                "dynamic_profile_max_leaf_items": max_or_empty(dynamic_max_leaf),
            }
        )

    with summary_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()) if out_rows else [])
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)


def _write_det_overhead(summary_csv: Path, out_csv: Path) -> None:
    if not summary_csv.exists():
        raise FileNotFoundError(f"missing summary.csv: {summary_csv}")

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

    def pick_metric(row: dict[str, str], preferred: str, fallback: str) -> Optional[float]:
        v = as_float(row.get(preferred, ""))
        if v is not None:
            return v
        return as_float(row.get(fallback, ""))

    mode_norm = {
        "postgres": "pg",
        "pg": "pg",
        "safedb": "det",
        "det": "det",
        "aria": "nondet",
        "nondet": "nondet",
    }

    by_key_mode: dict[tuple[str, int, int, int, int, str], dict[str, str]] = {}
    for r in rows:
        workload = (r.get("workload") or "").strip()
        th = as_int(r.get("threads", ""))
        rate = as_int(r.get("rate", ""))
        signing = as_int(r.get("signing", ""))
        if signing is None:
            signing = 0
        enforce_signatures = as_int(r.get("enforce_signatures", ""))
        if enforce_signatures is None:
            enforce_signatures = 0
        mode_raw = (r.get("mode") or "").strip().lower()
        mode = mode_norm.get(mode_raw, mode_raw)
        if workload == "" or th is None or rate is None or mode not in ("pg", "det"):
            continue
        by_key_mode[(workload, th, rate, signing, enforce_signatures, mode)] = r

    out_rows: list[dict[str, Any]] = []
    keys: set[tuple[str, int, int, int, int]] = set((w, t, r, s, e) for (w, t, r, s, e, _m) in by_key_mode.keys())
    for workload, th, rate, signing, enforce_signatures in sorted(keys, key=lambda x: (x[0], x[1], x[2], x[3], x[4])):
        pg = by_key_mode.get((workload, th, rate, signing, enforce_signatures, "pg"))
        det = by_key_mode.get((workload, th, rate, signing, enforce_signatures, "det"))
        if pg is None or det is None:
            continue

        pg_tps = pick_metric(pg, "median_throughput_tps", "mean_throughput_tps")
        det_tps = pick_metric(det, "median_throughput_tps", "mean_throughput_tps")
        pg_ms = pick_metric(pg, "median_workload_overall_ms", "mean_workload_overall_ms")
        det_ms = pick_metric(det, "median_workload_overall_ms", "mean_workload_overall_ms")
        pg_wait_ms = pick_metric(pg, "median_workload_wait_ms", "mean_workload_wait_ms")
        det_wait_ms = pick_metric(det, "median_workload_wait_ms", "mean_workload_wait_ms")
        pg_retries = as_float(pg.get("mean_retries_total", ""))
        det_retries = as_float(det.get("mean_retries_total", ""))

        tps_delta_abs = (det_tps - pg_tps) if (det_tps is not None and pg_tps is not None) else None
        tps_delta_pct = (100.0 * tps_delta_abs / pg_tps) if (tps_delta_abs is not None and pg_tps and pg_tps != 0) else None
        overhead_pct_from_tps = (100.0 * (pg_tps - det_tps) / pg_tps) if (det_tps is not None and pg_tps and pg_tps != 0) else None
        extra_ms_abs = (det_ms - pg_ms) if (det_ms is not None and pg_ms is not None) else None
        extra_ms_pct = (100.0 * extra_ms_abs / pg_ms) if (extra_ms_abs is not None and pg_ms and pg_ms != 0) else None
        wait_extra_ms_abs = (det_wait_ms - pg_wait_ms) if (det_wait_ms is not None and pg_wait_ms is not None) else None
        wait_extra_ms_pct = (100.0 * wait_extra_ms_abs / pg_wait_ms) if (wait_extra_ms_abs is not None and pg_wait_ms and pg_wait_ms != 0) else None
        retries_extra = (det_retries - pg_retries) if (det_retries is not None and pg_retries is not None) else None

        if overhead_pct_from_tps is None:
            overhead_class = "unknown"
        elif overhead_pct_from_tps > 0:
            overhead_class = "det_slower"
        elif overhead_pct_from_tps < 0:
            overhead_class = "det_faster"
        else:
            overhead_class = "equal"

        out_rows.append(
            {
                "workload": workload,
                "threads": th,
                "rate": rate,
                "signing": signing,
                "enforce_signatures": enforce_signatures,
                "pg_runs": as_int(pg.get("runs", "")) or "",
                "det_runs": as_int(det.get("runs", "")) or "",
                "pg_pass_rate_merkle_verify": pg.get("pass_rate_merkle_verify", ""),
                "det_pass_rate_merkle_verify": det.get("pass_rate_merkle_verify", ""),
                "pg_median_tps": f"{pg_tps:.3f}" if pg_tps is not None else "",
                "det_median_tps": f"{det_tps:.3f}" if det_tps is not None else "",
                "det_tps_delta_abs": f"{tps_delta_abs:.3f}" if tps_delta_abs is not None else "",
                "det_tps_delta_pct_vs_pg": f"{tps_delta_pct:.3f}" if tps_delta_pct is not None else "",
                "det_overhead_pct_from_tps": f"{overhead_pct_from_tps:.3f}" if overhead_pct_from_tps is not None else "",
                "pg_median_workload_ms": f"{pg_ms:.3f}" if pg_ms is not None else "",
                "det_median_workload_ms": f"{det_ms:.3f}" if det_ms is not None else "",
                "det_extra_workload_ms_abs": f"{extra_ms_abs:.3f}" if extra_ms_abs is not None else "",
                "det_extra_workload_ms_pct_vs_pg": f"{extra_ms_pct:.3f}" if extra_ms_pct is not None else "",
                "pg_wait_ms": f"{pg_wait_ms:.3f}" if pg_wait_ms is not None else "",
                "det_wait_ms": f"{det_wait_ms:.3f}" if det_wait_ms is not None else "",
                "det_extra_wait_ms_abs": f"{wait_extra_ms_abs:.3f}" if wait_extra_ms_abs is not None else "",
                "det_extra_wait_ms_pct_vs_pg": f"{wait_extra_ms_pct:.3f}" if wait_extra_ms_pct is not None else "",
                "pg_mean_retries_total": f"{pg_retries:.3f}" if pg_retries is not None else "",
                "det_mean_retries_total": f"{det_retries:.3f}" if det_retries is not None else "",
                "det_extra_retries_total": f"{retries_extra:.3f}" if retries_extra is not None else "",
                "pg_cv_tps_pct": pg.get("cv_throughput_tps_pct", ""),
                "det_cv_tps_pct": det.get("cv_throughput_tps_pct", ""),
                "overhead_class": overhead_class,
            }
        )

    with out_csv.open("w", newline="") as f:
        fieldnames = list(out_rows[0].keys()) if out_rows else [
            "workload",
            "threads",
            "rate",
            "signing",
            "enforce_signatures",
            "pg_runs",
            "det_runs",
            "pg_pass_rate_merkle_verify",
            "det_pass_rate_merkle_verify",
            "pg_median_tps",
            "det_median_tps",
            "det_tps_delta_abs",
            "det_tps_delta_pct_vs_pg",
            "det_overhead_pct_from_tps",
            "pg_median_workload_ms",
            "det_median_workload_ms",
            "det_extra_workload_ms_abs",
            "det_extra_workload_ms_pct_vs_pg",
            "pg_wait_ms",
            "det_wait_ms",
            "det_extra_wait_ms_abs",
            "det_extra_wait_ms_pct_vs_pg",
            "pg_mean_retries_total",
            "det_mean_retries_total",
            "det_extra_retries_total",
            "pg_cv_tps_pct",
            "det_cv_tps_pct",
            "overhead_class",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)


def _write_signing_overhead(summary_csv: Path, out_csv: Path) -> None:
    if not summary_csv.exists():
        raise FileNotFoundError(f"missing summary.csv: {summary_csv}")

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

    def pick_metric(row: dict[str, str], preferred: str, fallback: str) -> Optional[float]:
        v = as_float(row.get(preferred, ""))
        if v is not None:
            return v
        return as_float(row.get(fallback, ""))

    mode_norm = {
        "postgres": "pg",
        "pg": "pg",
        "safedb": "det",
        "det": "det",
        "aria": "nondet",
        "nondet": "nondet",
    }

    by_key_signing: dict[tuple[str, str, int, int, int, int], dict[str, str]] = {}
    for r in rows:
        workload = (r.get("workload") or "").strip()
        th = as_int(r.get("threads", ""))
        rate = as_int(r.get("rate", ""))
        signing = as_int(r.get("signing", ""))
        enforce_signatures = as_int(r.get("enforce_signatures", ""))
        if enforce_signatures is None:
            enforce_signatures = 0
        mode_raw = (r.get("mode") or "").strip().lower()
        mode = mode_norm.get(mode_raw, mode_raw)
        if workload == "" or th is None or rate is None or signing is None:
            continue
        by_key_signing[(mode, workload, th, rate, enforce_signatures, signing)] = r

    out_rows: list[dict[str, Any]] = []
    keys: set[tuple[str, str, int, int, int]] = set((m, w, t, r, e) for (m, w, t, r, e, _s) in by_key_signing.keys())
    for mode, workload, th, rate, enforce_signatures in sorted(keys, key=lambda x: (x[0], x[1], x[2], x[3], x[4])):
        unsigned = by_key_signing.get((mode, workload, th, rate, enforce_signatures, 0))
        signed = by_key_signing.get((mode, workload, th, rate, enforce_signatures, 1))
        if unsigned is None or signed is None:
            continue

        unsigned_tps = pick_metric(unsigned, "median_throughput_tps", "mean_throughput_tps")
        signed_tps = pick_metric(signed, "median_throughput_tps", "mean_throughput_tps")
        unsigned_ms = pick_metric(unsigned, "median_workload_overall_ms", "mean_workload_overall_ms")
        signed_ms = pick_metric(signed, "median_workload_overall_ms", "mean_workload_overall_ms")
        unsigned_wait_ms = pick_metric(unsigned, "median_workload_wait_ms", "mean_workload_wait_ms")
        signed_wait_ms = pick_metric(signed, "median_workload_wait_ms", "mean_workload_wait_ms")
        unsigned_retries = as_float(unsigned.get("mean_retries_total", ""))
        signed_retries = as_float(signed.get("mean_retries_total", ""))

        tps_delta_abs = (signed_tps - unsigned_tps) if (signed_tps is not None and unsigned_tps is not None) else None
        tps_delta_pct = (100.0 * tps_delta_abs / unsigned_tps) if (tps_delta_abs is not None and unsigned_tps and unsigned_tps != 0) else None
        overhead_pct_from_tps = (100.0 * (unsigned_tps - signed_tps) / unsigned_tps) if (signed_tps is not None and unsigned_tps and unsigned_tps != 0) else None
        extra_ms_abs = (signed_ms - unsigned_ms) if (signed_ms is not None and unsigned_ms is not None) else None
        extra_ms_pct = (100.0 * extra_ms_abs / unsigned_ms) if (extra_ms_abs is not None and unsigned_ms and unsigned_ms != 0) else None
        wait_extra_ms_abs = (signed_wait_ms - unsigned_wait_ms) if (signed_wait_ms is not None and unsigned_wait_ms is not None) else None
        wait_extra_ms_pct = (100.0 * wait_extra_ms_abs / unsigned_wait_ms) if (wait_extra_ms_abs is not None and unsigned_wait_ms and unsigned_wait_ms != 0) else None
        retries_extra = (signed_retries - unsigned_retries) if (signed_retries is not None and unsigned_retries is not None) else None

        if overhead_pct_from_tps is None:
            overhead_class = "unknown"
        elif overhead_pct_from_tps > 0:
            overhead_class = "signed_slower"
        elif overhead_pct_from_tps < 0:
            overhead_class = "signed_faster"
        else:
            overhead_class = "equal"

        out_rows.append(
            {
                "mode": mode,
                "workload": workload,
                "threads": th,
                "rate": rate,
                "enforce_signatures": enforce_signatures,
                "unsigned_runs": as_int(unsigned.get("runs", "")) or "",
                "signed_runs": as_int(signed.get("runs", "")) or "",
                "unsigned_pass_rate_merkle_verify": unsigned.get("pass_rate_merkle_verify", ""),
                "signed_pass_rate_merkle_verify": signed.get("pass_rate_merkle_verify", ""),
                "unsigned_median_tps": f"{unsigned_tps:.3f}" if unsigned_tps is not None else "",
                "signed_median_tps": f"{signed_tps:.3f}" if signed_tps is not None else "",
                "signing_tps_delta_abs": f"{tps_delta_abs:.3f}" if tps_delta_abs is not None else "",
                "signing_tps_delta_pct_vs_unsigned": f"{tps_delta_pct:.3f}" if tps_delta_pct is not None else "",
                "signing_overhead_pct_from_tps": f"{overhead_pct_from_tps:.3f}" if overhead_pct_from_tps is not None else "",
                "unsigned_median_workload_ms": f"{unsigned_ms:.3f}" if unsigned_ms is not None else "",
                "signed_median_workload_ms": f"{signed_ms:.3f}" if signed_ms is not None else "",
                "signing_extra_workload_ms_abs": f"{extra_ms_abs:.3f}" if extra_ms_abs is not None else "",
                "signing_extra_workload_ms_pct_vs_unsigned": f"{extra_ms_pct:.3f}" if extra_ms_pct is not None else "",
                "unsigned_wait_ms": f"{unsigned_wait_ms:.3f}" if unsigned_wait_ms is not None else "",
                "signed_wait_ms": f"{signed_wait_ms:.3f}" if signed_wait_ms is not None else "",
                "signing_extra_wait_ms_abs": f"{wait_extra_ms_abs:.3f}" if wait_extra_ms_abs is not None else "",
                "signing_extra_wait_ms_pct_vs_unsigned": f"{wait_extra_ms_pct:.3f}" if wait_extra_ms_pct is not None else "",
                "unsigned_mean_retries_total": f"{unsigned_retries:.3f}" if unsigned_retries is not None else "",
                "signed_mean_retries_total": f"{signed_retries:.3f}" if signed_retries is not None else "",
                "signing_extra_retries_total": f"{retries_extra:.3f}" if retries_extra is not None else "",
                "unsigned_cv_tps_pct": unsigned.get("cv_throughput_tps_pct", ""),
                "signed_cv_tps_pct": signed.get("cv_throughput_tps_pct", ""),
                "overhead_class": overhead_class,
            }
        )

    with out_csv.open("w", newline="") as f:
        fieldnames = list(out_rows[0].keys()) if out_rows else [
            "mode",
            "workload",
            "threads",
            "rate",
            "enforce_signatures",
            "unsigned_runs",
            "signed_runs",
            "unsigned_pass_rate_merkle_verify",
            "signed_pass_rate_merkle_verify",
            "unsigned_median_tps",
            "signed_median_tps",
            "signing_tps_delta_abs",
            "signing_tps_delta_pct_vs_unsigned",
            "signing_overhead_pct_from_tps",
            "unsigned_median_workload_ms",
            "signed_median_workload_ms",
            "signing_extra_workload_ms_abs",
            "signing_extra_workload_ms_pct_vs_unsigned",
            "unsigned_wait_ms",
            "signed_wait_ms",
            "signing_extra_wait_ms_abs",
            "signing_extra_wait_ms_pct_vs_unsigned",
            "unsigned_mean_retries_total",
            "signed_mean_retries_total",
            "signing_extra_retries_total",
            "unsigned_cv_tps_pct",
            "signed_cv_tps_pct",
            "overhead_class",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
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

    by_workload: dict[tuple[str, str, int, int, int], list[dict[str, str]]] = {}
    for r in rows:
        by_workload.setdefault(
            (
                r.get("workload", ""),
                r.get("mode", ""),
                int(r.get("rate") or 0),
                int(r.get("signing") or 0),
                int(r.get("enforce_signatures") or 0),
            ),
            [],
        ).append(r)

    print("")
    print("Scaling check (throughput should be non-decreasing with threads):")
    for (workload, mode, rate, signing, enforce_signatures) in sorted(by_workload.keys()):
        rs = sorted(by_workload[(workload, mode, rate, signing, enforce_signatures)], key=lambda x: int(x.get("threads") or 0))
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
            f"  workload={workload} mode={mode} rate={rate} signing={signing} enforce={enforce_signatures} median_non_decreasing={int(med_ok)} mean_non_decreasing={int(mean_ok)} median_tps_by_threads=[{' '.join(pairs)}]"
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
        series: dict[str, list[tuple[int, float]]] = {}

        for r in rs:
            raw_mode = (r.get("mode") or "").strip().lower()
            mode = mode_norm.get(raw_mode, raw_mode)
            if mode not in mode_order:
                continue
            signing = as_int(r.get("signing", ""))
            if signing is None:
                signing = 0
            enforce_signatures = as_int(r.get("enforce_signatures", ""))
            if enforce_signatures is None:
                enforce_signatures = 0
            label = mode
            if signing == 1:
                label += "+sig"
            if enforce_signatures == 1:
                label += "+enf"

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
                    stmt_count = as_int(r.get("statement_count", "")) or 20000
                    tps = 1000.0 * stmt_count / overall_ms
            if th is None or tps is None:
                continue
            series.setdefault(label, []).append((th, tps))

        # Skip graph if no data points at all for this workload/rate.
        if not any(series.values()):
            continue

        fig, ax = plt.subplots(figsize=(9.5, 5.5), dpi=130)
        label_order = []
        for mode in mode_order:
            label_order.append(mode)
            label_order.append(f"{mode}+sig")
            label_order.append(f"{mode}+enf")
            label_order.append(f"{mode}+sig+enf")
        for label in label_order:
            points = sorted(series.get(label, []), key=lambda x: x[0])
            if not points:
                continue
            mode = label.replace("+sig", "").replace("+enf", "")
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            linestyle = "--" if "+sig" in label else "-"
            ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=4.5, linestyle=linestyle, color=mode_colors[mode], label=label)

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
        "--signing-modes",
        default="0",
        help="Comma-separated signing modes for deterministic runs: 0=unsigned, 1=signed.",
    )
    parser.add_argument(
        "--signing-privkey",
        default="",
        help="PEM private key path used by the inner workload when signing mode includes 1.",
    )
    parser.add_argument(
        "--enforce-signatures",
        type=int,
        choices=[0, 1],
        default=0,
        help="Set bcdb_enforce_signatures in workload sessions.",
    )
    parser.add_argument(
        "--dynamic-merkle",
        action="store_true",
        help=(
            "Use the native dynamic-Merkle restore for deterministic modes. "
            "Plain PostgreSQL mode continues to use the non-Merkle baseline restore."
        ),
    )
    parser.add_argument(
        "--dynamic-merkle-index",
        default="public.usertable_small_dynamic_merkle_idx",
        help="Dynamic Merkle index used for explicit post-run verification.",
    )
    parser.add_argument(
        "--dynamic-merkle-profile",
        action="store_true",
        help="Enable native dynamic-Merkle split/merge counters and record them per local run.",
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
    parser.add_argument(
        "--transaction-isolation",
        choices=["read committed", "repeatable read", "serializable"],
        default="serializable",
        help="Isolation requested in every PG and DET workload session (default: serializable).",
    )
    parser.add_argument("--out-dir", default="", help="Output directory. Default: scripts/bench_results/<timestamp>.")
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Do not run workloads. Rebuild summary.csv (if needed) and generate det_overhead.csv for --out-dir.",
    )
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
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help=(
            "Number of un-measured warmup workload executions to run after restore, before the"
            " first measured run. Warmup heats the buffer pool / OS page cache so that pg and det"
            " are compared on an equal footing. Output is discarded and not written to results.csv."
            " Default: 1. Set to 0 to disable."
        ),
    )

    args = parser.parse_args()

    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent

    start_server = scripts_dir / "start_server.sh"
    restore_sql = scripts_dir / "restore_usertable_small.sql"
    dynamic_restore_sql = scripts_dir / "distributed" / "sql" / "restore_usertable_small_dynamic.sql"
    workload_py = scripts_dir / "generic-saicopg-traffic-load+logSkip-safedb+pg.py"

    if not start_server.exists():
        print(f"ERROR: missing {start_server}", file=sys.stderr)
        return 2
    if not restore_sql.exists():
        print(f"ERROR: missing {restore_sql}", file=sys.stderr)
        return 2
    if args.dynamic_merkle and not dynamic_restore_sql.exists():
        print(f"ERROR: missing {dynamic_restore_sql}", file=sys.stderr)
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
    signing_modes = _split_csv_ints(args.signing_modes)
    if not signing_modes:
        print("ERROR: no signing modes selected", file=sys.stderr)
        return 2
    if any(s not in (0, 1) for s in signing_modes):
        print("ERROR: --signing-modes supports only 0 and 1", file=sys.stderr)
        return 2
    if 1 in signing_modes and not args.signing_privkey:
        print("ERROR: --signing-privkey is required when --signing-modes includes 1", file=sys.stderr)
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
    det_overhead_csv = out_dir / "det_overhead.csv"
    signing_overhead_csv = out_dir / "signing_overhead.csv"
    meta_json = out_dir / "run_meta.json"

    psql_env = os.environ.get("ARIABC_PSQL", "").strip()
    if psql_env:
        psql_path = psql_env
    else:
        psql_src = repo_root / "src" / "bin" / "psql" / "psql"
        psql_path = str(psql_src) if psql_src.exists() else "/work/ARIABC/install/bin/psql"
    python_env = os.environ.get("ARIABC_PYTHON", "").strip()
    if python_env:
        python_path = python_env
    else:
        venv_python = repo_root / ".venv" / "bin" / "python3"
        python_path = str(venv_python) if venv_python.exists() else (sys.executable or "python3")
    server_log_path = repo_root / "server.log"

    pg_config_path = str(Path(psql_path).with_name("pg_config"))

    def _pg_config_value(option: str) -> str:
        try:
            proc = subprocess.run(
                [pg_config_path, option],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
            return proc.stdout.strip() if proc.returncode == 0 else ""
        except Exception:
            return ""

    pg_config_configure = _pg_config_value("--configure")
    pg_config_cflags = _pg_config_value("--cflags")

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
    env["BENCH_TX_ISOLATION"] = args.transaction_isolation
    # Merkle is mode-specific: plain PG is the baseline without AriaBC/Merkle;
    # deterministic modes include Merkle maintenance and verification.

    meta = {
        "created_utc": _now_utc_iso(),
        "repo_root": str(repo_root),
        "scripts_dir": str(scripts_dir),
        "threads": threads,
        "workloads": workloads,
        "runs": args.runs,
        "modes": modes,
        "signing_modes": signing_modes,
        "signing_privkey": args.signing_privkey,
        "enforce_signatures": args.enforce_signatures,
        "transaction_isolation": args.transaction_isolation,
        "rate": args.rate,
        "rates": rates,
        "db": {"name": args.db, "user": args.user, "port": args.port},
        "timeouts": {
            "workload_s": args.timeout_workload_s,
            "workload_det_s": args.timeout_workload_det_s,
        },
        "retry_policy": {
            "serialization_retry_backoff_s": env.get(
                "SERIALIZATION_RETRY_BACKOFF_SEC", "0.001"
            ),
            "generic_retry_backoff_s": env.get("RETRY_BACKOFF_SEC", "0.1"),
        },
        "commands": {
            "start_server": str(start_server),
            "restore_sql": str(restore_sql),
            "dynamic_restore_sql": str(dynamic_restore_sql) if args.dynamic_merkle else "",
            "dynamic_merkle": bool(args.dynamic_merkle),
            "dynamic_merkle_index": args.dynamic_merkle_index,
            "workload_py": str(workload_py),
            "psql": psql_path,
            "python": python_path,
        },
        "build_profile": {
            "pg_config": pg_config_path,
            "configure": pg_config_configure,
            "cflags": pg_config_cflags,
            "optimized_release_contract": (
                "-O0" not in pg_config_cflags
                and any(flag in pg_config_cflags.split() for flag in ("-O1", "-O2", "-O3", "-Os", "-Ofast"))
                and "--enable-debug" not in pg_config_configure
                and "--enable-cassert" not in pg_config_configure
            ),
        },
        "env_config": {
            key: env.get(key, "")
            for key in [
                "BCDB_EXTRA_GUCS",
                "BCDB_BLOCK_RETURN_ACTUAL_RESULTS",
                "BCDB_DT_SKIP_READONLY_GATE",
                "BCDB_DT_PARSE_BARRIER",
                "BCDB_DT_LIGHT_SNAPSHOT",
                "ARIABC_PSYCOPG_CLIENT_CURSOR",
                "BENCH_TX_ISOLATION",
                "SERIALIZATION_RETRY_BACKOFF_SEC",
                "PGOPTIONS",
            ]
        },
    }
    meta_json.write_text(json.dumps(meta, indent=2) + "\n")

    if args.analyze_only:
        if not args.out_dir:
            print("ERROR: --analyze-only requires --out-dir pointing to an existing run directory.", file=sys.stderr)
            return 2
        if not summary_csv.exists():
            if not raw_csv.exists():
                print(f"ERROR: missing both {raw_csv} and {summary_csv}", file=sys.stderr)
                return 2
            _write_summary(raw_csv, summary_csv)
        _write_det_overhead(summary_csv, det_overhead_csv)
        _write_signing_overhead(summary_csv, signing_overhead_csv)
        print(f"Wrote summary:      {summary_csv}")
        print(f"Wrote det overhead: {det_overhead_csv}")
        print(f"Wrote sign overhead:{signing_overhead_csv}")
        return 0

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
        signing=0,
        enforce_signatures=0,
        transaction_isolation="",
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
        dynamic_profile_splits=None,
        dynamic_profile_merges=None,
        dynamic_profile_max_leaf_items=None,
        workload_log="",
        start_server_log="",
        restore_log="",
        notes="",
        statement_count=None,
    )).keys())

    write_header = not raw_csv.exists()
    with raw_csv.open("a", newline="") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        total = sum(
            len(workloads) * len(threads) * len(rates) * args.runs *
            (len(signing_modes) if MODE_NAME_TO_DB_TYPE[mode] == 1 else 1)
            for mode in modes
        )
        # Single global warmup flag: we only need to warm the buffer pool once
        # Warm each mode once.  PG and DET have different indexes/execution
        # machinery, so warming only the first mode biases the comparison.
        warmed_modes: set[str] = set()

        done = 0

        for mode in modes:
            db_type = MODE_NAME_TO_DB_TYPE[mode]
            effective_signing_modes = signing_modes if db_type == 1 else [0]
            for workload in workloads:
                workload_path = scripts_dir / workload
                if not workload_path.exists():
                    print(f"WARNING: missing workload file {workload_path}, skipping", file=sys.stderr)
                    continue
                workload_stmt_count = _count_workload_statements(workload_path)

                for th in threads:
                    for rate in rates:
                        for signing in effective_signing_modes:
                            for run_idx in range(1, args.runs + 1):
                                key = RunKey(mode=mode, workload=workload, threads=th, rate=rate, signing=signing, enforce_signatures=args.enforce_signatures, run=run_idx)
                                if key in existing:
                                    done += 1
                                    continue

                                case_dir = (
                                    out_dir
                                    / f"mode={mode}"
                                    / f"workload={workload_path.stem}"
                                    / f"rate={rate}"
                                    / f"signing={signing}"
                                    / f"enforce={args.enforce_signatures}"
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
                                profile_log_offset = 0
                                profile_splits = None
                                profile_merges = None
                                profile_max_leaf_items = None
                                if args.dynamic_merkle_profile and server_log_path.exists():
                                    try:
                                        profile_log_offset = server_log_path.stat().st_size
                                    except OSError:
                                        profile_log_offset = 0

                                t0 = time.monotonic()

                                mode_env = dict(env)
                                merkle_for_mode = db_type != 0
                                case_restore_sql = (
                                    dynamic_restore_sql
                                    if args.dynamic_merkle and merkle_for_mode
                                    else restore_sql
                                )
                                _force_pgoption(
                                    mode_env,
                                    "enable_merkle_index",
                                    "on" if merkle_for_mode else "off",
                                )
                                if args.dynamic_merkle and merkle_for_mode:
                                    # The dynamic restore intentionally performs a destructive
                                    # benchmark reset.  Pass the guard as a session GUC; an
                                    # environment variable alone is not visible to PostgreSQL.
                                    _force_pgoption(
                                        mode_env,
                                        "ariabc.allow_destructive_benchmark_reset",
                                        "on",
                                    )
                                    if args.dynamic_merkle_profile:
                                        _force_pgoption(
                                            mode_env,
                                            "merkle_native_profile_enabled",
                                            "on",
                                        )

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

                                expected_data_dir = _canonical_path_str(env.get("ARIABC_PGDATA", "/work/ARIABC/pgdata"))
                                live_data_dir = None
                                live_data_dir_raw = _psql_value(
                                    psql_path,
                                    db=args.db,
                                    port=args.port,
                                    user=args.user,
                                    query="show data_directory;",
                                    cwd=scripts_dir,
                                    env=mode_env,
                                )
                                live_data_dir = _canonical_path_str(live_data_dir_raw)

                                merkle_enable_notes: list[str] = []
                                merkle_enabled_ok, merkle_enable_notes = _ensure_merkle_index_mode(
                                    psql_path,
                                    db=args.db,
                                    port=args.port,
                                    user=args.user,
                                    cwd=scripts_dir,
                                    env=mode_env,
                                    enabled=merkle_for_mode,
                                )

                                restore_exit = 0
                                if not merkle_enabled_ok:
                                    restore_exit = 98
                                    restore_log_err.write_text(
                                        "enable_merkle_index_not_on=1\n"
                                        + "\n".join(merkle_enable_notes)
                                        + "\n"
                                    )
                                elif expected_data_dir and live_data_dir != expected_data_dir:
                                    restore_exit = 97
                                    restore_log_err.write_text(
                                        f"expected_data_directory={expected_data_dir}\n"
                                        f"live_data_directory={live_data_dir_raw or ''}\n"
                                        "data_directory_mismatch=1\n"
                                    )
                                else:
                                    print(
                                        f"  [restore] mode={mode} workload={workload} "
                                        f"threads={th} run={run_idx}",
                                        flush=True,
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
                                            "-v",
                                            "ON_ERROR_STOP=1",
                                            "-v",
                                            f"bench_enable_merkle={1 if merkle_for_mode else 0}",
                                            "-f",
                                            str(case_restore_sql),
                                        ],
                                        cwd=scripts_dir,
                                        env=mode_env,
                                        stdout_path=restore_log_out,
                                        stderr_path=restore_log_err,
                                    )
                                    if restore_exit == 0 and db_type == 0:
                                        pg_ready, pg_ready_msg = _prepare_plain_pg_baseline(
                                            psql_path,
                                            db=args.db,
                                            port=args.port,
                                            user=args.user,
                                            cwd=scripts_dir,
                                            env=mode_env,
                                        )
                                        if not pg_ready:
                                            restore_exit = 96
                                            restore_log_err.write_text(
                                                "plain_pg_baseline_prepare_failed=1\n"
                                                + pg_ready_msg
                                                + "\n"
                                            )

                                workload_exit = -1
                                if restore_exit == 0:
                                    case_env = dict(mode_env)
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
                                            "signing": signing,
                                            "enforce_signatures": args.enforce_signatures,
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
                                        str(rate),
                                        "--signing",
                                        str(signing),
                                        "--enforce-signatures",
                                        str(args.enforce_signatures),
                                    ]
                                    if signing == 1:
                                        workload_argv.extend(["--privkey", args.signing_privkey])

                                    timeout_workload_s = args.timeout_workload_s
                                    if db_type == 1 and args.timeout_workload_det_s > 0:
                                        timeout_workload_s = args.timeout_workload_det_s

                                    timeout_for_run: Optional[int] = None
                                    if timeout_workload_s > 0:
                                        timeout_for_run = int(timeout_workload_s)

                                    # --- Warmup runs (not recorded) ---
                                    # Run the workload args.warmup_runs times before measuring.
                                    # This ensures the pg buffer pool, OS page cache, and BCDB
                                    # workers are hot before the first measured run, eliminating
                                    # the cold-start bias that makes pg look slower than det at
                                    # low thread counts.
                                    if mode not in warmed_modes and args.warmup_runs > 0:
                                        for warmup_i in range(args.warmup_runs):
                                            warmup_env = dict(case_env)
                                            if db_type == 1:
                                                warmup_env["DET_START_SEQ"] = "0"
                                            print(
                                                f"  [warmup {warmup_i + 1}/{args.warmup_runs}]"
                                                f" mode={mode} workload={workload} threads={th} run={run_idx}",
                                                flush=True,
                                            )
                                            _run(
                                                workload_argv,
                                                cwd=scripts_dir,
                                                env=warmup_env,
                                                stdout_path=case_dir / f"warmup_{warmup_i + 1}.out",
                                                stderr_path=case_dir / f"warmup_{warmup_i + 1}.err",
                                                timeout_s=timeout_for_run,
                                            )
                                        warmed_modes.add(mode)
                                        print(
                                            f"  [restore-after-warmup] mode={mode} workload={workload} "
                                            f"threads={th} run={run_idx}",
                                            flush=True,
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
                                                "-v",
                                                "ON_ERROR_STOP=1",
                                                "-v",
                                                f"bench_enable_merkle={1 if merkle_for_mode else 0}",
                                                "-f",
                                                str(case_restore_sql),
                                            ],
                                            cwd=scripts_dir,
                                            env=mode_env,
                                            stdout_path=case_dir / "restore_after_warmup.out",
                                            stderr_path=case_dir / "restore_after_warmup.err",
                                        )
                                        if restore_exit == 0 and db_type == 0:
                                            pg_ready, pg_ready_msg = _prepare_plain_pg_baseline(
                                                psql_path,
                                                db=args.db,
                                                port=args.port,
                                                user=args.user,
                                                cwd=scripts_dir,
                                                env=mode_env,
                                            )
                                            if not pg_ready:
                                                restore_exit = 96
                                                (case_dir / "restore_after_warmup.err").write_text(
                                                    "plain_pg_baseline_prepare_failed=1\n"
                                                    + pg_ready_msg
                                                    + "\n"
                                                )
                                        # Reset DET txid counter to 0 for the actual measured run.
                                        if db_type == 1:
                                            case_env["DET_START_SEQ"] = "0"
                                    # --- End warmup ---

                                    if restore_exit == 0:
                                        # Profile only the measured workload.  The dynamic
                                        # restore/index build can legitimately split leaves,
                                        # but those transitions are setup rather than workload
                                        # activity and must not contaminate the recorded count.
                                        if args.dynamic_merkle_profile and server_log_path.exists():
                                            try:
                                                profile_log_offset = server_log_path.stat().st_size
                                            except OSError:
                                                profile_log_offset = 0
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

                                if args.dynamic_merkle_profile and profile_log_offset >= 0:
                                    try:
                                        with server_log_path.open("rb") as _profile_log:
                                            _profile_log.seek(profile_log_offset)
                                            profile_text = _profile_log.read().decode(errors="replace")
                                        profile_rows = re.findall(
                                            r"NATIVE_MERKLE_PROFILE index_oid=\d+ splits=(\d+) merges=(\d+)",
                                            profile_text,
                                        )
                                        if profile_rows:
                                            profile_splits = sum(int(s) for s, _ in profile_rows)
                                            profile_merges = sum(int(m) for _, m in profile_rows)
                                    except OSError:
                                        pass

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
                                verification_error = ""
                                timeout_diag_path: Optional[Path] = None
                                if restore_exit == 0 and workload_exit == 0:
                                    if db_type == 0:
                                        count_s = _psql_value(
                                            psql_path,
                                            db=args.db,
                                            port=args.port,
                                            user=args.user,
                                            query="select count(*) from usertable_small;",
                                            cwd=scripts_dir,
                                            env=mode_env,
                                        )
                                        verify = "not_applicable"
                                        if count_s is None:
                                            verification_error = "post_workload_row_count_failed"
                                        pg_merkle_indexes = _psql_value(
                                            psql_path,
                                            db=args.db,
                                            port=args.port,
                                            user=args.user,
                                            query=(
                                                "select count(*) from pg_class c "
                                                "join pg_am a on a.oid = c.relam "
                                                "where c.relname = 'usertable_merkle_multikey_variable' "
                                                "and a.amname = 'merkle';"
                                            ),
                                            cwd=scripts_dir,
                                            env=mode_env,
                                        )
                                        pg_pending_deltas = _psql_value(
                                            psql_path,
                                            db=args.db,
                                            port=args.port,
                                            user=args.user,
                                            query="select count(*) from ariabc_internal.merkle_local_delta;",
                                            cwd=scripts_dir,
                                            env=mode_env,
                                        )
                                        if pg_merkle_indexes != "0":
                                            verification_error = "pg_baseline_has_merkle_index"
                                        elif pg_pending_deltas != "0":
                                            verification_error = "pg_baseline_created_merkle_deltas"
                                    else:
                                        apply_ok, apply_msg = _psql_exec(
                                            psql_path,
                                            db=args.db,
                                            port=args.port,
                                            user=args.user,
                                            query="select merkle_apply_pending();",
                                            cwd=scripts_dir,
                                            env=mode_env,
                                        )
                                        if not apply_ok:
                                            verification_error = "merkle_apply_pending_failed: " + apply_msg
                                        else:
                                            count_s = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select count(*) from usertable_small;", cwd=scripts_dir, env=mode_env)
                                            if args.dynamic_merkle:
                                                dynamic_index = args.dynamic_merkle_index.replace("'", "''")
                                                root_hash = _psql_value(
                                                    psql_path,
                                                    db=args.db,
                                                    port=args.port,
                                                    user=args.user,
                                                    query=(
                                                        "select merkle_root_hash_index('"
                                                        + dynamic_index
                                                        + "'::regclass);"
                                                    ),
                                                    cwd=scripts_dir,
                                                    env=mode_env,
                                                )
                                                verify = _psql_value(
                                                    psql_path,
                                                    db=args.db,
                                                    port=args.port,
                                                    user=args.user,
                                                    query=(
                                                        "select merkle_dynamic_verify('"
                                                        + dynamic_index
                                                        + "'::regclass);"
                                                    ),
                                                    cwd=scripts_dir,
                                                    env=mode_env,
                                                )
                                                if args.dynamic_merkle_profile:
                                                    profile_values = _psql_value(
                                                        psql_path,
                                                        db=args.db,
                                                        port=args.port,
                                                        user=args.user,
                                                        query=(
                                                            "SELECT COALESCE(s->>'split_count','0') || '|' || "
                                                            "COALESCE(s->>'merge_count','0') || '|' || "
                                                            "(s->>'max_leaf_items') FROM (SELECT "
                                                            "merkle_dynamic_tree_stats('" + dynamic_index + "'::regclass)::jsonb AS s) q;"
                                                        ),
                                                        cwd=scripts_dir,
                                                        env=mode_env,
                                                    )
                                                    parts = (profile_values or '').split('|')
                                                    if len(parts) == 3 and all(p.isdigit() for p in parts):
                                                        if profile_splits is None:
                                                            profile_splits = int(parts[0])
                                                        if profile_merges is None:
                                                            profile_merges = int(parts[1])
                                                        profile_max_leaf_items = int(parts[2])
                                                    elif profile_splits is None:
                                                        verification_error = "dynamic_profile_query_failed"
                                            else:
                                                root_hash = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_root_hash('usertable_small');", cwd=scripts_dir, env=mode_env)
                                                verify = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_verify('usertable_small');", cwd=scripts_dir, env=mode_env)
                                            if count_s is None or root_hash is None or verify is None:
                                                verification_error = "post_workload_verification_query_failed"
                                    if verification_error:
                                        (case_dir / "verification.err").write_text(verification_error + "\n")
                                if restore_exit == 0 and workload_exit == 124:
                                    timeout_diag_path = _capture_timeout_diagnostics(
                                        case_dir=case_dir,
                                        psql=psql_path,
                                        db=args.db,
                                        port=args.port,
                                        user=args.user,
                                        cwd=scripts_dir,
                                        env=mode_env,
                                        server_log=server_log_path,
                                    )

                                row_count = None
                                if count_s is not None and count_s.strip().isdigit():
                                    row_count = int(count_s.strip())

                                notes_parts: list[str] = []
                                bcdb_extra_gucs = env.get("BCDB_EXTRA_GUCS", "")
                                if bcdb_extra_gucs:
                                    notes_parts.append(
                                        "bcdb_extra_gucs=" + bcdb_extra_gucs.replace(",", "|")
                                    )
                                notes_parts.extend(merkle_enable_notes)
                                for env_key in [
                                    "BCDB_BLOCK_RETURN_ACTUAL_RESULTS",
                                    "BCDB_DT_SKIP_READONLY_GATE",
                                    "BCDB_DT_PARSE_BARRIER",
                                    "BCDB_DT_LIGHT_SNAPSHOT",
                                    "PGOPTIONS",
                                ]:
                                    env_value = env.get(env_key, "")
                                    if env_value:
                                        notes_parts.append(f"{env_key.lower()}={env_value}")
                                if restart_server and start_exit != 0:
                                    notes_parts.append(f"start_server_exit={start_exit}")
                                if expected_data_dir and live_data_dir != expected_data_dir:
                                    notes_parts.append(f"expected_data_directory={expected_data_dir}")
                                    notes_parts.append(f"live_data_directory={live_data_dir or live_data_dir_raw or 'unknown'}")
                                    notes_parts.append("data_directory_mismatch=1")
                                if restore_exit != 0:
                                    notes_parts.append(f"restore_exit={restore_exit}")
                                    notes_parts.append("workload_skipped=1")
                                elif workload_exit != 0:
                                    notes_parts.append(f"workload_exit={workload_exit}")
                                    if workload_exit == 124:
                                        notes_parts.append("workload_timeout=1")
                                        if timeout_diag_path is not None:
                                            notes_parts.append(f"timeout_diag={timeout_diag_path.name}")
                                elif verify is not None and verify.strip().lower() not in (
                                    "t", "f", "", "not_applicable"
                                ):
                                    notes_parts.append(f"unexpected_verify={verify!r}")
                                if verification_error:
                                    notes_parts.append("verification_error=1")
                                notes = ";".join(notes_parts)

                                result = RunResult(
                                    ts_utc=_now_utc_iso(),
                                    mode=mode,
                                    db_type=db_type,
                                    workload=workload,
                                    threads=th,
                                    run=run_idx,
                                    rate=rate,
                                    signing=signing,
                                    enforce_signatures=args.enforce_signatures,
                                    transaction_isolation=args.transaction_isolation,
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
                                    dynamic_profile_splits=profile_splits,
                                    dynamic_profile_merges=profile_merges,
                                    dynamic_profile_max_leaf_items=profile_max_leaf_items,
                                    workload_log=str(workload_log_out),
                                    start_server_log=str(start_log_out),
                                    restore_log=str(restore_log_out),
                                    notes=notes,
                                    statement_count=workload_stmt_count,
                                )

                                writer.writerow(asdict(result))
                                fcsv.flush()

                                done += 1
                                print(f"[{done}/{total}] {mode} workload={workload} rate={rate} signing={signing} enforce={args.enforce_signatures} threads={th} run={run_idx} exit={workload_exit} verify={verify}")

                                if restore_exit != 0:
                                    print(
                                        f"ERROR: restore failed for mode={mode} workload={workload} "
                                        f"threads={th} run={run_idx} exit={restore_exit}; aborting matrix",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    return 1
                                if workload_exit != 0 or permanent_failures not in (None, 0):
                                    print(
                                        f"ERROR: workload failed for mode={mode} workload={workload} "
                                        f"threads={th} run={run_idx} exit={workload_exit} "
                                        f"permanent_failures={permanent_failures}; aborting matrix",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    return 1
                                expected_verify = "not_applicable" if db_type == 0 else "t"
                                if verification_error or verify != expected_verify:
                                    print(
                                        f"ERROR: Merkle verification failed for mode={mode} workload={workload} "
                                        f"threads={th} run={run_idx} verify={verify!r}; aborting matrix",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    return 1

    try:
        _write_summary(raw_csv, summary_csv)
    except Exception as e:
        print(f"WARNING: failed to write summary: {e}", file=sys.stderr)

    try:
        _write_det_overhead(summary_csv, det_overhead_csv)
    except Exception as e:
        print(f"WARNING: failed to write det overhead summary: {e}", file=sys.stderr)

    try:
        _write_signing_overhead(summary_csv, signing_overhead_csv)
    except Exception as e:
        print(f"WARNING: failed to write signing overhead summary: {e}", file=sys.stderr)

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
    print(f"Wrote det overhead:{det_overhead_csv}")
    print(f"Wrote sign overhead:{signing_overhead_csv}")
    print(f"Wrote metadata:    {meta_json}")
    if graph_paths:
        print("Wrote graphs:")
        for p in graph_paths:
            print(f"  {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
