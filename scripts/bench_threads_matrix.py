#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional


DEFAULT_THREADS = [1] + list(range(4, 65, 4))
DEFAULT_WORKLOADS = [
    "ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt",
    "ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt",
]

MODE_NAME_TO_DB_TYPE = {
    "det": 1,
    "nondet": 2,
}


@dataclass(frozen=True)
class RunKey:
    mode: str
    workload: str
    threads: int
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
                keys.add(
                    RunKey(
                        mode=row["mode"],
                        workload=row["workload"],
                        threads=int(row["threads"]),
                        run=int(row["run"]),
                    )
                )
            except Exception:
                continue
    return keys


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

    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for r in rows:
        key = (r.get("mode", ""), r.get("workload", ""), int(r.get("threads") or 0))
        groups.setdefault(key, []).append(r)

    def mean(nums: list[float]) -> Optional[float]:
        if not nums:
            return None
        return sum(nums) / len(nums)

    def pass_rate(ys: list[bool]) -> float:
        if not ys:
            return 0.0
        return sum(1 for y in ys if y) / len(ys)

    out_rows: list[dict[str, Any]] = []
    for (mode, workload, threads), rs in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
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

        verify = [(r.get("db_merkle_verify", "") or "").strip().lower() == "t" for r in rs]

        def max_or_empty(nums: list[int]) -> str:
            return str(max(nums)) if nums else ""

        out_rows.append(
            {
                "mode": mode,
                "workload": workload,
                "threads": threads,
                "runs": len(rs),
                "pass_rate_merkle_verify": f"{pass_rate(verify):.3f}",
                "mean_wall_time_s": f"{mean(wall):.6f}" if wall else "",
                "mean_workload_overall_ms": f"{mean(overall):.3f}" if overall else "",
                "mean_workload_wait_ms": f"{mean(wait):.3f}" if wait else "",
                "mean_duplicate_key_errors": f"{mean([float(x) for x in dups]):.3f}" if dups else "",
                "mean_retries_total": f"{mean([float(x) for x in retries_total]):.3f}" if retries_total else "",
                "mean_serialization_retries": f"{mean([float(x) for x in serialization_retries]):.3f}" if serialization_retries else "",
                "mean_reconnects": f"{mean([float(x) for x in reconnects]):.3f}" if reconnects else "",
                "mean_permanent_failures": f"{mean([float(x) for x in permanent_failures]):.3f}" if permanent_failures else "",
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark AriaBC determinism/nondeterminism across thread counts.")
    parser.add_argument("--threads", default=",".join(str(x) for x in DEFAULT_THREADS), help="Comma-separated thread counts.")
    parser.add_argument("--runs", type=int, default=3, help="Runs per (mode, workload, threads).")
    parser.add_argument("--modes", default="det,nondet", help="Comma-separated modes: det,nondet.")
    parser.add_argument("--rate", type=int, default=800, help="Qrate passed to workload script (optional).")
    parser.add_argument("--db", default="postgres", help="Database name.")
    parser.add_argument("--user", default="postgres", help="DB user.")
    parser.add_argument("--port", type=int, default=5438, help="DB port.")
    parser.add_argument("--out-dir", default="", help="Output directory. Default: scripts/bench_results/<timestamp>.")
    parser.add_argument("--no-resume", action="store_true", help="Do not skip runs already present in results.csv.")
    parser.add_argument("--no-server-restart", action="store_true", help="Do not restart server before each run.")

    args = parser.parse_args()

    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent

    start_server = scripts_dir / "start_server.sh"
    restore_sql = scripts_dir / "restore_usertable.sql"
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
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for m in modes:
        if m not in MODE_NAME_TO_DB_TYPE:
            print(f"ERROR: unknown mode {m!r} (supported: {', '.join(MODE_NAME_TO_DB_TYPE)})", file=sys.stderr)
            return 2

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir) if args.out_dir else (scripts_dir / "bench_results" / timestamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_csv = out_dir / "results.csv"
    summary_csv = out_dir / "summary.csv"
    meta_json = out_dir / "run_meta.json"

    psql_path = os.environ.get("ARIABC_PSQL", "/work/ARIABC/install/bin/psql")
    python_path = os.environ.get("ARIABC_PYTHON", sys.executable or "python3")

    env = dict(os.environ)
    env.setdefault("PGHOST", "localhost")
    env.setdefault("PGPORT", str(args.port))
    env.setdefault("PGUSER", args.user)
    env.setdefault("PGDATABASE", args.db)

    meta = {
        "created_utc": _now_utc_iso(),
        "repo_root": str(repo_root),
        "scripts_dir": str(scripts_dir),
        "threads": threads,
        "runs": args.runs,
        "modes": modes,
        "rate": args.rate,
        "db": {"name": args.db, "user": args.user, "port": args.port},
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

        total = len(modes) * len(DEFAULT_WORKLOADS) * len(threads) * args.runs
        done = 0

        for mode in modes:
            db_type = MODE_NAME_TO_DB_TYPE[mode]
            for workload in DEFAULT_WORKLOADS:
                workload_path = scripts_dir / workload
                if not workload_path.exists():
                    print(f"WARNING: missing workload file {workload_path}, skipping", file=sys.stderr)
                    continue

                for th in threads:
                    for run_idx in range(1, args.runs + 1):
                        key = RunKey(mode=mode, workload=workload, threads=th, run=run_idx)
                        if key in existing:
                            done += 1
                            continue

                        case_dir = out_dir / f"mode={mode}" / f"workload={workload_path.stem}" / f"threads={th}" / f"run={run_idx}"
                        case_dir.mkdir(parents=True, exist_ok=True)

                        start_log_out = case_dir / "start_server.out"
                        start_log_err = case_dir / "start_server.err"
                        restore_log_out = case_dir / "restore.out"
                        restore_log_err = case_dir / "restore.err"
                        workload_log_out = case_dir / "workload.out"
                        workload_log_err = case_dir / "workload.err"

                        t0 = time.monotonic()

                        start_exit = 0
                        if not args.no_server_restart:
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

                        workload_exit = _run(
                            [
                                python_path,
                                str(workload_py),
                                args.db,
                                str(workload_path),
                                str(db_type),
                                str(th),
                                str(args.rate),
                            ],
                            cwd=scripts_dir,
                            env=env,
                            stdout_path=workload_log_out,
                            stderr_path=workload_log_err,
                        )

                        t1 = time.monotonic()

                        log_text = ""
                        try:
                            log_text = workload_log_out.read_text(errors="replace") + "\n" + workload_log_err.read_text(errors="replace")
                        except Exception:
                            pass

                        overall_ms, wait_ms, dup_keys = _parse_workload_metrics(log_text)
                        retries_total, serialization_retries, reconnects, permanent_failures = _parse_retry_summary(log_text)

                        count_s = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select count(*) from usertable;", cwd=scripts_dir, env=env)
                        root_hash = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_root_hash('usertable');", cwd=scripts_dir, env=env)
                        verify = _psql_value(psql_path, db=args.db, port=args.port, user=args.user, query="select merkle_verify('usertable');", cwd=scripts_dir, env=env)

                        row_count = None
                        if count_s is not None and count_s.strip().isdigit():
                            row_count = int(count_s.strip())

                        notes = ""
                        if workload_exit != 0:
                            notes = f"workload_exit={workload_exit}"
                        elif verify is not None and verify.strip().lower() not in ("t", "f", ""):
                            notes = f"unexpected_verify={verify!r}"

                        result = RunResult(
                            ts_utc=_now_utc_iso(),
                            mode=mode,
                            db_type=db_type,
                            workload=workload,
                            threads=th,
                            run=run_idx,
                            rate=args.rate,
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
                        print(f"[{done}/{total}] {mode} workload={workload} threads={th} run={run_idx} exit={workload_exit} verify={verify}")

    try:
        _write_summary(raw_csv, summary_csv)
    except Exception as e:
        print(f"WARNING: failed to write summary: {e}", file=sys.stderr)

    print("")
    print(f"Wrote raw results: {raw_csv}")
    print(f"Wrote summary:     {summary_csv}")
    print(f"Wrote metadata:    {meta_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
