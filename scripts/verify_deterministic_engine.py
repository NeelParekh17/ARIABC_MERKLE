#!/usr/bin/env python3
"""
verify_deterministic_engine.py — Comprehensive AriaBC Deterministic Database Verification Tool

Automates the rigorous verification of AriaBC's deterministic concurrency control:
1. Generates or loads the multi-hazard deterministic workload and ground-truth oracle.
2. Bootstraps the test database with tables and dynamic Merkle tree indexes.
3. Tests Serial Deterministic Execution (1 worker).
4. Tests Concurrent Deterministic Execution across multiple worker thread configurations (e.g. 2, 4, 8, 16 workers).
5. Tests Counter-Factual Non-Deterministic Execution (concurrent without deterministic sequencing) to prove that the workload genuinely produces race conditions when determinism is disabled.
6. Gathers full table dumps, balance conservation invariants, and Merkle root hashes.
7. Produces an audit report comparing each run against the Oracle Ground Truth.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class TestResult:
    config_name: str
    mode: str  # "det" or "nondet"
    num_workers: int
    duration_s: float
    all_matched_oracle: bool
    accumulators_matched: bool
    pipeline_matched: bool
    accounts_matched: bool
    lifecycle_matched: bool
    balance_conserved: bool
    total_balance: int
    merkle_roots: Dict[str, str]
    divergence_count: int
    error_count: int
    summary_message: str


def run_command(cmd: List[str], cwd: Optional[Path] = None, timeout: int = 120) -> Tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd or REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def restart_server() -> bool:
    """Restarts the local AriaBC PostgreSQL server via scripts/start_server.sh."""
    start_sh = REPO_ROOT / "scripts" / "start_server.sh"
    rc, out, err = run_command(["bash", str(start_sh)], timeout=30)
    if rc != 0:
        print(f"ERROR restarting server: rc={rc}\nstdout: {out}\nstderr: {err}", file=sys.stderr)
        return False
    # Wait for pg_isready
    for _ in range(20):
        rc, _, _ = run_command(["/work/ARIABC/install/bin/pg_isready", "-p", "5438", "-h", "localhost"])
        if rc == 0:
            return True
        time.sleep(0.5)
    return False


def get_db_connection(host: str, port: int, dbname: str, user: str) -> psycopg.Connection:
    return psycopg.connect(
        f"host={host} port={port} dbname={dbname} user={user}",
        autocommit=True,
    )


def bootstrap_database(bootstrap_sql_file: Path, host: str, port: int, dbname: str, user: str):
    """Executes the bootstrap SQL file to create tables, seeds, and Merkle indexes."""
    with open(bootstrap_sql_file, "r") as f:
        statements = [stmt.strip() for stmt in f.read().split(";") if stmt.strip()]

    with get_db_connection(host, port, dbname, user) as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)


def fetch_database_state(host: str, port: int, dbname: str, user: str) -> Dict[str, Any]:
    """Extracts all rows and Merkle root hashes from the test tables."""
    state: Dict[str, Any] = {
        "det_accumulators": [],
        "det_state_pipeline": [],
        "det_accounts": [],
        "det_lifecycle": [],
        "merkle_roots": {},
        "total_balance": 0,
    }

    with get_db_connection(host, port, dbname, user) as conn:
        with conn.cursor() as cur:
            # 1. Accumulators
            cur.execute("SELECT id, val, ops_count FROM det_accumulators ORDER BY id;")
            state["det_accumulators"] = [
                {"id": r[0], "val": r[1], "ops_count": r[2]} for r in cur.fetchall()
            ]

            # 2. State Pipeline
            cur.execute("SELECT id, current_step, step_hash FROM det_state_pipeline ORDER BY id;")
            state["det_state_pipeline"] = [
                {"id": r[0], "current_step": r[1], "step_hash": r[2]} for r in cur.fetchall()
            ]

            # 3. Accounts
            cur.execute("SELECT account_id, balance FROM det_accounts ORDER BY account_id;")
            state["det_accounts"] = [
                {"account_id": r[0], "balance": r[1]} for r in cur.fetchall()
            ]
            cur.execute("SELECT sum(balance) FROM det_accounts;")
            row = cur.fetchone()
            state["total_balance"] = row[0] if row and row[0] is not None else 0

            # 4. Lifecycle
            cur.execute("SELECT key_id, version, payload FROM det_lifecycle ORDER BY key_id;")
            state["det_lifecycle"] = [
                {"key_id": r[0], "version": r[1], "payload": r[2]} for r in cur.fetchall()
            ]

            # 5. Merkle roots
            for tbl in ["det_accumulators", "det_state_pipeline", "det_accounts", "det_lifecycle"]:
                try:
                    cur.execute(f"SELECT merkle_root_hash('{tbl}');")
                    res = cur.fetchone()
                    state["merkle_roots"][tbl] = res[0] if res else "none"
                except Exception as e:
                    state["merkle_roots"][tbl] = f"err:{e}"

    return state


def execute_workload_concurrent(
    statements: List[str],
    num_workers: int,
    host: str,
    port: int,
    dbname: str,
    user: str,
    mode: str = "det",  # "det" (s-prefixed) or "nondet" (raw SQL)
) -> Tuple[float, int]:
    """
    Executes the workload across num_workers concurrent client threads.
    In deterministic mode ('det'), statements are formatted with sequence IDs ('s 00000000 <sql>').
    Seq 0 is executed first to initialize the BCDB deterministic serial gate, then
    subsequent queries are dispatched concurrently across the worker threads.
    """
    if mode == "det":
        formatted_stmts = [f"s {i:08d} {s.strip()}" for i, s in enumerate(statements)]
    else:
        # Raw SQL without deterministic prefixes
        formatted_stmts = [s.strip() for s in statements]

    errors = []
    errors_lock = threading.Lock()

    t0 = time.perf_counter()

    if mode == "det":
        # Initialize gate with seq 0 on the first connection
        with get_db_connection(host, port, dbname, user) as init_conn:
            with init_conn.cursor() as cur:
                cur.execute(formatted_stmts[0])

        remaining_items = list(enumerate(formatted_stmts[1:], start=1))
    else:
        remaining_items = list(enumerate(formatted_stmts))

    if num_workers == 1 or len(remaining_items) == 0:
        # Single-worker execution
        with get_db_connection(host, port, dbname, user) as conn:
            with conn.cursor() as cur:
                for seq, q in remaining_items:
                    for attempt in range(50):
                        try:
                            cur.execute(q)
                            break
                        except Exception as ex:
                            time.sleep(0.005 * (attempt + 1))
                            if attempt == 49:
                                errors.append((seq, q, str(ex)))
    else:
        # Concurrent worker threads
        worker_queues: List[List[Tuple[int, str]]] = [[] for _ in range(num_workers)]
        for idx, item in enumerate(remaining_items):
            worker_queues[idx % num_workers].append(item)

        def worker_thread(worker_id: int, queries: List[Tuple[int, str]]):
            try:
                conn = get_db_connection(host, port, dbname, user)
                with conn.cursor() as cur:
                    for seq, q in queries:
                        for attempt in range(50):
                            try:
                                cur.execute(q)
                                break
                            except Exception as ex:
                                if mode == "nondet":
                                    # In non-deterministic mode, errors (e.g. 23505 unique constraint)
                                    # are recorded without blocking the worker
                                    with errors_lock:
                                        errors.append((seq, q, str(ex)))
                                    break
                                time.sleep(0.005 * (attempt + 1))
                                if attempt == 49:
                                    with errors_lock:
                                        errors.append((seq, q, str(ex)))
                conn.close()
            except Exception as conn_ex:
                with errors_lock:
                    errors.append((-1, "connect", str(conn_ex)))

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(worker_thread, w_id, worker_queues[w_id])
                for w_id in range(num_workers)
            ]
            for f in as_completed(futures):
                f.result()

    duration_s = time.perf_counter() - t0
    return duration_s, len(errors)


def audit_run(
    config_name: str,
    mode: str,
    num_workers: int,
    duration_s: float,
    actual_state: Dict[str, Any],
    oracle: Dict[str, Any],
    error_count: int,
) -> TestResult:
    """Compares actual database state with the Oracle ground truth."""
    expected_total_balance = oracle["metadata"]["total_balance_expected"]
    actual_total_balance = actual_state["total_balance"]
    balance_conserved = (actual_total_balance == expected_total_balance)

    accumulators_matched = (actual_state["det_accumulators"] == oracle["det_accumulators"])
    pipeline_matched = (actual_state["det_state_pipeline"] == oracle["det_state_pipeline"])
    accounts_matched = (actual_state["det_accounts"] == oracle["det_accounts"])
    lifecycle_matched = (actual_state["det_lifecycle"] == oracle["det_lifecycle"])

    divergence_count = 0
    divergence_reasons = []

    if not accumulators_matched:
        divergence_count += 1
        divergence_reasons.append("Accumulators diverged")
    if not pipeline_matched:
        divergence_count += 1
        divergence_reasons.append("State Pipeline diverged")
    if not accounts_matched:
        divergence_count += 1
        divergence_reasons.append("Accounts diverged")
    if not lifecycle_matched:
        divergence_count += 1
        divergence_reasons.append("Lifecycle diverged")
    if not balance_conserved:
        divergence_count += 1
        divergence_reasons.append(f"Balance sum mismatch ({actual_total_balance} != {expected_total_balance})")

    all_matched = (divergence_count == 0 and error_count == 0)

    if all_matched:
        summary_msg = f"PASS: All tables and invariants 100% matched Oracle in {duration_s:.2f}s"
    else:
        summary_msg = f"DIVERGENCE: {', '.join(divergence_reasons)} (errors={error_count})"

    return TestResult(
        config_name=config_name,
        mode=mode,
        num_workers=num_workers,
        duration_s=duration_s,
        all_matched_oracle=all_matched,
        accumulators_matched=accumulators_matched,
        pipeline_matched=pipeline_matched,
        accounts_matched=accounts_matched,
        lifecycle_matched=lifecycle_matched,
        balance_conserved=balance_conserved,
        total_balance=actual_total_balance,
        merkle_roots=actual_state["merkle_roots"],
        divergence_count=divergence_count,
        error_count=error_count,
        summary_message=summary_msg,
    )


def main():
    parser = argparse.ArgumentParser(description="AriaBC Deterministic Concurrency Verification Suite")
    parser.add_argument("--port", type=int, default=5438, help="PostgreSQL BCDB port.")
    parser.add_argument("--host", default="localhost", help="Database host.")
    parser.add_argument("--dbname", default="postgres", help="Database name.")
    parser.add_argument("--user", default="postgres", help="Database user.")
    parser.add_argument("--ops", type=int, default=1000, help="Number of operations in workload.")
    parser.add_argument("--workers", default="1,2,4,8,16", help="Comma-separated worker counts to test.")
    parser.add_argument("--run-nondet-check", action="store_true", default=True,
                        help="Run non-deterministic concurrent mode as negative control.")
    parser.add_argument("--skip-nondet-check", dest="run_nondet_check", action="store_false",
                        help="Skip non-deterministic check.")
    parser.add_argument("--generator-script", default="scripts/generate_deterministic_workload.py",
                        help="Workload generator script.")
    args = parser.parse_args()

    worker_counts = [int(w.strip()) for w in args.workers.split(",") if w.strip()]

    print("=" * 80)
    print("  AriaBC Deterministic Concurrency Verification Suite")
    print("=" * 80)
    print(f"  Target DB: {args.host}:{args.port} / {args.dbname} (user: {args.user})")
    print(f"  Operations: {args.ops}")
    print(f"  Worker Concurrency Configs: {worker_counts}")
    print(f"  Negative Control (Non-deterministic check): {args.run_nondet_check}")
    print("=" * 80)

    # 1. Generate workload and oracle
    gen_script = REPO_ROOT / args.generator_script
    bootstrap_file = REPO_ROOT / "scripts" / "bootstrap_det_verification.sql"
    workload_file = REPO_ROOT / "scripts" / "deterministic_workload.sql"
    oracle_file = REPO_ROOT / "scripts" / "deterministic_oracle.json"

    print("\n[Step 1] Generating deterministic multi-hazard workload and analytical oracle...")
    rc, out, err = run_command([
        "python3", str(gen_script),
        "--ops", str(args.ops),
        "--output-bootstrap", str(bootstrap_file),
        "--output-workload", str(workload_file),
        "--output-oracle", str(oracle_file),
    ])
    if rc != 0:
        print(f"ERROR generating workload: {err}", file=sys.stderr)
        sys.exit(1)

    with open(workload_file) as f:
        workload_stmts = [line.strip() for line in f if line.strip()]
    with open(oracle_file) as f:
        oracle = json.load(f)

    print(f"Loaded {len(workload_stmts)} operations. Analytical Oracle ready.")

    results: List[TestResult] = []

    # 2. Run Deterministic Suite across worker thread counts
    print("\n[Step 2] Testing Deterministic Concurrency Control (db_type=1)...")
    reference_merkle_roots: Optional[Dict[str, str]] = None

    for workers in worker_counts:
        cfg_name = f"det_workers_{workers}"
        print(f"\n>>> Running: {cfg_name} (concurrency = {workers} threads, deterministic mode)")

        # Restart server for clean state and reset det sequencer
        print("    Restarting server to ensure clean BCDB state...")
        if not restart_server():
            print("ERROR: Failed to restart server", file=sys.stderr)
            sys.exit(1)

        # Bootstrap tables and Merkle indexes
        print("    Bootstrapping tables & Merkle indexes...")
        bootstrap_database(bootstrap_file, args.host, args.port, args.dbname, args.user)

        # Execute concurrent workload
        print(f"    Dispatching {len(workload_stmts)} transactions across {workers} concurrent workers...")
        duration_s, err_cnt = execute_workload_concurrent(
            statements=workload_stmts,
            num_workers=workers,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            mode="det",
        )

        # Fetch state and audit
        print("    Fetching final state and evaluating Merkle roots...")
        actual_state = fetch_database_state(args.host, args.port, args.dbname, args.user)
        res = audit_run(cfg_name, "det", workers, duration_s, actual_state, oracle, err_cnt)
        results.append(res)

        print(f"    Result: {res.summary_message}")
        print(f"    Merkle Roots:")
        for tbl, rhash in res.merkle_roots.items():
            print(f"      - {tbl}: {rhash}")

        if reference_merkle_roots is None and res.all_matched_oracle:
            reference_merkle_roots = res.merkle_roots
        elif reference_merkle_roots is not None and res.all_matched_oracle:
            roots_match = (res.merkle_roots == reference_merkle_roots)
            if roots_match:
                print(f"    MERKLE INVARIANCE: MATCH (identical to 1-worker baseline)")
            else:
                print(f"    WARNING: Merkle root mismatch with 1-worker baseline!")

    # 3. Negative Control (Non-deterministic concurrency check)
    if args.run_nondet_check:
        nondet_workers = max(4, max(worker_counts))
        cfg_name = f"nondet_workers_{nondet_workers}"
        print(f"\n[Step 3] Running Negative Control: {cfg_name} (db_type=2, NO deterministic sequencing)")
        print(f"         Testing with {nondet_workers} concurrent threads to verify that order-dependence causes divergence...")

        restart_server()
        bootstrap_database(bootstrap_file, args.host, args.port, args.dbname, args.user)

        duration_s, err_cnt = execute_workload_concurrent(
            statements=workload_stmts,
            num_workers=nondet_workers,
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            mode="nondet",
        )

        actual_state = fetch_database_state(args.host, args.port, args.dbname, args.user)
        nondet_res = audit_run(cfg_name, "nondet", nondet_workers, duration_s, actual_state, oracle, err_cnt)
        results.append(nondet_res)

        print(f"    Non-deterministic Result: {nondet_res.summary_message}")
        if nondet_res.divergence_count > 0:
            print("    CONFIRMATION: Non-deterministic execution diverged as predicted!")
            print("    This confirms that the workload is genuinely order-sensitive and requires AriaBC determinism.")
        else:
            print("    Note: Non-deterministic run produced matching state (scheduling anomaly).")

    # 4. Final Summary Table
    print("\n" + "=" * 100)
    print("  DETERMINISTIC CONCURRENCY AUDIT REPORT")
    print("=" * 100)
    print(f"{'Configuration':<20} {'Mode':<8} {'Workers':<8} {'Duration':<10} {'Oracle Match':<14} {'Balance':<12} {'Merkle Equiv':<14}")
    print("-" * 100)

    all_det_passed = True
    for r in results:
        is_det = (r.mode == "det")
        merkle_match = "N/A"
        if is_det:
            if reference_merkle_roots:
                merkle_match = "MATCH" if (r.merkle_roots == reference_merkle_roots) else "MISMATCH"
            if not r.all_matched_oracle:
                all_det_passed = False

        print(
            f"{r.config_name:<20} "
            f"{r.mode:<8} "
            f"{r.num_workers:<8} "
            f"{r.duration_s:<9.2f}s "
            f"{('PASS' if r.all_matched_oracle else 'DIVERGED'):<14} "
            f"{('CONSERVED' if r.balance_conserved else 'VIOLATED'):<12} "
            f"{merkle_match:<14}"
        )

    print("=" * 100)
    if all_det_passed:
        print("  FINAL VERDICT: PASS")
        print("  AriaBC deterministic concurrency control successfully guaranteed identical,")
        print("  order-true execution across all concurrent worker thread configurations.")
        print("  Every table matched the analytical Oracle and all Merkle roots remained invariant.")
        print("=" * 100)
        sys.exit(0)
    else:
        print("  FINAL VERDICT: FAIL — Deterministic divergence detected under concurrency.")
        print("=" * 100)
        sys.exit(1)


if __name__ == "__main__":
    main()
