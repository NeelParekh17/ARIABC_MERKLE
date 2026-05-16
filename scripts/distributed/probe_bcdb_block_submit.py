#!/usr/bin/env python3
"""Measure the BCDB queued block-submit path on one PostgreSQL node.

This intentionally uses the same server-side API as ariabc_pg_server:
SELECT bcdb_block_submit_results(<json block>);
It is not a replacement for the trusted 4-node benchmark; it isolates whether
the queued block path itself is already slower than direct "s <seq> <sql>" DET.
"""

import argparse
import json
import statistics
import subprocess
import time
from pathlib import Path

import psycopg


def load_workload(path: Path, limit: int) -> list[str]:
    out: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("--") or s.startswith("/*") or s.startswith("\\"):
                continue
            out.append(s)
            if limit > 0 and len(out) >= limit:
                break
    return out


def run_psql_file(args: argparse.Namespace, sql_file: Path) -> None:
    argv = [
        args.psql,
        "-X",
        "-q",
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-U",
        args.user,
        "-d",
        args.db,
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        str(sql_file),
    ]
    subprocess.run(argv, check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5438)
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--db", default="postgres")
    ap.add_argument("--psql", default="/work/ARIABC/install/bin/psql")
    ap.add_argument("--workload", default="scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt")
    ap.add_argument("--restore-sql", default="scripts/restore_usertable_small.sql")
    ap.add_argument("--skip-restore", action="store_true")
    ap.add_argument("--workers", type=int, default=256)
    ap.add_argument("--block-size", type=int, default=256)
    ap.add_argument("--limit", type=int, default=20513)
    ap.add_argument("--key-base", type=int, default=70000000)
    args = ap.parse_args()

    workload = load_workload(Path(args.workload), args.limit)
    if not workload:
        raise SystemExit("empty workload")

    if not args.skip_restore:
        run_psql_file(args, Path(args.restore_sql))

    conninfo = f"host={args.host} port={args.port} dbname={args.db} user={args.user}"
    block_ms: list[float] = []
    total_rows = 0
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bcdb_init(True, %s);", (args.workers,))
            cur.fetchone()

            t0 = time.perf_counter()
            bid = 1
            for off in range(0, len(workload), args.block_size):
                txs = []
                chunk = workload[off : off + args.block_size]
                for i, sql in enumerate(chunk):
                    txs.append({
                        "hash": str(args.key_base + bid * 1024 + i),
                        "sql": sql,
                    })
                payload = json.dumps({"bid": bid, "txs": txs}, separators=(",", ":"))
                b0 = time.perf_counter()
                cur.execute("SELECT bcdb_block_submit_results(%s);", (payload,))
                cur.fetchone()
                block_ms.append((time.perf_counter() - b0) * 1000.0)
                total_rows += len(chunk)
                bid += 1
            total_ms = (time.perf_counter() - t0) * 1000.0

            cur.execute("SELECT count(*), merkle_root_hash('usertable_small'), merkle_verify('usertable_small') FROM usertable_small;")
            count, root, verify = cur.fetchone()

    p95 = statistics.quantiles(block_ms, n=20)[18] if len(block_ms) >= 20 else max(block_ms)
    print(f"blocks={len(block_ms)} txs={total_rows} workers={args.workers} block_size={args.block_size}")
    print(f"overall_ms={total_ms:.3f} tps={total_rows / (total_ms / 1000.0):.2f}")
    print(f"block_avg_ms={statistics.mean(block_ms):.3f} block_p95_ms={p95:.3f} block_max_ms={max(block_ms):.3f}")
    print(f"rows={count} root={root} verify={verify}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
