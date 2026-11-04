#!/usr/bin/env python3
"""Fault Injector for testing online Merkle recovery.

Injects controlled data corruption (UPDATE, INSERT, or DELETE) into a specific replica's
database to simulate silent bit-rot, memory corruption, or Byzantine faults.
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from typing import Optional

try:
    from .compare_states import parse_node_arg
    from .remote_db import NodeConnection
except (ImportError, ValueError):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from compare_states import parse_node_arg
    from remote_db import NodeConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [fault_injector] %(message)s",
)
logger = logging.getLogger("fault_injector")


def _random_keys(node, table: str, n: int, exclude=()) -> list:
    """n distinct random existing keys, found through the primary-key index.

    Keys are drawn uniformly from [min, max] and kept if they exist, so a large
    table is never scanned or sorted (ORDER BY random() reads the whole heap:
    minutes on a 100M-row replica, during the measured workload).  Falls back to
    ORDER BY random() only when the key space is too sparse.
    """
    lo = node.scalar(f"SELECT min(ycsb_key) FROM {table}")
    hi = node.scalar(f"SELECT max(ycsb_key) FROM {table}")
    if lo is None or hi is None:
        return []
    lo, hi = int(lo), int(hi)
    found = []
    seen = set(int(k) for k in exclude)
    for _ in range(20):
        need = n - len(found)
        if need <= 0:
            break
        cand = list({random.randint(lo, hi) for _ in range(need * 4 + 8)} - seen)
        rows = node.execute(f"SELECT ycsb_key FROM {table} WHERE ycsb_key = ANY(%s)", (cand,))
        for r in rows:
            k = int(r["ycsb_key"])
            if k not in seen and len(found) < n:
                found.append(k)
                seen.add(k)
    if len(found) < n:
        rows = node.execute(
            f"SELECT ycsb_key FROM {table} WHERE ycsb_key != ALL(%s) ORDER BY random() LIMIT %s",
            (list(seen), n - len(found)),
        )
        found.extend(int(r["ycsb_key"]) for r in rows)
    return found


def inject_fault(
    host: str,
    port: int,
    table: str = "usertable_small",
    fault_type: str = "mixed",
    key: Optional[int] = None,
    num_rows: int = 1,
    db_user: str = "postgres",
    db_name: str = "postgres",
    db_password: Optional[str] = None,
) -> bool:
    """Inject controlled corruption fault(s) into the target replica."""
    node = NodeConnection(
        node_id="target",
        host=host,
        port=port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )

    try:
        cur_root = node.get_merkle_root_hash(table)
        logger.info("Before injection: table '%s' root hash = %s", table, cur_root)

        if num_rows <= 0:
            num_rows = 1

        if fault_type == "mixed":
            n_update = num_rows // 3
            n_delete = num_rows // 3
            n_insert = num_rows - (n_update + n_delete)
        elif fault_type == "update":
            n_update = num_rows
            n_delete = 0
            n_insert = 0
        elif fault_type == "delete":
            n_update = 0
            n_delete = num_rows
            n_insert = 0
        elif fault_type == "insert":
            n_update = 0
            n_delete = 0
            n_insert = num_rows
        else:
            logger.error("Unknown fault type: %s", fault_type)
            return False

        updated_keys = []
        deleted_keys = []
        inserted_keys = []

        import psycopg
        max_attempts = 10
        injection_ok = False
        for attempt in range(1, max_attempts + 1):
            try:
                node.conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
                with node.conn.transaction():
                    # 1. Injected UPDATEs
                    if n_update > 0:
                        if key is not None and num_rows == 1:
                            updated_keys = [key]
                        else:
                            updated_keys = _random_keys(node, table, n_update)
                            if not updated_keys:
                                logger.error("Table '%s' has no rows for UPDATE corruption", table)
                                return False

                        corrupt_val = f"CORRUPTED_{int(time.time())}"
                        logger.info(
                            "Injecting UPDATE corruption on %d tuples (sample key %d, attempt %d)",
                            len(updated_keys),
                            updated_keys[0],
                            attempt,
                        )
                        node.execute(
                            f"UPDATE {table} SET field1 = %s || '_' || ycsb_key || '_' || floor(random()*10000)::text WHERE ycsb_key = ANY(%s)",
                            (corrupt_val, updated_keys),
                        )

                    # 2. Injected DELETEs
                    if n_delete > 0:
                        if key is not None and num_rows == 1 and n_update == 0:
                            deleted_keys = [key]
                        else:
                            deleted_keys = _random_keys(node, table, n_delete, exclude=updated_keys)
                            if not deleted_keys:
                                logger.error("Table '%s' has no rows for DELETE corruption", table)
                                return False

                        logger.info(
                            "Injecting DELETE corruption on %d tuples (sample key %d, attempt %d)",
                            len(deleted_keys),
                            deleted_keys[0],
                            attempt,
                        )
                        node.execute(
                            f"DELETE FROM {table} WHERE ycsb_key = ANY(%s)",
                            (deleted_keys,),
                        )

                    # 3. Injected Spurious INSERTs
                    if n_insert > 0:
                        max_key = node.scalar(f"SELECT coalesce(max(ycsb_key), 0) FROM {table}") or 0
                        start_k = int(max_key) + 900000 + random.randint(1, 1000)
                        end_k = start_k + n_insert - 1
                        inserted_keys = list(range(start_k, end_k + 1))
                        logger.info(
                            "Injecting spurious INSERT corruption on %d tuples (keys %d to %d, attempt %d)",
                            n_insert,
                            start_k,
                            end_k,
                            attempt,
                        )
                        node.execute(
                            f"INSERT INTO {table} (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) "
                            f"SELECT g, 'spurious_' || g, 'spurious', 'spurious', 'spurious', 'spurious', 'spurious', 'spurious', 'spurious', 'spurious', 'spurious' "
                            f"FROM generate_series(%s, %s) AS g",
                            (start_k, end_k),
                        )

                injection_ok = True
                break
            except Exception as e:
                logger.warning("Fault injection attempt %d/%d encountered error: %s", attempt, max_attempts, e)
                if attempt == max_attempts:
                    logger.error("All %d fault injection attempts failed", max_attempts)
                    return False
                time.sleep(0.2 * attempt)

        if not injection_ok:
            return False

        new_root = node.get_merkle_root_hash(table)
        total_corrupted = len(updated_keys) + len(deleted_keys) + len(inserted_keys)
        logger.info(
            "After corruption: table '%s' root hash = %s (changed: %s, total corrupted: %d [upd=%d, del=%d, ins=%d])",
            table,
            new_root,
            new_root != cur_root,
            total_corrupted,
            len(updated_keys),
            len(deleted_keys),
            len(inserted_keys),
        )
        return True

    finally:
        node.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Inject corruption into target replica for recovery testing")
    parser.add_argument(
        "--target-node",
        required=True,
        help="Target node endpoint (e.g. '10.0.0.3:5432' or 'node3=10.0.0.3:5432')",
    )
    parser.add_argument(
        "--table",
        default="usertable_small",
        help="Target table with Merkle index (default: usertable_small)",
    )
    parser.add_argument(
        "--fault-type",
        choices=["update", "delete", "insert", "mixed"],
        default="mixed",
        help="Type of fault to inject: update, delete, insert, or mixed (default: mixed)",
    )
    parser.add_argument(
        "--count",
        "-n",
        "--num-rows",
        dest="count",
        type=int,
        default=1,
        help="Number of tuples to corrupt (default: 1)",
    )
    parser.add_argument(
        "--key",
        type=int,
        default=None,
        help="Specific primary key to corrupt when count=1 (default: random)",
    )
    parser.add_argument(
        "--delay-sec",
        type=float,
        default=0.0,
        help="Optional delay in seconds before injecting fault",
    )
    parser.add_argument("--db-user", default="postgres", help="Database username")
    parser.add_argument("--db-name", default="postgres", help="Database name")
    parser.add_argument("--db-password", default=None, help="Database password")

    args = parser.parse_args()

    _, host, port = parse_node_arg(args.target_node)

    if args.delay_sec > 0:
        logger.info("Waiting %.2f seconds before injecting fault...", args.delay_sec)
        time.sleep(args.delay_sec)

    ok = inject_fault(
        host=host,
        port=port,
        table=args.table,
        fault_type=args.fault_type,
        key=args.key,
        num_rows=args.count,
        db_user=args.db_user,
        db_name=args.db_name,
        db_password=args.db_password,
    )

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
