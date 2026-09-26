#!/usr/bin/env python3
"""Passive Recovery Mode: Periodic comparestates multi-table consistency monitor (Algorithm 2).

Periodically evaluates Merkle root hashes across cluster nodes on all tables dynamically (e.g. every 200ms).
Discovers relations on the fly from PostgreSQL system catalogs (amname='merkle'), supporting both single-table
workloads (YCSB) and multi-table workloads (TPC-C: warehouse, district, customer, stock, orders, etc.).
"""

from __future__ import annotations

import argparse
import collections
import logging
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

try:
    from .recovery_engine import run_recovery
    from .remote_db import NodeConnection
except (ImportError, ValueError):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from recovery_engine import run_recovery
    from remote_db import NodeConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [compare_states] %(message)s",
)
logger = logging.getLogger("compare_states")


def parse_node_arg(node_str: str) -> Tuple[str, str, int]:
    """Parse node spec into (node_id, host, port)."""
    node_str = node_str.strip()
    if "=" in node_str:
        node_id, addr = node_str.split("=", 1)
    else:
        node_id = node_str
        addr = node_str

    if ":" in addr:
        host, port_str = addr.split(":", 1)
        port = int(port_str)
    else:
        host = addr
        port = 5432

    return node_id, host, port


def check_and_recover_once(
    nodes: List[NodeConnection],
    target_table: Optional[str] = None,
    auto_recover: bool = True,
    db_user: str = "postgres",
    db_name: str = "postgres",
    db_password: Optional[str] = None,
) -> bool:
    """Perform one consistency check round across all nodes for all Merkle tables."""
    # Step 1: Determine tables to check
    if target_table and target_table != "auto":
        tables_to_check = [target_table]
    else:
        node_tables: Dict[str, Dict[str, Dict[str, Any]]] = {}
        all_table_names: Set[str] = set()

        for node in nodes:
            try:
                discovered = node.discover_merkle_tables()
                node_tables[node.node_id] = discovered
                all_table_names.update(discovered.keys())
            except Exception as e:
                logger.warning("Failed to discover tables on node %s: %s", node.node_id, e)
                node_tables[node.node_id] = {}

        if node_tables and all(node_tables.values()):
            common_tables = set.intersection(*(set(t.keys()) for t in node_tables.values()))
        else:
            common_tables = set()

        if not common_tables:
            logger.warning("No common Merkle relations found across all nodes.")
            return True

        tables_to_check = sorted(common_tables)

    logger.info("Evaluating Merkle root consistency across %d table(s): %s", len(tables_to_check), tables_to_check)

    all_tables_consistent = True

    for tbl in tables_to_check:
        roots: Dict[str, str] = {}
        for node in nodes:
            try:
                roots[node.node_id] = node.get_merkle_root_hash(tbl)
            except Exception as e:
                roots[node.node_id] = f"ERROR:{e}"

        # Group by hash
        hash_groups: Dict[str, List[str]] = collections.defaultdict(list)
        for node_id, h in roots.items():
            hash_groups[h].append(node_id)

        # Match check
        if len(hash_groups) == 1 and not list(hash_groups.keys())[0].startswith("ERROR"):
            logger.info("Table '%s': PASS (root=%s across all %d nodes)", tbl, list(hash_groups.keys())[0], len(nodes))
            continue

        # Mismatch detected on table tbl
        all_tables_consistent = False
        logger.warning(
            "MISMATCH on table '%s'! Root distribution across %d nodes: %s",
            tbl,
            len(nodes),
            dict(hash_groups),
        )

        # Majority quorum check
        majority_hash = None
        majority_nodes: List[str] = []
        for h, n_list in hash_groups.items():
            if not h.startswith("ERROR") and len(n_list) > len(majority_nodes):
                majority_hash = h
                majority_nodes = n_list

        if not majority_hash or len(majority_nodes) < (len(nodes) // 2):
            logger.error("Table '%s': No majority quorum found among nodes: %s", tbl, roots)
            continue

        ref_node_id = majority_nodes[0]
        ref_node = next(n for n in nodes if n.node_id == ref_node_id)
        damaged_nodes = [n for n in nodes if n.node_id not in majority_nodes]

        logger.info(
            "Table '%s': Quorum root %s on nodes %s. Damaged outlier node(s): %s",
            tbl,
            majority_hash,
            majority_nodes,
            [d.node_id for d in damaged_nodes],
        )

        if not auto_recover:
            continue

        all_recovered = True
        for dmg_node in damaged_nodes:
            logger.info(
                "Triggering Online Recovery for node %s on table '%s' against reference %s...",
                dmg_node.node_id,
                tbl,
                ref_node.node_id,
            )
            res = run_recovery(
                damaged_host=dmg_node.host,
                damaged_port=dmg_node.port,
                reference_host=ref_node.host,
                reference_port=ref_node.port,
                table=tbl,
                db_user=db_user,
                db_name=db_name,
                db_password=db_password,
                damaged_node_id=dmg_node.node_id,
                reference_node_id=ref_node.node_id,
            )
            if res.get("status") == "PASS":
                logger.info("Table '%s' recovery PASS for node %s: %s", tbl, dmg_node.node_id, res)
            else:
                all_recovered = False
                logger.error("Table '%s' recovery FAILED for node %s: %s", tbl, dmg_node.node_id, res)

        if all_recovered:
            all_tables_consistent = True

    return all_tables_consistent


def main() -> int:
    parser = argparse.ArgumentParser(description="Passive Mode: dynamic multi-table comparestates monitor")
    parser.add_argument(
        "--nodes",
        required=True,
        help="Comma-separated node endpoints (e.g. '10.0.0.1:5432,10.0.0.2:5432' or 'node1=10.0.0.1:5432,...')",
    )
    parser.add_argument(
        "--table",
        default="auto",
        help="Target table with Merkle index or 'auto' to discover all tables dynamically (default: auto)",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=200,
        help="Check interval in milliseconds (default: 200ms)",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=0.0,
        help="Total duration to run in seconds (0 = run once or until stopped)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run check exactly once and exit",
    )
    parser.add_argument(
        "--auto-recover",
        action="store_true",
        default=True,
        help="Automatically trigger online recovery on mismatch (default: true)",
    )
    parser.add_argument(
        "--check-only",
        "--no-auto-recover",
        dest="auto_recover",
        action="store_false",
        help="Check consistency only; do not perform recovery",
    )
    parser.add_argument("--db-user", default="postgres", help="Database username")
    parser.add_argument("--db-name", default="postgres", help="Database name")
    parser.add_argument("--db-password", default=None, help="Database password")

    args = parser.parse_args()

    node_specs = [parse_node_arg(s) for s in args.nodes.split(",") if s.strip()]
    if not node_specs:
        logger.error("No valid nodes provided in --nodes")
        return 1

    nodes: List[NodeConnection] = [
        NodeConnection(
            node_id=spec[0],
            host=spec[1],
            port=spec[2],
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
        )
        for spec in node_specs
    ]

    try:
        if args.once or args.duration_sec <= 0:
            ok = check_and_recover_once(
                nodes=nodes,
                target_table=args.table,
                auto_recover=args.auto_recover,
                db_user=args.db_user,
                db_name=args.db_name,
                db_password=args.db_password,
            )
            return 0 if ok else 1

        start_time = time.time()
        interval_sec = max(0.01, args.interval_ms / 1000.0)

        while (time.time() - start_time) < args.duration_sec:
            check_and_recover_once(
                nodes=nodes,
                target_table=args.table,
                auto_recover=args.auto_recover,
                db_user=args.db_user,
                db_name=args.db_name,
                db_password=args.db_password,
            )
            time.sleep(interval_sec)

        return 0

    finally:
        for node in nodes:
            node.close()


if __name__ == "__main__":
    sys.exit(main())
