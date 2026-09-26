#!/usr/bin/env python3
"""Active Recovery Mode: In-band per-transaction divergence handler with dynamic table discovery.

Directly invoked when the Gateway VoteStore detects divergent execution hashes on transaction completion.
Dynamically isolates which Merkle relations diverged between the damaged node and reference node without
requiring static table configuration.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Tuple

try:
    from .compare_states import parse_node_arg
    from .recovery_engine import run_recovery
except (ImportError, ValueError):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from compare_states import parse_node_arg
    from recovery_engine import run_recovery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [active_recovery] %(message)s",
)
logger = logging.getLogger("active_recovery")


def main() -> int:
    parser = argparse.ArgumentParser(description="Active Mode: In-band recovery hook triggered on tx divergence")
    parser.add_argument(
        "--damaged-node",
        required=True,
        help="Endpoint of damaged node (e.g. '10.0.0.3:5432' or 'node3=10.0.0.3:5432')",
    )
    parser.add_argument(
        "--reference-node",
        required=True,
        help="Endpoint of healthy reference node (e.g. '10.0.0.1:5432' or 'node1=10.0.0.1:5432')",
    )
    parser.add_argument(
        "--req-num",
        type=int,
        default=0,
        help="Request or deterministic sequence number where divergence was detected",
    )
    parser.add_argument(
        "--table",
        default="auto",
        help="Target table or 'auto' to discover divergent table(s) dynamically (default: auto)",
    )
    parser.add_argument("--db-user", default="postgres", help="Database username")
    parser.add_argument("--db-name", default="postgres", help="Database name")
    parser.add_argument("--db-password", default=None, help="Database password")

    args = parser.parse_args()

    dmg_id, dmg_host, dmg_port = parse_node_arg(args.damaged_node)
    ref_id, ref_host, ref_port = parse_node_arg(args.reference_node)

    logger.info(
        "Active Mode Triggered: req_num=%d, damaged_node=%s (%s:%d), reference_node=%s (%s:%d), table=%s",
        args.req_num,
        dmg_id,
        dmg_host,
        dmg_port,
        ref_id,
        ref_host,
        ref_port,
        args.table,
    )

    res = run_recovery(
        damaged_host=dmg_host,
        damaged_port=dmg_port,
        reference_host=ref_host,
        reference_port=ref_port,
        table=None if args.table == "auto" else args.table,
        db_user=args.db_user,
        db_name=args.db_name,
        db_password=args.db_password,
        damaged_node_id=dmg_id,
        reference_node_id=ref_id,
    )

    logger.info("Active Recovery Result: %s", json.dumps(res, indent=2))

    return 0 if res.get("status") == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
