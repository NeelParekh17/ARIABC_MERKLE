#!/usr/bin/env python3
"""External Phase 6 Corruption Trigger & Recovery Watcher.

Monitors the cluster execution, waits for Phase 6 (active workload on ariabc_pg_gateway),
injects controlled corruption into a specified replica while transactions are running,
and tracks the gateway's background compare_states auto-recovery in real-time.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from typing import Optional, Tuple

try:
    from .compare_states import parse_node_arg
    from .fault_injector import inject_fault
    from .remote_db import NodeConnection
except (ImportError, ValueError):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from compare_states import parse_node_arg
    from fault_injector import inject_fault
    from remote_db import NodeConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [corrupt_phase6] %(message)s",
)
logger = logging.getLogger("corrupt_phase6")

# Cluster node defaults
DEFAULT_NODES = {
    "node1": "10.129.148.247:5438",
    "admin123": "10.129.148.247:5438",
    "node2": "10.129.148.246:5438",
    "user4": "10.129.148.246:5438",
    "node4": "10.129.148.248:5438",
    "utkarsh": "10.129.148.248:5438",
}


def resolve_node_endpoint(node_input: str) -> Tuple[str, str, int]:
    """Resolve node identifier (name or host:port) into (node_name, host, port)."""
    node_input = node_input.strip()
    if node_input in DEFAULT_NODES:
        endpoint = DEFAULT_NODES[node_input]
        host, port = endpoint.split(":")
        return node_input, host, int(port)

    node_id, host, port = parse_node_arg(node_input)
    return node_id, host, port


def is_gateway_running(gateway_host: str = "10.129.27.111") -> Tuple[bool, Optional[str]]:
    """Check if ariabc_pg_gateway is currently running."""
    # Check if running locally
    try:
        res = subprocess.run(
            ["pidof", "ariabc_pg_gateway"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=2,
        )
        if res.returncode == 0 and res.stdout.strip():
            pids = res.stdout.strip().split()
            return True, pids[0]
    except Exception:
        pass

    # Check on gateway host via ssh if remote
    if gateway_host and gateway_host not in ("127.0.0.1", "localhost"):
        try:
            cmd = [
                "ssh",
                "-o", "BatchMode=yes",
                "-o", "ConnectTimeout=3",
                f"neel@{gateway_host}",
                "pidof ariabc_pg_gateway || true",
            ]
            res = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=4,
            )
            out = res.stdout.strip()
            if out:
                return True, out.split()[0]
        except Exception:
            pass

    return False, None


def wait_for_phase6(gateway_host: str, timeout_sec: float = 300.0) -> str:
    """Block until Phase 6 gateway starts running."""
    logger.info("Monitoring for Phase 6 workload start (gateway_host=%s)...", gateway_host)
    t0 = time.time()
    last_log = 0.0
    while time.time() - t0 < timeout_sec:
        running, pid = is_gateway_running(gateway_host)
        if running:
            logger.info(">>> Phase 6 is ACTIVE! Found running ariabc_pg_gateway (PID %s)", pid)
            return str(pid)
        now = time.time()
        if now - last_log > 5.0:
            logger.info("Still waiting for Phase 6 to begin... (elapsed %.1fs)", now - t0)
            last_log = now
        time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for Phase 6 after {timeout_sec}s")


def watch_recovery(
    target_host: str,
    target_port: int,
    ref_host: str,
    ref_port: int,
    table: str,
    db_user: str = "postgres",
    db_name: str = "postgres",
    db_password: Optional[str] = None,
    timeout_sec: float = 60.0,
) -> bool:
    """Watch the target node until its root hash matches the reference node."""
    target_node = NodeConnection("target", target_host, target_port, dbname=db_name, user=db_user, password=db_password)
    ref_node = NodeConnection("ref", ref_host, ref_port, dbname=db_name, user=db_user, password=db_password)

    try:
        ref_root = ref_node.get_merkle_root_hash(table)
        logger.info("Reference node root hash: %s", ref_root)
        t0 = time.time()
        poll_count = 0

        while time.time() - t0 < timeout_sec:
            poll_count += 1
            cur_root = target_node.get_merkle_root_hash(table)
            ref_root = ref_node.get_merkle_root_hash(table)
            if cur_root and ref_root and cur_root == ref_root:
                dur_ms = (time.time() - t0) * 1000.0
                logger.info(
                    "*** HEALED! Target node root hash matches reference quorum (%s) in %.2f ms (polls: %d)! ***",
                    cur_root,
                    dur_ms,
                    poll_count,
                )
                return True
            time.sleep(0.1)

        dur_ms = (time.time() - t0) * 1000.0
        logger.warning("Target node did not heal within %.2fs (last_root=%s vs ref=%s)", timeout_sec, cur_root, ref_root)
        return False
    finally:
        target_node.close()
        ref_node.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Trigger corruption during active Phase 6 workload and watch compare_states auto-recovery"
    )
    parser.add_argument(
        "--target-node",
        "-t",
        default="utkarsh",
        help="Target node to corrupt (e.g. 'utkarsh', 'user4', 'admin123', or 'host:port'). Default: utkarsh",
    )
    parser.add_argument(
        "--reference-node",
        "-r",
        default="admin123",
        help="Reference healthy node for verification (default: admin123)",
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=300,
        help="Number of tuples to corrupt (default: 300)",
    )
    parser.add_argument(
        "--fault-type",
        choices=["update", "delete", "insert", "mixed"],
        default="update",
        help="Fault type to inject (default: update)",
    )
    parser.add_argument(
        "--table",
        default="usertable_small",
        help="Target table with Merkle index (default: usertable_small)",
    )
    parser.add_argument(
        "--delay-sec",
        type=float,
        default=3.0,
        help="Delay in seconds after Phase 6 starts before injecting corruption (default: 3.0)",
    )
    parser.add_argument(
        "--gateway-host",
        default="10.129.27.111",
        help="Host where ariabc_pg_gateway runs (default: 10.129.27.111)",
    )
    parser.add_argument(
        "--immediate",
        action="store_true",
        help="Inject corruption immediately without waiting for Phase 6 start",
    )
    parser.add_argument(
        "--watch-timeout",
        type=float,
        default=60.0,
        help="Timeout in seconds to watch for auto-recovery healing (default: 60.0)",
    )
    parser.add_argument("--db-user", default="postgres", help="PostgreSQL user (default: postgres)")
    parser.add_argument("--db-name", default="postgres", help="PostgreSQL database name (default: postgres)")
    parser.add_argument("--db-password", default=None, help="PostgreSQL password")

    args = parser.parse_args()

    t_name, t_host, t_port = resolve_node_endpoint(args.target_node)
    r_name, r_host, r_port = resolve_node_endpoint(args.reference_node)

    logger.info("=== External Phase 6 Corruption Tool ===")
    logger.info("Target node:    %s (%s:%d)", t_name, t_host, t_port)
    logger.info("Reference node: %s (%s:%d)", r_name, r_host, r_port)
    logger.info("Table:          %s", args.table)
    logger.info("Corruption:     %d tuples (%s)", args.count, args.fault_type)

    if not args.immediate:
        try:
            wait_for_phase6(args.gateway_host)
        except KeyboardInterrupt:
            logger.warning("Cancelled waiting for Phase 6.")
            return 130

        if args.delay_sec > 0:
            logger.info("Workload running. Waiting %.2fs delay before fault injection...", args.delay_sec)
            time.sleep(args.delay_sec)
    else:
        logger.info("Immediate flag set: skipping Phase 6 wait.")

    logger.info(">>> INJECTING CORRUPTION: Corrupting %d tuples on %s (%s:%d)...", args.count, t_name, t_host, t_port)
    ok = inject_fault(
        host=t_host,
        port=t_port,
        table=args.table,
        fault_type=args.fault_type,
        num_rows=args.count,
        db_user=args.db_user,
        db_name=args.db_name,
        db_password=args.db_password,
    )
    if not ok:
        logger.error("Failed to inject corruption into %s", t_name)
        return 1

    logger.info("Corruption successfully injected! Monitoring online auto-recovery...")
    healed = watch_recovery(
        target_host=t_host,
        target_port=t_port,
        ref_host=r_host,
        ref_port=r_port,
        table=args.table,
        db_user=args.db_user,
        db_name=args.db_name,
        db_password=args.db_password,
        timeout_sec=args.watch_timeout,
    )

    if healed:
        logger.info("=== Phase 6 Online Recovery Demonstration: COMPLETE (PASS) ===")
        return 0
    else:
        logger.error("=== Phase 6 Online Recovery Demonstration: FAILED (TIMEOUT) ===")
        return 2


if __name__ == "__main__":
    sys.exit(main())
