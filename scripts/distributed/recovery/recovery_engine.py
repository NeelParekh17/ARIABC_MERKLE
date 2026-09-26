"""ProtectDB Algorithm 2 (Section 5.3) Dynamic Multi-Table Recovery Engine."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from .remote_db import NodeConnection
except (ImportError, ValueError):
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from remote_db import NodeConnection

logger = logging.getLogger("recovery.engine")


class OnlineRecoveryEngine:
    """Implements non-blocking online recovery with on-the-fly table and schema discovery."""

    def __init__(
        self,
        damaged_node: NodeConnection,
        reference_node: NodeConnection,
        table: Optional[str] = None,
        batch_size: int = 500,
    ):
        self.damaged_node = damaged_node
        self.reference_node = reference_node
        self.table = table  # None or "auto" means discover all tables on the fly
        self.batch_size = batch_size

    def run_recovery(self) -> Dict[str, Any]:
        """Execute recovery for all divergent tables discovered dynamically."""
        start_time = time.perf_counter()
        stats: Dict[str, Any] = {
            "status": "FAILED",
            "damaged_node_id": self.damaged_node.node_id,
            "reference_node_id": self.reference_node.node_id,
            "tables_recovered": [],
            "total_rows_updated": 0,
            "total_rows_inserted": 0,
            "total_rows_deleted": 0,
            "discovery_ms": 0.0,
            "total_ms": 0.0,
        }

        try:
            # -------------------------------------------------------------
            # Phase 1: Target Table Selection / On-the-Fly Discovery
            # -------------------------------------------------------------
            tables_to_repair: List[str] = []
            if self.table and self.table != "auto":
                tables_to_repair = [self.table]
                logger.info("Target table explicitly specified: %s (skipping relation discovery)", tables_to_repair)
            else:
                t_disc_0 = time.perf_counter()
                ref_tables = self.reference_node.discover_merkle_tables()
                dmg_tables = self.damaged_node.discover_merkle_tables()
                stats["discovery_ms"] = (time.perf_counter() - t_disc_0) * 1000.0

                all_table_names = sorted(set(ref_tables.keys()) | set(dmg_tables.keys()))
                logger.info("Discovered %d Merkle relations across replicas: %s", len(all_table_names), all_table_names)

                for tbl in all_table_names:
                    try:
                        ref_h = self.reference_node.get_merkle_root_hash(tbl)
                    except Exception as e:
                        logger.warning("Error fetching ref root hash for %s: %s", tbl, e)
                        ref_h = ""
                    try:
                        dmg_h = self.damaged_node.get_merkle_root_hash(tbl)
                    except Exception as e:
                        logger.warning("Error fetching dmg root hash for %s: %s", tbl, e)
                        dmg_h = ""
                    if ref_h != dmg_h or not ref_h:
                        logger.warning("Table '%s' root hash diverged: ref=%s vs dmg=%s", tbl, ref_h, dmg_h)
                        tables_to_repair.append(tbl)

            if not tables_to_repair:
                logger.info("All discovered Merkle tables have matching root hashes. No repair needed.")
                stats["status"] = "PASS"
                stats["total_ms"] = (time.perf_counter() - start_time) * 1000.0
                return stats

            # -------------------------------------------------------------
            # Phase 2: Recover each divergent table via Algorithm 2
            # -------------------------------------------------------------
            overall_pass = True
            for tbl in tables_to_repair:
                table_stats = self._recover_single_table(tbl)
                stats["tables_recovered"].append(table_stats)
                stats["total_rows_updated"] += table_stats.get("rows_updated", 0)
                stats["total_rows_inserted"] += table_stats.get("rows_inserted", 0)
                stats["total_rows_deleted"] += table_stats.get("rows_deleted", 0)
                if table_stats.get("status") != "PASS":
                    overall_pass = False

            stats["status"] = "PASS" if overall_pass else "FAIL"

        except Exception as e:
            logger.exception("Exception during dynamic recovery: %s", e)
            stats["status"] = "ERROR"
            stats["error"] = str(e)

        finally:
            stats["total_ms"] = (time.perf_counter() - start_time) * 1000.0

        return stats

    def _recover_single_table(self, table_name: str) -> Dict[str, Any]:
        """Execute Algorithm 2 for a single dynamically introspected table."""
        t_start = time.perf_counter()
        t_stats: Dict[str, Any] = {
            "table": table_name,
            "status": "FAILED",
            "mismatched_partitions": [],
            "rows_updated": 0,
            "rows_inserted": 0,
            "rows_deleted": 0,
            "localisation_ms": 0.0,
            "diff_ms": 0.0,
            "dml_ms": 0.0,
            "verify_ms": 0.0,
            "total_ms": 0.0,
        }

        # Dynamically introspect schema and primary keys
        schema = self.reference_node.introspect_table_schema(table_name)
        pk_cols = schema["pk_cols"]
        all_cols = schema["all_cols"]
        data_cols = schema["data_cols"]
        partitions_total = schema["partitions"]

        logger.info(
            "Introspected table '%s': PK=%s, data_columns=%d, partitions=%d",
            table_name,
            pk_cols,
            len(data_cols),
            partitions_total,
        )

        # 1. Localisation via partition root hashes
        t_loc_0 = time.perf_counter()
        ref_parts = self.reference_node.get_partition_root_hashes(table_name)
        dmg_parts = self.damaged_node.get_partition_root_hashes(table_name)
        t_stats["localisation_ms"] = (time.perf_counter() - t_loc_0) * 1000.0

        all_parts = sorted(set(ref_parts.keys()) | set(dmg_parts.keys()))
        mismatched = [p for p in all_parts if ref_parts.get(p) != dmg_parts.get(p)]
        t_stats["mismatched_partitions"] = mismatched
        logger.info("Table '%s' localisation: %d mismatched partitions (%s)", table_name, len(mismatched), mismatched)

        if not mismatched:
            t_stats["status"] = "PASS"
            t_stats["total_ms"] = (time.perf_counter() - t_start) * 1000.0
            return t_stats

        # 2. Candidate fetch and row diffing with leaf-level descent
        t_diff_0 = time.perf_counter()
        all_inserts: List[Dict[str, Any]] = []
        all_updates: List[Dict[str, Any]] = []
        all_deletes: List[Tuple[Any, ...]] = []

        for part_id in mismatched:
            ref_leaves = self.reference_node.get_partition_leaf_nodes(table_name, part_id)
            dmg_leaves = self.damaged_node.get_partition_leaf_nodes(table_name, part_id)
            all_leaf_keys = sorted(set(ref_leaves.keys()) | set(dmg_leaves.keys()))
            differing_leaves = [
                k for k in all_leaf_keys if ref_leaves.get(k) != dmg_leaves.get(k)
            ]

            if differing_leaves:
                logger.info(
                    "Partition %d localized down to %d differing leaves (out of %d total leaves)",
                    part_id,
                    len(differing_leaves),
                    len(all_leaf_keys),
                )
                for (p_id, node_id, prefix_len) in differing_leaves:
                    hrows = self.reference_node.fetch_leaf_bounded_rows(
                        table_name, p_id, partitions_total, node_id, prefix_len, pk_cols, all_cols
                    )
                    drows = self.damaged_node.fetch_leaf_bounded_rows(
                        table_name, p_id, partitions_total, node_id, prefix_len, pk_cols, all_cols
                    )
                    hkeys = set(hrows.keys())
                    dkeys = set(drows.keys())
                    for k in sorted(hkeys - dkeys):
                        all_inserts.append(hrows[k])
                    for k in sorted(dkeys - hkeys):
                        all_deletes.append(k)
                    for k in sorted(hkeys & dkeys):
                        if hrows[k] != drows[k]:
                            all_updates.append(hrows[k])
            else:
                # Fallback to partition-wide fetch if dedicated leaf table has unsplit root or not available
                hrows = self.reference_node.fetch_partition_rows(
                    table_name, part_id, partitions_total, pk_cols, all_cols
                )
                drows = self.damaged_node.fetch_partition_rows(
                    table_name, part_id, partitions_total, pk_cols, all_cols
                )
                hkeys = set(hrows.keys())
                dkeys = set(drows.keys())
                for k in sorted(hkeys - dkeys):
                    all_inserts.append(hrows[k])
                for k in sorted(dkeys - hkeys):
                    all_deletes.append(k)
                for k in sorted(hkeys & dkeys):
                    if hrows[k] != drows[k]:
                        all_updates.append(hrows[k])

        t_stats["diff_ms"] = (time.perf_counter() - t_diff_0) * 1000.0

        # 3. Direct Targeted Repair DML on Damaged Node (Consolidated Transaction Block)
        # per Dynamic_merkle_docs/repair_write_optimisation.md Layer 1 Consolidation
        t_dml_0 = time.perf_counter()
        if all_deletes or all_updates or all_inserts:
            with self.damaged_node.conn.transaction():
                self.damaged_node.execute("SET LOCAL synchronous_commit = off")
                if all_deletes:
                    t_stats["rows_deleted"] = self.damaged_node.repair_delete_keys(
                        table_name, pk_cols, all_deletes, self.batch_size
                    )
                if all_updates:
                    t_stats["rows_updated"] = self.damaged_node.repair_update_rows(
                        table_name, pk_cols, data_cols, all_updates, self.batch_size
                    )
                if all_inserts:
                    t_stats["rows_inserted"] = self.damaged_node.repair_insert_rows(
                        table_name, all_cols, all_inserts, self.batch_size, pk_cols=pk_cols
                    )
        t_stats["dml_ms"] = (time.perf_counter() - t_dml_0) * 1000.0

        # 4. Post-Repair Confirmation
        t_ver_0 = time.perf_counter()
        post_dmg_parts = self.damaged_node.get_partition_root_hashes(table_name)
        t_stats["verify_ms"] = (time.perf_counter() - t_ver_0) * 1000.0

        remaining = [p for p in mismatched if ref_parts.get(p) != post_dmg_parts.get(p)]
        if not remaining:
            t_stats["status"] = "PASS"
            logger.info("Table '%s' recovery SUCCEEDED in %.2f ms", table_name, (time.perf_counter() - t_start) * 1000.0)
        else:
            t_stats["status"] = "FAIL"
            logger.error("Table '%s' verification FAILED: partitions %s still mismatch", table_name, remaining)

        t_stats["total_ms"] = (time.perf_counter() - t_start) * 1000.0
        return t_stats


def run_recovery(
    damaged_host: str,
    damaged_port: int,
    reference_host: str,
    reference_port: int,
    table: Optional[str] = None,
    db_user: str = "postgres",
    db_name: str = "postgres",
    db_password: Optional[str] = None,
    damaged_node_id: str = "damaged",
    reference_node_id: str = "reference",
) -> Dict[str, Any]:
    """Helper entry point for triggering recovery with connection parameters."""
    damaged_conn = NodeConnection(
        node_id=damaged_node_id,
        host=damaged_host,
        port=damaged_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    reference_conn = NodeConnection(
        node_id=reference_node_id,
        host=reference_host,
        port=reference_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )

    try:
        engine = OnlineRecoveryEngine(
            damaged_node=damaged_conn,
            reference_node=reference_conn,
            table=table,
        )
        return engine.run_recovery()
    finally:
        damaged_conn.close()
        reference_conn.close()
