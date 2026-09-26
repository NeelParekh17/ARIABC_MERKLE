"""Dynamic remote database connection and catalog introspection for online recovery."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("recovery.remote_db")


class NodeConnection:
    """Manages a connection to a specific PostgreSQL replica in the cluster."""

    def __init__(
        self,
        node_id: int | str,
        host: str,
        port: int = 5432,
        dbname: str = "postgres",
        user: str = "postgres",
        password: Optional[str] = None,
        connect_timeout: int = 10,
    ):
        self.node_id = str(node_id)
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self._conn: Optional[psycopg.Connection] = None
        self._schema_cache: Dict[str, Dict[str, Any]] = {}

    def connect(self, autocommit: bool = True) -> psycopg.Connection:
        """Establish connection with dict_row factory and optimal tuning flags."""
        conn_params = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "connect_timeout": self.connect_timeout,
            "autocommit": autocommit,
            "row_factory": dict_row,
        }
        if self.password:
            conn_params["password"] = self.password

        self._conn = psycopg.connect(**conn_params)
        with self._conn.cursor() as cur:
            cur.execute("SET enable_merkle_index = on")
            cur.execute("SET enable_seqscan = off")
            cur.execute("SET max_parallel_workers_per_gather = 0")
            cur.execute("SET default_transaction_isolation = 'read committed'")
        return self._conn

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
            except Exception as e:
                logger.debug("Error closing connection to node %s: %s", self.node_id, e)
            self._conn = None

    def execute(self, sql: str, params: Optional[Tuple[Any, ...] | Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query and return all rows as dicts."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is not None:
                return cur.fetchall()
            return []

    def scalar(self, sql: str, params: Optional[Tuple[Any, ...] | Dict[str, Any]] = None) -> Any:
        """Execute query and return the first scalar value."""
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            return next(iter(row.values()))

    # --- On-the-Fly Dynamic Relation Discovery ---

    def discover_merkle_tables(self) -> Dict[str, Dict[str, Any]]:
        """Discover all tables with Merkle indexes and their current root hashes on the fly."""
        sql = """
        SELECT t.relname AS table_name,
               n.nspname AS schema_name,
               c.relname AS index_name,
               c.oid AS index_oid,
               t.oid AS table_oid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE am.amname = 'merkle'
        ORDER BY n.nspname, t.relname;
        """
        rows = self.execute(sql)
        return {
            r["table_name"]: {
                "table_name": r["table_name"],
                "schema_name": r["schema_name"],
                "index_name": r["index_name"],
                "index_oid": int(r["index_oid"]),
                "table_oid": int(r["table_oid"]),
                "root_hash": "",
            }
            for r in rows
        }

    def introspect_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Dynamically inspect column names, types, primary key(s), and Merkle geometry."""
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        # 1. Primary key columns (supports single and composite primary keys)
        pk_sql = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum);
        """
        pk_rows = self.execute(pk_sql, (table_name,))
        pk_cols = [r["attname"] for r in pk_rows]

        # Fallback if no explicit PK: use first unique index or first column
        if not pk_cols:
            alt_sql = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisunique
            ORDER BY array_position(i.indkey, a.attnum);
            """
            alt_rows = self.execute(alt_sql, (table_name,))
            pk_cols = [r["attname"] for r in alt_rows]

        # 2. All user columns
        col_sql = """
        SELECT attname, atttypid::regtype::text AS data_type
        FROM pg_attribute
        WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum;
        """
        col_rows = self.execute(col_sql, (table_name,))
        all_cols = [r["attname"] for r in col_rows]
        data_cols = [c for c in all_cols if c not in pk_cols]

        # 3. Merkle geometry (partitions, fanout)
        stats_raw = self.scalar(f"SELECT merkle_tree_stats('{table_name}'::regclass)")
        partitions = 200
        if stats_raw:
            try:
                stats_json = json.loads(stats_raw) if isinstance(stats_raw, str) else stats_raw
                partitions = int(stats_json.get("partitions", 200))
            except Exception:
                pass

        schema_info = {
            "table_name": table_name,
            "pk_cols": pk_cols,
            "all_cols": all_cols,
            "data_cols": data_cols,
            "partitions": partitions,
        }
        self._schema_cache[table_name] = schema_info
        return schema_info

    # --- Merkle API Helpers ---

    def get_merkle_root_hash(self, table: str) -> str:
        """Fetch the top-level Merkle root hash for the specified table."""
        # Fast non-blocking path: read partition roots directly from ariabc_internal catalog table and XOR them
        try:
            rows = self.execute(
                f"SELECT hash FROM ariabc_internal.merkle_node_{table} WHERE prefix_len = 0 ORDER BY partition_id"
            )
            if rows:
                xor_hash = bytearray(32)
                for r in rows:
                    h = bytes(r["hash"])
                    for i in range(min(len(xor_hash), len(h))):
                        xor_hash[i] ^= h[i]
                return xor_hash.hex()
        except Exception:
            pass
        # Fallback to C function
        try:
            res = self.scalar(f"SELECT merkle_root_hash('{table}'::regclass)")
            if res is not None:
                return str(res).strip()
        except Exception:
            pass
        return ""

    def get_partition_root_hashes(self, table: str) -> Dict[int, str]:
        """Fetch all partition root hashes as a mapping of partition_id -> hex_hash."""
        # Fast non-blocking path: read partition roots directly from ariabc_internal catalog table
        try:
            rows = self.execute(
                f"SELECT partition_id, encode(hash, 'hex') AS hash FROM ariabc_internal.merkle_node_{table} "
                f"WHERE prefix_len = 0 ORDER BY partition_id"
            )
            if rows:
                return {int(r["partition_id"]): str(r["hash"]).strip() for r in rows}
        except Exception:
            pass
        sql = f"""
        SELECT partition, hash
        FROM merkle_get_partition_root_hashes(
            (SELECT c.oid FROM pg_catalog.pg_index i
             JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
             JOIN pg_catalog.pg_am am ON am.oid = c.relam
             WHERE i.indrelid = '{table}'::regclass AND am.amname = 'merkle'
             LIMIT 1)
        )
        ORDER BY partition
        """
        rows = self.execute(sql)
        return {int(r["partition"]): str(r["hash"]).strip() for r in rows}

    def verify_merkle(self, table: str) -> bool:
        """Run full internal Merkle verification."""
        res = self.scalar(f"SELECT merkle_verify('{table}'::regclass)")
        return bool(res)

    def get_partition_leaf_nodes(self, table: str, partition_id: int) -> Dict[Tuple[int, bytes, int], str]:
        """Fetch leaf nodes for a partition from the dedicated merkle_node table."""
        node_table = f"ariabc_internal.merkle_node_{table}"
        try:
            sql = f"""
            SELECT partition_id, node_id, prefix_len, is_leaf, encode(hash, 'hex') AS hash_hex
            FROM {node_table}
            WHERE partition_id = %s AND is_leaf = true
            """
            rows = self.execute(sql, (partition_id,))
            return {
                (int(r["partition_id"]), bytes(r["node_id"]), int(r["prefix_len"])): str(r["hash_hex"]).strip()
                for r in rows
            }
        except Exception as e:
            logger.debug("Failed querying dedicated node table %s: %s", node_table, e)
            return {}

    # --- Dynamic Candidate Fetch & Row Lookups ---

    def fetch_leaf_bounded_rows(
        self,
        table: str,
        partition_id: int,
        partitions_total: int,
        node_id: bytes,
        prefix_len: int,
        pk_cols: List[str],
        all_cols: List[str],
    ) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
        """Fetch rows bounded by a specific Merkle leaf range using the lookup B-tree."""
        cols_str = ", ".join(all_cols)
        pk_expr = pk_cols[0] if len(pk_cols) == 1 else f"({', '.join(pk_cols)})::text"

        # Compute lower and upper bounds for the leaf prefix
        lower = node_id
        res = bytearray(node_id)
        full_bytes = prefix_len // 8
        rem = prefix_len % 8
        if rem > 0:
            mask = 0xFF >> rem
            res[full_bytes] |= mask
            first_free = full_bytes + 1
        else:
            first_free = full_bytes
        for i in range(first_free, 8):
            res[i] = 0xFF
        upper = bytes(res)

        sql = f"""
        SELECT {cols_str}
        FROM {table}
        WHERE merkle_key_hash({pk_expr}) BETWEEN %s AND %s
          AND merkle_partition_for_hash(merkle_key_hash({pk_expr}), %s) = %s
        """
        rows = self.execute(sql, (lower, upper, partitions_total, partition_id))
        result: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for r in rows:
            key_tuple = tuple(r[c] for c in pk_cols)
            result[key_tuple] = r
        return result

    def fetch_partition_rows(
        self,
        table: str,
        partition_id: int,
        partitions_total: int,
        pk_cols: List[str],
        all_cols: List[str],
    ) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
        """Fetch all rows belonging to a partition, keyed by composite primary key tuple."""
        cols_str = ", ".join(all_cols)
        pk_expr = pk_cols[0] if len(pk_cols) == 1 else f"({', '.join(pk_cols)})::text"

        sql = f"""
        SELECT {cols_str}
        FROM {table}
        WHERE merkle_partition_for_hash(merkle_key_hash({pk_expr}), %s) = %s
        """
        rows = self.execute(sql, (partitions_total, partition_id))
        result: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for r in rows:
            key_tuple = tuple(r[c] for c in pk_cols)
            result[key_tuple] = r
        return result

    # --- Dynamic Repair DML Operations (Supporting Composite PKs & Arbitrary Columns) ---

    def repair_update_rows(
        self,
        table: str,
        pk_cols: List[str],
        data_cols: List[str],
        rows: List[Dict[str, Any]],
        batch_size: int = 500,
    ) -> int:
        """Execute batched multi-row UPDATE queries using UPDATE ... FROM (VALUES ...)
        per Dynamic_merkle_docs/repair_write_optimisation.md Run 2 architecture."""
        if not rows or not data_cols:
            return 0
        total_updated = 0

        set_clause = ", ".join([f"{c} = v.{c}" for c in data_cols])
        where_clause = " AND ".join([f"u.{pk} = v.{pk}" for pk in pk_cols])
        all_cols = pk_cols + data_cols
        cols_sql = ", ".join(all_cols)

        for i in range(0, len(rows), batch_size):
            sub_batch = rows[i : i + batch_size]
            row_pattern = "(" + ", ".join(["%s"] * len(all_cols)) + ")"
            values_sql = ", ".join([row_pattern] * len(sub_batch))

            sql = (
                f"UPDATE {table} AS u "
                f"SET {set_clause} "
                f"FROM (VALUES {values_sql}) AS v({cols_sql}) "
                f"WHERE {where_clause}"
            )

            params_list: List[Any] = []
            for r in sub_batch:
                for c in all_cols:
                    val = r.get(c)
                    if isinstance(val, bytes):
                        val = val.decode("utf-8", errors="replace")
                    params_list.append(val)

            self.execute(sql, tuple(params_list))
            total_updated += len(sub_batch)

        return total_updated

    def repair_insert_rows(
        self,
        table: str,
        all_cols: List[str],
        rows: List[Dict[str, Any]],
        batch_size: int = 500,
        pk_cols: Optional[List[str]] = None,
    ) -> int:
        """Execute batched INSERT queries for missing rows using dynamic columns and ON CONFLICT."""
        if not rows:
            return 0
        total_inserted = 0
        cols_str = ", ".join(all_cols)
        placeholders = "(" + ", ".join(["%s"] * len(all_cols)) + ")"

        conflict_clause = ""
        if pk_cols:
            non_pk = [c for c in all_cols if c not in pk_cols]
            if non_pk:
                set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk])
                conflict_clause = f" ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {set_clause}"
            else:
                conflict_clause = f" ON CONFLICT ({', '.join(pk_cols)}) DO NOTHING"

        for i in range(0, len(rows), batch_size):
            sub_batch = rows[i : i + batch_size]
            vals_sql = ", ".join([placeholders] * len(sub_batch))
            sql = f"INSERT INTO {table} ({cols_str}) VALUES {vals_sql}{conflict_clause}"
            params_list: List[Any] = []
            for r in sub_batch:
                for c in all_cols:
                    val = r.get(c)
                    if isinstance(val, bytes):
                        val = val.decode("utf-8", errors="replace")
                    params_list.append(val)
            self.execute(sql, tuple(params_list))
            total_inserted += len(sub_batch)

        return total_inserted

    def repair_delete_keys(
        self,
        table: str,
        pk_cols: List[str],
        keys: List[Tuple[Any, ...]],
        batch_size: int = 500,
    ) -> int:
        """Execute batched DELETE queries for spurious rows supporting composite keys."""
        if not keys:
            return 0
        total_deleted = 0

        for i in range(0, len(keys), batch_size):
            sub_keys = keys[i : i + batch_size]
            if len(pk_cols) == 1:
                col = pk_cols[0]
                in_clause = ", ".join(["%s"] * len(sub_keys))
                sql = f"DELETE FROM {table} WHERE {col} IN ({in_clause})"
                params = [k[0] for k in sub_keys]
                self.execute(sql, tuple(params))
                total_deleted += len(sub_keys)
            else:
                for k in sub_keys:
                    where_clause = " AND ".join([f"{col} = %s" for col in pk_cols])
                    sql = f"DELETE FROM {table} WHERE {where_clause}"
                    self.execute(sql, k)
                    total_deleted += 1

        return total_deleted
