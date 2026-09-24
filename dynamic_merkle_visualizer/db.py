"""Live PostgreSQL inspection. Hashing and tree maintenance belong to PostgreSQL."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import time

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


class InspectorError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Settings:
    conninfo: str
    table: str = ""
    index: str = ""
    allow_writes: bool = False
    statement_timeout_ms: int = 15000
    lock_timeout_ms: int = 2000
    node_limit: int = 1000
    row_limit: int = 50


CATALOG = """
SELECT i.indexrelid::int AS index_oid, i.indrelid::int AS table_oid,
       ni.nspname AS index_schema, ci.relname AS index_name,
       nt.nspname AS table_schema, ct.relname AS table_name,
       i.indisvalid AND i.indisready AS ready, ct.relpersistence,
       i.indnkeyatts, i.indkey::smallint[] AS key_attnums,
       i.indexprs IS NOT NULL AS expressions, i.indpred IS NOT NULL AS partial,
       ct.relrowsecurity AS row_security,
       pg_get_indexdef(i.indexrelid) AS definition
FROM pg_index i
JOIN pg_class ci ON ci.oid = i.indexrelid
JOIN pg_class ct ON ct.oid = i.indrelid
JOIN pg_namespace ni ON ni.oid = ci.relnamespace
JOIN pg_namespace nt ON nt.oid = ct.relnamespace
JOIN pg_am am ON am.oid = ci.relam
WHERE am.amname = 'merkle'
"""


def integer(value, name, low, high):
    try:
        if isinstance(value, bool):
            raise ValueError
        result = int(str(value))
    except (TypeError, ValueError):
        raise InspectorError(f"{name} must be an integer") from None
    if not low <= result <= high:
        raise InspectorError(f"{name} must be between {low} and {high}")
    return result


def coordinate(node_id="0000000000000000", prefix_len=0):
    prefix_len = integer(prefix_len, "prefix_len", 0, 60)
    if not isinstance(node_id, str) or not re.fullmatch(r"[0-9a-fA-F]{16}", node_id):
        raise InspectorError("node_id must be an eight-byte hexadecimal prefix")
    value = int(node_id, 16)
    mask = ((1 << prefix_len) - 1) << (64 - prefix_len) if prefix_len else 0
    if value & mask != value:
        raise InspectorError("node_id has nonzero bits outside its prefix")
    return bytes.fromhex(node_id), prefix_len


def attach_parents(nodes):
    """Only draw edges between observed stored prefixes; never synthesize nodes."""
    seen = {}
    for node in nodes:
        key = f"{node['node_id']}:{node['prefix_len']}"
        value = int(node["node_id"], 16)
        parent = None
        for length in range(node["prefix_len"] - 1, -1, -1):
            masked = (value >> (64 - length)) << (64 - length) if length else 0
            parent = seen.get((length, masked))
            if parent:
                break
        node.update(id=key, parent=parent)
        seen[(node["prefix_len"], value)] = key
    return nodes


class Inspector:
    def __init__(self, settings):
        self.settings = settings

    @contextmanager
    def connect(self):
        with psycopg.connect(self.settings.conninfo, autocommit=True,
                             row_factory=dict_row, connect_timeout=5,
                             application_name="ariabc_merkle_visualizer") as conn:
            # Bounded reads; no parallel executor dependency for inspection.
            for name, value in (("statement_timeout", self.settings.statement_timeout_ms),
                                ("lock_timeout", self.settings.lock_timeout_ms),
                                ("max_parallel_workers_per_gather", 0)):
                conn.execute("SELECT set_config(%s, %s, false)", (name, str(value)))
            conn.execute("SET search_path = pg_catalog")
            yield conn

    def _catalog(self, conn):
        rows = conn.execute(CATALOG + " ORDER BY nt.nspname, ct.relname, ci.relname").fetchall()
        if self.settings.table:
            oid = conn.execute("SELECT to_regclass(%s)::oid AS oid", (self.settings.table,)).fetchone()["oid"]
            rows = [r for r in rows if r["table_oid"] == oid]
        if self.settings.index:
            oid = conn.execute("SELECT to_regclass(%s)::oid AS oid", (self.settings.index,)).fetchone()["oid"]
            rows = [r for r in rows if r["index_oid"] == oid]
        return rows

    def catalog(self):
        with self.connect() as conn:
            identity = conn.execute("""SELECT current_database() AS database,
                current_user AS role, role.rolsuper AS superuser, version() AS version,
                inet_server_addr()::text AS address, inet_server_port() AS port
                FROM pg_roles AS role WHERE role.rolname = current_user""").fetchone()
            indexes = self._catalog(conn)
            return {"database": identity, "indexes": indexes,
                    "allow_writes": self.settings.allow_writes,
                    "node_limit": self.settings.node_limit, "row_limit": self.settings.row_limit}

    def _resolve(self, conn, index_oid):
        oid = integer(index_oid, "index_oid", 1, 4294967295)
        target = next((r for r in self._catalog(conn) if r["index_oid"] == oid), None)
        if not target:
            raise InspectorError("Merkle index is absent or outside the configured target", 404)
        if not target["ready"] or target["relpersistence"] != "p":
            raise InspectorError("The target must be a valid, ready Merkle index on a permanent logged table", 409)
        if target["row_security"]:
            raise InspectorError("Row-security tables cannot be inspected as a full-table view", 409)
        return target

    @staticmethod
    def table(target):
        return sql.Identifier(target["table_schema"], target["table_name"])

    @staticmethod
    def nodes_table(target):
        return sql.Identifier("ariabc_internal", f"merkle_node_{target['index_oid']}")

    @contextmanager
    def locked(self, conn, index_oid, write=False):
        target = self._resolve(conn, index_oid)
        with conn.transaction():
            # READ COMMITTED, with the heap locked BEFORE taking inspection snapshots.
            # Native maintenance takes RowExclusiveLock on this same heap. We deliberately
            # do not use READ ONLY: PostgreSQL rejects SHARE locks in read-only transactions.
            mode = sql.SQL("SHARE ROW EXCLUSIVE" if write else "SHARE")
            conn.execute(sql.SQL("LOCK TABLE {} IN {} MODE").format(self.table(target), mode))
            current = self._resolve(conn, index_oid)
            if current["table_oid"] != target["table_oid"]:
                raise InspectorError("Target changed while acquiring its lock; refresh", 409)
            yield current

    def metadata(self, conn, target):
        stats_text = conn.execute("SELECT merkle_tree_stats(%s::oid::regclass) AS value",
                                  (target["table_oid"],)).fetchone()["value"]
        try:
            stats = json.loads(stats_text)
        except ValueError:
            raise InspectorError("PostgreSQL returned invalid merkle_tree_stats JSON; inspect the native function", 409) from None
        actual = tuple(stats.get(k) for k in ("version", "route_format_version", "row_hash_format_version"))
        if actual != (10, 4, 1):
            raise InspectorError(f"Unsupported native Merkle formats {actual}; this inspector expects 10 / 4 / 1", 409)
        exists = conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok",
                              (f"ariabc_internal.merkle_node_{target['index_oid']}",)).fetchone()["ok"]
        if not exists:
            raise InspectorError("Dedicated native Merkle node relation is missing", 409)
        columns = conn.execute("""SELECT attnum, attname AS name,
                   format_type(atttypid, atttypmod) AS type, atttypid::int AS type_oid,
                   atttypmod AS typmod, attnotnull AS not_null,
                   attgenerated AS generated, attidentity AS identity
            FROM pg_attribute WHERE attrelid = %s AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum""", (target["table_oid"],)).fetchall()
        # The native single-value helper constructs typmod=-1. Never pretend it
        # reproduces multi-key, expression, partial or typmod-sensitive routing.
        key = None
        if target["indnkeyatts"] == 1 and not target["expressions"] and not target["partial"]:
            key = next((c for c in columns if c["attnum"] == target["key_attnums"][0] and c["typmod"] == -1), None)
        return stats, columns, key

    @staticmethod
    def roots(conn, target):
        return conn.execute("SELECT partition, hash FROM merkle_get_partition_root_hashes(%s::oid::regclass) ORDER BY partition",
                            (target["index_oid"],)).fetchall()

    def snapshot(self, index_oid, partition=0, node_id="0000000000000000", prefix_len=0, verify=False):
        started = time.monotonic()
        node_id, prefix_len = coordinate(node_id, prefix_len)
        with self.connect() as conn, self.locked(conn, index_oid) as target:
            stats, columns, key = self.metadata(conn, target)
            partition = integer(partition, "partition", 0, stats["partitions"] - 1)
            focus = conn.execute(sql.SQL("SELECT is_leaf FROM {} WHERE partition_id=%s AND node_id=%s AND prefix_len=%s").format(self.nodes_table(target)),
                                 (partition, node_id, prefix_len)).fetchone()
            if not focus and prefix_len:
                raise InspectorError("The selected prefix no longer exists; reload its partition", 409)
            nodes = conn.execute(sql.SQL("""SELECT partition_id, prefix_len, is_leaf, tuple_count,
                encode(node_id, 'hex') AS node_id, encode(hash, 'hex') AS hash
                FROM {} WHERE partition_id=%s AND prefix_len >= %s
                  AND node_id BETWEEN %s AND merkle_node_upper_bound(%s, %s)
                ORDER BY prefix_len, node_id LIMIT %s""").format(self.nodes_table(target)),
                (partition, prefix_len, node_id, node_id, prefix_len, self.settings.node_limit + 1)).fetchall()
            truncated = len(nodes) > self.settings.node_limit
            nodes = attach_parents(nodes[:self.settings.node_limit])
            root = conn.execute("SELECT merkle_root_hash_index(%s::oid::regclass) AS hash", (target["index_oid"],)).fetchone()["hash"]
            audit = None
            if verify:
                audit = conn.execute("SELECT merkle_verify_index(%s::oid::regclass) AS ok", (target["index_oid"],)).fetchone()["ok"]
            settings = conn.execute("""SELECT current_setting('enable_merkle_index') AS maintenance,
                current_setting('merkle_apply_synchronous_direct') AS direct_apply,
                current_setting('synchronous_commit') AS synchronous_commit,
                current_setting('fsync') AS fsync""").fetchone()
            return {"target": target, "stats": stats, "columns": columns, "root": root,
                    "partitions": self.roots(conn, target), "partition": partition,
                    "focus": {"node_id": node_id.hex(), "prefix_len": prefix_len},
                    "nodes": nodes, "truncated": truncated, "node_limit": self.settings.node_limit,
                    "leaf_rows_supported": key is not None,
                    "leaf_rows_reason": None if key else "Native leaf lookup requires one plain key with typmod -1. This index remains inspectable; use table rows for its data.",
                    "verification": audit, "settings": settings,
                    "snapshot_at": datetime.now(timezone.utc).isoformat(),
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}

    @staticmethod
    def row_projection(columns):
        # Text-valued fields preserve int8/numeric precision, bytea and timestamps
        # across JSON and JavaScript. NULL remains JSON null. PostgreSQL casts edits.
        pairs = []
        for col in columns:
            pairs.extend((sql.Literal(col["name"]), sql.SQL("t.{}::text").format(sql.Identifier(col["name"]))))
        return sql.SQL("jsonb_build_object({}) AS values, t.ctid::text AS ctid, t.xmin::text AS xmin, encode(merkle_tuple_hash(t), 'hex') AS hash").format(sql.SQL(", ").join(pairs))

    def rows(self, index_oid, offset=0, partition=None, node_id="0000000000000000", prefix_len=0):
        offset = integer(offset, "offset", 0, 1000000)
        with self.connect() as conn, self.locked(conn, index_oid) as target:
            stats, columns, key = self.metadata(conn, target)
            predicate = sql.SQL("TRUE")
            params = []
            if partition is not None:
                if not key:
                    raise InspectorError("This index cannot use the native single-key leaf lookup; select table rows", 409)
                partition = integer(partition, "partition", 0, stats["partitions"] - 1)
                node_id, prefix_len = coordinate(node_id, prefix_len)
                exists = conn.execute(sql.SQL("SELECT 1 FROM {} WHERE partition_id=%s AND node_id=%s AND prefix_len=%s").format(self.nodes_table(target)),
                                      (partition, node_id, prefix_len)).fetchone()
                if not exists:
                    raise InspectorError("Selected node no longer exists; refresh", 409)
                route = sql.SQL("merkle_key_hash(t.{})").format(sql.Identifier(key["name"]))
                predicate = sql.SQL("{} BETWEEN %s AND merkle_node_upper_bound(%s,%s) AND merkle_partition_for_hash({}, %s)=%s").format(route, route)
                params = [node_id, node_id, prefix_len, stats["partitions"], partition]
            query = sql.SQL("SELECT {} FROM {} AS t WHERE {} ORDER BY t.ctid LIMIT %s OFFSET %s").format(
                self.row_projection(columns), self.table(target), predicate)
            rows = conn.execute(query, params + [self.settings.row_limit + 1, offset]).fetchall()
            return {"rows": rows[:self.settings.row_limit], "has_more": len(rows) > self.settings.row_limit,
                    "offset": offset, "limit": self.settings.row_limit,
                    "scope": "node range" if partition is not None else "table",
                    "snapshot_at": datetime.now(timezone.utc).isoformat()}

    def mutate(self, request):
        if not self.settings.allow_writes:
            raise InspectorError("Writes are disabled for this connection", 403)
        operation = request.get("operation")
        if operation not in ("insert", "update", "delete"):
            raise InspectorError("operation must be insert, update or delete")
        values = request.get("values", {})
        if not isinstance(values, dict) or any(not isinstance(v, (str, type(None))) for v in values.values()):
            raise InspectorError("Column values must be strings or null; PostgreSQL performs type conversion")
        dry_run = request.get("rollback", False)
        if not isinstance(dry_run, bool):
            raise InspectorError("rollback must be a boolean")
        with self.connect() as conn:
            if not conn.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()["rolsuper"]:
                raise InspectorError("Writes require a PostgreSQL superuser because native Merkle maintenance controls are superuser-only", 403)
            with self.locked(conn, request.get("index_oid"), write=True) as target:
                _, columns, _ = self.metadata(conn, target)
                allowed = {c["name"] for c in columns if not c["generated"] and c["identity"] != "a"}
                if set(values) - allowed:
                    raise InspectorError("Unknown, generated, or always-identity column in edit")
                if operation != "delete" and not values:
                    raise InspectorError("Supply at least one editable column")
                # These are session-local controls, never changes to server defaults.
                conn.execute("SET LOCAL enable_merkle_index = on")
                conn.execute("SET LOCAL merkle_apply_synchronous_direct = on")
                conn.execute("SET LOCAL synchronous_commit = on")
                table = self.table(target)
                typed = sql.SQL("jsonb_populate_record(NULL::{}, %s::jsonb)").format(table)
                names = sql.SQL(", ").join(sql.Identifier(k) for k in values)
                if operation == "insert":
                    query = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} AS v RETURNING ctid::text AS ctid").format(table, names, names, typed)
                    params = [json.dumps(values)]
                else:
                    locator = request.get("row", {})
                    if not isinstance(locator, dict) or not re.fullmatch(r"\(\d+,\d+\)", str(locator.get("ctid", ""))) or not re.fullmatch(r"\d+", str(locator.get("xmin", ""))):
                        raise InspectorError("Select a current row before editing or deleting it")
                    row_hash = locator.get("hash", "")
                    if not re.fullmatch(r"[0-9a-f]{64}", str(row_hash)):
                        raise InspectorError("The selected row must include its native hash")
                    where = sql.SQL("t.ctid=%s::tid AND t.xmin::text=%s AND encode(merkle_tuple_hash(t),'hex')=%s")
                    params = [locator["ctid"], locator["xmin"], row_hash]
                    if operation == "update":
                        assignments = sql.SQL(", ").join(sql.SQL("{}=v.{}").format(sql.Identifier(k), sql.Identifier(k)) for k in values)
                        query = sql.SQL("UPDATE {} AS t SET {} FROM {} AS v WHERE {} RETURNING t.ctid::text AS ctid").format(table, assignments, typed, where)
                        params.insert(0, json.dumps(values))
                    else:
                        query = sql.SQL("DELETE FROM {} AS t WHERE {} RETURNING t.ctid::text AS ctid").format(table, where)
                changed = conn.execute(query, params).fetchall()
                if len(changed) != 1:
                    raise InspectorError("Row changed or disappeared since it was read; refresh before retrying", 409)
                if dry_run:
                    # Raising Rollback is handled by psycopg's transaction context.
                    raise psycopg.Rollback()
            # Root reads only occur AFTER this transaction context has committed/rolled back.
        return {"committed": not dry_run, "rolled_back": dry_run, "affected_rows": len(changed),
                "operation": operation, "message": "Transaction rolled back" if dry_run else "PostgreSQL committed the row and Merkle changes"}
