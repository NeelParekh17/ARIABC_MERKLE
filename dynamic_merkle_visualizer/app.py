#!/usr/bin/env python3
"""Local web visualizer backed by AriaBC's real native dynamic Merkle index."""

from __future__ import annotations

import argparse
import csv
import errno
import io
import json
import mimetypes
import os
import re
import subprocess
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ImportError as exc:  # pragma: no cover
    raise SystemExit("psycopg is required: use the repository .venv or install psycopg[binary]") from exc


ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
STATIC = ROOT / "static"
SCHEMA = "merkle_viz"
TABLE = "data"
INDEX = "data_dynamic_merkle_idx"
INDEX_REGCLASS = f"{SCHEMA}.{INDEX}"
TABLE_REGCLASS = f"{SCHEMA}.{TABLE}"
PROBE_TABLE = "route_probe"
PROBE_INDEX = "route_probe_dynamic_merkle_idx"
PROBE_TABLE_REGCLASS = f"{SCHEMA}.{PROBE_TABLE}"
PROBE_INDEX_REGCLASS = f"{SCHEMA}.{PROBE_INDEX}"
WORKLOAD_TABLE_RE = re.compile(r"\b(?:public\.)?usertable_small\b", re.IGNORECASE)
NATIVE_LAYOUT_VERSION = 5
NATIVE_LOGICAL_FANOUT = 32
NATIVE_PHYSICAL_FANOUT = 2
DIGEST_BITS = 256
LOGICAL_BITS = 5


def split_sql(text: str) -> list[str]:
    """Split SQL statements without breaking on semicolons inside quoted strings."""
    statements, buf = [], []
    quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    buf.append(text[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == ";":
            value = "".join(buf).strip()
            if value:
                statements.append(value)
            buf = []
        else:
            buf.append(ch)
        i += 1
    value = "".join(buf).strip()
    if value:
        statements.append(value)
    return statements


def normalize_workload_statement(statement: str) -> str:
    cleaned = statement.strip()
    if not re.match(r"^(SELECT|INSERT|UPDATE|DELETE)\b", cleaned, re.IGNORECASE):
        raise ValueError("only SELECT, INSERT, UPDATE, and DELETE workload statements are allowed")
    return WORKLOAD_TABLE_RE.sub(TABLE_REGCLASS, cleaned)


def prefix_bits(prefix_hex: str, length: int) -> str:
    if length <= 0:
        return "root"
    raw = bytes.fromhex(prefix_hex)
    bits = "".join(f"{value:08b}" for value in raw)
    return bits[:length]


def truncate_prefix_hex(prefix_hex: str, length: int) -> str:
    """Return the canonical 256-bit prefix value at exactly ``length`` bits."""
    raw = bytearray.fromhex(prefix_hex)
    if length < 256:
        byte = length // 8
        remainder = length % 8
        if remainder:
            raw[byte] &= 0xFF << (8 - remainder)
            byte += 1
        raw[byte:] = b"\x00" * (32 - byte)
    return raw.hex()


def child_prefix_hex(prefix_hex: str, prefix_len: int, ordinal: int,
                     width: int = LOGICAL_BITS) -> str:
    """Return the canonical MSB-first child prefix used by native routing."""
    if not 0 <= prefix_len < DIGEST_BITS:
        raise ValueError("parent prefix length must be between 0 and 255")
    width = min(width, DIGEST_BITS - prefix_len)
    if not 0 <= ordinal < (1 << width):
        raise ValueError("logical child ordinal is outside the prefix width")
    base = int(truncate_prefix_hex(prefix_hex, prefix_len), 16)
    base |= ordinal << (DIGEST_BITS - prefix_len - width)
    return f"{base:064x}"


def is_power_of(value: int, base: int) -> bool:
    if value < base:
        return False
    while value > 1 and value % base == 0:
        value //= base
    return value == 1


@dataclass
class AppState:
    workload: list[str]
    cursor: int = 0

    def __init__(self) -> None:
        self.workload = []
        self.cursor = 0
        self.compatible = True
        self.lock = threading.Lock()


class MerkleDatabase:
    def __init__(self, conninfo: str):
        self.conninfo = conninfo
        self.route_probe_lock = threading.Lock()

    def connect(self, autocommit: bool = True):
        try:
            conn = psycopg.connect(self.conninfo, row_factory=dict_row, autocommit=autocommit)
        except psycopg.OperationalError as exc:
            raise RuntimeError(
                "PostgreSQL is unavailable for the visualizer. Run "
                "dynamic_merkle_visualizer/start_postgres.sh or start the server "
                "named by MERKLE_VIZ_CONNINFO. Original error: " + str(exc)
            ) from exc
        conn.execute("SET enable_merkle_index = on")
        # The visualizer derives split/merge events from committed physical
        # frontier changes.  Backend profiling writes an optional distributed
        # side table and is not required for native-page topology or hashes.
        conn.execute("SET merkle_native_profile_enabled = off")
        return conn

    @staticmethod
    def validate_config(config: dict[str, Any]) -> dict[str, Any]:
        values = {
            "partitions": int(config.get("partitions", 150)),
            "leaves_per_partition": int(config.get("leaves_per_partition", 1024)),
            "fanout": int(config.get("fanout", 32)),
            "leaf_capacity": int(config.get("leaf_capacity", 32)),
            "merge_threshold": int(config.get("merge_threshold", 8)),
            "leaf_byte_capacity": int(config.get("leaf_byte_capacity", 65536)),
            "max_key_bytes": int(config.get("max_key_bytes", 1024)),
            "update_mode": str(config.get("update_mode", "synchronous_cow")),
        }
        if values["fanout"] != NATIVE_LOGICAL_FANOUT:
            raise ValueError("native dynamic Merkle logical fanout is fixed at 32")
        if not 1 <= values["partitions"] <= 10000:
            raise ValueError("partitions must be between 1 and 10000")
        if not 2 <= values["leaves_per_partition"] <= 1024 or not is_power_of(
                values["leaves_per_partition"], values["fanout"]):
            raise ValueError("leaves_per_partition must be a power of 32 between 2 and 1024 (32 or 1024)")
        if not 1 <= values["leaf_capacity"] <= 1024:
            raise ValueError("leaf_capacity must be between 1 and 1024")
        if not 0 <= values["merge_threshold"] < values["leaf_capacity"]:
            raise ValueError("merge_threshold must be >= 0 and below leaf_capacity")
        if not 1024 <= values["leaf_byte_capacity"] <= 16 * 1024 * 1024:
            raise ValueError("leaf_byte_capacity must be between 1024 and 16777216")
        if not 64 <= values["max_key_bytes"] <= 2000:
            raise ValueError("max_key_bytes must be between 64 and 2000")
        if values["max_key_bytes"] > values["leaf_byte_capacity"]:
            raise ValueError("max_key_bytes cannot exceed leaf_byte_capacity")
        if values["update_mode"] not in {"synchronous_cow", "pending_log"}:
            raise ValueError("update_mode must be synchronous_cow or pending_log")
        return values

    def _create_table(self, conn) -> None:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}") .format(sql.Identifier(SCHEMA)))
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(sql.Identifier(SCHEMA), sql.Identifier(TABLE)))
        conn.execute(sql.SQL("""
            CREATE TABLE {}.{} (
                ycsb_key integer PRIMARY KEY,
                field1 text, field2 text, field3 text, field4 text, field5 text,
                field6 text, field7 text, field8 text, field9 text, field10 text
            )
        """).format(sql.Identifier(SCHEMA), sql.Identifier(TABLE)))
        conn.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.profile_state (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), split_count bigint NOT NULL DEFAULT 0, merge_count bigint NOT NULL DEFAULT 0)")
        conn.execute(f"INSERT INTO {SCHEMA}.profile_state(singleton,split_count,merge_count) VALUES (true,0,0) ON CONFLICT (singleton) DO UPDATE SET split_count=0,merge_count=0")

    def build(self, payload: dict[str, Any]) -> dict[str, Any]:
        config = self.validate_config(payload.get("config") or {})
        source = payload.get("source", "upload")
        content = payload.get("content", "")
        fmt = payload.get("format", "csv")
        with self.connect() as conn:
            self._create_table(conn)
            if source == "existing_usertable_small":
                exists = conn.execute("SELECT to_regclass('public.usertable_small') IS NOT NULL AS ok").fetchone()["ok"]
                if not exists:
                    raise ValueError("public.usertable_small does not exist")
                conn.execute(f"INSERT INTO {TABLE_REGCLASS} SELECT ycsb_key,field1,field2,field3,field4,field5,field6,field7,field8,field9,field10 FROM public.usertable_small")
            elif source == "canonical_restore":
                self._load_canonical_restore(conn)
            elif fmt == "csv":
                self._load_csv(conn, content)
            elif fmt == "jsonl":
                self._load_jsonl(conn, content)
            else:
                raise ValueError("dataset format must be csv or jsonl")
            opts = sql.SQL(", ").join([sql.SQL("dynamic = true")] + [
                sql.SQL("{} = {}").format(sql.Identifier(key), sql.Literal(value))
                for key, value in config.items()
            ])
            conn.execute(sql.SQL("CREATE INDEX {} ON {}.{} USING merkle (ycsb_key) WITH ({})").format(
                sql.Identifier(INDEX), sql.Identifier(SCHEMA), sql.Identifier(TABLE), opts
            ))
            self.reset_counters(conn)
        return self.snapshot(include_nodes=True)

    def _load_canonical_restore(self, conn) -> None:
        restore = REPO_ROOT / "scripts" / "restore_usertable_small.sql"
        rows = []
        in_copy = False
        for line in restore.read_text().splitlines():
            if line.startswith("COPY public.usertable_small "):
                in_copy = True
                continue
            if not in_copy:
                continue
            if line == r"\.":
                break
            values = [None if value == r"\N" else value for value in line.split("\t")]
            if len(values) != 11:
                raise ValueError("canonical restore COPY row has unexpected column count")
            rows.append((int(values[0]), *values[1:]))
        if not rows:
            raise ValueError("canonical restore COPY data was not found")
        with conn.cursor() as cur:
            cur.executemany(f"INSERT INTO {TABLE_REGCLASS} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)

    def _load_csv(self, conn, content: str) -> None:
        reader = csv.DictReader(io.StringIO(content))
        if not reader.fieldnames:
            raise ValueError("CSV requires a header")
        rows = []
        for row in reader:
            lowered = {str(k).lower(): v for k, v in row.items()}
            key = lowered.get("ycsb_key", lowered.get("key"))
            if key is None:
                raise ValueError("CSV requires ycsb_key or key column")
            rows.append((int(key),) + tuple(lowered.get(f"field{i}") for i in range(1, 11)))
        if rows:
            with conn.cursor() as cur:
                cur.executemany(f"INSERT INTO {TABLE_REGCLASS} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)

    def _load_jsonl(self, conn, content: str) -> None:
        rows = []
        for line in content.splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            key = obj.get("ycsb_key", obj.get("key"))
            if key is None:
                raise ValueError("each JSON line requires ycsb_key or key")
            rows.append((int(key),) + tuple(obj.get(f"field{i}") for i in range(1, 11)))
        if rows:
            with conn.cursor() as cur:
                cur.executemany(f"INSERT INTO {TABLE_REGCLASS} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)

    def reset_counters(self, conn=None) -> None:
        own = conn is None
        conn = conn or self.connect()
        try:
            if conn.execute(
                "SELECT to_regclass('ariabc_internal.merkle_dynamic_state') IS NOT NULL AS ok"
            ).fetchone()["ok"]:
                conn.execute("UPDATE ariabc_internal.merkle_dynamic_state SET split_count=0, merge_count=0, updated_at=clock_timestamp() WHERE index_oid=%s::regclass", (INDEX_REGCLASS,))
            conn.execute(f"UPDATE {SCHEMA}.profile_state SET split_count=0,merge_count=0 WHERE singleton")
        finally:
            if own:
                conn.close()

    def execute(self, statement: str) -> dict[str, Any]:
        normalized = normalize_workload_statement(statement)
        before = self.stats()
        is_mutation = bool(re.match(r"^(INSERT|UPDATE|DELETE)\b", normalized, re.IGNORECASE))
        with self.connect() as conn:
            old_leaves = self._leaf_identity_set(conn) if is_mutation else set()
            cursor = conn.execute(normalized)
            rows = cursor.fetchall()[:20] if cursor.description else []
            new_leaves = self._leaf_identity_set(conn) if is_mutation else set()
        after = self.stats()
        split_delta, merge_delta = self._leaf_transition_delta(old_leaves, new_leaves)
        self._record_structure_delta(split_delta, merge_delta)
        after = self.stats()
        return {
            "statement": normalized,
            "rows": rows,
            "split_delta": split_delta,
            "merge_delta": merge_delta,
            "added_leaves": self._leaf_refs(new_leaves - old_leaves),
            "removed_leaves": self._leaf_refs(old_leaves - new_leaves),
            "operation": normalized.split(None, 1)[0].lower(),
            "key": self.statement_key(normalized),
            "stats": after,
        }

    def execute_many(self, statements: list[str]) -> dict[str, Any]:
        before = self.stats()
        mutations = 0
        split_delta = merge_delta = 0
        with self.connect() as conn:
            previous_leaves = self._leaf_identity_set(conn)
            for statement in statements:
                normalized = normalize_workload_statement(statement)
                conn.execute(normalized)
                if re.match(r"^(INSERT|UPDATE|DELETE)\b", normalized, re.IGNORECASE):
                    mutations += 1
                    current_leaves = self._leaf_identity_set(conn)
                    splits, merges = self._leaf_transition_delta(previous_leaves, current_leaves)
                    split_delta += splits
                    merge_delta += merges
                    previous_leaves = current_leaves
            self._record_structure_delta(split_delta, merge_delta, conn)
        after = self.stats()
        return {
            "statement": f"bulk run: {len(statements)} statements ({mutations} mutations)",
            "split_delta": split_delta,
            "merge_delta": merge_delta,
            "stats": after,
        }

    def mutate(self, payload: dict[str, Any]) -> dict[str, Any]:
        operation = payload.get("operation")
        key = int(payload["key"])
        fields = payload.get("fields") or {}
        expected_leaf = payload.get("expected_leaf")
        target_leaf = None
        before = self.stats()
        with self.connect() as conn:
            old_leaves = self._leaf_identity_set(conn)
        with self.connect(autocommit=False) as conn:
            if operation == "insert":
                values = [fields.get(f"field{i}", "") for i in range(1, 11)]
                conn.execute(f"INSERT INTO {TABLE_REGCLASS} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (key, *values))
            elif operation == "update":
                assignments = []
                params = []
                for name, value in fields.items():
                    if not re.fullmatch(r"field(?:10|[1-9])", name.lower()):
                        raise ValueError(f"invalid field: {name}")
                    assignments.append(sql.SQL("{}=%s").format(sql.Identifier(name.lower())))
                    params.append(value)
                if not assignments:
                    raise ValueError("update requires at least one field")
                params.append(key)
                conn.execute(sql.SQL("UPDATE {}.{} SET {} WHERE ycsb_key=%s").format(
                    sql.Identifier(SCHEMA), sql.Identifier(TABLE), sql.SQL(",").join(assignments)
                ), params)
            elif operation == "delete":
                conn.execute(f"DELETE FROM {TABLE_REGCLASS} WHERE ycsb_key=%s", (key,))
            else:
                raise ValueError("operation must be insert, update, or delete")
        with self.connect() as conn:
            new_leaves = self._leaf_identity_set(conn)
        if operation == "insert":
            with self.connect() as conn:
                target_leaf = self._key_leaf(conn, key)
        after = self.stats()
        splits, merges = self._leaf_transition_delta(old_leaves, new_leaves)
        self._record_structure_delta(splits, merges)
        result = self.snapshot(include_nodes=True)
        result["transition"] = {
            "operation": operation,
            "key": key,
            "split_delta": splits,
            "merge_delta": merges,
            "added_leaves": self._leaf_refs(new_leaves - old_leaves),
            "removed_leaves": self._leaf_refs(old_leaves - new_leaves),
            "target_leaf": target_leaf,
            "selected_leaf_match": (
                self._leaf_is_within(target_leaf, expected_leaf)
                if expected_leaf else None
            ),
        }
        return result

    @staticmethod
    def _leaf_is_within(actual: dict[str, Any] | None, expected: dict[str, Any]) -> bool:
        if actual is None or int(actual["partition_id"]) != int(expected["partition_id"]):
            return False
        expected_bits = prefix_bits(str(expected["prefix_hex"]), int(expected["prefix_len"]))
        return str(actual["prefix_bits"]).startswith(expected_bits)

    @staticmethod
    def _leaf_refs(leaves: set[tuple[Any, ...]]) -> list[dict[str, Any]]:
        return [
            {"partition_id": partition, "prefix_len": prefix_len,
             "prefix_hex": prefix_hex, "prefix_bits": prefix_bits(prefix_hex, prefix_len)}
            for partition, prefix_len, prefix_hex in sorted(leaves)
        ]

    @staticmethod
    def _structure_delta(before: dict[str, Any], after: dict[str, Any]) -> tuple[int, int]:
        old_internal = int(before.get("node_count", 0)) - int(before.get("leaf_count", 0))
        new_internal = int(after.get("node_count", 0)) - int(after.get("leaf_count", 0))
        change = new_internal - old_internal
        return (max(0, change), max(0, -change))

    @staticmethod
    def _leaf_transition_delta(before: set[tuple[Any, ...]], after: set[tuple[Any, ...]]) -> tuple[int, int]:
        added = len(after - before)
        removed = len(before - after)
        # Native binary subtree rebuild semantics: replacing one leaf with N
        # leaves creates N-1 split nodes; replacing N leaves with one creates
        # N-1 merges. Prefix relocation can add and remove multiple identities,
        # so net leaf-count growth is insufficient.
        if added > 1:
            return added - 1, 0
        if removed > 1:
            return 0, removed - 1
        return 0, 0

    def _leaf_identity_set(self, conn) -> set[tuple[Any, ...]]:
        rows = conn.execute("""
            SELECT partition_id,prefix_len,encode(prefix,'hex') AS prefix_hex
            FROM merkle_dynamic_get_leaf_frontier(%s::regclass)
        """, (INDEX_REGCLASS,)).fetchall()
        return {(r["partition_id"], r["prefix_len"], r["prefix_hex"]) for r in rows}

    def _record_structure_delta(self, splits: int, merges: int, conn=None) -> None:
        if not splits and not merges:
            return
        own = conn is None
        conn = conn or self.connect()
        try:
            conn.execute(f"UPDATE {SCHEMA}.profile_state SET split_count=split_count+%s,merge_count=merge_count+%s WHERE singleton", (splits, merges))
        finally:
            if own:
                conn.close()

    def _native_stats(self, conn) -> dict[str, Any]:
        if not conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (INDEX_REGCLASS,)).fetchone()["ok"]:
            return {"state": "NOT_BUILT", "split_count": 0, "merge_count": 0}
        row = conn.execute("SELECT merkle_dynamic_tree_stats(%s::regclass)::jsonb AS stats", (INDEX_REGCLASS,)).fetchone()
        stats = row["stats"]
        expected = {
            "authority": "native_index_pages",
            "layout_version": NATIVE_LAYOUT_VERSION,
            "logical_fanout": NATIVE_LOGICAL_FANOUT,
            "physical_node_fanout": NATIVE_PHYSICAL_FANOUT,
        }
        mismatches = [
            f"{name}={stats.get(name)!r} (expected {wanted!r})"
            for name, wanted in expected.items()
            if stats.get(name) != wanted
        ]
        if mismatches:
            raise RuntimeError(
                "native Merkle contract mismatch: " + ", ".join(mismatches) +
                "; rebuild/install/restart PostgreSQL from the current source tree"
            )
        return stats

    def stats(self) -> dict[str, Any]:
        with self.connect() as conn:
            stats = self._native_stats(conn)
            if stats.get("state") == "NOT_BUILT":
                return stats
            profile = conn.execute(f"SELECT split_count,merge_count FROM {SCHEMA}.profile_state WHERE singleton").fetchone()
            stats["split_count"] = profile["split_count"]
            stats["merge_count"] = profile["merge_count"]
            return stats

    def snapshot(self, include_nodes: bool = True) -> dict[str, Any]:
        stats = self.stats()
        if stats.get("state") == "NOT_BUILT":
            return {"stats": stats, "verify": False, "nodes": [], "items": []}
        with self.connect() as conn:
            verify = conn.execute("SELECT merkle_dynamic_verify(%s::regclass) AS ok", (INDEX_REGCLASS,)).fetchone()["ok"]
            row_count = conn.execute(f"SELECT count(*) AS n FROM {TABLE_REGCLASS}").fetchone()["n"]
            nodes = []
            if include_nodes:
                frontier = conn.execute("""
                    SELECT partition_id,prefix_len,encode(prefix,'hex') AS prefix_hex
                    FROM merkle_dynamic_get_leaf_frontier(%s::regclass)
                    ORDER BY partition_id,prefix_len,prefix
                """, (INDEX_REGCLASS,)).fetchall()
                frontier_keys = {
                    (int(leaf["partition_id"]), int(leaf["prefix_len"]), leaf["prefix_hex"])
                    for leaf in frontier
                }
                requested = {
                    (int(leaf["partition_id"]), depth,
                     truncate_prefix_hex(leaf["prefix_hex"], depth))
                    for leaf in frontier
                    for depth in range(int(leaf["prefix_len"]) + 1)
                }
                ranges = [
                    {"partition_id": partition, "prefix_length": depth,
                     "prefix_value": prefix_hex}
                    for partition, depth, prefix_hex in sorted(requested)
                ]
                nodes = conn.execute("""
                    SELECT partition_id,prefix_len,encode(prefix,'hex') AS prefix_hex,
                           is_leaf,tuple_count,0::bigint AS subtree_bytes,
                           encode(data_xor,'hex') AS data_xor,
                           NULL::text AS structure_hash,0::bigint AS last_seq
                    FROM merkle_dynamic_get_ranges(%s::regclass,%s::jsonb)
                    ORDER BY partition_id,prefix_len,prefix
                """, (INDEX_REGCLASS, json.dumps(ranges))).fetchall()
                for node in nodes:
                    # get_ranges().is_leaf describes whether a requested
                    # logical range is bounded, not always whether it is a
                    # physical frontier leaf. Frontier membership is the
                    # authoritative physical node type.
                    node["is_leaf"] = (
                        int(node["partition_id"]), int(node["prefix_len"]), node["prefix_hex"]
                    ) in frontier_keys
                    node["node_kind"] = "physical"
                    node["physical"] = True
                    node["physical_depth"] = int(node["prefix_len"])
                    node["prefix_bits"] = prefix_bits(node["prefix_hex"], node["prefix_len"])
                    node["id"] = f"p{node['partition_id']}:{node['prefix_len']}:{node['prefix_bits']}"
                    if node["prefix_len"] > 0:
                        parent_depth = node["prefix_len"] - 1
                        parent_bits = prefix_bits(node["prefix_hex"], parent_depth)
                        node["parent_id"] = f"p{node['partition_id']}:{parent_depth}:{parent_bits}"
                    else:
                        node["parent_id"] = None
                expected_nodes = int(stats.get("node_count", len(nodes)))
                if len(nodes) != expected_nodes:
                    raise RuntimeError(
                        f"native hierarchy reconstruction returned {len(nodes)} nodes; "
                        f"tree stats report {expected_nodes}"
                    )
                expected_leaves = int(stats.get("leaf_count", len(frontier_keys)))
                actual_leaves = sum(1 for node in nodes if node["is_leaf"])
                if actual_leaves != expected_leaves:
                    raise RuntimeError(
                        f"native hierarchy reconstruction returned {actual_leaves} leaves; "
                        f"tree stats report {expected_leaves}"
                    )
            return {"stats": stats, "verify": verify, "row_count": row_count, "nodes": nodes}

    @staticmethod
    def _xor_hex(values: list[str]) -> str:
        result = 0
        for value in values:
            result ^= int(value or "0", 16)
        return f"{result:064x}"

    def _range_summaries(self, conn, ranges: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not ranges:
            return []
        returned = conn.execute("""
            SELECT partition_id,prefix_len,encode(prefix,'hex') AS prefix_hex,
                   is_leaf,tuple_count,encode(data_xor,'hex') AS data_xor
            FROM merkle_dynamic_get_ranges(%s::regclass,%s::jsonb)
            ORDER BY partition_id,prefix_len,prefix
        """, (INDEX_REGCLASS, json.dumps(ranges))).fetchall()
        by_key = {
            (int(row["partition_id"]), int(row["prefix_len"]), row["prefix_hex"]): row
            for row in returned
        }
        completed = []
        for requested in ranges:
            key = (int(requested["partition_id"]), int(requested["prefix_length"]),
                   truncate_prefix_hex(str(requested["prefix_value"]),
                                       int(requested["prefix_length"])))
            completed.append(by_key.get(key, {
                "partition_id": key[0], "prefix_len": key[1], "prefix_hex": key[2],
                "is_leaf": True, "tuple_count": 0, "data_xor": "00" * 32,
            }))
        unexpected = set(by_key) - {
            (int(item["partition_id"]), int(item["prefix_length"]),
             truncate_prefix_hex(str(item["prefix_value"]), int(item["prefix_length"])))
            for item in ranges
        }
        if unexpected:
            raise RuntimeError("native range API returned an unrequested logical prefix")
        return completed

    def logical_tree(self, partition: int, include_empty: bool = True,
                     max_nodes: int = 50000) -> dict[str, Any]:
        """Materialize the same 32-way bounded ranges used by recovery.

        These are query-time logical ranges over the native binary trie, not
        stored nodes.  A range is terminal exactly when get_ranges().is_leaf
        reports it bounded by the native leaf item/byte capacities.
        """
        stats = self.stats()
        if stats.get("state") == "NOT_BUILT":
            return {"nodes": [], "summary": {"state": "NOT_BUILT"}}
        partitions = int(stats["partitions"])
        if not 0 <= partition < partitions:
            raise ValueError(f"partition must be between 0 and {partitions - 1}")

        root_request = [{"partition_id": partition, "prefix_length": 0,
                         "prefix_value": "00" * 32}]
        nodes: list[dict[str, Any]] = []
        with self.connect() as conn:
            frontier = self._range_summaries(conn, root_request)
            while frontier:
                next_requests = []
                for row in frontier:
                    depth = int(row["prefix_len"])
                    bits = prefix_bits(row["prefix_hex"], depth)
                    bounded = bool(row["is_leaf"])
                    node = {
                        **row,
                        "node_kind": "logical_range",
                        "physical": False,
                        "bounded": bounded,
                        "empty": int(row["tuple_count"]) == 0,
                        "prefix_bits": bits,
                        "logical_level": (depth + LOGICAL_BITS - 1) // LOGICAL_BITS,
                        "id": f"logical:p{partition}:{depth}:{bits}",
                        "parent_id": None,
                        "slot": None,
                    }
                    if depth:
                        parent_depth = max(0, depth - LOGICAL_BITS)
                        parent_hex = truncate_prefix_hex(row["prefix_hex"], parent_depth)
                        parent_bits = prefix_bits(parent_hex, parent_depth)
                        node["parent_id"] = f"logical:p{partition}:{parent_depth}:{parent_bits}"
                        width = depth - parent_depth
                        node["slot"] = (int(row["prefix_hex"], 16) >> (DIGEST_BITS - depth)) & ((1 << width) - 1)
                    nodes.append(node)
                    if not bounded and depth < DIGEST_BITS and int(row["tuple_count"]) > 0:
                        width = min(LOGICAL_BITS, DIGEST_BITS - depth)
                        for ordinal in range(1 << width):
                            next_requests.append({
                                "partition_id": partition,
                                "prefix_length": depth + width,
                                "prefix_value": child_prefix_hex(row["prefix_hex"], depth, ordinal, width),
                            })
                if len(nodes) + len(next_requests) > max_nodes:
                    raise RuntimeError(
                        f"logical range view exceeds {max_nodes} nodes; increase leaf capacity or inspect a smaller dataset"
                    )
                if not next_requests:
                    break
                children = self._range_summaries(conn, next_requests)
                by_parent: dict[tuple[int, str], list[dict[str, Any]]] = {}
                for child in children:
                    parent_depth = max(0, int(child["prefix_len"]) - LOGICAL_BITS)
                    parent_hex = truncate_prefix_hex(child["prefix_hex"], parent_depth)
                    by_parent.setdefault((parent_depth, parent_hex), []).append(child)
                for parent in frontier:
                    if bool(parent["is_leaf"]) or int(parent["tuple_count"]) == 0:
                        continue
                    key = (int(parent["prefix_len"]), parent["prefix_hex"])
                    siblings = by_parent.get(key, [])
                    if sum(int(child["tuple_count"]) for child in siblings) != int(parent["tuple_count"]):
                        raise RuntimeError("native logical range count conservation failed")
                    if self._xor_hex([child["data_xor"] for child in siblings]) != parent["data_xor"]:
                        raise RuntimeError("native logical range XOR conservation failed")
                frontier = children

        visible = nodes if include_empty else [node for node in nodes if not node["empty"]]
        visible_ids = {node["id"] for node in visible}
        # When empty slots are hidden every non-root visible range still has a
        # non-empty ancestor, but keep this defensive for a partial final level.
        for node in visible:
            if node["parent_id"] not in visible_ids:
                node["parent_id"] = None
        summary = {
            "partition": partition,
            "logical_fanout": NATIVE_LOGICAL_FANOUT,
            "bits_per_level": LOGICAL_BITS,
            "physical_node_fanout": NATIVE_PHYSICAL_FANOUT,
            "range_count": len(visible),
            "all_range_count": len(nodes),
            "nonempty_range_count": sum(not node["empty"] for node in nodes),
            "bounded_range_count": sum(node["bounded"] and (include_empty or not node["empty"])
                                         for node in nodes),
            "levels": max((node["logical_level"] for node in visible), default=0),
            "include_empty": include_empty,
        }
        return {"nodes": visible, "summary": summary}

    def leaf_items(self, partition: int, prefix_len: int, prefix_hex: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return conn.execute("""
                SELECT encode(key_data,'hex') AS key_data_hex,key_text,
                       encode(route_digest,'hex') AS route_digest,
                       encode(tuple_hash,'hex') AS tuple_hash
                FROM merkle_dynamic_get_range_items(
                    %s::regclass,
                    jsonb_build_array(jsonb_build_object(
                        'partition_id',%s::int,'prefix_length',%s::int,
                        'prefix_value',%s::text)))
                LIMIT 500
            """, (INDEX_REGCLASS, partition, prefix_len, prefix_hex)).fetchall()

    def find_leaf_keys(self, payload: dict[str, Any]) -> dict[str, Any]:
        partition = int(payload["partition_id"])
        prefix_len = int(payload["prefix_len"])
        prefix_hex = str(payload["prefix_hex"])
        wanted = max(1, min(int(payload.get("count", 5)), 20))
        max_attempts = max(256, min(int(payload.get("max_attempts", 100000)), 500000))
        if not 0 <= prefix_len <= 256 or len(prefix_hex) != 64:
            raise ValueError("selected leaf prefix is invalid")
        target_bits = prefix_bits(prefix_hex, prefix_len)

        with self.route_probe_lock:
            stats = self.stats()
            partitions = int(stats.get("partitions", 0))
            if not 0 <= partition < partitions:
                raise ValueError("selected leaf does not belong to the active native index")
            with self.connect() as conn:
                highest = conn.execute(f"SELECT COALESCE(max(ycsb_key),0) AS key FROM {TABLE_REGCLASS}").fetchone()["key"]
            start = int(payload.get("start_key", highest + 1))
            if start > 2147483647 or start < -2147483648:
                raise ValueError("candidate start key is outside PostgreSQL integer range")
            self._ensure_route_probe(partitions)

            matches = []
            attempted = 0
            candidate = start
            batch_size = 2048
            while len(matches) < wanted and attempted < max_attempts and candidate <= 2147483647:
                limit = min(batch_size, max_attempts - attempted, 2147483648 - candidate)
                keys = list(range(candidate, candidate + limit))
                candidate += limit
                attempted += limit
                with self.connect() as conn:
                    existing = conn.execute(
                        f"SELECT ycsb_key FROM {TABLE_REGCLASS} WHERE ycsb_key=ANY(%s)", (keys,)
                    ).fetchall()
                existing_keys = {row["ycsb_key"] for row in existing}
                probe_keys = [key for key in keys if key not in existing_keys]
                if not probe_keys:
                    continue
                try:
                    with self.connect() as conn:
                        with conn.cursor() as cursor:
                            cursor.executemany(
                                f"INSERT INTO {PROBE_TABLE_REGCLASS}(ycsb_key) VALUES (%s)",
                                [(key,) for key in probe_keys],
                            )
                    with self.connect() as conn:
                        routed = conn.execute("""
                            WITH requests AS (
                              SELECT jsonb_agg(jsonb_build_object(
                                  'partition_id',partition_id,
                                  'prefix_length',prefix_len,
                                  'prefix_value',encode(prefix,'hex'))) AS ranges
                              FROM merkle_dynamic_get_leaf_frontier(%s::regclass)
                            )
                            SELECT item.key_text,item.partition_id,
                                   encode(item.route_digest,'hex') AS route_digest
                            FROM requests
                            CROSS JOIN LATERAL merkle_dynamic_get_range_items(
                                %s::regclass,requests.ranges) AS item
                        """, (PROBE_INDEX_REGCLASS, PROBE_INDEX_REGCLASS)).fetchall()
                    for row in routed:
                        digest_bits = prefix_bits(row["route_digest"], 256)
                        if int(row["partition_id"]) == partition and digest_bits.startswith(target_bits):
                            key = int(row["key_text"])
                            fields = {f"field{i}": f"leaf-{partition}-{target_bits or 'root'}-key-{key}-field{i}"
                                      for i in range(1, 11)}
                            matches.append({"key": key, "partition_id": partition,
                                            "prefix_len": prefix_len, "prefix_hex": prefix_hex,
                                            "route_digest": row["route_digest"], "fields": fields})
                            if len(matches) == wanted:
                                break
                finally:
                    with self.connect() as conn:
                        conn.execute(f"DELETE FROM {PROBE_TABLE_REGCLASS}")

            return {"matches": matches, "attempted": attempted, "next_start_key": candidate,
                    "partition_id": partition, "prefix_len": prefix_len,
                    "prefix_hex": prefix_hex, "complete": len(matches) == wanted}

    def _ensure_route_probe(self, partitions: int) -> None:
        with self.connect() as conn:
            exists = conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok",
                                  (PROBE_INDEX_REGCLASS,)).fetchone()["ok"]
            current_partitions = None
            if exists:
                current = conn.execute(
                    "SELECT merkle_dynamic_tree_stats(%s::regclass)::jsonb AS stats",
                    (PROBE_INDEX_REGCLASS,)).fetchone()["stats"]
                current_partitions = int(current.get("partitions", 0))
            if current_partitions == partitions:
                conn.execute(f"DELETE FROM {PROBE_TABLE_REGCLASS}")
                return
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(SCHEMA), sql.Identifier(PROBE_TABLE)))
            conn.execute(sql.SQL("""
                CREATE TABLE {}.{} (
                    ycsb_key integer PRIMARY KEY,
                    field1 text, field2 text, field3 text, field4 text, field5 text,
                    field6 text, field7 text, field8 text, field9 text, field10 text)
            """).format(sql.Identifier(SCHEMA), sql.Identifier(PROBE_TABLE)))
            conn.execute(sql.SQL("""
                CREATE INDEX {} ON {}.{} USING merkle (ycsb_key) WITH (
                    dynamic=true,partitions={},leaves_per_partition=32,fanout=32,
                    leaf_capacity=1024,merge_threshold=0,
                    leaf_byte_capacity=16777216,max_key_bytes=1024,
                    update_mode='synchronous_cow')
            """).format(sql.Identifier(PROBE_INDEX), sql.Identifier(SCHEMA),
                         sql.Identifier(PROBE_TABLE), sql.Literal(partitions)))

    @staticmethod
    def statement_key(statement: str) -> int | None:
        match = re.search(r"\bycsb_key\s*=\s*(-?\d+)", statement, re.IGNORECASE)
        if match:
            return int(match.group(1))
        match = re.search(r"^\s*INSERT\s+INTO\s+\S+\s+VALUES\s*\(\s*(-?\d+)", statement, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def _key_partition(self, conn, key: int) -> int | None:
        leaf = self._key_leaf(conn, key)
        return leaf["partition_id"] if leaf else None

    def _key_leaf(self, conn, key: int) -> dict[str, Any] | None:
        row = conn.execute("""
            SELECT partition_id,prefix_len,encode(prefix,'hex') AS prefix_hex
            FROM merkle_dynamic_get_range_items(
                %s::regclass,
                (SELECT jsonb_agg(jsonb_build_object(
                    'partition_id',partition_id,'prefix_length',prefix_len,
                    'prefix_value',encode(prefix,'hex')))
                 FROM merkle_dynamic_get_leaf_frontier(%s::regclass)))
            WHERE key_text=%s::text LIMIT 1
        """, (INDEX_REGCLASS, INDEX_REGCLASS, str(key))).fetchone()
        if row:
            row["prefix_bits"] = prefix_bits(row["prefix_hex"], row["prefix_len"])
        return row

    def preview_statement(self, statement: str) -> dict[str, Any]:
        normalized = normalize_workload_statement(statement)
        key = self.statement_key(normalized)
        if key is None:
            return {"statement": normalized, "key": None, "partition": None, "kind": "read/unresolved"}
        conn = self.connect(autocommit=False)
        try:
            before_partition = self._key_partition(conn, key)
            try:
                conn.execute(normalized)
                partition = self._key_partition(conn, key)
            except Exception:
                conn.rollback()
                partition = before_partition
            finally:
                conn.rollback()
            return {"statement": normalized, "key": key, "partition": partition, "kind": normalized.split(None, 1)[0].upper()}
        finally:
            conn.close()

    def workload_conflicts(self, statements: list[str]) -> list[int]:
        keys = [self.statement_key(s) for s in statements if re.match(r"^\s*INSERT\b", s, re.IGNORECASE)]
        keys = sorted({key for key in keys if key is not None})
        if not keys:
            return []
        with self.connect() as conn:
            rows = conn.execute(f"SELECT ycsb_key FROM {TABLE_REGCLASS} WHERE ycsb_key=ANY(%s)", (keys,)).fetchall()
            return [row["ycsb_key"] for row in rows]


class ApiHandler(BaseHTTPRequestHandler):
    db: MerkleDatabase
    state: AppState

    def log_message(self, fmt: str, *args) -> None:
        print(f"[http] {self.address_string()} {fmt % args}")

    def json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def send_json(self, value: Any, status: int = 200) -> None:
        body = json.dumps(value, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/api/snapshot":
                self.send_json(self.db.snapshot(include_nodes=True)); return
            if parsed.path == "/api/leaf-items":
                q = parse_qs(parsed.query)
                self.send_json(self.db.leaf_items(int(q["partition"][0]), int(q["prefix_len"][0]), q["prefix_hex"][0])); return
            if parsed.path == "/api/logical-tree":
                q = parse_qs(parsed.query)
                include_empty = q.get("include_empty", ["true"])[0].lower() in {"1", "true", "yes", "on"}
                self.send_json(self.db.logical_tree(int(q["partition"][0]), include_empty)); return
            self.serve_static(parsed.path)
        except Exception as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)

    def do_POST(self) -> None:
        try:
            payload = self.json_body()
            if self.path == "/api/build":
                result = self.db.build(payload)
            elif self.path == "/api/mutate":
                result = self.db.mutate(payload)
            elif self.path == "/api/leaf-key-candidates":
                result = self.db.find_leaf_keys(payload)
            elif self.path == "/api/workload/load":
                statements = [normalize_workload_statement(s) for s in split_sql(payload.get("content", ""))]
                conflicts = self.db.workload_conflicts(statements)
                with self.state.lock:
                    self.state.workload = statements
                    self.state.cursor = 0
                    self.state.compatible = not conflicts
                result = {"loaded": len(statements), "cursor": 0,
                          "compatible": not conflicts,
                          "conflicting_insert_count": len(conflicts),
                          "conflicting_insert_keys": conflicts[:20],
                          "next": self.db.preview_statement(statements[0]) if statements else None}
            elif self.path == "/api/workload/step":
                result = self.workload_step(int(payload.get("count", 1)))
            elif self.path == "/api/workload/run":
                if not self.state.compatible:
                    raise ValueError("workload inserts already exist in this dataset; build from the canonical 12k base before Run all")
                result = self.workload_step(10**9)
            elif self.path == "/api/reset-counters":
                self.db.reset_counters(); result = self.db.snapshot(include_nodes=True)
            else:
                self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND); return
            self.send_json(result)
        except Exception as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)

    def workload_step(self, count: int) -> dict[str, Any]:
        events = []
        with self.state.lock:
            end = min(len(self.state.workload), self.state.cursor + max(1, count))
            if end - self.state.cursor > 100:
                try:
                    events.append(self.db.execute_many(self.state.workload[self.state.cursor:end]))
                    self.state.cursor = end
                except Exception as exc:
                    events.append({"statement": self.state.workload[self.state.cursor], "error": str(exc), "split_delta": 0, "merge_delta": 0})
                    self.state.cursor += 1
            else:
                while self.state.cursor < end:
                    try:
                        events.append(self.db.execute(self.state.workload[self.state.cursor]))
                    except Exception as exc:
                        events.append({"statement": self.state.workload[self.state.cursor], "error": str(exc), "split_delta": 0, "merge_delta": 0})
                    self.state.cursor += 1
            return {
                "events": events,
                "cursor": self.state.cursor,
                "total": len(self.state.workload),
                "done": self.state.cursor == len(self.state.workload),
                "next": self.db.preview_statement(self.state.workload[self.state.cursor])
                        if self.state.cursor < len(self.state.workload) else None,
                "snapshot": self.db.snapshot(include_nodes=True),
            }

    def serve_static(self, path: str) -> None:
        relative = "index.html" if path == "/" else path.lstrip("/")
        target = (STATIC / relative).resolve()
        if STATIC.resolve() not in target.parents and target != STATIC.resolve():
            self.send_error(HTTPStatus.FORBIDDEN); return
        if not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND); return
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(str(target))[0] or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers(); self.wfile.write(data)


def verify_current_workload(db: MerkleDatabase, workload_path: Path) -> int:
    print("Building isolated merkle_viz index from the canonical 11,994-row base...")
    db.build({"source": "canonical_restore", "config": {}})
    statements = [normalize_workload_statement(s) for s in split_sql(workload_path.read_text())]
    db.execute_many(statements)
    snapshot = db.snapshot(include_nodes=False)
    stats = snapshot["stats"]
    print(json.dumps({"statements": len(statements), "verify": snapshot["verify"], "row_count": snapshot["row_count"], "stats": stats}, indent=2, default=str))
    return 0 if snapshot["verify"] and int(stats.get("split_count", -1)) == 20 and int(stats.get("merge_count", -1)) == 0 else 1


def ensure_default_postgres(db: MerkleDatabase) -> None:
    """Start the isolated persistent cluster only for the built-in conninfo."""
    try:
        with db.connect():
            return
    except RuntimeError:
        pass
    helper = ROOT / "start_postgres.sh"
    try:
        subprocess.run([str(helper)], cwd=REPO_ROOT, check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(
            f"could not auto-start visualizer PostgreSQL with {helper}; "
            "inspect dynamic_merkle_visualizer/postgres.log"
        ) from exc
    with db.connect():
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--conninfo", default=os.environ.get("MERKLE_VIZ_CONNINFO", "host=127.0.0.1 port=5438 dbname=postgres user=postgres"))
    parser.add_argument("--no-auto-start-postgres", action="store_true",
                        help="do not start the isolated local PostgreSQL cluster when the default connection is down")
    parser.add_argument("--verify-current-workload", type=Path)
    args = parser.parse_args()
    db = MerkleDatabase(args.conninfo)
    if ("MERKLE_VIZ_CONNINFO" not in os.environ and
            not args.no_auto_start_postgres):
        ensure_default_postgres(db)
    if args.verify_current_workload:
        return verify_current_workload(db, args.verify_current_workload)
    ApiHandler.db = db
    ApiHandler.state = AppState()
    try:
        server = ThreadingHTTPServer((args.host, args.port), ApiHandler)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            print(f"Dynamic Merkle Visualizer is already running at http://{args.host}:{args.port}")
            return 0
        raise
    print(f"Dynamic Merkle Visualizer: http://{args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
