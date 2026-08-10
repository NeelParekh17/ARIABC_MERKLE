"""Corruption manifest: choose which leaves/keys to corrupt, then apply."""

from __future__ import annotations

import random
import time
from typing import Any

from .config import ALL_COLUMNS, FIELDS, leaf_key_json
from .db import execute, scalar, geometry

_MANIFEST_KEY_CACHE: dict[tuple, dict[str, Any]] = {}


def bytea_lower_bound(node_id_hex: str, prefix_len: int) -> bytes:
    return bytes.fromhex(node_id_hex)


def bytea_upper_bound(node_id_hex: str, prefix_len: int) -> bytes:
    res = bytearray(bytes.fromhex(node_id_hex))
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
    return bytes(res)


def _batch_find_existing_keys_for_leaves(
    conn,
    leaf_requests: list[dict[str, Any]],
    partitions: int,
    tuple_count: int,
    rng: random.Random,
) -> dict[int, list[int]]:
    """Batch fetch existing keys from healthy.usertable for all requested leaves using 1 LATERAL index query."""
    if not leaf_requests:
        return {}

    values_clauses = []
    params = []
    for req in leaf_requests:
        values_clauses.append("(%s::int4, %s::int4, %s::bytea, %s::bytea, %s::int4)")
        params.extend([
            req["leaf_idx"],
            req["partition_id"] if req["partition_id"] is not None and req["partition_id"] >= 0 else 0,
            req["lower"],
            req["upper"],
            req["want"],
        ])

    sql = f"""
        WITH targets(leaf_idx, pid, lower_b, upper_b, want) AS (
            VALUES {', '.join(values_clauses)}
        )
        SELECT t.leaf_idx, u.ycsb_key
        FROM targets t
        CROSS JOIN LATERAL (
            SELECT ycsb_key
            FROM healthy.usertable
            WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key), {int(partitions)}) = t.pid
              AND merkle_key_hash(ycsb_key) >= t.lower_b
              AND merkle_key_hash(ycsb_key) <= t.upper_b
            LIMIT t.want * 10 + 20
        ) u
    """
    rows = execute(conn, sql, tuple(params))

    leaf_keys_map: dict[int, list[int]] = {req["leaf_idx"]: [] for req in leaf_requests}
    for r in rows:
        idx = int(r["leaf_idx"])
        key = int(r["ycsb_key"])
        if 1 <= key <= tuple_count:
            if key not in leaf_keys_map[idx]:
                leaf_keys_map[idx].append(key)

    final_map: dict[int, list[int]] = {}
    used_global_keys: set[int] = set()

    for req in leaf_requests:
        idx = req["leaf_idx"]
        want = req["want"]
        found = [k for k in leaf_keys_map[idx] if k not in used_global_keys]

        if len(found) < want:
            pid = req["partition_id"]
            lower = req["lower"]
            upper = req["upper"]
            if pid is not None and pid >= 0:
                fallback_sql = f"""
                    SELECT ycsb_key FROM healthy.usertable
                    WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key), {int(partitions)}) = %s
                      AND merkle_key_hash(ycsb_key) >= %s AND merkle_key_hash(ycsb_key) <= %s
                    LIMIT %s
                """
                fallback_params = (pid, lower, upper, (want - len(found)) * 20 + 50)
            else:
                fallback_sql = """
                    SELECT ycsb_key FROM healthy.usertable
                    WHERE merkle_key_hash(ycsb_key) >= %s AND merkle_key_hash(ycsb_key) <= %s
                    LIMIT %s
                """
                fallback_params = (lower, upper, (want - len(found)) * 20 + 50)

            f_rows = execute(conn, fallback_sql, fallback_params)
            for r in f_rows:
                k = int(r["ycsb_key"])
                if 1 <= k <= tuple_count and k not in used_global_keys and k not in found:
                    found.append(k)
                    if len(found) >= want:
                        break

        if len(found) < want:
            raise RuntimeError(f"could not find {want} existing keys for leaf idx {idx} (found {len(found)})")

        if len(found) > want:
            rng.shuffle(found)
            selected = found[:want]
        else:
            selected = found

        for k in selected:
            used_global_keys.add(k)
        final_map[idx] = selected

    return final_map


def _batch_find_spurious_keys(
    conn,
    spurious_requests: list[dict[str, Any]],
    partitions: int,
    rng: random.Random,
) -> dict[int, int]:
    """Batch compute spurious keys in parallel using PostgreSQL C function merkle_find_spurious_key."""
    if not spurious_requests:
        return {}

    dsn = None
    try:
        if hasattr(conn, "info") and hasattr(conn.info, "dsn"):
            dsn = conn.info.dsn
    except Exception:
        dsn = None

    num_workers = min(16, len(spurious_requests))
    chunk_size = (len(spurious_requests) + num_workers - 1) // num_workers
    request_chunks = [
        spurious_requests[i : i + chunk_size]
        for i in range(0, len(spurious_requests), chunk_size)
    ]

    def _worker(sub_requests: list[dict[str, Any]]) -> dict[int, int]:
        import psycopg
        from psycopg.rows import dict_row

        local_conn = None
        try:
            if dsn:
                local_conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
                with local_conn.cursor() as cur:
                    cur.execute("SET enable_merkle_index = on")
                    cur.execute("SET max_parallel_workers_per_gather = 0")
                use_conn = local_conn
            else:
                use_conn = conn

            values_clauses = []
            params = []
            max_attempts = 200_000_000

            for req in sub_requests:
                item_idx = req["item_idx"]
                base_offset = (item_idx + 1) * 1_000_000_000
                values_clauses.append("(%s::int4, %s::bytea, %s::bytea, %s::int4, %s::bigint, %s::int4)")
                params.extend([
                    item_idx,
                    req["lower"],
                    req["upper"],
                    req["partition_id"] if req["partition_id"] is not None else -1,
                    base_offset,
                    max_attempts,
                ])

            sql = f"""
                WITH targets(item_idx, lower_b, upper_b, target_pid, base_offset, max_attempts) AS (
                    VALUES {', '.join(values_clauses)}
                )
                SELECT t.item_idx,
                       merkle_find_spurious_key(t.lower_b, t.upper_b, t.target_pid, {int(partitions)}, t.base_offset, t.max_attempts) AS candidate
                FROM targets t
            """
            rows = execute(use_conn, sql, tuple(params))
            res: dict[int, int] = {}
            for r in rows:
                if r["candidate"] is not None:
                    res[int(r["item_idx"])] = int(r["candidate"])
            return res
        finally:
            if local_conn is not None and dsn:
                local_conn.close()

    final_res: dict[int, int] = {}
    if dsn and num_workers > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(_worker, chunk) for chunk in request_chunks]
            for f in futures:
                final_res.update(f.result())
    else:
        final_res = _worker(spurious_requests)

    if len(final_res) < len(spurious_requests):
        raise RuntimeError(f"could not find spurious key for all items (got {len(final_res)}/{len(spurious_requests)})")

    return final_res


def choose_corruption_manifest(
    conn,
    experiment: str,
    tuple_count: int,
    fanout: int = 4,
    bad_leaf_count: int = 10,
    corrupted_tuple_count: int = 300,
    seed: int = 0,
    corruption_mode: str = "mixed",
    forced_bad_leaves: list[list] | None = None,
    *args,
    **kwargs,
) -> dict[str, Any]:

    cache_key = (
        tuple_count,
        fanout,
        bad_leaf_count,
        corrupted_tuple_count,
        seed,
        corruption_mode,
        tuple(tuple(x) for x in forced_bad_leaves) if forced_bad_leaves is not None else None,
    )
    if cache_key in _MANIFEST_KEY_CACHE:
        return dict(_MANIFEST_KEY_CACHE[cache_key])
    
    rng = random.Random(f"{seed}_{tuple_count}_{bad_leaf_count}")
    
    t0 = time.perf_counter()
    c_count = int(corrupted_tuple_count)
    b_count = int(bad_leaf_count)
    base = c_count // b_count
    rem = c_count % b_count
    min_required_rows = base + (1 if rem > 0 else 0)

    t_occ0 = time.perf_counter()
    occ = execute(
        conn,
        """
        SELECT partition_id, node_id, prefix_len, tuple_count
        FROM ariabc_internal.merkle_node
        WHERE index_oid = 'healthy.usertable_merkle_idx'::regclass
          AND is_leaf = true
          AND tuple_count >= %s
        ORDER BY partition_id, prefix_len, node_id
        """,
        (min_required_rows,),
    )
    t_occ = (time.perf_counter() - t_occ0) * 1000.0

    partitions = int(geometry(conn, "healthy").get("partitions", 200))

    occ_map = {}
    for r in occ:
        partition_id = int(r["partition_id"])
        node_hex = bytes(r["node_id"]).hex()
        plen = int(r["prefix_len"])
        count = int(r["tuple_count"])
        occ_map[f"{partition_id}_{node_hex}_{plen}"] = {
            "partition_id": partition_id,
            "node_id": node_hex,
            "prefix_len": plen,
            "count": count,
        }

    eligible = [k for k, v in occ_map.items() if v["count"] >= min_required_rows]

    if forced_bad_leaves is not None:
        leaves = []
        for x in forced_bad_leaves:
            if len(x) == 3:
                leaves.append(f"{int(x[0])}_{x[1]}_{int(x[2])}")
            else:
                matches = [key for key, value in occ_map.items()
                           if value["node_id"] == x[0]
                           and value["prefix_len"] == int(x[1])]
                if len(matches) != 1:
                    raise RuntimeError(f"legacy forced leaf {x} is ambiguous across partitions")
                leaves.append(matches[0])
        missing = [leaf for leaf in leaves if leaf not in occ_map or occ_map[leaf]["count"] < min_required_rows]
        if missing:
            raise RuntimeError(f"forced bad leaves are not eligible")
    else:
        if len(eligible) < bad_leaf_count:
            raise RuntimeError(f"only {len(eligible)} leaves have >= {min_required_rows} rows")
        leaves = sorted(rng.sample(eligible, bad_leaf_count))

    selected_leaf_capacities = {leaf: occ_map[leaf]["count"] for leaf in leaves}
    selected_bad_leaf_row_capacity = sum(selected_leaf_capacities.values())

    leaf_requests = []
    for pos, leaf_key in enumerate(leaves):
        want = base + (1 if pos < rem else 0)
        node_id_hex = occ_map[leaf_key]["node_id"]
        prefix_len = occ_map[leaf_key]["prefix_len"]
        partition_id = occ_map[leaf_key]["partition_id"]
        lower = bytea_lower_bound(node_id_hex, prefix_len)
        upper = bytea_upper_bound(node_id_hex, prefix_len)
        leaf_requests.append({
            "leaf_idx": pos,
            "leaf_key": leaf_key,
            "partition_id": partition_id,
            "node_id_hex": node_id_hex,
            "prefix_len": prefix_len,
            "lower": lower,
            "upper": upper,
            "want": want,
        })

    t_exist0 = time.perf_counter()
    existing_keys_map = _batch_find_existing_keys_for_leaves(
        conn, leaf_requests, partitions, tuple_count, rng
    )
    t_exist = (time.perf_counter() - t_exist0) * 1000.0

    spurious_requests = []
    entry_prep = []

    for req in leaf_requests:
        pos = req["leaf_idx"]
        keys = existing_keys_map[pos]
        partition_id = req["partition_id"]
        node_id_hex = req["node_id_hex"]
        prefix_len = req["prefix_len"]
        lower = req["lower"]
        upper = req["upper"]

        for k in keys:
            global_entry_index = len(entry_prep)
            op = "update"
            if corruption_mode == "delete-only":
                op = "delete"
            elif corruption_mode == "insert-only":
                op = "insert"
            elif corruption_mode == "mixed":
                op = ("update", "delete", "insert")[global_entry_index % 3]

            item_idx = len(entry_prep)
            prep_info = {
                "leaf_id": [partition_id, node_id_hex, prefix_len],
                "op": op,
                "reference_key": k,
                "lower": lower,
                "upper": upper,
                "partition_id": partition_id,
            }
            entry_prep.append(prep_info)

            if op == "insert":
                base_offset = (item_idx + 1) * 10_000_000
                spurious_requests.append({
                    "item_idx": item_idx,
                    "lower": lower,
                    "upper": upper,
                    "partition_id": partition_id,
                    "base_offset": base_offset,
                })

    t_spur0 = time.perf_counter()
    spurious_keys_map = _batch_find_spurious_keys(conn, spurious_requests, partitions, rng)
    t_spur = (time.perf_counter() - t_spur0) * 1000.0

    print(
        f"  [manifest profile] occ={t_occ:.1f}ms exist={t_exist:.1f}ms (reqs={len(leaf_requests)}) spur={t_spur:.1f}ms (reqs={len(spurious_requests)}) total={(time.perf_counter()-t0)*1000.0:.1f}ms",
        flush=True,
    )

    entries: list[dict[str, Any]] = []
    for item_idx, prep in enumerate(entry_prep):
        op = prep["op"]
        ref_key = prep["reference_key"]
        if op == "insert":
            entry_key = spurious_keys_map[item_idx]
        else:
            entry_key = ref_key

        entries.append({
            "leaf_id": prep["leaf_id"],
            "ycsb_key": entry_key,
            "op": op,
            "reference_key": ref_key,
        })

    import hashlib
    import json

    selection_dict = {
        "tuple_count": tuple_count,
        "bad_leaf_count": bad_leaf_count,
        "corrupted_tuple_count": corrupted_tuple_count,
        "seed": seed,
        "bad_leaves": [
            leaf_key_json([occ_map[l]["partition_id"], occ_map[l]["node_id"], occ_map[l]["prefix_len"]])
            for l in leaves
        ],
        "corruptions": [{"ycsb_key": e["ycsb_key"], "op": e["op"]} for e in entries],
    }
    selection_json = json.dumps(selection_dict, sort_keys=True)
    corruption_selection_sha256 = hashlib.sha256(selection_json.encode('utf-8')).hexdigest()

    leaf_selection_dict = {
        "profile_label": experiment,
        "bad_leaf_count": bad_leaf_count,
        "bad_leaves": [
            leaf_key_json([occ_map[l]["partition_id"], occ_map[l]["node_id"], occ_map[l]["prefix_len"]])
            for l in leaves
        ],
        "seed": seed,
    }
    leaf_selection_json = json.dumps(leaf_selection_dict, sort_keys=True)
    bad_leaf_selection_sha256 = hashlib.sha256(leaf_selection_json.encode('utf-8')).hexdigest()

    return {
        "experiment": experiment,
        "corruption_mode": corruption_mode,
        "tuple_count": tuple_count,
        "fanout": fanout,
        "seed": seed,
        "bad_leaves": [
            leaf_key_json([occ_map[l]["partition_id"], occ_map[l]["node_id"], occ_map[l]["prefix_len"]])
            for l in leaves
        ],
        "partitions": partitions,
        "corruptions": entries,
        "corrupted_tuple_count": corrupted_tuple_count,
        "required_rows_per_bad_leaf": min_required_rows,
        "selected_bad_leaf_row_capacity": selected_bad_leaf_row_capacity,
        "selected_leaf_capacities": selected_leaf_capacities,
        "corruption_selection_sha256": corruption_selection_sha256,
        "bad_leaf_selection_sha256": bad_leaf_selection_sha256,
    }


def validate_manifest_leaf_mapping(conn, manifest: dict[str, Any]) -> None:
    if not manifest["corruptions"]:
        return

    partitions = int(manifest.get("partitions", 200))
    params = []
    values_clauses = []
    for idx, entry in enumerate(manifest["corruptions"]):
        check_key = entry["ycsb_key"]
        leaf_spec = entry["leaf_id"]
        partition_id = int(leaf_spec[0]) if len(leaf_spec) == 3 else -1
        node_hex = leaf_spec[1] if len(leaf_spec) == 3 else leaf_spec[0]
        prefix_len = int(leaf_spec[2]) if len(leaf_spec) == 3 else int(leaf_spec[1])
        lower = bytea_lower_bound(node_hex, prefix_len)
        upper = bytea_upper_bound(node_hex, prefix_len)

        values_clauses.append(f"(%s::bigint, %s::bytea, %s::bytea, %s::int4, %s::int4)")
        params.extend([check_key, lower, upper, partition_id, idx])

    sql = f"""
        WITH items(check_key, lower_b, upper_b, partition_id, idx) AS (
            VALUES {', '.join(values_clauses)}
        )
        SELECT idx, check_key,
               (merkle_key_hash(check_key) BETWEEN lower_b AND upper_b
                AND (partition_id < 0 OR merkle_partition_for_hash(merkle_key_hash(check_key), {partitions}) = partition_id)) AS valid
        FROM items
    """
    rows = execute(conn, sql, tuple(params))
    mismatches = [
        manifest["corruptions"][r["idx"]]
        for r in rows if not r["valid"]
    ]
    if mismatches:
        raise RuntimeError(
            f"corruption manifest rows do not map to intended leaves: {mismatches[:5]}"
        )


def apply_corruption(conn, manifest: dict[str, Any]) -> None:
    """Apply deterministic damage in one setup transaction.

    Corruption is outside the measured recovery interval. Batching this setup
    avoids 300 unrelated WAL commits and leaves the measured repair path with
    a stable transaction/cache state.
    """
    seed = manifest["seed"]

    with conn.transaction():
        for entry in manifest["corruptions"]:
            op = entry["op"]
            key = entry["ycsb_key"]
            ref = entry["reference_key"]

            if op == "update":
                execute(
                    conn,
                    "UPDATE damaged.usertable "
                    "SET field9 = public.recovery_corrupted_value(ycsb_key, %s) "
                    "WHERE ycsb_key = %s",
                    (seed, key),
                )
            elif op == "delete":
                execute(
                    conn,
                    "DELETE FROM damaged.usertable WHERE ycsb_key = %s",
                    (key,),
                )
            elif op == "insert":
                ref_row = execute(
                    conn,
                    "SELECT * FROM healthy.usertable WHERE ycsb_key = %s",
                    (ref,),
                )
                if not ref_row:
                    raise RuntimeError(f"reference key {ref} not found in healthy.usertable")
                vals = ref_row[0]
                execute(
                    conn,
                    "INSERT INTO damaged.usertable VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    tuple([key] + [
                        vals[f].decode("utf-8") if isinstance(vals[f], bytes) else vals[f]
                        for f in FIELDS
                    ]),
                )
            else:
                raise ValueError(f"unknown op {op!r} in manifest entry")
