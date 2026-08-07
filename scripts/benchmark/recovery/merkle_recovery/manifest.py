"""Corruption manifest: choose which leaves/keys to corrupt, then apply."""

from __future__ import annotations

import random
from typing import Any

from .config import ALL_COLUMNS, FIELDS, leaf_key_json
from .db import execute, scalar, geometry

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


def _find_spurious_key_for_leaf(
    conn,
    lower_bound: bytes,
    upper_bound: bytes,
    rng: random.Random,
    used_keys: set[int],
    *,
    partition_id: int | None = None,
    partitions: int = 200,
    batch_size: int = 500_000,
    max_attempts: int = 50_000_000,
) -> int:
    used_list = list(used_keys) if used_keys else []
    base_offset = -rng.randint(1, 10_000_000)
    for i in range(max_attempts // batch_size):
        chunk_start = base_offset - (i * batch_size)
        chunk_end = chunk_start - batch_size + 1
        rows = execute(
            conn,
            """
            SELECT c AS candidate
            FROM generate_series(%s::bigint, %s::bigint, -1) AS c
            WHERE merkle_key_hash(c) BETWEEN %s AND %s
              AND (%s::int4 < 0 OR
                   merkle_partition_for_hash(merkle_key_hash(c), %s) = %s)
              AND NOT (c = ANY(%s::bigint[]))
            LIMIT 1
            """,
            (chunk_start, chunk_end, lower_bound, upper_bound,
             -1 if partition_id is None else partition_id,
             partitions, -1 if partition_id is None else partition_id,
             used_list),
        )
        if rows:
            candidate = int(rows[0]["candidate"])
            used_keys.add(candidate)
            return candidate

    raise RuntimeError("could not find a spurious key mapping to leaf")


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
    
    rng = random.Random(f"{seed}_{tuple_count}_{bad_leaf_count}")
    
    occ = execute(
        conn,
        """
        SELECT partition_id, node_id, prefix_len, tuple_count
        FROM ariabc_internal.merkle_node
        WHERE index_oid = 'healthy.usertable_merkle_idx'::regclass AND is_leaf = true
        ORDER BY partition_id, prefix_len, node_id
        """
    )

    partitions = int(geometry(conn, "healthy").get("partitions", 200))
    
    c_count = int(corrupted_tuple_count)
    b_count = int(bad_leaf_count)
    base = c_count // b_count
    rem = c_count % b_count
    min_required_rows = base + (1 if rem > 0 else 0)

    # The same node/prefix coordinate exists under every partition root, so
    # partition_id is part of the durable leaf identity.
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
        # Accept old [node_id_hex, prefix_len] and canonical
        # [partition_id, node_id_hex, prefix_len] coordinates.
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
    entries: list[dict[str, Any]] = []

    used_spurious_keys: set[int] = set()

    for pos, leaf_key in enumerate(leaves):
        want = base + (1 if pos < rem else 0)
        node_id_hex = occ_map[leaf_key]["node_id"]
        prefix_len = occ_map[leaf_key]["prefix_len"]
        partition_id = occ_map[leaf_key]["partition_id"]
        lower = bytea_lower_bound(node_id_hex, prefix_len)
        upper = bytea_upper_bound(node_id_hex, prefix_len)
        
        keys = execute(
            conn,
            """
            SELECT ycsb_key
            FROM healthy.usertable
            WHERE merkle_key_hash(ycsb_key) BETWEEN %s AND %s
              AND merkle_partition_for_hash(merkle_key_hash(ycsb_key), %s) = %s
            ORDER BY ycsb_key
            LIMIT %s
            """,
            (lower, upper, partitions, partition_id, want),
        )
        if len(keys) < want:
            raise RuntimeError(f"leaf {leaf_key} has {len(keys)} rows, need {want}")

        for row in keys:
            k = int(row["ycsb_key"])
            op = "update"
            if corruption_mode == "delete-only":
                op = "delete"
            elif corruption_mode == "insert-only":
                op = "insert"
            elif corruption_mode == "mixed":
                global_entry_index = len(entries)
                op = ("update", "delete", "insert")[global_entry_index % 3]

            if op == "insert":
                entry_key = _find_spurious_key_for_leaf(
                    conn, lower, upper, rng, used_spurious_keys,
                    partition_id=partition_id, partitions=partitions,
                )
            else:
                entry_key = k

            entries.append(
                {
                    "leaf_id": [partition_id, node_id_hex, prefix_len],
                    "ycsb_key": entry_key,
                    "op": op,
                    "reference_key": k,
                }
            )

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
    mismatches: list[dict[str, Any]] = []
    for entry in manifest["corruptions"]:
        check_key = entry["ycsb_key"]
        leaf_spec = entry["leaf_id"]
        if len(leaf_spec) == 3:
            partition_id, node_id_hex, prefix_len = int(leaf_spec[0]), leaf_spec[1], int(leaf_spec[2])
        else:
            partition_id, node_id_hex, prefix_len = None, leaf_spec[0], int(leaf_spec[1])
        lower = bytea_lower_bound(node_id_hex, prefix_len)
        upper = bytea_upper_bound(node_id_hex, prefix_len)
        
        actual = scalar(
            conn,
            """SELECT merkle_key_hash(%s::bigint) BETWEEN %s AND %s
                      AND (%s::int4 IS NULL OR
                           merkle_partition_for_hash(merkle_key_hash(%s::bigint), %s) = %s)""",
            (check_key, lower, upper, partition_id, check_key,
             int(manifest.get("partitions", 200)), partition_id),
        )
        if not actual:
            mismatches.append(
                {
                    "ycsb_key": check_key,
                    "op": entry["op"],
                    "expected": entry["leaf_id"],
                }
            )
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
