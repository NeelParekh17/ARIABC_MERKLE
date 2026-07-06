"""Corruption manifest: choose which leaves/keys to corrupt, then apply.

Corruption modes
----------------
paper-update-only  Original paper profile. Existing rows have ``field9``
                   replaced by a deterministic corrupted value.
update-only        Semantically identical to paper-update-only; kept as a
                   separate label so correctness tests can distinguish the
                   two profiles without altering any SQL.
delete-only        Each targeted row is deleted from damaged.usertable so
                   the reference (healthy) copy has rows that the damaged
                   copy lacks.
insert-only        Spurious rows that do not exist in healthy.usertable are
                   inserted into damaged.usertable.  The spurious keys are
                   chosen from the negative range -(seed...) so they never
                   collide with real ycsb_key values.
mixed              Deterministic 1/3–1/3–1/3 split of update / delete /
                   insert across the targeted entries.
"""

from __future__ import annotations

import random
from typing import Any

from .config import ALL_COLUMNS, FIELDS
from .db import execute, scalar


def row_expr(schema: str) -> str:
    return f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"


def choose_corruption_manifest(
    conn,
    experiment: str,
    tuple_count: int,
    partitions: int,
    leaves_per_partition: int,
    fanout: int,
    bad_leaf_count: int,
    corrupted_tuple_count: int,
    seed: int,
    corruption_mode: str = "paper-update-only",
) -> dict[str, Any]:
    """Select *bad_leaf_count* non-empty leaves and pick row keys to corrupt.

    The manifest records every planned corruption with its leaf_id,
    ycsb_key, and the corruption operation type (``op``).
    """
    rng = random.Random(seed + tuple_count * 31 + partitions * 17 + bad_leaf_count)
    occ = execute(
        conn,
        f"""
        SELECT {row_expr('healthy')}::bigint AS leaf_id, count(*)::bigint AS tuple_count
        FROM healthy.usertable
        GROUP BY 1
        ORDER BY 1
        """,
    )
    eligible = [int(r["leaf_id"]) for r in occ if int(r["tuple_count"]) > 0]
    if len(eligible) < bad_leaf_count:
        raise RuntimeError(
            f"only {len(eligible)} non-empty leaves available, need {bad_leaf_count}"
        )
    leaves = sorted(rng.sample(eligible, bad_leaf_count))
    base = corrupted_tuple_count // bad_leaf_count
    rem = corrupted_tuple_count % bad_leaf_count
    entries: list[dict[str, Any]] = []

    # For insert-only, we need synthetic keys that won't collide with real ones.
    # We use a negative counter starting at -(seed + 1).
    spurious_key_counter = -(abs(seed) + 1)

    for pos, leaf_id in enumerate(leaves):
        want = base + (1 if pos < rem else 0)
        keys = execute(
            conn,
            f"""
            SELECT ycsb_key
            FROM healthy.usertable
            WHERE {row_expr('healthy')} = %s
            ORDER BY ycsb_key
            LIMIT %s
            """,
            (leaf_id, want),
        )
        if len(keys) < want:
            raise RuntimeError(f"leaf {leaf_id} has {len(keys)} rows, need {want}")

        for i, row in enumerate(keys):
            k = int(row["ycsb_key"])
            if corruption_mode in ("paper-update-only", "update-only"):
                op = "update"
            elif corruption_mode == "delete-only":
                op = "delete"
            elif corruption_mode == "insert-only":
                op = "insert"
            elif corruption_mode == "mixed":
                bucket = i % 3
                op = ["update", "delete", "insert"][bucket]
            else:
                raise ValueError(f"unknown corruption_mode: {corruption_mode!r}")

            if op == "insert":
                # Use a synthetic spurious key for the damaged copy.
                entry_key = spurious_key_counter
                spurious_key_counter -= 1
            else:
                entry_key = k

            entries.append(
                {
                    "leaf_id": leaf_id,
                    "ycsb_key": entry_key,
                    "op": op,
                    # For mixed/insert we keep the "reference_key" so tests
                    # can verify leaf mapping of the original healthy row.
                    "reference_key": k,
                }
            )

    return {
        "experiment": experiment,
        "corruption_mode": corruption_mode,
        "tuple_count": tuple_count,
        "partitions": partitions,
        "leaves_per_partition": leaves_per_partition,
        "fanout": fanout,
        "seed": seed,
        "bad_leaves": leaves,
        "corruptions": entries,
    }


def validate_manifest_leaf_mapping(conn, manifest: dict[str, Any]) -> None:
    """Assert that every update/delete entry maps to its intended leaf.

    insert-only entries use synthetic keys that don't exist in the healthy
    table, so we validate their reference_key instead.
    """
    mismatches: list[dict[str, Any]] = []
    for entry in manifest["corruptions"]:
        check_key = entry["reference_key"]
        actual = scalar(
            conn,
            "SELECT merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, %s)",
            (check_key,),
        )
        if int(actual) != int(entry["leaf_id"]):
            mismatches.append(
                {
                    "ycsb_key": check_key,
                    "op": entry["op"],
                    "expected": entry["leaf_id"],
                    "actual": int(actual),
                }
            )
    if mismatches:
        raise RuntimeError(
            f"corruption manifest rows do not map to intended leaves: {mismatches[:5]}"
        )


def apply_corruption(conn, manifest: dict[str, Any]) -> None:
    """Apply all corruptions described in *manifest* to damaged.usertable."""
    seed = manifest["seed"]
    mode = manifest.get("corruption_mode", "paper-update-only")

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
            # Fetch the healthy reference row to base field values on,
            # then insert a spurious row with the synthetic key.
            ref_row = execute(
                conn,
                f"SELECT * FROM healthy.usertable WHERE ycsb_key = %s",
                (ref,),
            )
            if not ref_row:
                raise RuntimeError(f"reference key {ref} not found in healthy.usertable")
            vals = ref_row[0]
            # Build a spurious row: same fields as the reference row, but
            # with a synthetic (negative) ycsb_key.
            execute(
                conn,
                "INSERT INTO damaged.usertable VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                tuple([key] + [vals[f] for f in FIELDS]),
            )
        else:
            raise ValueError(f"unknown op {op!r} in manifest entry")
