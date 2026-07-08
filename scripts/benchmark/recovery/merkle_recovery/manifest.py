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
                   **guaranteed** to hash to the same Merkle leaf as their
                   reference row, so ``merkle_bucket_for_key(entry[ycsb_key])
                   == entry[leaf_id]`` holds for every entry including inserts.
mixed              Deterministic 1/3–1/3–1/3 split of update / delete /
                   insert across **all** entries (global index, not per-leaf).

Invariant
---------
For every manifest entry, regardless of ``op``:

    merkle_bucket_for_key('healthy.usertable_merkle_idx', entry['ycsb_key'])
    == entry['leaf_id']

validate_manifest_leaf_mapping() asserts this for **all** entries,
including inserts.
"""

from __future__ import annotations

import random
from typing import Any

from .config import ALL_COLUMNS, FIELDS
from .db import execute, scalar


def row_expr(schema: str) -> str:
    return f"merkle_bucket_for_key('{schema}.usertable_merkle_idx'::regclass, ycsb_key)"


def _find_spurious_key_for_leaf(
    conn,
    target_leaf_id: int,
    rng: random.Random,
    used_keys: set[int],
    *,
    batch_size: int = 10_000,
    max_attempts: int = 1_000_000,
) -> int:
    """Return a negative key whose Merkle bucket equals *target_leaf_id*.

    The key is chosen deterministically via *rng* and guaranteed not to appear
    in *used_keys*.  Raises RuntimeError after *max_attempts* misses.
    """
    for _ in range(max_attempts // batch_size):
        # Generate a candidate batch in the range [-2^31, -1] using the RNG so the
        # sequence is fully reproducible from the seed.
        candidates = [-(rng.randint(1, 2**31 - 1)) for _ in range(batch_size)]
        candidates = [c for c in candidates if c not in used_keys]
        if not candidates:
            continue

        rows = execute(
            conn,
            """
            SELECT c AS candidate
            FROM unnest(%s::bigint[]) AS c
            WHERE merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, c) = %s
            LIMIT 1
            """,
            (candidates, target_leaf_id),
        )
        if rows:
            candidate = int(rows[0]["candidate"])
            used_keys.add(candidate)
            return candidate

    raise RuntimeError(
        f"could not find a spurious key mapping to leaf {target_leaf_id} "
        f"after {max_attempts} attempts"
    )


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
    forced_bad_leaves: list[int] | None = None,
) -> dict[str, Any]:
    """Select *bad_leaf_count* non-empty leaves and pick row keys to corrupt.

    The manifest records every planned corruption with its leaf_id,
    ycsb_key, and the corruption operation type (``op``).

    For every entry:  merkle_bucket_for_key(entry['ycsb_key']) == entry['leaf_id']
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
    base = corrupted_tuple_count // bad_leaf_count
    rem = corrupted_tuple_count % bad_leaf_count
    min_required_rows = base + (1 if rem > 0 else 0)

    occ_map = {int(r["leaf_id"]): int(r["tuple_count"]) for r in occ}
    eligible = [leaf_id for leaf_id, count in occ_map.items() if count >= min_required_rows]

    if forced_bad_leaves is not None:
        leaves = sorted(int(v) for v in forced_bad_leaves)
        missing = [
            leaf for leaf in leaves
            if leaf not in occ_map or occ_map[leaf] < min_required_rows
        ]
        if missing:
            raise RuntimeError(
                f"forced bad leaves are not eligible: {missing[:10]} "
                f"need >= {min_required_rows} rows per leaf"
            )
    else:
        if len(eligible) < bad_leaf_count:
            raise RuntimeError(
                f"only {len(eligible)} leaves have >= {min_required_rows} rows, need {bad_leaf_count}"
            )
        leaves = sorted(rng.sample(eligible, bad_leaf_count))

    selected_leaf_capacities = {leaf_id: occ_map[leaf_id] for leaf_id in leaves}
    selected_bad_leaf_row_capacity = sum(selected_leaf_capacities.values())
    entries: list[dict[str, Any]] = []

    # Track all synthetic keys globally to prevent duplicates.
    used_spurious_keys: set[int] = set()

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

        for row in keys:
            k = int(row["ycsb_key"])

            # ── choose op ──────────────────────────────────────────────────
            if corruption_mode in ("paper-update-only", "update-only"):
                op = "update"
            elif corruption_mode == "delete-only":
                op = "delete"
            elif corruption_mode == "insert-only":
                op = "insert"
            elif corruption_mode == "mixed":
                # Use the global entry count so the op assignment is
                # independent of per-leaf position and always covers all
                # three operation types when corrupted_tuple_count >= 3.
                global_entry_index = len(entries)
                op = ("update", "delete", "insert")[global_entry_index % 3]
            else:
                raise ValueError(f"unknown corruption_mode: {corruption_mode!r}")

            # ── choose ycsb_key ────────────────────────────────────────────
            if op == "insert":
                # The spurious key must hash to *this* leaf so that the
                # Merkle descent localises the correct leaf and the audit
                # invariant holds for every entry.
                entry_key = _find_spurious_key_for_leaf(
                    conn, leaf_id, rng, used_spurious_keys
                )
            else:
                entry_key = k

            entries.append(
                {
                    "leaf_id": leaf_id,
                    "ycsb_key": entry_key,
                    "op": op,
                    # reference_key is the healthy row whose field values are
                    # copied into the spurious insert row.  Not used for leaf
                    # mapping validation — entry['ycsb_key'] is used for that.
                    "reference_key": k,
                }
            )

    import hashlib
    import json

    # Blocker 4: Add fair-comparison provenance
    selection_dict = {
        "tuple_count": tuple_count,
        "partitions": partitions,
        "leaves_per_partition": leaves_per_partition,
        "bad_leaf_count": bad_leaf_count,
        "corrupted_tuple_count": corrupted_tuple_count,
        "seed": seed,
        "bad_leaves": leaves,
        "corruptions": [{"ycsb_key": e["ycsb_key"], "op": e["op"]} for e in entries],
    }
    selection_json = json.dumps(selection_dict, sort_keys=True)
    corruption_selection_sha256 = hashlib.sha256(selection_json.encode('utf-8')).hexdigest()

    # bad_leaf_selection_sha256: excludes tuple_count and corruptions keys
    leaf_selection_dict = {
        "profile_label": experiment,
        "partitions": partitions,
        "leaves_per_partition": leaves_per_partition,
        "fanout": fanout,
        "bad_leaf_count": bad_leaf_count,
        "bad_leaves": leaves,
        "seed": seed,
    }
    leaf_selection_json = json.dumps(leaf_selection_dict, sort_keys=True)
    bad_leaf_selection_sha256 = hashlib.sha256(leaf_selection_json.encode('utf-8')).hexdigest()

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
        "corrupted_tuple_count": corrupted_tuple_count,
        "required_rows_per_bad_leaf": min_required_rows,
        "selected_bad_leaf_row_capacity": selected_bad_leaf_row_capacity,
        "selected_leaf_capacities": selected_leaf_capacities,
        "corruption_selection_sha256": corruption_selection_sha256,
        "bad_leaf_selection_sha256": bad_leaf_selection_sha256,
    }


def validate_manifest_leaf_mapping(conn, manifest: dict[str, Any]) -> None:
    """Assert that every entry's ycsb_key maps to its intended leaf.

    This validates ALL entries including inserts — the invariant is:
        merkle_bucket_for_key(entry['ycsb_key']) == entry['leaf_id']
    for every entry regardless of op.
    """
    mismatches: list[dict[str, Any]] = []
    for entry in manifest["corruptions"]:
        # Always validate the actual entry key, not the reference_key.
        check_key = entry["ycsb_key"]
        actual = scalar(
            conn,
            "SELECT merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, %s)",
            (check_key,),
        )
        if actual is None or int(actual) != int(entry["leaf_id"]):
            mismatches.append(
                {
                    "ycsb_key": check_key,
                    "op": entry["op"],
                    "expected": entry["leaf_id"],
                    "actual": int(actual) if actual is not None else None,
                }
            )
    if mismatches:
        raise RuntimeError(
            f"corruption manifest rows do not map to intended leaves: {mismatches[:5]}"
        )


def apply_corruption(conn, manifest: dict[str, Any]) -> None:
    """Apply all corruptions described in *manifest* to damaged.usertable."""
    seed = manifest["seed"]

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
            # Fetch the healthy reference row to copy field values from, then
            # insert a spurious row with the leaf-mapped synthetic key.
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
                tuple([key] + [vals[f] for f in FIELDS]),
            )
        else:
            raise ValueError(f"unknown op {op!r} in manifest entry")
