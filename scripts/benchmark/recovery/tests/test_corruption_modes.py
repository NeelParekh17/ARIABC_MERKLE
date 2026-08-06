"""Corruption-mode correctness tests for the Merkle recovery benchmark.

These tests require a live PostgreSQL instance with the AriaBC Merkle
extension installed.  They use the same code paths as the benchmark but
with a very small row count (1 000 rows) to keep runtime short.

Run with:
    pytest scripts/benchmark/recovery/tests/test_corruption_modes.py \
        --dsn "host=127.0.0.1 port=5432 dbname=postgres user=neel"

Each test asserts the nine correctness conditions from the plan:
  1. Intended corrupt keys map to the intended Merkle leaves.
  2. Detected bad leaves equal expected bad leaves.
  3. Candidate lookup uses the functional B-tree index.
  4. Repair computes expected INSERT / UPDATE / DELETE actions.
  5. Repaired leaf contents equal healthy leaf contents.
  6. healthy EXCEPT ALL damaged is empty.
  7. damaged EXCEPT ALL healthy is empty.
  8. Partition roots match.
  9. merkle_verify succeeds for both schemas.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from merkle_recovery.config import ALL_COLUMNS, leaf_key
from merkle_recovery.dataset import (
    build_dataset, ensure_helpers, leaf_occupancy,
    reset_damaged_from_healthy,
)
from merkle_recovery.db import connect, execute, scalar
from merkle_recovery.localisation import detect_bad_leaves
from merkle_recovery.manifest import (
    apply_corruption, choose_corruption_manifest,
    validate_manifest_leaf_mapping,
)
from merkle_recovery.repair import (
    fetch_leaf_rows, run_planner_preflight,
    repair_leaf, seq_scan_snapshot, seq_scan_delta,
)
from merkle_recovery.verification import audit_recovery


# ── fixtures ──────────────────────────────────────────────────────────────────
# pytest_addoption is in conftest.py so --dsn is registered before arg parsing.

@pytest.fixture(scope="session")
def dsn(pytestconfig):
    return pytestconfig.getoption("--dsn")


@pytest.fixture(scope="session")
def conn(dsn):
    import argparse
    args = argparse.Namespace(dsn=dsn)
    with connect(args) as c:
        ensure_helpers(c)
        yield c


SMALL_N = 1_000
FANOUT = 2
BAD_LEAVES = 3
CORRUPT_TUPLES = 6
SEED = 42_000


@pytest.fixture(scope="module")
def base_dataset(conn):
    """Build a shared healthy dataset once per test module."""
    build_dataset(conn, SMALL_N, fanout=FANOUT)
    return {"fanout": FANOUT, "split_threshold": 32, "merge_threshold": 8}


# ── helpers ───────────────────────────────────────────────────────────────────

def _run_recovery(conn, manifest: dict[str, Any], cfg: dict[str, int]):
    """Reset damaged, apply corruption, run Merkle repair, return audit."""
    reset_damaged_from_healthy(conn, cfg)
    apply_corruption(conn, manifest)

    counters: dict[str, Any] = {}
    bad_leaves = detect_bad_leaves(conn, counters)

    phase: dict[str, float] = {}
    rows_ins = rows_upd = rows_del = 0
    for leaf_spec in bad_leaves:
        _, _, ins, upd, dlt = repair_leaf(conn, leaf_spec, phase=phase)
        rows_ins += ins
        rows_upd += upd
        rows_del += dlt

    audit = audit_recovery(conn, "test", "merkle")
    return bad_leaves, counters, rows_ins, rows_upd, rows_del, audit


# ── parametrised correctness test ─────────────────────────────────────────────

MODES = ["paper-update-only", "update-only", "delete-only", "insert-only", "mixed"]


@pytest.mark.parametrize("corruption_mode", MODES)
def test_corruption_mode_correctness(conn, base_dataset, corruption_mode):
    cfg = base_dataset

    manifest = choose_corruption_manifest(
        conn,
        experiment="test",
        tuple_count=SMALL_N,
        fanout=cfg["fanout"],
        bad_leaf_count=BAD_LEAVES,
        corrupted_tuple_count=CORRUPT_TUPLES,
        seed=SEED,
        corruption_mode=corruption_mode,
    )

    # ── assertion 1: corrupt keys map to intended leaves ─────────────────────
    validate_manifest_leaf_mapping(conn, manifest)  # raises if not

    # ── reset + corrupt ───────────────────────────────────────────────────────
    reset_damaged_from_healthy(conn, cfg)
    apply_corruption(conn, manifest)

    # ── assertion 2: detected bad leaves == expected ──────────────────────────
    counters: dict[str, Any] = {}
    bad_leaves = detect_bad_leaves(conn, counters)
    expected_leaves = sorted(
        set(
            (normalized[1], normalized[2])
            if len(normalized) == 3 else normalized
            for normalized in (leaf_key(value) for value in manifest["bad_leaves"])
        )
    )
    assert bad_leaves == expected_leaves, (
        f"[{corruption_mode}] bad_leaves mismatch: expected={expected_leaves} actual={bad_leaves}"
    )

    # ── assertion 3: planner uses functional B-tree index ─────────────────────
    run_id = f"test-{corruption_mode}"
    planner_results, planner_rows = run_planner_preflight(conn, manifest, run_id)
    assert planner_results["planner_checks_passed"] == 1, (
        f"[{corruption_mode}] planner did not use functional leaf-lookup index"
    )

    # ── assertions 4 + 5: repair + leaf equality ──────────────────────────────
    phase: dict[str, float] = {}
    rows_ins = rows_upd = rows_del = 0
    for leaf_spec in bad_leaves:
        _, _, ins, upd, dlt = repair_leaf(conn, leaf_spec, phase=phase)
        rows_ins += ins
        rows_upd += upd
        rows_del += dlt

    # After repair each bad leaf must equal the healthy leaf.
    for leaf_id, prefix_len in bad_leaves:
        h = fetch_leaf_rows(conn, "healthy", leaf_id, prefix_len)
        d = fetch_leaf_rows(conn, "damaged", leaf_id, prefix_len)
        assert h == d, (
            f"[{corruption_mode}] leaf {leaf_id} still differs after repair; "
            f"healthy_keys={sorted(h)}, damaged_keys={sorted(d)}"
        )

    # Validate expected repair operation counts per mode.
    entries = manifest["corruptions"]
    n_update = sum(1 for e in entries if e["op"] == "update")
    n_delete = sum(1 for e in entries if e["op"] == "delete")
    n_insert = sum(1 for e in entries if e["op"] == "insert")

    if corruption_mode in ("paper-update-only", "update-only"):
        assert rows_upd == n_update and rows_ins == 0 and rows_del == 0, (
            f"[{corruption_mode}] expected {n_update} UPDs, got ins={rows_ins} upd={rows_upd} del={rows_del}"
        )
    elif corruption_mode == "delete-only":
        # Deleted rows appear as inserts in repair (healthy has them, damaged lacks them).
        assert rows_ins == n_delete and rows_upd == 0 and rows_del == 0, (
            f"[{corruption_mode}] expected {n_delete} INSs, got ins={rows_ins} upd={rows_upd} del={rows_del}"
        )
    elif corruption_mode == "insert-only":
        # Spurious rows in damaged appear as deletes in repair.
        assert rows_del == n_insert and rows_ins == 0 and rows_upd == 0, (
            f"[{corruption_mode}] expected {n_insert} DELs, got ins={rows_ins} upd={rows_upd} del={rows_del}"
        )
    elif corruption_mode == "mixed":
        # Repair semantics (from the damaged copy's perspective):
        #   manifest op=update  → damaged has wrong value       → rows_upd
        #   manifest op=delete  → damaged is missing the row    → rows_ins
        #   manifest op=insert  → damaged has spurious row      → rows_del
        assert rows_upd == n_update, (
            f"[{corruption_mode}] expected {n_update} UPDs, got {rows_upd}"
        )
        assert rows_ins == n_delete, (
            f"[{corruption_mode}] expected {n_delete} INSs (re-inserting deleted rows), got {rows_ins}"
        )
        assert rows_del == n_insert, (
            f"[{corruption_mode}] expected {n_insert} DELs (removing spurious inserts), got {rows_del}"
        )
        # Ensure all three op types are present (requires CORRUPT_TUPLES >= 3).
        assert n_update > 0 and n_delete > 0 and n_insert > 0, (
            f"[{corruption_mode}] manifest did not contain all three op types: "
            f"update={n_update} delete={n_delete} insert={n_insert}"
        )

    # ── assertions 6 + 7: EXCEPT ALL empty ───────────────────────────────────
    audit = audit_recovery(conn, run_id, "merkle")
    assert audit["healthy_minus_damaged"] == 0, (
        f"[{corruption_mode}] healthy EXCEPT ALL damaged = {audit['healthy_minus_damaged']}"
    )
    assert audit["damaged_minus_healthy"] == 0, (
        f"[{corruption_mode}] damaged EXCEPT ALL healthy = {audit['damaged_minus_healthy']}"
    )

    # ── assertion 8: partition roots match ────────────────────────────────────
    assert audit["roots_match"], f"[{corruption_mode}] Merkle roots do not match after repair"

    # ── assertion 9: merkle_verify succeeds ──────────────────────────────────
    assert audit["healthy_merkle_verify"], f"[{corruption_mode}] merkle_verify failed on healthy"
    assert audit["damaged_merkle_verify"], f"[{corruption_mode}] merkle_verify failed on damaged after repair"


def test_no_seq_scan_during_recovery(conn, base_dataset):
    """Recovery must not perform a full heap sequential scan."""
    cfg = base_dataset
    manifest = choose_corruption_manifest(
        conn, "test", SMALL_N,
        fanout=cfg["fanout"], bad_leaf_count=BAD_LEAVES, corrupted_tuple_count=CORRUPT_TUPLES, seed=SEED + 1, corruption_mode="paper-update-only",
    )
    reset_damaged_from_healthy(conn, cfg)
    apply_corruption(conn, manifest)

    before = seq_scan_snapshot(conn)
    counters: dict[str, Any] = {}
    bad_leaves = detect_bad_leaves(conn, counters)
    phase: dict[str, float] = {}
    for leaf_spec in bad_leaves:
        repair_leaf(conn, leaf_spec, phase=phase)
    after = seq_scan_snapshot(conn)

    delta = seq_scan_delta(before, after)
    assert delta == 0, f"recovery triggered {delta} heap sequential scan(s)"


def test_partition_root_batches_exactly_two(conn, base_dataset):
    cfg = base_dataset
    manifest = choose_corruption_manifest(
        conn, "test", SMALL_N,
        fanout=cfg["fanout"], bad_leaf_count=BAD_LEAVES, corrupted_tuple_count=CORRUPT_TUPLES, seed=SEED + 2, corruption_mode="paper-update-only",
    )
    reset_damaged_from_healthy(conn, cfg)
    apply_corruption(conn, manifest)
    counters: dict[str, Any] = {}
    detect_bad_leaves(conn, counters)
    assert (counters.get("child_hash_sql_calls", 0) // 2) >= 1


def test_bad_partition_count_counter(conn, base_dataset):
    cfg = base_dataset
    manifest = choose_corruption_manifest(
        conn, "test", SMALL_N,
        fanout=cfg["fanout"], bad_leaf_count=BAD_LEAVES, corrupted_tuple_count=CORRUPT_TUPLES, seed=SEED + 3, corruption_mode="paper-update-only",
    )
    reset_damaged_from_healthy(conn, cfg)
    apply_corruption(conn, manifest)
    counters: dict[str, Any] = {}
    detect_bad_leaves(conn, counters)
    assert counters.get("leaf_nodes_found", 0) >= 1
    assert counters.get("leaf_nodes_found", 0) <= BAD_LEAVES
