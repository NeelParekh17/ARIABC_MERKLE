"""Official-run smoke tests for fanout-width-sweep geometries.

These tests validate that the selected geometries can successfully
construct a real dataset, generate a 300-row corruption manifest,
repair correctly without heap scans, and pass all verification checks.
"""

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from merkle_recovery.dataset import build_dataset, reset_damaged_from_healthy, ensure_helpers
from merkle_recovery.db import connect, execute, scalar
from merkle_recovery.manifest import choose_corruption_manifest, validate_manifest_leaf_mapping, apply_corruption
from merkle_recovery.localisation import detect_bad_leaves
from merkle_recovery.repair import repair_leaf, seq_scan_snapshot, seq_scan_delta, fetch_leaf_rows
from merkle_recovery.verification import audit_recovery
from run_merkle_recovery_benchmark import validate_backend_profile_stats
import json

SMOKE_N = 5_000_000
K = 20
C = 300

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

@pytest.mark.integration
@pytest.mark.parametrize("f", [
    64,
    512,
    1024,
])
def test_official_smoke_geometry(conn, f):
    # 1. Build dataset
    build_dataset(conn, SMOKE_N, fanout=f)

    # 2. 300 distinct corruptions, 20 bad leaves
    manifest = choose_corruption_manifest(
        conn, "smoke", SMOKE_N, fanout=f, bad_leaf_count=K, corrupted_tuple_count=C,
        seed=1000 + f, corruption_mode="paper-update-only"
    )
    
    assert len(set(tuple(v) if isinstance(v, list) else v for v in manifest["bad_leaves"])) == K, f"Expected exactly {K} bad leaves"
    assert len(manifest["corruptions"]) == C, f"Expected exactly {C} corruptions"
    
    validate_manifest_leaf_mapping(conn, manifest)

    # Reset and apply
    reset_damaged_from_healthy(conn, {"fanout": f})
    apply_corruption(conn, manifest)

    before_seq = seq_scan_snapshot(conn)

    # Reset profiler using the correct benchmark API
    execute(conn, "SELECT merkle_recovery_profile_reset()")
    execute(conn, "SET merkle_recovery_profile_enabled = on")

    # 3. Initial localisation
    counters: dict[str, Any] = {}
    bad_leaves = detect_bad_leaves(conn, counters)
    assert len(bad_leaves) == K, f"Detection found {len(bad_leaves)} leaves, expected {K}"
    expected_leaves = sorted((bytes.fromhex(v[0]), int(v[1])) if isinstance(v, (list, tuple)) else v for v in manifest["bad_leaves"])
    assert bad_leaves == expected_leaves

    # 4. Repair
    phase: dict[str, float] = {}
    inserted = updated = deleted = 0
    for leaf_spec in bad_leaves:
        _, _, ins, upd, dlt = repair_leaf(conn, leaf_spec, phase=phase)
        inserted += ins
        updated += upd
        deleted += dlt
    
    assert inserted == 0, f"expected 0 inserted, got {inserted}"
    assert deleted == 0, f"expected 0 deleted, got {deleted}"
    assert updated == C, f"expected {C} updated, got {updated}"

    # 5. Targeted post-repair localisation
    post_repair_counters: dict[str, Any] = {}
    remaining_bad_leaves = detect_bad_leaves(
        conn,
        post_repair_counters,
        prefix="targeted_confirmation_",
        operation_prefix="confirmation_",
        stage_name="targeted_confirmation",
    )
    assert remaining_bad_leaves == [], "targeted post-repair confirmation failed, leaves still damaged"

    # 6. Confirmation fetches for repaired leaves
    for leaf_id, prefix_len in bad_leaves:
        h = fetch_leaf_rows(conn, "healthy", leaf_id, prefix_len)
        d = fetch_leaf_rows(conn, "damaged", leaf_id, prefix_len)
        assert h == d, f"leaf {leaf_id} mismatch after repair"

    after_seq = seq_scan_snapshot(conn)
    
    # Use the correct benchmark API
    backend_json = scalar(conn, "SELECT merkle_recovery_profile_stats()")
    backend_prof = json.loads(backend_json) if backend_json else {}

    # 7. Verification checks
    audit = audit_recovery(conn, f"smoke_f{f}", "merkle")
    assert audit["roots_match"], "Merkle roots do not match after repair"
    assert audit["healthy_minus_damaged"] == 0, "Healthy EXCEPT Damaged is not empty"
    assert audit["damaged_minus_healthy"] == 0, "Damaged EXCEPT Healthy is not empty"
    assert audit["healthy_merkle_verify"], "Healthy merkle_verify failed"
    assert audit["damaged_merkle_verify"], "Damaged merkle_verify failed"

    # Recovery sequential scans remain zero
    delta_seq = seq_scan_delta(before_seq, after_seq)
    assert delta_seq == 0, f"Recovery triggered {delta_seq} heap sequential scans"

    # 8. Backend profiler validation with post_repair_counters
    discrepancies = validate_backend_profile_stats(
        backend_prof,
        {"fanout": f},
        counters,
        post_repair_counters,
        rows_updated=C,
        rows_inserted=0,
        rows_deleted=0,
    )
    assert not discrepancies, f"Profiler invariants failed: {discrepancies}"
