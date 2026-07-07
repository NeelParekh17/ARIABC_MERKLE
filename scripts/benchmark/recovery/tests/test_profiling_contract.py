from __future__ import annotations

import sys
from pathlib import Path
from argparse import Namespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

import run_merkle_recovery_benchmark as bench
import package_recovery_artifacts
from run_merkle_recovery_benchmark import (
    PROFILE_OPERATION_FIELDS,
    _run_deep_diagnostics,
    _series_for_profile,
    recovery_run_id,
    validate_backend_profile_stats,
    validate_geometry,
)
from merkle_recovery.profiling import (
    group_profile_rows_with_denominators,
    group_profile_rows_with_fraction,
    validate_profile_invariants,
)
from merkle_recovery.metrics import Metrics


def test_profile_invariants_and_summary():
    phase = {
        "tree_localisation_ms": 2.0,
        "candidate_row_fetch_ms": 4.0,
        "row_comparison_ms": 1.0,
        "repair_write_ms": 3.0,
        "targeted_post_repair_confirmation_ms": 1.0,
    }
    ops = [
        {"run_id": "r1", "stage": "localisation", "operation": "root_hashes_healthy", "client_wall_ms": "1.0", "rows_returned": 10},
        {"run_id": "r1", "stage": "localisation", "operation": "root_hashes_damaged", "client_wall_ms": "1.0", "rows_returned": 10},
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_healthy", "client_wall_ms": "2.0", "rows_returned": 5},
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_damaged", "client_wall_ms": "2.0", "rows_returned": 5},
        {"run_id": "r1", "stage": "comparison", "operation": "key_set_build_cpu", "client_wall_ms": "0.5", "rows_returned": 10},
        {"run_id": "r1", "stage": "comparison", "operation": "row_payload_comparison_cpu", "client_wall_ms": "0.5", "rows_returned": 10},
        {"run_id": "r1", "stage": "repair", "operation": "insert_dml", "client_wall_ms": "1.0", "rows_returned": 0},
        {"run_id": "r1", "stage": "repair", "operation": "update_dml", "client_wall_ms": "1.0", "rows_returned": 0},
        {"run_id": "r1", "stage": "repair", "operation": "delete_dml", "client_wall_ms": "1.0", "rows_returned": 0},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_healthy", "client_wall_ms": "0.4", "rows_returned": 5},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_damaged", "client_wall_ms": "0.4", "rows_returned": 5},
    ]
    reasons = validate_profile_invariants(
        phase=phase,
        operations=ops,
        bad_leaf_count=1,
        run_id="r1",
        tolerance_ms=10.0,
    )
    assert reasons == []

    summary = group_profile_rows_with_fraction(ops, restore_repair_ms=10.0)
    assert any(row["stage"] == "repair" for row in summary)
    assert all("fraction_restore_repair_ms" in row for row in summary)


def test_confirmation_profile_sum_is_validated():
    phase = {
        "tree_localisation_ms": 0.0,
        "candidate_row_fetch_ms": 0.0,
        "repair_write_ms": 0.0,
        "targeted_post_repair_confirmation_ms": 10.0,
    }
    ops = [
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_healthy", "client_wall_ms": "0.4", "rows_returned": 5},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_damaged", "client_wall_ms": "0.4", "rows_returned": 5},
    ]
    reasons = validate_profile_invariants(
        phase=phase,
        operations=ops,
        bad_leaf_count=1,
        run_id="r1",
        tolerance_ms=1.0,
    )
    assert any("targeted confirmation profile sum" in reason for reason in reasons)


def test_profile_summary_can_group_by_geometry():
    rows = [
        {
            "run_id": "r-l16",
            "experiment": "recovery-scaling-diagnosis",
            "tuple_count": 5000000,
            "partitions": 200,
            "leaves_per_partition": 16,
            "fanout": 2,
            "profile_label": "baseline_l16",
            "bad_leaf_count": 10,
            "corrupted_tuple_count": 300,
            "stage": "candidate_fetch",
            "operation": "leaf_fetch_healthy",
            "client_wall_ms": "10.0",
            "rows_returned": 100,
        },
        {
            "run_id": "r-l128",
            "experiment": "recovery-scaling-diagnosis",
            "tuple_count": 5000000,
            "partitions": 200,
            "leaves_per_partition": 128,
            "fanout": 2,
            "profile_label": "preprovisioned_l128",
            "bad_leaf_count": 10,
            "corrupted_tuple_count": 300,
            "stage": "candidate_fetch",
            "operation": "leaf_fetch_healthy",
            "client_wall_ms": "2.0",
            "rows_returned": 20,
        },
    ]
    summary = group_profile_rows_with_fraction(
        rows,
        restore_repair_ms=12.0,
        group_keys=("profile_label", "tuple_count", "leaves_per_partition"),
    )
    assert {(row["profile_label"], row["leaves_per_partition"]) for row in summary} == {
        ("baseline_l16", 16),
        ("preprovisioned_l128", 128),
    }


def test_per_run_fractions_use_matching_run_denominator():
    rows = [
        {"run_id": "r1", "manifest_sha256": "m1", "stage": "repair", "operation": "update", "client_wall_ms": "10.0", "rows_returned": 0},
        {"run_id": "r2", "manifest_sha256": "m2", "stage": "repair", "operation": "update", "client_wall_ms": "5.0", "rows_returned": 0},
    ]
    summary = group_profile_rows_with_denominators(
        rows,
        {("r1", "m1"): 20.0, ("r2", "m2"): 10.0},
        group_keys=("run_id", "manifest_sha256"),
    )
    assert {row["run_id"]: row["fraction_restore_repair_ms"] for row in summary} == {
        "r1": "0.500000",
        "r2": "0.500000",
    }


def test_by_geometry_fractions_use_geometry_denominator():
    rows = [
        {"profile_label": "l16", "tuple_count": 1_000_000, "stage": "repair", "operation": "update", "client_wall_ms": "10.0", "rows_returned": 0},
        {"profile_label": "l128", "tuple_count": 1_000_000, "stage": "repair", "operation": "update", "client_wall_ms": "2.0", "rows_returned": 0},
    ]
    summary = group_profile_rows_with_denominators(
        rows,
        {("l16", 1_000_000): 20.0, ("l128", 1_000_000): 4.0},
        group_keys=("profile_label", "tuple_count"),
    )
    assert {row["profile_label"]: row["fraction_restore_repair_ms"] for row in summary} == {
        "l16": "0.500000",
        "l128": "0.500000",
    }


def test_profile_operation_header_has_no_duplicate_profile_label():
    assert len(PROFILE_OPERATION_FIELDS) == len(set(PROFILE_OPERATION_FIELDS))
    assert PROFILE_OPERATION_FIELDS.count("profile_label") == 1
    assert "manifest_sha256" in PROFILE_OPERATION_FIELDS


def test_packager_allows_profiling_artifacts():
    for name in [
        "profile_operations.csv",
        "profile_summary.csv",
        "profile_summary_per_run.csv",
        "profile_summary_by_geometry.csv",
        "merkle_backend_profile.csv",
        "profiling_report.md",
    ]:
        assert name in package_recovery_artifacts.ALLOWED


def test_backend_profile_update_only_invariants_require_row_hash_and_ns_time():
    cfg = {"partitions": 200, "leaves_per_partition": 16, "fanout": 2}
    counters = {"child_hash_sql_calls": 40, "child_hash_nodes_read": 80}
    post = {
        "targeted_confirmation_child_hash_sql_calls": 40,
        "targeted_confirmation_child_hash_nodes_read": 80,
    }
    good_backend = {
        "root_hash_helper_calls": 4,
        "root_hash_nodes_returned": 800,
        "child_hash_helper_calls": 80,
        "child_hash_nodes_returned": 160,
        "row_hash_compute_calls": 600,
        "tree_path_update_calls": 600,
        "tree_path_nodes_touched": 3000,
        "tree_path_update_ns": 1,
    }
    assert validate_backend_profile_stats(
        good_backend,
        cfg,
        counters,
        post,
        rows_updated=300,
        rows_inserted=0,
        rows_deleted=0,
    ) == []

    bad_backend = dict(good_backend)
    bad_backend["row_hash_compute_calls"] = 0
    bad_backend["tree_path_update_ns"] = 0
    reasons = validate_backend_profile_stats(
        bad_backend,
        cfg,
        counters,
        post,
        rows_updated=300,
        rows_inserted=0,
        rows_deleted=0,
    )
    assert any("row_hash_compute_calls" in reason for reason in reasons)
    assert any("tree_path_update_ns" in reason for reason in reasons)


def test_backend_profile_depth_expectations_for_static_geometries():
    for leaves_per_partition, expected_depth in [(16, 5), (32, 6), (64, 7), (128, 8)]:
        cfg = {"partitions": 200, "leaves_per_partition": leaves_per_partition, "fanout": 2}
        backend = {
            "root_hash_helper_calls": 4,
            "root_hash_nodes_returned": 800,
            "child_hash_helper_calls": 0,
            "child_hash_nodes_returned": 0,
            "row_hash_compute_calls": 600,
            "tree_path_update_calls": 600,
            "tree_path_nodes_touched": 600 * expected_depth,
            "tree_path_update_ns": 1,
        }
        assert validate_backend_profile_stats(
            backend,
            cfg,
            {},
            {},
            rows_updated=300,
            rows_inserted=0,
            rows_deleted=0,
        ) == []


def test_invalid_geometry_fails_before_database_construction():
    with pytest.raises(ValueError, match="partitions"):
        validate_geometry(0, 16, 2)
    with pytest.raises(ValueError, match="fanout"):
        validate_geometry(200, 16, 1)
    with pytest.raises(ValueError, match="leaves_per_partition"):
        validate_geometry(200, 0, 2)
    with pytest.raises(ValueError, match="exact power"):
        validate_geometry(200, 12, 2)
    validate_geometry(200, 128, 2)


def test_manual_geometry_override_gets_truthful_label():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        leaves_per_partition=64,
        fanout=2,
        partitions=200,
        tuple_count=1_000_000,
        bad_leaf_count=10,
        geometry_label="baseline_l16",
    )
    specs = _series_for_profile(args, object())
    assert {spec["geometry_label"] for spec in specs} == {"manual-p200-l64-f2"}


def test_deep_diagnostics_orders_candidate_repair_confirmation(monkeypatch, tmp_path):
    state = {"repaired": False, "repairs": 0}
    calls: list[tuple[str, str, bool]] = []

    def fake_show_setting(conn, name):
        return "off"

    def fake_reset(conn, cfg):
        state["repaired"] = False

    def fake_apply(conn, manifest):
        state["repaired"] = False

    def fake_repair_leaf(conn, leaf_id, *, phase, profiler=None):
        state["repaired"] = True
        state["repairs"] += 1
        return {}, {}, 0, 1, 0

    def fake_plan(conn, schema, leaf_id):
        calls.append((schema, str(leaf_id), state["repaired"]))
        return {"Plan": {"Actual Rows": 0 if state["repaired"] else 1}, "Planning Time": 0.1, "Execution Time": 0.2}

    monkeypatch.setattr(bench, "show_setting", fake_show_setting)
    monkeypatch.setattr(bench, "reset_damaged_from_healthy", fake_reset)
    monkeypatch.setattr(bench, "apply_corruption", fake_apply)
    monkeypatch.setattr(bench, "repair_leaf", fake_repair_leaf)
    import merkle_recovery.repair as repair_mod
    monkeypatch.setattr(repair_mod, "lookup_explain_plan_json", fake_plan)

    rows = _run_deep_diagnostics(
        object(),
        {
            "experiment": "diagnosis",
            "tuple_count": 100,
            "partitions": 10,
            "leaves_per_partition": 16,
            "fanout": 2,
            "bad_leaves": [3],
            "corruptions": [1],
        },
        tmp_path,
        run_id="r1",
        tuple_count=100,
        partitions=10,
        leaves_per_partition=16,
        fanout=2,
        profile_label="l16",
        repetition=0,
    )
    assert state["repairs"] == 1
    assert [row["kind"] for row in rows] == ["candidate", "candidate", "confirmation", "confirmation"]
    assert [row["plan_order"] for row in rows] == [1, 1, 2, 2]
    assert all(row["diagnostic_cache_mode"] == "post_recovery_warm" for row in rows)
    assert calls[:2] == [("healthy", "3", False), ("damaged", "3", False)]
    assert calls[2:] == [("healthy", "3", True), ("damaged", "3", True)]


def test_deep_diagnostics_runs_once_per_manifest_not_per_repetition(monkeypatch, tmp_path):
    calls = {"deep": 0}

    monkeypatch.setattr(bench, "emit_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr(bench, "reset_damaged_from_healthy", lambda *args, **kwargs: None)
    monkeypatch.setattr(bench, "apply_corruption", lambda *args, **kwargs: None)
    monkeypatch.setattr(bench, "run_planner_preflight", lambda *args, **kwargs: ({}, []))
    monkeypatch.setattr(bench, "execute", lambda *args, **kwargs: [])
    monkeypatch.setattr(bench, "validate_profile_invariants", lambda *args, **kwargs: [])

    def fake_repair_merkle(conn, manifest, tuple_count, repetition, planner_results, schema_rows_out, **kwargs):
        run_id = recovery_run_id(manifest, repetition, kwargs["profile_label"])
        return Metrics(
            run_id=run_id,
            experiment=manifest["experiment"],
            method="merkle",
            tuple_count=tuple_count,
            partitions=manifest["partitions"],
            leaves_per_partition=manifest["leaves_per_partition"],
            fanout=manifest["fanout"],
            bad_leaf_count=len(manifest["bad_leaves"]),
            corrupted_tuple_count=len(manifest["corruptions"]),
            repetition=repetition,
            profile_label=kwargs["profile_label"],
            profiling_mode=kwargs["profiling_mode"],
            restore_repair_ms=10.0,
            phase={
                "tree_localisation_ms": 0.0,
                "candidate_row_fetch_ms": 0.0,
                "row_comparison_ms": 0.0,
                "repair_write_ms": 0.0,
                "targeted_post_repair_confirmation_ms": 0.0,
            },
            counters={"manifest_sha256": bench.manifest_sha256(manifest)},
        )

    def fake_deep(*args, **kwargs):
        calls["deep"] += 1
        return [{"run_id": kwargs["run_id"], "kind": "candidate"}]

    monkeypatch.setattr(bench, "repair_merkle", fake_repair_merkle)
    monkeypatch.setattr(bench, "_run_deep_diagnostics", fake_deep)

    metrics = bench.run_one_manifest(
        object(),
        {
            "experiment": "diagnosis",
            "tuple_count": 100,
            "partitions": 10,
            "leaves_per_partition": 16,
            "fanout": 2,
            "bad_leaves": [3],
            "corruptions": [1],
            "corruption_mode": "paper-update-only",
        },
        3,
        [],
        [],
        [],
        [],
        [],
        tmp_path,
        {"completed_runs": 0, "total_runs": 3},
        profile_label="l16",
        profiling_mode="deep",
    )
    assert len(metrics) == 3
    assert calls["deep"] == 1


def test_diagnosis_run_ids_are_unique_across_geometries():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        leaves_per_partition=None,
        fanout=None,
        partitions=None,
        tuple_count=5000000,
        bad_leaf_count=None,
        geometry_label=None,
    )
    specs = _series_for_profile(args, object())
    manifests = [
        {
            "experiment": spec["experiment"],
            "tuple_count": spec["tuple_count"],
            "partitions": spec["partitions"],
            "leaves_per_partition": spec["leaves_per_partition"],
            "fanout": spec["fanout"],
            "bad_leaves": list(range(spec["bad_leaf_count"])),
            "corruptions": list(range(spec["corrupted_tuple_count"])),
        }
        for spec in specs
    ]
    run_ids = [
        recovery_run_id(manifest, 0, spec["geometry_label"])
        for manifest, spec in zip(manifests, specs)
    ]
    assert len(run_ids) == len(set(run_ids))


def test_profile_report_contract_markers_for_l16_l128_tables():
    report = "\n".join(
        [
            "## Phase Medians And P95",
            "| baseline_l16 | 1000000 | 200 | 16 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.000 | 1.000 |",
            "| baseline_l16 | 3000000 | 200 | 16 | 2 | 10 | 300 | candidate_row_fetch_ms | 2.000 | 2.000 |",
            "| baseline_l16 | 5000000 | 200 | 16 | 2 | 10 | 300 | candidate_row_fetch_ms | 3.000 | 3.000 |",
            "| preprovisioned_l128 | 1000000 | 200 | 128 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.000 | 1.000 |",
            "| preprovisioned_l128 | 3000000 | 200 | 128 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.100 | 1.100 |",
            "| preprovisioned_l128 | 5000000 | 200 | 128 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.200 | 1.200 |",
            "## Growth Ratios",
            "- highest-growing phase: `candidate_row_fetch_ms`",
        ]
    )
    for label, leaf_count in (("baseline_l16", 16), ("preprovisioned_l128", 128)):
        for tuple_count in (1_000_000, 3_000_000, 5_000_000):
            assert f"| {label} | {tuple_count} | 200 | {leaf_count} |" in report
    assert "highest-growing phase" in report


def test_manual_geometry_requires_label():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        leaves_per_partition=16,
        fanout=None,
        partitions=None,
        tuple_count=1_000_000,
        bad_leaf_count=None,
        geometry_label=None,
    )
    with pytest.raises(ValueError, match="manual geometry overrides require --geometry-label"):
        _series_for_profile(args, object())


def test_manual_geometry_with_label_produces_one_run():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        leaves_per_partition=16,
        fanout=2,
        partitions=200,
        tuple_count=1_000_000,
        bad_leaf_count=None,
        geometry_label="baseline_l16",
    )
    series = _series_for_profile(args, object())
    assert len(series) == 1
    assert series[0]["tuple_count"] == 1_000_000
    assert series[0]["partitions"] == 200
    assert series[0]["leaves_per_partition"] == 16
    assert series[0]["fanout"] == 2


def test_full_recovery_phase_reconciliation():
    m = Metrics(
        run_id="test_run",
        experiment="test_exp",
        method="merkle",
        tuple_count=1000,
        partitions=10,
        leaves_per_partition=16,
        fanout=2,
        bad_leaf_count=1,
        corrupted_tuple_count=10,
        repetition=0,
    )
    m.restore_repair_ms = 100.0
    m.phase["tree_localisation_ms"] = 10.0
    m.phase["candidate_row_fetch_ms"] = 20.0
    m.phase["row_comparison_ms"] = 5.0
    m.phase["repair_write_ms"] = 15.0
    m.phase["targeted_post_repair_confirmation_ms"] = 10.0
    m.phase["recovery_observability_ms"] = 5.0

    total_recovery_ms = m.restore_repair_ms
    accounted_ms = (
        m.phase.get("tree_localisation_ms", 0.0)
        + m.phase.get("candidate_row_fetch_ms", 0.0)
        + m.phase.get("row_comparison_ms", 0.0)
        + m.phase.get("repair_write_ms", 0.0)
        + m.phase.get("targeted_post_repair_confirmation_ms", 0.0)
        + m.phase.get("recovery_observability_ms", 0.0)
    )
    m.phase["recovery_orchestration_ms"] = max(0.0, total_recovery_ms - accounted_ms)

    assert m.phase["recovery_orchestration_ms"] == 35.0

    total_reconciled = (
        m.phase["tree_localisation_ms"]
        + m.phase["candidate_row_fetch_ms"]
        + m.phase["row_comparison_ms"]
        + m.phase["repair_write_ms"]
        + m.phase["targeted_post_repair_confirmation_ms"]
        + m.phase["recovery_observability_ms"]
        + m.phase["recovery_orchestration_ms"]
    )
    assert abs(total_reconciled - m.restore_repair_ms) < 1e-6
