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


def test_profile_invariants_accept_two_batched_leaf_fetches():
    phase = {
        "tree_localisation_ms": 0.0,
        "candidate_row_fetch_ms": 4.0,
        "repair_write_ms": 0.0,
        "targeted_post_repair_confirmation_ms": 2.0,
    }
    ops = [
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_healthy", "client_wall_ms": "2.0"},
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_damaged", "client_wall_ms": "2.0"},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_healthy", "client_wall_ms": "1.0"},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_damaged", "client_wall_ms": "1.0"},
    ]
    assert validate_profile_invariants(
        phase=phase,
        operations=ops,
        bad_leaf_count=10,
        run_id="r1",
        tolerance_ms=0.1,
    ) == []


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
            "fanout": 2,
            "split_threshold": 32,
            "merge_threshold": 8,
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
            "fanout": 2,
            "split_threshold": 32,
            "merge_threshold": 8,
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
        group_keys=("profile_label", "tuple_count", "split_threshold"),
    )
    assert {(row["profile_label"], row["split_threshold"]) for row in summary} == {
        ("baseline_l16", 32),
        ("preprovisioned_l128", 32),
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


def test_backend_profile_dynamic_invariants_require_batched_children():
    cfg = {"fanout": 2, "split_threshold": 32, "merge_threshold": 8}
    counters = {"child_hash_sql_calls": 40, "child_hash_nodes_read": 80}
    post = {
        "targeted_confirmation_child_hash_sql_calls": 40,
        "targeted_confirmation_child_hash_nodes_read": 80,
    }
    good_backend = {
        "child_hash_helper_calls": 80,
        "child_hash_nodes_returned": 160,
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
    bad_backend["child_hash_nodes_returned"] = 0
    reasons = validate_backend_profile_stats(
        bad_backend,
        cfg,
        counters,
        post,
        rows_updated=300,
        rows_inserted=0,
        rows_deleted=0,
    )
    assert any("child_hash_nodes_returned" in reason for reason in reasons)


def test_backend_profile_depth_expectations_for_dynamic_geometries():
    cfg = {"fanout": 2, "split_threshold": 32, "merge_threshold": 8}
    backend = {
        "child_hash_helper_calls": 0,
        "child_hash_nodes_returned": 0,
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
    with pytest.raises(ValueError, match="fanout"):
        validate_geometry(1)
    with pytest.raises(ValueError, match="split_threshold"):
        validate_geometry(2, split_threshold=8, merge_threshold=16)
    validate_geometry(2, split_threshold=32, merge_threshold=8)


def test_manual_geometry_override_gets_truthful_label():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        fanout=2,
        split_threshold=32,
        merge_threshold=8,
        tuple_count=1_000_000,
        bad_leaf_count=10,
        geometry_label="baseline_l16",
    )
    specs = _series_for_profile(args, object())
    assert {spec["geometry_label"] for spec in specs} == {"manual-f2-s32-m8"}


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
            "fanout": 2,
            "bad_leaves": [3],
            "corruptions": [1],
        },
        tmp_path,
        run_id="r1",
        tuple_count=100,
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
    monkeypatch.setattr(bench, "assert_synchronous_merkle_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(bench, "validate_profile_invariants", lambda *args, **kwargs: [])

    def fake_repair_merkle(conn, manifest, tuple_count, repetition, planner_results, schema_rows_out, **kwargs):
        run_id = recovery_run_id(manifest, repetition, kwargs["profile_label"])
        return Metrics(
            run_id=run_id,
            experiment=manifest["experiment"],
            method="merkle",
            tuple_count=tuple_count,
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
        fanout=None,
        tuple_count=5000000,
        bad_leaf_count=None,
        geometry_label=None,
    )
    specs = _series_for_profile(args, object())
    manifests = [
        {
            "experiment": spec["experiment"],
            "tuple_count": spec["tuple_count"],
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
            "| baseline_l16 | 1000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.000 | 1.000 |",
            "| baseline_l16 | 3000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 2.000 | 2.000 |",
            "| baseline_l16 | 5000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 3.000 | 3.000 |",
            "| preprovisioned_l128 | 1000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.000 | 1.000 |",
            "| preprovisioned_l128 | 3000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.100 | 1.100 |",
            "| preprovisioned_l128 | 5000000 | 2 | 10 | 300 | candidate_row_fetch_ms | 1.200 | 1.200 |",
            "## Growth Ratios",
            "- highest-growing phase: `candidate_row_fetch_ms`",
        ]
    )
    for label in ("baseline_l16", "preprovisioned_l128"):
        for tuple_count in (1_000_000, 3_000_000, 5_000_000):
            assert f"| {label} | {tuple_count} |" in report
    assert "highest-growing phase" in report


def test_manual_geometry_requires_label():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        fanout=2,
        split_threshold=32,
        tuple_count=1_000_000,
        bad_leaf_count=None,
        geometry_label=None,
    )
    with pytest.raises(ValueError, match="manual geometry overrides require --geometry-label"):
        _series_for_profile(args, object())


def test_manual_geometry_with_label_produces_one_run():
    args = Namespace(
        profile="recovery-scaling-diagnosis",
        fanout=2,
        split_threshold=32,
        merge_threshold=8,
        tuple_count=1_000_000,
        bad_leaf_count=None,
        geometry_label="baseline_l16",
    )
    series = _series_for_profile(args, object())
    assert len(series) == 1
    assert series[0]["tuple_count"] == 1_000_000
    assert series[0]["fanout"] == 2


def test_full_recovery_phase_reconciliation():
    m = Metrics(
        run_id="test_run",
        experiment="test_exp",
        method="merkle",
        tuple_count=1000,
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



# -- fanout-width-sweep tests -------------------------------------------------

# Canonical 19-geometry list
_ALL_SWEEP_LABELS = [
    "fanout_f2_l16", "fanout_f4_l16", "fanout_f16_l16",
    "fanout_f2_l64", "fanout_f4_l64", "fanout_f8_l64", "fanout_f64_l64",
    "fanout_f2_l128", "fanout_f128_l128",
    "fanout_f2_l256", "fanout_f4_l256", "fanout_f16_l256", "fanout_f256_l256",
    "fanout_f2_l512", "fanout_f512_l512",
    "fanout_f2_l1024", "fanout_f4_l1024", "fanout_f32_l1024", "fanout_f1024_l1024",
]


def _sweep_args(**overrides):
    base = dict(
        profile="fanout-width-sweep",
        fanout=None,
        tuple_count=None,
        bad_leaf_count=None,
        geometry_label=None,
    )
    base.update(overrides)
    return Namespace(**base)


def _sweep_config():
    from merkle_recovery.config import profile_config
    return profile_config("fanout-width-sweep")


def test_fanout_sweep_produces_19_unique_labels():
    specs = _series_for_profile(_sweep_args(), _sweep_config())
    labels = [spec["geometry_label"] for spec in specs]
    assert len(labels) == 19, f"expected 19, got {len(labels)}: {labels}"
    assert set(labels) == set(_ALL_SWEEP_LABELS), (
        f"extra={set(labels)-set(_ALL_SWEEP_LABELS)}, "
        f"missing={set(_ALL_SWEEP_LABELS)-set(labels)}"
    )


def test_fanout_sweep_run_ids_are_unique():
    config = _sweep_config()
    specs = _series_for_profile(_sweep_args(), config)
    run_ids = []
    for spec in specs:
        manifest = {
            "experiment": spec["experiment"],
            "tuple_count": spec["tuple_count"],
            "fanout": spec["fanout"],
            "bad_leaves": list(range(spec["bad_leaf_count"])),
            "corruptions": list(range(spec["corrupted_tuple_count"])),
        }
        for rep in range(config.repetitions):
            run_ids.append(recovery_run_id(manifest, rep, spec["geometry_label"]))
    assert len(run_ids) == len(set(run_ids)), "duplicate run_ids in fanout-width-sweep"


def test_fanout_sweep_all_geometries_pass_validation():
    specs = _series_for_profile(_sweep_args(), _sweep_config())
    for spec in specs:
        validate_geometry(spec["fanout"], spec.get("split_threshold", 32), spec.get("merge_threshold", 8))


def test_fanout_sweep_rejects_manual_fanout_override():
    with pytest.raises(ValueError, match="fixed geometry labels"):
        _series_for_profile(_sweep_args(fanout=4), _sweep_config())


def test_fanout_sweep_label_filter_f16_l16():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f16_l16"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 16


def test_fanout_sweep_label_filter_f128_l128():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f128_l128"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 128


def test_fanout_sweep_label_filter_f256_l256():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f256_l256"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 256


def test_fanout_sweep_label_filter_f64_l64():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f64_l64"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 64

def test_fanout_sweep_label_filter_f512_l512():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f512_l512"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 512


def test_fanout_sweep_label_filter_f1024_l1024():
    specs = _series_for_profile(_sweep_args(geometry_label="fanout_f1024_l1024"), _sweep_config())
    assert len(specs) == 1
    assert specs[0]["fanout"] == 1024


def test_fanout_sweep_corrupted_tuple_count_is_always_300():
    specs = _series_for_profile(_sweep_args(), _sweep_config())
    for spec in specs:
        assert spec["corrupted_tuple_count"] == 300, (
            f"{spec['geometry_label']}: got {spec['corrupted_tuple_count']}"
        )


def test_fanout_sweep_bad_leaf_count_default_is_20():
    specs = _series_for_profile(_sweep_args(), _sweep_config())
    for spec in specs:
        assert spec["bad_leaf_count"] == 20, (
            f"{spec['geometry_label']}: bad_leaf_count={spec['bad_leaf_count']}"
        )


def test_validate_geometry_accepts_all_sweep_geometries():
    from merkle_recovery.config import profile_config
    import json, pathlib
    matrix_path = pathlib.Path(__file__).resolve().parents[1] / "recovery_geometry_matrix.json"
    matrix = json.loads(matrix_path.read_text())
    for label in _ALL_SWEEP_LABELS:
        geo = matrix[label]
        validate_geometry(geo.get("fanout", 4), geo.get("split_threshold", 32), geo.get("merge_threshold", 8))


def test_fanout_sweep_rejects_manual_tuple_count_override():
    with pytest.raises(ValueError, match="fixed geometry labels"):
        _series_for_profile(_sweep_args(tuple_count=1000), _sweep_config())

def test_fanout_sweep_rejects_manual_bad_leaf_count_override():
    with pytest.raises(ValueError, match="fixed geometry labels"):
        _series_for_profile(_sweep_args(bad_leaf_count=50), _sweep_config())

def test_fanout_sweep_rejects_invalid_corruption_mode():
    from run_merkle_recovery_benchmark import main
    import sys
    from unittest.mock import patch

    test_args = ["run_merkle_recovery_benchmark.py", "--profile", "fanout-width-sweep", "--corruption-mode", "invalid"]
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):
            main()


def _size_scaling_args(**overrides):
    base = dict(
        profile="size-scaling-k75-c300",
        fanout=None,
        tuple_count=None,
        bad_leaf_count=None,
        geometry_label=None,
    )
    base.update(overrides)
    return Namespace(**base)


def _size_scaling_config():
    from merkle_recovery.config import profile_config
    return profile_config("size-scaling-k75-c300")


def test_size_scaling_produces_eleven_runs_by_default():
    specs = _series_for_profile(_size_scaling_args(), _size_scaling_config())
    assert len(specs) == 11
    geometries = [(s["geometry_label"], s["tuple_count"]) for s in specs]
    expected = [
        ("fanout_f4_l16", 1000000),
        ("fanout_f4_l16", 3000000),
        ("fanout_f4_l16", 5000000),
        ("fanout_f4_l16", 7000000),
        ("fanout_f4_l16", 10000000),
        ("fanout_f4_l16", 15000000),
        ("fanout_f4_l16", 20000000),
        ("fanout_f4_l16", 25000000),
        ("fanout_f4_l16", 30000000),
        ("fanout_f4_l16", 40000000),
        ("fanout_f4_l16", 50000000),
    ]
    assert geometries == expected
    for spec in specs:
        assert spec["bad_leaf_count"] == 75
        assert spec["corrupted_tuple_count"] == 300


def test_size_scaling_accepts_custom_tuple_counts():
    specs = _series_for_profile(
        _size_scaling_args(tuple_count="1000000,3000000,5000000,7000000,10000000,15000000,20000000,25000000,26000000,27000000,28000000,29000000,30000000,40000000,50000000"),
        _size_scaling_config(),
    )
    assert len(specs) == 15
    assert [spec["tuple_count"] for spec in specs] == [
        1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000,
        15_000_000, 20_000_000, 25_000_000, 26_000_000, 27_000_000,
        28_000_000, 29_000_000, 30_000_000, 40_000_000, 50_000_000,
    ]


def test_size_scaling_rejects_manual_overrides():
    config = _size_scaling_config()
    with pytest.raises(ValueError, match="positive integer"):
        _series_for_profile(_size_scaling_args(tuple_count=-1000), config)
    with pytest.raises(ValueError, match="do not override geometry"):
        _series_for_profile(_size_scaling_args(fanout=4), config)
    with pytest.raises(ValueError, match="uses fixed --bad-leaf-count=75"):
        _series_for_profile(_size_scaling_args(bad_leaf_count=50), config)


def test_size_scaling_rejects_invalid_corruption_mode():
    from run_merkle_recovery_benchmark import main
    import sys
    from unittest.mock import patch

    test_args = ["run_merkle_recovery_benchmark.py", "--profile", "size-scaling-k75-c300", "--corruption-mode", "invalid"]
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):
            main()


def _best_scaling_args(**overrides):
    base = dict(
        profile="best-scaling-f32-l1024-k75-c300",
        fanout=None,
        tuple_count=None,
        bad_leaf_count=None,
        geometry_label=None,
    )
    base.update(overrides)
    return Namespace(**base)


def _best_scaling_config():
    from merkle_recovery.config import profile_config
    return profile_config("best-scaling-f32-l1024-k75-c300")


def test_best_scaling_produces_eleven_runs():
    specs = _series_for_profile(_best_scaling_args(), _best_scaling_config())
    assert len(specs) == 11
    geometries = [(s["geometry_label"], s["tuple_count"]) for s in specs]
    expected = [
        ("fanout_f32_l1024", 1000000),
        ("fanout_f32_l1024", 3000000),
        ("fanout_f32_l1024", 5000000),
        ("fanout_f32_l1024", 7000000),
        ("fanout_f32_l1024", 10000000),
        ("fanout_f32_l1024", 15000000),
        ("fanout_f32_l1024", 20000000),
        ("fanout_f32_l1024", 25000000),
        ("fanout_f32_l1024", 30000000),
        ("fanout_f32_l1024", 40000000),
        ("fanout_f32_l1024", 50000000),
    ]
    assert geometries == expected
    for spec in specs:
        assert spec["bad_leaf_count"] == 75
        assert spec["corrupted_tuple_count"] == 300


def test_best_scaling_rejects_manual_overrides():
    config = _best_scaling_config()
    with pytest.raises(ValueError, match="fixed geometry"):
        _series_for_profile(_best_scaling_args(fanout=4), config)
    with pytest.raises(ValueError, match="uses fixed --bad-leaf-count=75"):
        _series_for_profile(_best_scaling_args(bad_leaf_count=50), config)
    with pytest.raises(ValueError, match="only supports geometry_label=fanout_f32_l1024"):
        _series_for_profile(_best_scaling_args(geometry_label="fanout_f2_l16"), config)


def test_best_scaling_rejects_invalid_corruption_mode():
    from run_merkle_recovery_benchmark import main
    import sys
    from unittest.mock import patch

    test_args = ["run_merkle_recovery_benchmark.py", "--profile", "best-scaling-f32-l1024-k75-c300", "--corruption-mode", "invalid"]
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):
            main()


def test_contract_with_skipped_audit():
    from merkle_recovery.reporting import assert_benchmark_contract
    from merkle_recovery.metrics import Metrics
    m = Metrics(
        run_id="run_1",
        experiment="figure12",
        method="merkle",
        tuple_count=1000000,
        bad_leaf_count=75,
        corrupted_tuple_count=300,
        repetition=0,
        corruption_mode="paper-update-only",
        fanout=32,
    )
    # Set all necessary counters
    m.counters.update({
        "paper_end_before_audit_start": 1,
        "end_to_end_covers_paper_and_audit": 1,
        "schema_fidelity_ok": 1,
        "partition_root_batches": 2,
        "partition_root_batches_ok": 1,
        "planner_checks_passed": 1,
        "total_rows_repaired": 300,
        "recovery_user_table_seq_scan_delta": 0,
        "bad_leaf_count": 75,
        "full_audit_skipped": 1,
        "audit_validation_positive": 0,  # normally triggers error, but skipped
    })
    # This should pass without raising RuntimeError
    assert_benchmark_contract("best-scaling-f32-l1024-k75-c300", [m])


@pytest.mark.parametrize(
    ("k", "chunk", "expected"),
    [
        (0,   64, 0),
        (10,  64, 2),
        (64,  64, 2),
        (65,  64, 4),
        (75,  64, 4),
        (200, 64, 8),
        (75,   0, 2),
    ],
)
def test_profile_invariants_chunk_boundaries(k, chunk, expected):
    phase = {
        "tree_localisation_ms": 0.0,
        "candidate_row_fetch_ms": 1.0,
        "repair_write_ms": 0.0,
        "targeted_post_repair_confirmation_ms": 1.0,
    }
    ops = []
    for _ in range(expected // 2):
        ops.append({"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_healthy", "client_wall_ms": "1.0"})
        ops.append({"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_damaged", "client_wall_ms": "1.0"})
        ops.append({"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_healthy", "client_wall_ms": "1.0"})
        ops.append({"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_damaged", "client_wall_ms": "1.0"})
    
    assert validate_profile_invariants(
        phase=phase,
        operations=ops,
        bad_leaf_count=k,
        run_id="r1",
        tolerance_ms=10.0,
        leaf_fetch_chunk_size=chunk,
    ) == []


def test_profile_invariants_rejects_wrong_call_count():
    phase = {
        "tree_localisation_ms": 0.0,
        "candidate_row_fetch_ms": 1.0,
        "repair_write_ms": 0.0,
        "targeted_post_repair_confirmation_ms": 1.0,
    }
    ops = [
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_healthy", "client_wall_ms": "2.0"},
        {"run_id": "r1", "stage": "candidate_fetch", "operation": "leaf_fetch_batch_damaged", "client_wall_ms": "2.0"},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_healthy", "client_wall_ms": "2.0"},
        {"run_id": "r1", "stage": "targeted_confirmation", "operation": "confirmation_leaf_fetch_batch_damaged", "client_wall_ms": "2.0"},
    ]
    reasons = validate_profile_invariants(
        phase=phase,
        operations=ops,
        bad_leaf_count=75,
        run_id="r1",
        tolerance_ms=10.0,
        leaf_fetch_chunk_size=64,
    )
    assert any("expected=4" in reason for reason in reasons)


def test_representative_leaf_ids_coverage():
    from merkle_recovery.repair import representative_leaf_ids
    assert representative_leaf_ids(16, 200) == list(range(16))
    assert len(representative_leaf_ids(204800, 75)) == 75
