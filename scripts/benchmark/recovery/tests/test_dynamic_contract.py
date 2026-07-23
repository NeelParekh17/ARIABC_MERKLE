from __future__ import annotations

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_merkle_recovery_benchmark as benchmark
from merkle_recovery import dataset, dynamic_benchmark, dynamic_db
from merkle_recovery import manifest as manifest_module
from merkle_recovery.config import (
    DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT,
    DYNAMIC_NATIVE_LAYOUT_VERSION,
    DYNAMIC_PROFILE,
    DYNAMIC_SIZE_SERIES,
    profile_config,
)
from merkle_recovery.dynamic import LogicalRange, RangeItem, RangeSummary, RepairKeys


def _args(**overrides):
    values = {
        "profile": DYNAMIC_PROFILE,
        "experiment": None,
        "tuple_count": None,
        "partitions": None,
        "bad_leaf_count": None,
        "leaves_per_partition": None,
        "fanout": None,
        "geometry_label": None,
        "dynamic_leaf_capacity": None,
        "dynamic_merge_threshold": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_dynamic_profile_is_the_exact_acceptance_matrix():
    config = profile_config(DYNAMIC_PROFILE)
    assert config.fig12_sizes == DYNAMIC_SIZE_SERIES
    assert config.repetitions == 5
    assert config.benchmark_schema_version == 7
    assert config.extra["dynamic_partitions"] == 200
    assert config.extra["dynamic_native_layout_version"] == 8
    assert config.extra["dynamic_logical_fanout"] == 32
    assert config.extra["dynamic_physical_node_fanout"] == 2
    assert config.extra["dynamic_leaf_capacity"] == 32
    assert config.extra["dynamic_merge_threshold"] == 8
    assert config.extra["bad_range_count"] == 75
    assert config.extra["corrupted_tuple_count"] == 300
    assert DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT == 4_800

    specs = benchmark._series_for_profile(_args(), config)
    assert [row["tuple_count"] for row in specs] == DYNAMIC_SIZE_SERIES
    for row in specs:
        assert row == {
            "experiment": DYNAMIC_PROFILE,
            "tuple_count": row["tuple_count"],
            "partitions": 200,
            "leaves_per_partition": 0,
            "fanout": 32,
            "bad_leaf_count": 75,
            "corrupted_tuple_count": 300,
            "geometry_label": "dynamic-p200-k32-cap32-merge8",
            "merkle_mode": "dynamic",
            "leaf_capacity": 32,
            "merge_threshold": 8,
        }


def test_dynamic_run_id_reports_logical_and_physical_fanout_separately():
    manifest = {
        "experiment": "dynamic-bounded",
        "tuple_count": 100,
        "partitions": 2,
        "fanout": 32,
        "leaf_capacity": 32,
        "merge_threshold": 8,
        "bad_ranges": [],
        "corruptions": [],
    }
    run_id = dynamic_benchmark.dynamic_recovery_run_id(manifest, 0, "test")
    assert "-lf32-pf2-" in run_id
    assert "-k32-" not in run_id


def test_dynamic_fanout_contract_binds_manifest_metadata_and_physical_shape():
    good = {
        "healthy": {"authority": "native_index_pages", "layout_version": DYNAMIC_NATIVE_LAYOUT_VERSION, "logical_fanout": 32, "physical_node_fanout": 2},
        "damaged": {"authority": "native_index_pages", "layout_version": DYNAMIC_NATIVE_LAYOUT_VERSION, "logical_fanout": 32, "physical_node_fanout": 2},
    }
    dynamic_benchmark._validate_fanout_contract(good, 32)

    bad = {
        **good,
        "damaged": {
            "authority": "native_index_pages",
            "layout_version": DYNAMIC_NATIVE_LAYOUT_VERSION,
            "logical_fanout": 2,
            "physical_node_fanout": 2,
        },
    }
    with pytest.raises(RuntimeError, match="index_metadata=2"):
        dynamic_benchmark._validate_fanout_contract(bad, 32)

    stale = {
        **good,
        "damaged": {**good["damaged"], "layout_version": 5},
    }
    with pytest.raises(RuntimeError, match="native layout mismatch"):
        dynamic_benchmark._validate_fanout_contract(stale, 32)


@pytest.mark.parametrize(
    "override,match",
    [
        ({"partitions": 100}, "partitions=200"),
        ({"fanout": 3}, "one of 2,4,8,16,32"),
        ({"bad_leaf_count": 74}, "bad-leaf-count=75"),
        ({"dynamic_leaf_capacity": 64}, "dynamic-leaf-capacity=32"),
        ({"dynamic_merge_threshold": 9}, "dynamic-merge-threshold=8"),
        ({"leaves_per_partition": 16}, "no fixed leaves-per-partition"),
    ],
)
def test_dynamic_profile_rejects_contract_drift(override, match):
    with pytest.raises(ValueError, match=match):
        benchmark._series_for_profile(_args(**override), profile_config(DYNAMIC_PROFILE))


def test_dynamic_profile_accepts_configurable_logical_fanout():
    specs = benchmark._series_for_profile(
        _args(fanout=4, tuple_count="1000000"), profile_config(DYNAMIC_PROFILE)
    )
    assert len(specs) == 1
    assert specs[0]["fanout"] == 4
    assert specs[0]["geometry_label"] == "dynamic-p200-k4-cap32-merge8"


def test_network_probe_records_distinct_database_endpoints(monkeypatch, tmp_path):
    monkeypatch.setattr(
        benchmark,
        "execute",
        lambda conn, sql, params=None: [{
            "server_addr": "10.0.0.20",
            "server_port": 5432,
            "client_addr": "10.0.0.10",
            "client_port": 42000,
            "backend_pid": 123,
            "ssl_enabled": "on",
        }],
    )
    monkeypatch.setattr(benchmark, "scalar", lambda conn, sql: 1)
    probe = benchmark.write_network_probe(object(), tmp_path, 3)
    stored = json.loads((tmp_path / "network_probe.json").read_text())
    assert probe["samples"] == 3
    assert stored["server_addr"] == "10.0.0.20"
    assert stored["client_addr_seen_by_server"] == "10.0.0.10"
    assert len(stored["round_trip_ms"]) == 3


def test_dynamic_index_creation_uses_reloptions_and_never_creates_static_lookup(monkeypatch):
    statements: list[str] = []

    def fake_execute(conn, sql, params=None):
        statements.append(" ".join(sql.split()))
        return []

    monkeypatch.setattr(dataset, "execute", fake_execute)
    dataset.create_dynamic_merkle_indexes(None, 200, 32, 32, 8)
    creates = [sql for sql in statements if sql.startswith("CREATE INDEX")]
    assert len(creates) == 2
    assert all("dynamic = on" in sql for sql in creates)
    assert all("partitions = 200" in sql for sql in creates)
    assert all("fanout = 32" in sql for sql in creates)
    assert all("leaf_capacity = 32" in sql for sql in creates)
    assert all("merge_threshold = 8" in sql for sql in creates)
    assert not any(
        sql.startswith("CREATE INDEX") and "leaf_lookup" in sql
        for sql in statements
    )
    assert {
        sql
        for sql in statements
        if sql.startswith("ANALYZE ariabc_internal.merkle_dynamic_")
    } == set()


def test_dynamic_index_creation_uses_requested_smaller_fanout(monkeypatch):
    statements: list[str] = []

    monkeypatch.setattr(
        dataset,
        "execute",
        lambda conn, sql, params=None: statements.append(" ".join(sql.split())) or [],
    )
    dataset.create_dynamic_merkle_indexes(None, 200, 4, 32, 8)
    creates = [sql for sql in statements if sql.startswith("CREATE INDEX")]
    assert len(creates) == 2
    assert all("fanout = 4" in sql for sql in creates)


def test_dynamic_adapter_matches_frozen_backend_rows(monkeypatch):
    prefix = LogicalRange(3, 2, 0b10)
    rows = [
        {
            "partition_id": 3,
            "prefix_len": 2,
            "prefix": prefix.prefix_bytes,
            "tuple_count": 1,
            "data_xor": bytes.fromhex("11" * 32),
            "is_leaf": True,
        }
    ]

    def fake_execute(conn, sql, params=None):
        assert "merkle_dynamic_get_ranges" in sql
        request = json.loads(params[0])
        assert request == [prefix.to_request()]
        return rows

    monkeypatch.setattr(dynamic_db, "execute", fake_execute)
    summaries = dynamic_db.range_summaries(None, "healthy", [prefix])
    assert summaries[prefix] == RangeSummary(prefix, 1, bytes.fromhex("11" * 32))


def test_dynamic_range_items_use_key_text_and_assign_requested_range(monkeypatch):
    prefix = LogicalRange(0, 1, 0)
    route = bytes(32)

    monkeypatch.setattr(
        dynamic_db,
        "execute",
        lambda conn, sql, params=None: [
            {
                "partition_id": 0,
                "prefix_len": 5,
                "prefix": bytes(32),
                "key_data": b"ignored-for-ycsb",
                "key_text": "42",
                "key_data": b"canonical-key",
                "route_digest": route,
                "tuple_hash": bytes.fromhex("22" * 32),
            }
        ],
    )
    items = dynamic_db.range_items(None, "healthy", [prefix])
    assert len(items) == 1
    assert items[0].logical_range == prefix
    assert items[0].key == 42
    assert items[0].encoded_bytes == len(b"canonical-key") + 64


def test_set_based_repairs_issue_one_upsert_and_one_delete(monkeypatch):
    calls: list[tuple[str, object]] = []

    def fake_execute(conn, sql, params=None):
        calls.append((" ".join(sql.split()), params))
        return []

    monkeypatch.setattr(dynamic_db, "execute", fake_execute)
    repairs = RepairKeys(inserts=(1,), updates=(2,), deletes=(3,))
    rows = []
    for key in (1, 2):
        row = {"ycsb_key": key}
        row.update({f"field{i}": f"v{i}-{key}" for i in range(10)})
        rows.append(row)
    result = dynamic_db.apply_set_based_repairs(None, repairs, rows)
    assert result.total == 3
    assert len(calls) == 2
    assert calls[0][0].startswith("INSERT INTO damaged.usertable")
    assert "ON CONFLICT (ycsb_key) DO UPDATE" in calls[0][0]
    assert calls[1][0].startswith("DELETE FROM damaged.usertable")


def test_dynamic_manifest_selects_75_bounded_ranges_and_exactly_300_updates(monkeypatch):
    leaves = [
        RangeSummary(LogicalRange.root(partition), 32, (partition + 1).to_bytes(32, "big"))
        for partition in range(80)
    ]

    def fake_items(conn, schema, ranges):
        out = []
        for logical_range in ranges:
            for offset in range(32):
                key = logical_range.partition_id * 1_000 + offset + 1
                out.append(
                    RangeItem(
                        logical_range,
                        key,
                        key.to_bytes(32, "big"),
                        (key + 1).to_bytes(32, "big"),
                    )
                )
        return out

    monkeypatch.setattr(dynamic_db, "physical_leaf_summaries", lambda conn, schema: leaves)
    monkeypatch.setattr(dynamic_db, "range_items", fake_items)
    result = manifest_module.choose_dynamic_corruption_manifest(
        None,
        DYNAMIC_PROFILE,
        1_000_000,
        200,
        32,
        32,
        8,
        75,
        300,
        20260703,
    )
    assert len(result["bad_ranges"]) == 75
    assert len(result["corruptions"]) == 300
    assert {entry["op"] for entry in result["corruptions"]} == {"update"}
    assert all(value == 32 for value in result["selected_leaf_capacities"].values())


def test_dynamic_cli_allows_targeted_audit_for_diagnostic_runs(
    monkeypatch, tmp_path: Path
):
    result_dir = tmp_path / "result"
    result_dir.mkdir()

    def fake_run(args):
        assert args.merkle_mode == "dynamic"
        assert args.audit_mode == "skip"
        return result_dir

    monkeypatch.setattr(benchmark, "run_benchmark", fake_run)
    assert benchmark.main(
        [
            "--profile", DYNAMIC_PROFILE,
            "--audit-mode", "skip",
            "--result-dir", str(tmp_path / "results"),
            "--scratch-dir", str(tmp_path / "scratch"),
        ]
    ) == 0
