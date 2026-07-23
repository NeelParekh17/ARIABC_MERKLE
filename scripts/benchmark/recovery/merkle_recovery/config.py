"""Benchmark configuration constants and profile definitions.

This module is the single source of truth for:
- Paper-profile parameters (Figure 12 and Figure 13)
- Smoke/preflight parameters
- All column and schema name constants
- Timing and schema version constants
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ── paths ──────────────────────────────────────────────────────────────────
BENCH_DIR: Path = Path(__file__).resolve().parents[1]
ROOT: Path = BENCH_DIR.parents[2]
RESULT_ROOT: Path = BENCH_DIR / "results"
GEOMETRY_MATRIX_PATH: Path = BENCH_DIR / "recovery_geometry_matrix.json"

# ── schema / column constants ───────────────────────────────────────────────
FIELDS = [f"field{i}" for i in range(10)]
ALL_COLUMNS = ["ycsb_key", *FIELDS]
LEAF_LOOKUP_INDEXES = {
    "healthy": "usertable_leaf_lookup_idx",
    "damaged": "usertable_leaf_lookup_idx",
}

BENCHMARK_SCHEMA_VERSION = 3   # v2 = three-method; v3 = Merkle-only static recovery
DYNAMIC_BENCHMARK_SCHEMA_VERSION = 7
TIMING_CONTRACT_VERSION = 1
ZERO_HASH = "0" * 64
MERKLE_MODES = ("static", "dynamic")
DYNAMIC_PROFILE = "dynamic-size-scaling-k75-c300"
DYNAMIC_NATIVE_LAYOUT_VERSION = 8
DYNAMIC_PARTITIONS = 200
DYNAMIC_LOGICAL_FANOUT = 32
DYNAMIC_PHYSICAL_NODE_FANOUT = 2
DYNAMIC_LEAF_CAPACITY = 32
DYNAMIC_MERGE_THRESHOLD = 8
DYNAMIC_BAD_RANGE_COUNT = 75
DYNAMIC_CORRUPTED_TUPLE_COUNT = 300
DYNAMIC_SIZE_SERIES = [
    1_000_000,
    3_000_000,
    5_000_000,
    7_000_000,
    10_000_000,
    15_000_000,
    20_000_000,
    25_000_000,
    30_000_000,
    40_000_000,
    50_000_000,
]
DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT = (
    2 * DYNAMIC_BAD_RANGE_COUNT * DYNAMIC_LEAF_CAPACITY
)

# Explicit scope metadata written into every config.json
BENCHMARK_SCOPE_METADATA: dict[str, str] = {
    "benchmark_scope": "merkle_only_static_recovery",
    "enabled_methods": "merkle",
    "paper_comparison_status": "merkle_series_only",
}

DYNAMIC_SCOPE_METADATA: dict[str, str] = {
    "benchmark_scope": "merkle_only_dynamic_recovery",
    "enabled_methods": "merkle_dynamic",
    "paper_comparison_status": "dynamic_recovery_series",
}

# ── corruption modes ────────────────────────────────────────────────────────
# paper-update-only  - original paper profile: existing rows get field9 mutated
# update-only        - same semantics, different label (correctness test)
# delete-only        - reference row present, damaged copy lacks it
# insert-only        - damaged copy has spurious rows absent from reference
# mixed              - deterministic 1/3 update, 1/3 delete, 1/3 insert split
CORRUPTION_MODES = ("paper-update-only", "update-only", "delete-only", "insert-only", "mixed")


@dataclass
class BenchmarkConfig:
    """Runtime-resolved profile settings."""

    fig12_sizes: list[int]
    fig13_sizes: list[int]
    fig13_k: list[int]
    repetitions: int
    benchmark_schema_version: int = BENCHMARK_SCHEMA_VERSION
    timing_contract_version: int = TIMING_CONTRACT_VERSION
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "fig12_sizes": self.fig12_sizes,
            "fig13_sizes": self.fig13_sizes,
            "fig13_k": self.fig13_k,
            "repetitions": self.repetitions,
            "benchmark_schema_version": self.benchmark_schema_version,
            "timing_contract_version": self.timing_contract_version,
            **self.extra,
        }


def profile_config(profile: str) -> BenchmarkConfig:
    """Return the BenchmarkConfig for the named profile.

    Profiles
    --------
    paper      Full Figure 12 + Figure 13 paper run (slow).
    preflight  Paper-shaped single pass at 1 M rows (fast).
    smoke      Tiny row counts for CI / local validation.
    """
    if profile == "paper":
        return BenchmarkConfig(
            fig12_sizes=[1_000_000, 3_000_000, 5_000_000],
            fig13_sizes=[3_000_000],
            fig13_k=list(range(1, 11)),
            repetitions=5,
        )
    if profile == "recovery-scaling-diagnosis":
        return BenchmarkConfig(
            fig12_sizes=[1_000_000, 3_000_000, 5_000_000],
            fig13_sizes=[],
            fig13_k=[],
            repetitions=5,
            extra={"campaign": "recovery_scaling_diagnosis"},
        )
    if profile == "fanout-width-sweep":
        return BenchmarkConfig(
            fig12_sizes=[5_000_000],
            fig13_sizes=[],
            fig13_k=[],
            repetitions=5,
            extra={
                "campaign": "fanout_width_sweep",
                "description": (
                    "Isolates fanout effect on recovery time. "
                    "19 geometries across 6 leaf-count tiers (L=16,64,128,256,512,1024). "
                    "Within each tier bucket density is fixed; only tree height and "
                    "child-hash payload change as fanout increases. "
                    "Fanouts covered: F=2,4,8,16,32,64,128,256,512,1024. "
                    "N=5M, P=200, K=20 bad leaves, C=300 update-only corruptions, "
                    "profiling=light, 5 repetitions. "
                    "K=20 ensures perfectly uniform distribution (15 corruptions/leaf). "
                    "Corruption capacity is validated before corruption and recovery."
                ),
            },
        )

    if profile == "size-scaling-k75-c300":
        return BenchmarkConfig(
            fig12_sizes=[1_000_000, 3_000_000, 5_000_000],
            fig13_sizes=[],
            fig13_k=[],
            repetitions=5,
            extra={
                "campaign": "size_scaling_k75_c300",
                "description": (
                    "Compares N=1M,3M,5M for three fixed geometries: "
                    "F=2,L=16; F=2,L=128; F=32,L=1024. "
                    "Uses K=75 bad leaves and C=300 update-only corruptions."
                ),
            },
        )

    if profile == "best-scaling-f32-l1024-k75-c300":
        return BenchmarkConfig(
            fig12_sizes=[
                1_000_000,
                3_000_000,
                5_000_000,
                7_000_000,
                10_000_000,
                15_000_000,
                20_000_000,
                25_000_000,
                30_000_000,
                40_000_000,
                50_000_000,
            ],
            fig13_sizes=[],
            fig13_k=[],
            repetitions=5,
            extra={
                "campaign": "best_scaling_f32_l1024_k75_c300",
                "description": (
                    "Extends best static Merkle geometry F=32,L=1024 from "
                    "1M to 20M rows. Uses K=75 bad leaves and C=300 "
                    "update-only corruptions."
                ),
            },
        )

    if profile == DYNAMIC_PROFILE:
        return BenchmarkConfig(
            fig12_sizes=list(DYNAMIC_SIZE_SERIES),
            fig13_sizes=[],
            fig13_k=[],
            repetitions=5,
            benchmark_schema_version=DYNAMIC_BENCHMARK_SCHEMA_VERSION,
            extra={
                "campaign": "dynamic_size_scaling_k75_c300",
                "merkle_mode": "dynamic",
                "dynamic_native_layout_version": DYNAMIC_NATIVE_LAYOUT_VERSION,
                "dynamic_partitions": DYNAMIC_PARTITIONS,
                "dynamic_logical_fanout": DYNAMIC_LOGICAL_FANOUT,
                "dynamic_physical_node_fanout": DYNAMIC_PHYSICAL_NODE_FANOUT,
                "dynamic_leaf_capacity": DYNAMIC_LEAF_CAPACITY,
                "dynamic_split_threshold": DYNAMIC_LEAF_CAPACITY,
                "dynamic_merge_threshold": DYNAMIC_MERGE_THRESHOLD,
                "bad_range_count": DYNAMIC_BAD_RANGE_COUNT,
                "corrupted_tuple_count": DYNAMIC_CORRUPTED_TUPLE_COUNT,
                "candidate_summary_item_limit": DYNAMIC_CANDIDATE_SUMMARY_ITEM_LIMIT,
                "description": (
                    "Dynamic Merkle recovery acceptance campaign at "
                    "N=1M,3M,5M,7M,10M,15M,20M,25M,30M,40M,50M; "
                    "P=200, configurable power-of-two logical K (default 32), "
                    "leaf/split capacity=32, merge=8, "
                    "75 corrupted leaf ranges, 300 update corruptions, five repetitions."
                ),
            },
        )

    if profile == "preflight":
        return BenchmarkConfig(
            fig12_sizes=[1_000_000],
            fig13_sizes=[1_000_000],
            fig13_k=[1, 10],
            repetitions=1,
        )
    # smoke (default)
    return BenchmarkConfig(
        fig12_sizes=[1_000],
        fig13_sizes=[1_200],
        fig13_k=[1, 2],
        repetitions=1,
    )
