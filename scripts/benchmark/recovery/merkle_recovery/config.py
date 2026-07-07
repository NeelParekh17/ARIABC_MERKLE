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
TIMING_CONTRACT_VERSION = 1
ZERO_HASH = "0" * 64

# Explicit scope metadata written into every config.json
BENCHMARK_SCOPE_METADATA: dict[str, str] = {
    "benchmark_scope": "merkle_only_static_recovery",
    "enabled_methods": "merkle",
    "paper_comparison_status": "merkle_series_only",
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
