"""Metrics dataclass and all timing-boundary helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Metrics:
    run_id: str
    experiment: str
    method: str
    tuple_count: int
    partitions: int
    leaves_per_partition: int
    fanout: int
    bad_leaf_count: int
    corrupted_tuple_count: int
    repetition: int
    corruption_mode: str = "paper-update-only"
    profile_label: str = ""
    profiling_mode: str = "off"
    valid: bool = True
    warning: str = ""
    paper_style_total_ms: float = 0.0
    restore_repair_ms: float = 0.0
    audit_validation_ms: float = 0.0
    end_to_end_observed_ms: float = 0.0
    cleanup_ms: float = 0.0
    phase: dict[str, float] = field(default_factory=dict)
    counters: dict[str, Any] = field(default_factory=dict)


def add_warning(m: Metrics, msg: str) -> None:
    m.warning = (m.warning + "; " if m.warning else "") + msg
    m.valid = False


def finalize_metrics(
    m: Metrics,
    *,
    total_start_ms: float,
    paper_start_ms: float,
    paper_end_ms: float,
    recovery_start_ms: float,
    recovery_end_ms: float,
    audit_start_ms: float,
    audit_end_ms: float,
    cleanup_end_ms: float,
    audit_skipped: bool = False,
) -> None:
    """Populate all aggregate timing fields and timing-contract counters."""
    m.paper_style_total_ms = paper_end_ms - paper_start_ms
    m.restore_repair_ms = recovery_end_ms - recovery_start_ms
    m.audit_validation_ms = audit_end_ms - audit_start_ms
    m.end_to_end_observed_ms = cleanup_end_ms - total_start_ms
    m.cleanup_ms = max(0.0, cleanup_end_ms - audit_end_ms)
    audit_positive = m.audit_validation_ms > 0
    m.counters.update(
        {
            "paper_end_before_audit_start": int(paper_end_ms <= audit_start_ms),
            "audit_validation_positive": int(audit_positive),
            "audit_validation_skipped": int(audit_skipped),
            "end_to_end_covers_paper_and_audit": int(
                m.end_to_end_observed_ms + 1e-6 >= m.paper_style_total_ms + m.audit_validation_ms
            ),
        }
    )
    if paper_end_ms > audit_start_ms:
        add_warning(m, "paper timing overlaps audit")
    if not audit_skipped and not audit_positive:
        add_warning(m, "audit timing is not positive")
    if m.end_to_end_observed_ms + 1e-6 < m.paper_style_total_ms + m.audit_validation_ms:
        add_warning(m, "end-to-end timing does not cover paper plus audit")
