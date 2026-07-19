from __future__ import annotations

from collections.abc import Sequence
import sys
from pathlib import Path

import pytest

# Allow running from the repository root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from merkle_recovery.dynamic import (
    DIGEST_BITS,
    LogicalRange,
    LocalisationTrace,
    RangeItem,
    RangeSummary,
    compare_range_items,
    enforce_candidate_summary_bound,
    digest_bytes,
    localise_bad_ranges,
    xor_digests,
)


def test_candidate_summary_bound_is_exactly_4800_and_fails_closed():
    assert enforce_candidate_summary_bound(
        2_400,
        2_400,
        bad_range_count=75,
        leaf_capacity=32,
    ) == 4_800
    with pytest.raises(RuntimeError, match="healthy dynamic candidate summaries"):
        enforce_candidate_summary_bound(
            2_401,
            2_399,
            bad_range_count=75,
            leaf_capacity=32,
        )


def _digest(prefix: str, suffix: int = 0) -> bytes:
    bits = prefix + format(suffix, f"0{DIGEST_BITS - len(prefix)}b")
    return int(bits, 2).to_bytes(32, "big")


def _hash(value: int) -> bytes:
    return value.to_bytes(32, "big")


def _summary(logical_range: LogicalRange, items: Sequence[tuple[int, bytes, bytes]]) -> RangeSummary:
    selected = [item for item in items if logical_range.contains_digest(item[1])]
    return RangeSummary(
        logical_range,
        len(selected),
        xor_digests(item[2] for item in selected),
    )


def test_logical_range_is_canonical_and_descends_msb_first():
    root = LogicalRange.root(7)
    left = root.child(0)
    right = root.child(1)
    target = right.child(0).child(1)  # 101

    assert target.prefix_length == 3
    assert target.prefix_value == 0b101
    assert target.prefix_bytes[0] == 0b10100000
    assert target.contains_digest(_digest("101"))
    assert not target.contains_digest(_digest("100"))
    assert root.contains_range(target)
    assert right.contains_range(target)
    assert not left.contains_range(target)
    assert target.label == "p7:101/3"

    dirty = bytearray(target.prefix_bytes)
    dirty[-1] = 1
    with pytest.raises(ValueError, match="unused suffix"):
        LogicalRange.from_prefix_bytes(7, 3, bytes(dirty))


def test_digest_parser_accepts_postgres_bytea_hex_and_rejects_truncation():
    expected = bytes(range(32))
    assert digest_bytes(expected) == expected
    assert digest_bytes("\\x" + expected.hex()) == expected
    assert digest_bytes("0x" + expected.hex()) == expected
    with pytest.raises(ValueError, match="64 characters"):
        digest_bytes("00")


def test_localisation_uses_logical_prefixes_across_shape_mismatch():
    # The healthy physical tree can represent 0* as one leaf while the damaged
    # tree represents 00* and 01* separately.  The callback deliberately hides
    # that physical shape and returns summaries for the exact requested prefix.
    healthy_items = [
        (1, _digest("000"), _hash(11)),
        (2, _digest("001"), _hash(12)),
        (3, _digest("010"), _hash(13)),
        (4, _digest("011"), _hash(14)),
        (5, _digest("100"), _hash(15)),
        (6, _digest("101"), _hash(16)),
    ]
    damaged_items = [
        (1, _digest("000"), _hash(11)),
        (2, _digest("001"), _hash(99)),  # update corruption
        (3, _digest("010"), _hash(13)),
        (4, _digest("011"), _hash(14)),
        (5, _digest("100"), _hash(15)),
        (6, _digest("101"), _hash(16)),
    ]
    root = LogicalRange.root(0)
    roots = {
        "healthy": {0: _summary(root, healthy_items)},
        "damaged": {0: _summary(root, damaged_items)},
    }

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        items = healthy_items if schema == "healthy" else damaged_items
        return {logical_range: _summary(logical_range, items) for logical_range in ranges}

    trace = LocalisationTrace()
    bad = localise_bad_ranges(
        roots["healthy"], roots["damaged"], fetch,
        leaf_capacity=2, logical_fanout=2, trace=trace,
    )

    assert bad == [LogicalRange(0, 2, 0b00)]
    assert trace.bad_partitions == 1
    assert trace.levels_visited == 2
    assert trace.logical_ranges_compared == 5
    assert trace.range_summary_rows == 10


def test_localisation_handles_insert_count_and_asymmetric_split_depth():
    healthy_items = [
        (1, _digest("000"), _hash(1)),
        (2, _digest("001"), _hash(2)),
        (3, _digest("010"), _hash(3)),
        (4, _digest("011"), _hash(4)),
    ]
    damaged_items = [*healthy_items, (99, _digest("0011"), _hash(99))]
    root = LogicalRange.root(0)

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        items = healthy_items if schema == "healthy" else damaged_items
        return {logical_range: _summary(logical_range, items) for logical_range in ranges}

    bad = localise_bad_ranges(
        {0: _summary(root, healthy_items)},
        {0: _summary(root, damaged_items)},
        fetch,
        leaf_capacity=2,
        logical_fanout=2,
    )
    # Damaged 00* contains three items, so bounded localisation descends one
    # more bit and isolates the changed 001* range (1 healthy, 2 damaged).
    assert bad == [LogicalRange(0, 3, 0b001)]


def test_localisation_rejects_inconsistent_backend_summaries():
    root = LogicalRange.root(0)
    root_summary = RangeSummary(root, 3, _hash(1))

    def broken_fetch(schema: str, ranges: Sequence[LogicalRange]):
        return {
            ranges[0]: RangeSummary(ranges[0], 1, _hash(1)),
            ranges[1]: RangeSummary(ranges[1], 1, _hash(0)),
        }

    with pytest.raises(RuntimeError, match="count conservation"):
        localise_bad_ranges(
            {0: root_summary}, {0: RangeSummary(root, 3, _hash(2))},
            broken_fetch, leaf_capacity=1, logical_fanout=2,
        )


def test_localisation_expands_one_fanout32_logical_level_at_a_time():
    healthy_items = [
        (value, _digest(format(value, "06b")), _hash(value + 1))
        for value in range(64)
    ]
    damaged_items = list(healthy_items)
    damaged_items[10] = (
        damaged_items[10][0],
        damaged_items[10][1],
        _hash(999),
    )
    root = LogicalRange.root(0)

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        items = healthy_items if schema == "healthy" else damaged_items
        return {logical_range: _summary(logical_range, items) for logical_range in ranges}

    trace = LocalisationTrace()
    bad = localise_bad_ranges(
        {0: _summary(root, healthy_items)},
        {0: _summary(root, damaged_items)},
        fetch,
        leaf_capacity=2,
        logical_fanout=32,
        trace=trace,
    )

    assert bad == [LogicalRange(0, 5, 0b00101)]
    assert trace.levels_visited == 1
    assert trace.logical_ranges_compared == 33
    assert trace.range_summary_rows == 66


@pytest.mark.parametrize("logical_fanout", [2, 4, 8, 16, 32])
def test_localisation_fanouts_preserve_the_same_corruption(logical_fanout: int):
    """Every supported logical fanout must isolate the same damaged digest."""
    healthy_items = [
        (value, _digest(format(value, "06b")), _hash(value + 1))
        for value in range(64)
    ]
    damaged_items = list(healthy_items)
    damaged_digest = damaged_items[10][1]
    damaged_items[10] = (10, damaged_digest, _hash(999))
    root = LogicalRange.root(0)

    def fetch(schema: str, ranges: Sequence[LogicalRange]):
        items = healthy_items if schema == "healthy" else damaged_items
        return {logical_range: _summary(logical_range, items) for logical_range in ranges}

    bad = localise_bad_ranges(
        {0: _summary(root, healthy_items)},
        {0: _summary(root, damaged_items)},
        fetch,
        leaf_capacity=2,
        logical_fanout=logical_fanout,
    )

    assert len(bad) == 1
    assert bad[0].contains_digest(damaged_digest)
    assert _summary(bad[0], healthy_items).tuple_count <= 2
    assert _summary(bad[0], healthy_items).data_xor != _summary(
        bad[0], damaged_items
    ).data_xor


def test_compare_range_items_returns_exact_insert_update_delete_keys():
    logical_range = LogicalRange.root(0).child(0)
    healthy = [
        RangeItem(logical_range, 1, _digest("0"), _hash(10)),
        RangeItem(logical_range, 2, _digest("0", 2), _hash(20)),
        RangeItem(logical_range, 3, _digest("0", 3), _hash(30)),
    ]
    damaged = [
        RangeItem(logical_range, 2, _digest("0", 2), _hash(99)),
        RangeItem(logical_range, 3, _digest("0", 3), _hash(30)),
        RangeItem(logical_range, 4, _digest("0", 4), _hash(40)),
    ]

    repairs = compare_range_items(healthy, damaged)
    assert repairs.inserts == (1,)
    assert repairs.updates == (2,)
    assert repairs.deletes == (4,)
    assert repairs.healthy_heap_keys == (1, 2)
    assert repairs.total == 3


def test_range_item_rejects_route_digest_outside_requested_prefix():
    with pytest.raises(ValueError, match="outside"):
        RangeItem(LogicalRange(0, 2, 0b10), 1, _digest("11"), _hash(1))


def test_compare_range_items_rejects_duplicate_keys():
    logical_range = LogicalRange.root(0)
    duplicate = RangeItem(logical_range, 1, _digest("0"), _hash(1))
    with pytest.raises(RuntimeError, match="duplicate key"):
        compare_range_items([duplicate, duplicate], [])
