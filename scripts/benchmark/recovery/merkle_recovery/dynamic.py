"""Shape-independent logical-prefix recovery for dynamic Merkle indexes.

The dynamic tree may have a different physical split layout on the healthy and
damaged replicas.  Recovery therefore never pairs physical node identifiers.
It asks both indexes for summaries of the *same logical key-hash prefix* and
compares ``(tuple_count, data_xor)``.  Mismatching prefixes are expanded by one
logical fanout level at a time (``log2(fanout)`` MSB-first bits), independent of
the backend's one-bit physical splits, until both sides contain at most the
configured leaf capacity.  Bounded key/hash summaries then identify the exact
repair keys.

This module deliberately contains no SQL.  The database adapter lives in
``dynamic_db.py``; keeping the algorithm pure makes the shape-mismatch and
boundary invariants testable without a running PostgreSQL cluster.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence


DIGEST_BITS = 256
DIGEST_BYTES = DIGEST_BITS // 8
ZERO_DIGEST = b"\x00" * DIGEST_BYTES


def digest_bytes(value: Any) -> bytes:
    """Return a canonical 32-byte digest from bytea/hex API values."""
    if value is None:
        return ZERO_DIGEST
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        if len(value) != DIGEST_BYTES:
            raise ValueError(f"digest must be {DIGEST_BYTES} bytes, got {len(value)}")
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("\\x") or text.startswith("0x"):
            text = text[2:]
        if len(text) != DIGEST_BYTES * 2:
            raise ValueError(f"digest hex must be {DIGEST_BYTES * 2} characters")
        try:
            return bytes.fromhex(text)
        except ValueError as exc:
            raise ValueError("digest is not valid hexadecimal") from exc
    raise TypeError(f"unsupported digest value {type(value).__name__}")


def xor_digests(values: Iterable[bytes]) -> bytes:
    result = bytearray(DIGEST_BYTES)
    for value in values:
        digest = digest_bytes(value)
        for pos, byte in enumerate(digest):
            result[pos] ^= byte
    return bytes(result)


def candidate_summary_item_limit(bad_range_count: int, leaf_capacity: int) -> int:
    """Return the two-replica, size-independent candidate-summary bound."""
    if bad_range_count <= 0:
        raise ValueError("bad_range_count must be positive")
    if leaf_capacity <= 0:
        raise ValueError("leaf_capacity must be positive")
    return 2 * bad_range_count * leaf_capacity


def enforce_candidate_summary_bound(
    healthy_count: int,
    damaged_count: int,
    *,
    bad_range_count: int,
    leaf_capacity: int,
) -> int:
    """Fail closed if either replica or their total exceeds the K*C bound."""
    if healthy_count < 0 or damaged_count < 0:
        raise ValueError("candidate summary counts must be non-negative")
    per_side_limit = bad_range_count * leaf_capacity
    total_limit = candidate_summary_item_limit(bad_range_count, leaf_capacity)
    if healthy_count > per_side_limit:
        raise RuntimeError(
            "healthy dynamic candidate summaries exceeded the size-independent "
            f"bound: fetched={healthy_count}, limit={per_side_limit}"
        )
    if damaged_count > per_side_limit:
        raise RuntimeError(
            "damaged dynamic candidate summaries exceeded the size-independent "
            f"bound: fetched={damaged_count}, limit={per_side_limit}"
        )
    if healthy_count + damaged_count > total_limit:
        raise RuntimeError(
            "dynamic candidate summaries exceeded the size-independent bound: "
            f"fetched={healthy_count + damaged_count}, limit={total_limit}"
        )
    return total_limit


@dataclass(frozen=True, order=True)
class LogicalRange:
    """A canonical MSB-first route-digest prefix within one partition.

    ``prefix_value`` stores exactly ``prefix_length`` significant bits as an
    integer.  For example, prefix ``101`` is represented by
    ``prefix_length=3, prefix_value=5``.  The database representation is a
    32-byte value with those bits left-aligned and every unused bit zero.
    """

    partition_id: int
    prefix_length: int
    prefix_value: int

    def __post_init__(self) -> None:
        if self.partition_id < 0:
            raise ValueError("partition_id must be non-negative")
        if not 0 <= self.prefix_length <= DIGEST_BITS:
            raise ValueError(f"prefix_length must be in [0,{DIGEST_BITS}]")
        limit = 1 << self.prefix_length if self.prefix_length else 1
        if not 0 <= self.prefix_value < limit:
            raise ValueError(
                f"prefix_value does not fit prefix_length={self.prefix_length}"
            )

    @classmethod
    def root(cls, partition_id: int) -> "LogicalRange":
        return cls(partition_id, 0, 0)

    @classmethod
    def from_prefix_bytes(
        cls,
        partition_id: int,
        prefix_length: int,
        value: bytes | bytearray | memoryview | str,
    ) -> "LogicalRange":
        """Construct from a canonical, MSB-aligned 32-byte prefix value."""
        raw = digest_bytes(value)
        full = int.from_bytes(raw, "big")
        prefix = full >> (DIGEST_BITS - prefix_length) if prefix_length else 0
        # Reject non-canonical garbage in unused suffix bits.  Otherwise the
        # same logical range could have multiple encodings in artifacts/API
        # requests, which is dangerous for deterministic comparison.
        unused = DIGEST_BITS - prefix_length
        if unused and full & ((1 << unused) - 1):
            raise ValueError("prefix has non-zero unused suffix bits")
        return cls(int(partition_id), int(prefix_length), prefix)

    @property
    def prefix_bytes(self) -> bytes:
        shift = DIGEST_BITS - self.prefix_length
        return (self.prefix_value << shift).to_bytes(DIGEST_BYTES, "big")

    @property
    def prefix_hex(self) -> str:
        return self.prefix_bytes.hex()

    @property
    def label(self) -> str:
        if self.prefix_length == 0:
            bits = "root"
        else:
            bits = format(self.prefix_value, f"0{self.prefix_length}b")
        return f"p{self.partition_id}:{bits}/{self.prefix_length}"

    def child(self, bit: int) -> "LogicalRange":
        if bit not in (0, 1):
            raise ValueError("child bit must be 0 or 1")
        if self.prefix_length >= DIGEST_BITS:
            raise ValueError("cannot descend past a 256-bit route digest")
        return LogicalRange(
            self.partition_id,
            self.prefix_length + 1,
            (self.prefix_value << 1) | bit,
        )

    def descend(self, suffix_value: int, suffix_width: int) -> "LogicalRange":
        """Append an MSB-first logical suffix of ``suffix_width`` bits."""
        if suffix_width <= 0:
            raise ValueError("suffix_width must be positive")
        if self.prefix_length + suffix_width > DIGEST_BITS:
            raise ValueError("cannot descend past a 256-bit route digest")
        if not 0 <= suffix_value < (1 << suffix_width):
            raise ValueError("suffix_value does not fit suffix_width")
        return LogicalRange(
            self.partition_id,
            self.prefix_length + suffix_width,
            (self.prefix_value << suffix_width) | suffix_value,
        )

    def contains_digest(self, route_digest: Any) -> bool:
        if self.prefix_length == 0:
            return True
        value = int.from_bytes(digest_bytes(route_digest), "big")
        return value >> (DIGEST_BITS - self.prefix_length) == self.prefix_value

    def contains_range(self, other: "LogicalRange") -> bool:
        if self.partition_id != other.partition_id:
            return False
        if self.prefix_length > other.prefix_length:
            return False
        if self.prefix_length == 0:
            return True
        shift = other.prefix_length - self.prefix_length
        return other.prefix_value >> shift == self.prefix_value

    def to_request(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "prefix_length": self.prefix_length,
            "prefix_value": self.prefix_hex,
        }


@dataclass(frozen=True)
class RangeSummary:
    logical_range: LogicalRange
    tuple_count: int
    data_xor: bytes

    def __post_init__(self) -> None:
        if self.tuple_count < 0:
            raise ValueError("tuple_count must be non-negative")
        object.__setattr__(self, "data_xor", digest_bytes(self.data_xor))
        if self.tuple_count == 0 and self.data_xor != ZERO_DIGEST:
            raise ValueError("empty logical range must have zero data_xor")

    @classmethod
    def empty(cls, logical_range: LogicalRange) -> "RangeSummary":
        return cls(logical_range, 0, ZERO_DIGEST)

    @property
    def signature(self) -> tuple[int, bytes]:
        return self.tuple_count, self.data_xor


@dataclass(frozen=True)
class RangeItem:
    logical_range: LogicalRange
    key: int
    route_digest: bytes
    tuple_hash: bytes
    encoded_bytes: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "route_digest", digest_bytes(self.route_digest))
        object.__setattr__(self, "tuple_hash", digest_bytes(self.tuple_hash))
        if self.encoded_bytes < 0:
            raise ValueError("encoded_bytes must be non-negative")
        if not self.logical_range.contains_digest(self.route_digest):
            raise ValueError(
                f"key {self.key} route digest is outside {self.logical_range.label}"
            )


@dataclass
class LocalisationTrace:
    """Evidence produced while descending logical ranges."""

    bad_partitions: int = 0
    levels_visited: int = 0
    logical_ranges_compared: int = 0
    range_summary_rows: int = 0
    healthy_summary_rows: list[RangeSummary] = field(default_factory=list)
    damaged_summary_rows: list[RangeSummary] = field(default_factory=list)


SummaryFetcher = Callable[
    [str, Sequence[LogicalRange]],
    Mapping[LogicalRange, RangeSummary],
]


def _complete_summaries(
    requested: Sequence[LogicalRange],
    returned: Mapping[LogicalRange, RangeSummary],
) -> dict[LogicalRange, RangeSummary]:
    unexpected = set(returned) - set(requested)
    if unexpected:
        labels = ", ".join(sorted(r.label for r in unexpected))
        raise RuntimeError(f"dynamic range API returned unrequested ranges: {labels}")
    completed: dict[LogicalRange, RangeSummary] = {}
    for logical_range in requested:
        summary = returned.get(logical_range, RangeSummary.empty(logical_range))
        if summary.logical_range != logical_range:
            raise RuntimeError("dynamic range summary key/payload range mismatch")
        completed[logical_range] = summary
    return completed


def _logical_children(
    parent: LogicalRange,
    logical_fanout: int,
) -> tuple[LogicalRange, ...]:
    if logical_fanout <= 1 or logical_fanout & (logical_fanout - 1):
        raise ValueError("logical_fanout must be a power of two greater than one")
    remaining = DIGEST_BITS - parent.prefix_length
    if remaining <= 0:
        raise ValueError("cannot descend past a 256-bit route digest")
    logical_width = logical_fanout.bit_length() - 1
    width = min(logical_width, remaining)
    return tuple(parent.descend(ordinal, width) for ordinal in range(1 << width))


def _validate_children(
    parent: RangeSummary,
    children: Sequence[RangeSummary],
    schema: str,
) -> None:
    child_count = sum(child.tuple_count for child in children)
    if child_count != parent.tuple_count:
        raise RuntimeError(
            f"{schema} dynamic summaries violate count conservation at "
            f"{parent.logical_range.label}: parent={parent.tuple_count}, "
            f"children={child_count}"
        )
    if xor_digests(child.data_xor for child in children) != parent.data_xor:
        raise RuntimeError(
            f"{schema} dynamic summaries violate XOR conservation at "
            f"{parent.logical_range.label}"
        )


def localise_bad_ranges(
    healthy_roots: Mapping[int, RangeSummary],
    damaged_roots: Mapping[int, RangeSummary],
    fetch_summaries: SummaryFetcher,
    *,
    leaf_capacity: int,
    logical_fanout: int = 32,
    trace: LocalisationTrace | None = None,
) -> list[LogicalRange]:
    """Return bounded mismatching logical ranges, independent of tree shape.

    ``fetch_summaries`` must aggregate the requested logical prefix even when
    that prefix cuts through a physical leaf or spans several physical nodes.
    Empty ranges may either be omitted or returned explicitly as count=0/XOR=0.
    """
    if leaf_capacity <= 0:
        raise ValueError("leaf_capacity must be positive")
    if logical_fanout <= 1 or logical_fanout & (logical_fanout - 1):
        raise ValueError("logical_fanout must be a power of two greater than one")
    if trace is None:
        trace = LocalisationTrace()

    partitions = sorted(set(healthy_roots) | set(damaged_roots))
    frontier: list[tuple[RangeSummary, RangeSummary]] = []
    bad: list[LogicalRange] = []
    for partition_id in partitions:
        root = LogicalRange.root(partition_id)
        healthy = healthy_roots.get(partition_id, RangeSummary.empty(root))
        damaged = damaged_roots.get(partition_id, RangeSummary.empty(root))
        if healthy.logical_range != root or damaged.logical_range != root:
            raise RuntimeError("partition root API returned a non-root prefix")
        trace.logical_ranges_compared += 1
        trace.range_summary_rows += 2
        trace.healthy_summary_rows.append(healthy)
        trace.damaged_summary_rows.append(damaged)
        if healthy.signature == damaged.signature:
            continue
        trace.bad_partitions += 1
        if max(healthy.tuple_count, damaged.tuple_count) <= leaf_capacity:
            bad.append(root)
        else:
            frontier.append((healthy, damaged))

    while frontier:
        trace.levels_visited += 1
        requested: list[LogicalRange] = []
        for healthy_parent, _ in frontier:
            parent = healthy_parent.logical_range
            if parent.prefix_length >= DIGEST_BITS:
                raise RuntimeError(
                    f"mismatch remains after all route bits at {parent.label}"
                )
            # Ordinal order is the canonical MSB-first logical-slot order.
            requested.extend(_logical_children(parent, logical_fanout))

        healthy_rows = _complete_summaries(
            requested, fetch_summaries("healthy", requested)
        )
        damaged_rows = _complete_summaries(
            requested, fetch_summaries("damaged", requested)
        )
        trace.range_summary_rows += len(requested) * 2
        trace.healthy_summary_rows.extend(healthy_rows[r] for r in requested)
        trace.damaged_summary_rows.extend(damaged_rows[r] for r in requested)

        next_frontier: list[tuple[RangeSummary, RangeSummary]] = []
        for healthy_parent, damaged_parent in frontier:
            parent = healthy_parent.logical_range
            children = _logical_children(parent, logical_fanout)
            _validate_children(
                healthy_parent,
                [healthy_rows[child] for child in children],
                "healthy",
            )
            _validate_children(
                damaged_parent,
                [damaged_rows[child] for child in children],
                "damaged",
            )
            for child in children:
                healthy = healthy_rows[child]
                damaged = damaged_rows[child]
                trace.logical_ranges_compared += 1
                if healthy.signature == damaged.signature:
                    continue
                if max(healthy.tuple_count, damaged.tuple_count) <= leaf_capacity:
                    bad.append(child)
                else:
                    next_frontier.append((healthy, damaged))
        frontier = next_frontier

    ordered = sorted(bad)
    for pos, logical_range in enumerate(ordered):
        for other in ordered[pos + 1:]:
            if logical_range.contains_range(other) or other.contains_range(logical_range):
                raise RuntimeError("localisation returned overlapping logical ranges")
    return ordered


@dataclass(frozen=True)
class RepairKeys:
    inserts: tuple[int, ...]
    updates: tuple[int, ...]
    deletes: tuple[int, ...]

    @property
    def healthy_heap_keys(self) -> tuple[int, ...]:
        return tuple(sorted((*self.inserts, *self.updates)))

    @property
    def total(self) -> int:
        return len(self.inserts) + len(self.updates) + len(self.deletes)


def compare_range_items(
    healthy_items: Sequence[RangeItem],
    damaged_items: Sequence[RangeItem],
) -> RepairKeys:
    """Compare authoritative key/hash summaries and return exact repair keys."""

    def item_map(items: Sequence[RangeItem], schema: str) -> dict[int, bytes]:
        result: dict[int, bytes] = {}
        for item in items:
            if item.key in result:
                raise RuntimeError(
                    f"{schema} dynamic summary returned duplicate key {item.key}"
                )
            result[item.key] = item.tuple_hash
        return result

    healthy = item_map(healthy_items, "healthy")
    damaged = item_map(damaged_items, "damaged")
    healthy_keys = set(healthy)
    damaged_keys = set(damaged)
    inserts = tuple(sorted(healthy_keys - damaged_keys))
    deletes = tuple(sorted(damaged_keys - healthy_keys))
    updates = tuple(
        key
        for key in sorted(healthy_keys & damaged_keys)
        if healthy[key] != damaged[key]
    )
    return RepairKeys(inserts=inserts, updates=updates, deletes=deletes)
