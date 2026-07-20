# Dynamic Merkle implementation audit (2026-07-14)

This document reconciles `DYNAMIC_MERKLE_FINAL_IMPLEMENTATION_PLAN.md` and
`issue_resolution.md` with the implementation and test contract used for the
July 14, 2026 work.  The two source documents are useful design inputs, but
neither was independently sufficient as an implementation specification.

## Verdict on the source documents

The central direction is valid:

- keep a fixed number of top-level partitions;
- route with a canonical BLAKE3-256 key digest;
- replace preallocated perfect-tree leaves with bounded, sparse prefix leaves;
- split and merge deterministically;
- retain count and data summaries that can be compared across different
  physical tree shapes;
- update only through the ordered, committed Merkle applier; and
- recover by comparing identical logical prefixes, not physical node IDs.

The documents also contained contradictions or missing contracts that made a
literal implementation unsafe:

1. One document treated a position-sensitive BLAKE3 structural hash as the
   cross-replica root, while the other used a shape-independent XOR summary.
   A physical structural hash cannot compare equal across different valid
   split shapes.
2. Canonical key identity, reversibility for repair, partition routing, prefix
   bit order, the 256-bit tail, and route-collision behavior were not specified
   precisely enough for independent implementations to agree.
3. The proposed leaf-item key was not valid for nullable or non-unique Merkle
   keys.  Exact dynamic repair needs one stable logical identity per row.
4. Repeated changes to the same key, subtransaction rollback, speculative
   insertion, HOT update, logical replication, and BCDB serial-apply paths did
   not have a complete old-image to new-image transition contract.
5. CREATE, REINDEX, TRUNCATE, DROP, stale relfilenodes, state-table ACLs, and
   crash retry/idempotence were described incompletely.
6. The shape-mismatch recovery outline could fetch an unbounded subtree and
   did not define a batch API for arbitrary logical prefixes.
7. `issue_resolution.md` marked issues resolved at design level even though the
   live tree was still static format v7 and none of the dynamic state or APIs
   existed.
8. Neither document defined an executable 1M/3M/5M acceptance campaign.

## Authoritative implementation contract

The implementation therefore uses the following reconciled contract.

- Static format-v7 indexes remain supported and are the default.  Dynamic mode
  is explicit with `dynamic=on` and a separate durable layout marker.
- Dynamic indexes require the same ordered set of plain, NOT NULL columns to
  be protected by an immediate, valid, non-partial unique index.  Partial or
  expression dynamic Merkle indexes are rejected.
- Canonical key bytes are versioned, length-prefixed PostgreSQL binary-send
  encodings.  The route is BLAKE3-256 over those exact bytes.
- `partition_id = first_64_bits_big_endian(route_digest) % partitions`.
- Prefixes consume route bits most-significant-bit first.  Logical recovery
  directories group five bits at a time (fanout 32), while physical sparse
  leaves split one bit at a time at depths `0..256`.  Empty binary children
  are not materialized and deletion merges only deterministic buddy ranges.
  An over-capacity set still indistinguishable at 256 bits is an explicit
  route-digest collision and fails closed.
- Default leaf capacity is 32 items, merge threshold is 8 items, and both a
  leaf byte limit and canonical-key byte limit are enforced before user commit.
- WAL-logged internal tables are keyed by index OID plus its complete physical
  relfilenode generation.  They store sparse nodes, materialized leaf items,
  generation state, and bounded same-sequence idempotence markers.
- A cross-shape summary is `(tuple_count, data_xor_256)`.  The public combined
  root is a domain-separated BLAKE3 commitment over the ordered partition
  summaries, so count is not discarded.  A separate position-sensitive BLAKE3
  structural commitment validates each local physical tree.
- DML stages semantic net transitions `old item -> new item`, not only XOR
  deltas.  Presence flags make an all-zero row digest a valid item.  Repeated
  same-key mutations compose within each subtransaction, committed child
  frames merge into their parent, aborted frames disappear, and identity net
  no-ops are omitted.
- Every heap mutation captures the old identity before mutation and stages it
  only after success.  The contract covers ordinary INSERT/UPDATE/DELETE,
  HOT/non-HOT and key-changing updates, speculative `ON CONFLICT`, BCDB
  deferred/serial apply, COPY/AM insertion, and logical replication.
- Dynamic transitions are applied in global sequence order regardless of the
  static page batch size.  One PostgreSQL transaction contains item changes,
  deterministic split/merge rebuilding, node summaries, idempotence state,
  and apply-position advancement.
- Static node-number and bucket APIs reject dynamic indexes.  Dynamic recovery
  uses partition roots, arbitrary logical-prefix summaries, bounded range
  items, structural verification, and tree statistics.
- Recovery compares the same logical prefix on both replicas even when their
  physical shapes differ.  Non-physical prefix depths aggregate the next
  physical sparse-node frontier; bounded final ranges use the route-ordered
  item index.  Timed recovery never scans a user table.

## Acceptance campaign

The executable dynamic recovery profile is:

- tuple counts: 1,000,000; 3,000,000; 5,000,000;
- partitions: 200;
- logical fanout: 32;
- leaf capacity: 32;
- merge threshold: 8;
- selected bad physical ranges: 75;
- update corruptions: 300;
- repetitions: 5 per tuple count; and
- full post-repair audit enabled.

Each run must prove all of the following, not merely exit successfully:

- both directional full-table differences are zero;
- ordered partition `(tuple_count,data_xor)` roots match and their counts equal
  the heap counts;
- structural dynamic verification passes on healthy and damaged copies;
- recovery state is `READY` and no logical mismatching range remains;
- maximum physical leaf occupancy is at most 32;
- exactly 300 rows are repaired;
- the update-only bounded summary fetch is at most 4,800 item rows;
- timed recovery adds no sequential scan of either user table;
- the exact healthy-row fetch uses `healthy.usertable_pkey`; and
- commands, configuration, manifests, range/item traces, tree statistics,
  plans, environment, and final validation results are retained in the result
  directory.

Build, regression, crash/restart, and the three scale results are recorded in
the final task handoff after they are executed; none is interchangeable with
another proof tier.
