# Dynamic Merkle tree: gap analysis and implementation plan

Cross-referenced against: `Meeting_transcript_9th_July.txt`, `13th`, `23rd`, `27th`, `MERKLE_DESIGN_23_27_JULY.md`, and the actual source at commit `1875206` (`merkle.h`, `merkleutil.c`, `merkleinsert.c`, `merklebuild.c`, `merkledelta.c`, `merkleapply.c`, `merkleverify.c`).

## Sourcing note

The target architecture throughout this document is the **23rd/27th July simplified design** (one relation mapping prefix → hash/is_leaf/tuple_count, reusing the base table's covering B-tree index instead of storing keys or child-ID arrays in the node itself). The 9th/13th July meetings explored a different, more complex structure — per-leaf arrays of key-hash pairs, per-internal-node arrays of child node IDs, `num_pointers`/`offset` bookkeeping to defer hash recomputation — which Sir discarded in favor of the simpler B-tree-backed approach. None of that discarded schema appears anywhere below.

9th/13th July are cited in exactly two places, both for architecture-agnostic correctness/algorithmic content that survived the simplification, not for any discarded storage mechanics: the determinism/order-independence requirement (Section 2), and the bottom-up merge-triggering condition (Section 9). Both apply identically to the 23rd/27th relation-based design.

## Verdict

`MERKLE_DESIGN_23_27_JULY.md` is not perfectly planned out. It's a correct and reasonably complete synthesis of the **logical** structure agreed on 23rd/27th July, but it has three defects:

1. It drops a hard requirement stated explicitly on 13th July (tree structure must be **order-independent** given concurrent, conflict-free transaction execution) — this never made it into the design doc at all.
2. It never reconciles the new logical design with the **existing physical implementation** it has to replace — specifically the WAL-safe, fixed-`leaf_id`-keyed, asynchronously-applied committed-delta pipeline in `merkledelta.c`/`merkleapply.c`. That pipeline is the reason your throughput work (async durable log store, CAS contention findings) exists at all, and the July design doc's Section 9 implementation order doesn't mention it once.
3. It leaves open whether the Merkle structure remains a custom PostgreSQL index access method (`merklehandler`/`IndexAmRoutine`) or dissolves into ordinary relations + a B-tree. The 27th July transcript answers this ("we don't have to implement all of this... the index is already there... we don't need a separate node") but the design doc doesn't say so explicitly, and this decision determines almost everything else (recovery API, VACUUM behavior, WAL, crash safety).

Below is the gap analysis first, then a concrete plan that resolves all three, grounded in what's actually in your repo today.

---

## 1. What's already true in the code (less new work than the design doc implies)

Checked directly against `merkleutil.c:515` (`merkle_compute_route`) and `merkle.h:212`:

- `MerkleRoute` already carries the **full 256-bit BLAKE3 digest** (`route_digest[32]`), not just the 64-bit `static_route_value` used for the fixed `leaf_id`. Requirement 2 of the design doc's Section 9 ("make the full routing hash available") is **already computed on every insert** — it's just discarded after `leaf_id` is derived from the first 8 bytes. The gap is *persistence and indexing*, not *computation*.
- `merkle_compute_row_hash()` / `merkle_compute_slot_hash()` already hash the **whole tuple** via canonical per-column send-function serialization (`merkle_hash_slot_canonical`), separately from the routing digest. The tuple-hash/routing-hash separation that the 23rd July meeting spent 40 minutes re-deriving (transcript lines 23–107) is already the invariant your code enforces. Good — don't let anyone talk you into conflating them during the rewrite.
- `merkle_get_children_batch()` (`merkleverify.c:2130`) already exists and returns **one level** of children hashes for a batch of parent nodes. This is the "one-level baseline" the 27th July transcript treats as already-standard practice before proposing 2–3 level batching. You don't need to build this from scratch — you need to generalize it from fixed-geometry `(partition, node_in_partition)` coordinates to variable-length prefixes, and extend it to walk N levels instead of 1.

None of this is a correction to the design doc's content — it's just evidence that steps 2 and part of 6 of the July 9-step implementation order are closer to done than the doc suggests. Say so explicitly if you write this up for Prof. Sudarshan; it changes the effort estimate.

---

## 2. Critical gap: determinism / order-independence (13th July, not in the design doc)

Transcript, 13th July, lines 483–509 (paraphrased, not quoted verbatim per the two people talking):

> Sudarshan raises that BCDB runs transactions concurrently when they don't conflict at the read/write level, but they *can* still land on the same Merkle leaf and split it. Two replicas executing insert(A) then insert(B) vs. insert(B) then insert(A) must not diverge in tree shape, or replica comparison breaks. He explicitly says any merge policy must also be deterministic, "based on the counts."

This is a first-class correctness requirement for a **BFT deterministic database**, and it is nowhere in `MERKLE_DESIGN_23_27_JULY.md`. It has to be stated as an invariant before you write a single line of split code:

> **Invariant D1**: For any two replicas that have applied the same committed-delta log up to sequence N, the dynamic Merkle structure (set of live prefixes, their `is_leaf`/`tuple_count`/hash, and any merge state) must be byte-identical, regardless of the physical order in which concurrently-committed transactions were executed at each replica, and regardless of `merkle_apply_batch_time_ms`-driven batch boundaries (which are wall-clock-based and therefore **not** guaranteed identical across replicas — see Section 3).

Practical consequence: split/merge decisions cannot be "check after each transaction commits locally." They must be a pure function evaluated over the **globally ordered delta log** (the same one Kafka result-voting already gives you), evaluated at points that don't depend on per-replica batching. Section 3 below gives the concrete mechanism.

If you don't pin this down before implementing splits, you will reproduce exactly the class of bug the 13th July meeting spent 25 minutes on with the "all keys hash to one side, gets split repeatedly" skew case (lines 465–479) — except now non-deterministically across replicas instead of just unbalanced on one replica.

---

## 3. Critical gap: the committed-delta apply pipeline assumes a fixed, static leaf_id

This is the gap most likely to blow up your implementation timeline if you don't plan for it now, and it's specific to *your* codebase, not something the meetings would have caught (Sudarshan wasn't looking at `merkledelta.c`).

**Current design** (`merkledelta.c`):
```c
typedef struct MerkleDeltaKey {
    Oid         index_oid;
    RelFileNode index_rnode;
    int32       leaf_id;        // <-- fixed, computed once at commit time
} MerkleDeltaKey;
```
Transactions never mutate Merkle pages directly. Each transaction stages an XOR delta keyed by `leaf_id`, computed once via `merkle_compute_route()` at DML time. A separate ordered applier (`merkleapply.c`, batched by `MERKLE_APPLY_DEFAULT_BATCH_ITEMS`/`_BYTES`/`_PAGES`/`_TIME_MS`) later replays these deltas in global sequence order and XORs them into fixed-position node arrays on WAL-safe v7 pages (`MerklePageOpaqueData.last_applied_seq`).

**Why this breaks under a dynamic tree**: `leaf_id` is computed once, at commit time, from a fixed modulus (`static_route_value % total_leaves`). In a dynamic tree, "which node does this tuple's hash belong to" is a function of the **current** tree shape, which can change (via a split) between when the delta is staged and when the async applier catches up — and `merkle_apply_batch_time_ms` means that gap is real, not theoretical. If you keep keying deltas by a `leaf_id` computed at commit time, a split that happens between commit and apply will cause the delta to land in the wrong (stale) place, permanently, on whichever replica happened to apply it before vs. after that split's own delta was processed. That's a silent correctness bug, not a performance one.

**Required change**: the delta record must carry the **full routing digest** (you already compute this — see Section 1), not a precomputed `leaf_id`/prefix. Resolution to a concrete node happens **at apply time**, by walking the dynamic node relation from the root down, consuming more bits until `is_leaf = true`, exactly the way the 23rd July transcript describes tree traversal — except this walk now has to happen inside the ordered applier, not inside `merkleInsert()`.

```
MerkleDeltaKey {
    Oid         index_oid;
    RelFileNode index_rnode;
    uint8       route_digest[8];   // first 64 bits, see Section 8 on width
}
```
This also means **splits themselves must be replayed through the same ordered applier**, as first-class entries in the delta log (not as an out-of-band side effect a backend does opportunistically). Concretely: the applier, after resolving a batch of leaf deltas to their current nodes, checks each touched node's `tuple_count` against the threshold and performs any resulting split *before* moving its `last_applied_seq` watermark past that point. Because the applier is already single-threaded and ordered (that's the whole point of the design you profiled), this gives you Invariant D1 for free — determinism doesn't have to be reasoned about per-transaction, it falls out of "the applier is a deterministic function of the ordered log," which is a property you already built and benchmarked. This is the strongest argument for *not* discarding the existing async delta pipeline and instead re-targeting it at the new node relation.

**Do not** re-derive the async batching machinery from scratch for the dynamic tree. Re-point it.

---

## 4. Architectural fork: does the custom index AM survive? — **Decided: yes**

**Decision: the Merkle structure stays a genuine PostgreSQL index access method. `merklehandler`/`IndexAmRoutine` is not retired, and maintenance is not done via an external SQL/PL-language trigger object.** This overrides my original lean toward dissolving the AM (below is the corrected version).

What "genuine index AM, not an external trigger" means concretely:

- `CREATE INDEX ... USING merkle(...)` continues to exist and continues to be backed by a real `IndexAmRoutine` (`merklehandler`). `ambuild`/`aminsert`/`ambulkdelete`/`amvacuumcleanup` remain the entry points that do real work — they are not thinned into stubs.
- The route-digest and tuple-hash values that populate the base table's auxiliary columns (Section 6b) are written by the **AM's own C callbacks** — i.e., `merkleInsert`/`merkleBuild` (and the executor hooks in `nodeModifyTable.c` that already call into them directly today) are extended to also populate those columns, using the exact same `merkle_compute_row_hash`/`merkle_compute_canonical_route_digest` calls they already make. There is no separate `CREATE TRIGGER` object, no PL/pgSQL, no independent trigger function in `pg_trigger` — the maintenance path is internal to the index AM, exactly as it is today for the fixed-geometry XOR update. Section 7 is revised accordingly.
- The one piece of this design that is *not* part of the Merkle AM and *has* to be an ordinary standard B-tree: the **covering index** used for index-only range scans during split/recovery (Section 6b). This isn't in tension with "keep it a genuine AM" — the Merkle AM's own build/DDL path is what creates and owns this auxiliary btree as a required companion object, rather than the user or a generic trigger managing it. The reason it must specifically be a standard btree (not part of the Merkle AM's own storage) is the one substantive point the 27th July transcript makes: PostgreSQL's index-only scan machinery is btree machinery, and reimplementing that inside a custom AM is exactly the reinvention the meeting is trying to avoid. So: one user-visible `USING merkle` index (real AM, drives the node relation), one internally-managed auxiliary btree (index-only scans for split/recovery), zero triggers.
- A small **node relation** (ordinary heap table, one row per Merkle node — see schema in Section 6a) holds `(node_id/prefix, prefix_len, is_leaf, hash, tuple_count)`. This is what the AM's callbacks (via `merkleapply.c`) mutate instead of raw index pages.

What you keep from the existing AM code, largely unchanged:
- `merkle_compute_row_hash`/`merkle_compute_slot_hash` (tuple hashing) — unchanged.
- `merkle_compute_canonical_route_digest` (routing digest) — unchanged, just stop truncating it away.
- The `merkledelta.c`/`merkleapply.c` async, ordered, crash-safe apply model — re-targeted (Section 3), not rewritten.
- `merkle_hash_xor`/zero/hex helpers — unchanged.
- `merklehandler`/`IndexAmRoutine` registration, `ambuild`/`aminsert`/`ambulkdelete` entry points — unchanged as entry points; their bodies are rewritten to target the node relation instead of raw pages.
- The GUCs (`merkle_apply_batch_*`, `merkle_read_lag_policy`, recovery profiling) — unchanged in spirit; the profiling counters need new fields for split counts and prefix-walk depth.

What gets removed/replaced:
- `MerkleMetaPageData`, `MerklePageOpaqueData`, packed `MerkleNode` page arrays, `merkle_geometry_*` fixed-arithmetic helpers (`global_node`/`leaf_node`/`parent_node`/`child_node`) — all of this is perfect-k-ary-forest arithmetic that has no meaning once nodes are variable-depth prefixes. It's replaced by ordinary relation lookups keyed by prefix, driven from inside the same AM callback functions.
- Cost estimation and scan support (`amrescan`/`amgettuple`-equivalent) stay exactly as vestigial as they are today — nothing ever queries the Merkle index via a normal index scan today either, so this isn't a new decision, just a continuation of the status quo.

---

## 5. Two-tier structure: routing/traversal vs. covering/split-recovery

Conflating these is the most common way this design goes wrong in practice, and the meetings themselves circle it more than once (13th July lines 223–261 vs. 27th July's simplification). State it explicitly:

| | Traversal index (hot path) | Covering hash index (cold path) |
|---|---|---|
| Backs | node relation (Section 6a) | base table generated/maintained column (Section 6b) |
| Used by | every INSERT/UPDATE/DELETE, to find the current leaf for a routing digest | split (regroup children), recovery rebuild, corruption re-derivation |
| Access pattern | point lookup by increasing prefix length | ordered range scan, index-only |
| Frequency | once per DML op (or once per delta-apply, per Section 3) | once per split (rare, threshold-gated), once per recovery pass |

The 27th July "we don't have to go to the tuple" simplification is about the **covering** index only. The hot path never touches it — it walks the node relation exactly as the static tree walks fixed geometry today, just with a variable number of steps.

---

## 6. Concrete schema

### 6a. Node relation (replaces the custom page format)

```sql
CREATE TABLE merkle_node (
    index_oid    oid    NOT NULL,
    node_id      bigint NOT NULL,   -- first prefix_len bits of the 64-bit route prefix, right-padded with 0
    prefix_len   smallint NOT NULL, -- 0..64
    is_leaf      boolean NOT NULL,
    tuple_count  bigint  NOT NULL DEFAULT 0,
    hash         bytea   NOT NULL,  -- 32 bytes, XOR-aggregated BLAKE3 tuple hash, unchanged width
    PRIMARY KEY (index_oid, node_id, prefix_len)
);
CREATE INDEX merkle_node_lookup_idx ON merkle_node (index_oid, node_id, prefix_len);
```

`node_id` stores the prefix left-justified in a 64-bit int (bits beyond `prefix_len` are zero), so that a node and all its eventual descendants share a numerically comparable prefix — this is what lets the covering-index range trick (Section 3 of the design doc) and this table's own descendant queries both use simple integer comparisons instead of bit-string manipulation. Traversal is: start at `prefix_len = 0`; look up `(index_oid, node_id, prefix_len)`; if `is_leaf`, stop; else take the next `log2(fanout)` bits of the routing digest, extend `node_id`/`prefix_len`, repeat.

This single relation replaces `MerkleMetaPageData` + the packed page arrays entirely for a given index. One relation instance can serve multiple indexed tables if you key by `index_oid`, or you can go with one node relation per indexed table (simpler, avoids a hot shared table across unrelated workloads — given your CAS-contention history, prefer **one node relation per Merkle-indexed table**, not a shared global one).

### 6b. Base table hash column + covering index

```sql
ALTER TABLE users
    ADD COLUMN merkle_route_digest bytea,   -- 8 bytes: full routing digest prefix used for node_id derivation
    ADD COLUMN merkle_tuple_hash   bytea;   -- 32 bytes: unchanged BLAKE3 row hash

CREATE INDEX users_merkle_covering_idx
    ON users (merkle_route_digest)
    INCLUDE (merkle_tuple_hash);
```

See Section 7 for why these are AM-callback-maintained columns (populated by `merkleInsert`/`merkleBuild` directly), not `GENERATED ALWAYS AS ... STORED` and not a separate SQL/PL trigger object.

---

## 7. Column maintenance: AM-internal, not a generated column and not a trigger

The 23rd/27th July transcripts leave this open ("generated column... or an actual column that we maintain"), and per Section 4's decision, the answer is neither of the two options the transcript floated — it's maintenance from inside the index AM's own callbacks, the same place this is already handled today.

Why not `GENERATED ALWAYS AS ... STORED`: `merkle_compute_row_hash`/`merkle_compute_slot_hash` build the canonical hash by walking a `TupleTableSlot` and calling each column's binary **send function** (`merkle_hash_slot_canonical`). That's C-level, slot-based serialization — it is not expressible as a single immutable SQL expression over "the other columns of this row" the way a generated column requires. Re-deriving an equivalent pure-SQL canonical serialization risks silently diverging from the existing hash semantics. `merkle_compute_canonical_route_digest` has the same shape.

Why not a separate SQL/PL trigger either, given Section 4's decision: introducing a `CREATE TRIGGER` object would mean the hash/route-digest maintenance lives partly inside the index AM (node relation, splits) and partly outside it (a generic trigger on the base table) — two different maintenance mechanisms for two halves of the same conceptual operation, and a trigger object visible in `pg_trigger` that has nothing to do with the "this is a real index AM" story. It also duplicates work: `nodeModifyTable.c` already calls directly into Merkle-specific executor hooks (`ExecInsertMerkleIndexes`/`ExecDeleteMerkleIndexes`) that in turn call `merkleInsert`; adding a trigger means the executor now does two separate passes over the row (one via the AM hook, one via the generic trigger machinery) to get two related pieces of derived data.

**Recommendation**: extend the existing AM callback path. `merkleInsert` (`merkleinsert.c`) already computes `route.route_digest` and the tuple hash on every call; have it also write `merkle_route_digest`/`merkle_tuple_hash` onto the heap tuple being inserted, in the same executor pass, via the same mechanism `nodeModifyTable.c` already uses to reach Merkle-specific code before/around the generic tuple insert. `merkleBuild` does the equivalent during bulk build. This reuses the exact existing hashing code, adds no new trigger object, and keeps every piece of Merkle-related row mutation under the index AM, consistent with Section 4.

This needs one implementation detail nailed down early: PostgreSQL's index AM contract normally receives `values`/`isnull` for the *indexed columns only*, computed *after* the heap tuple has already been formed and inserted — `aminsert` conventionally doesn't get to mutate other columns of the row being inserted. Confirm during step 2 of Section 15 whether your executor integration (already non-standard — `nodeModifyTable.c` calls Merkle functions directly rather than through the plain AM contract) gives you a hook point *before* the heap tuple is finalized, or whether you need an explicit two-step heap update (insert, then update the two auxiliary columns) inside the same executor-level Merkle hook, still without going through a generic SQL trigger. Either is consistent with "AM-internal, not an external trigger"; which one is possible depends on where exactly your existing hook fires relative to heap tuple formation, which I haven't traced in this pass.

---

## 8. Hot-path DML algorithm (unchanged O(1) semantics)

Applies inside the ordered applier (Section 3), not synchronously in `merkleInsert` — that's the whole reason the async delta pipeline exists and you shouldn't undo that.

```
apply_leaf_event(index_oid, route_digest_64, tuple_hash_delta, is_insert):
    node_id, prefix_len = 0, 0
    loop:
        row = SELECT is_leaf, hash, tuple_count
              FROM merkle_node
              WHERE index_oid = :index_oid AND node_id = :node_id AND prefix_len = :prefix_len
        if row.is_leaf:
            new_hash  = row.hash XOR tuple_hash_delta
            new_count = row.tuple_count + (1 if is_insert else -1)
            UPDATE merkle_node SET hash = new_hash, tuple_count = new_count
                WHERE index_oid = :index_oid AND node_id = :node_id AND prefix_len = :prefix_len
            propagate_xor_to_ancestors(index_oid, node_id, prefix_len, tuple_hash_delta)  # if internal nodes also carry combined hashes, per Section 8 design-doc table row "Aggregate"
            if new_count > SPLIT_THRESHOLD: enqueue_split(index_oid, node_id, prefix_len)   # deterministic check, Section 3
            if new_count < MERGE_THRESHOLD: enqueue_merge_check(index_oid, node_id, prefix_len)  # 13th July merge rule, Section 9
            return
        else:
            bits = next_bits(route_digest_64, prefix_len, BITS_PER_LEVEL)
            node_id, prefix_len = extend(node_id, prefix_len, bits, BITS_PER_LEVEL)
```

Whether internal nodes need their own combined hash (for cheap root/subtree comparison during recovery without descending) or whether the design intends only leaves to carry the aggregate and internal-node comparison is done by re-deriving from children on demand — the 23rd July transcript (lines 331–341) settles this: **every node, leaf or internal, stores a hash and `tuple_count`**, and only `is_leaf` differs in interpretation. Implement `propagate_xor_to_ancestors` accordingly; don't leave internal-node hashes unset, or recovery has to do a full descent to compare anything, defeating the point.

---

## 9. Split (and merge) algorithm

Matches design doc Section 5, made concrete against the schema above, plus the 13th July merge rule (lines 515–553: merge triggers when a non-leaf's total `tuple_count` drops below the leaf split threshold *and* all its children are currently leaves — check bottom-up so a non-leaf child with an even lower count is already resolved first).

```
do_split(index_oid, node_id, prefix_len):
    lower = node_id
    upper = node_id | ((1 << (64 - prefix_len)) - 1)
    rows = SELECT merkle_route_digest, merkle_tuple_hash
           FROM base_table
           WHERE merkle_route_digest BETWEEN :lower AND :upper   -- index-only scan, Section 6b
    resolve_children(rows, node_id, prefix_len)   -- bounded recursive resolution, see below

resolve_children(rows, node_id, prefix_len):
    child_prefix_len = prefix_len + BITS_PER_SPLIT     -- BITS_PER_SPLIT = log2(FANOUT), Section 13
    group rows (already in memory, no re-scan) by their next BITS_PER_SPLIT bits into up to FANOUT buckets
    for each non-empty bucket with rows R, child_node_id, child_count = |R|:
        if child_count > SPLIT_THRESHOLD and child_prefix_len < MAX_PREFIX_LEN:
            resolve_children(R, child_node_id, child_prefix_len)     -- recurse in-memory, no extra I/O
        else:
            child_hash = XOR of all merkle_tuple_hash in R
            INSERT INTO merkle_node (index_oid, node_id=child_node_id, prefix_len=child_prefix_len,
                                      is_leaf=true, tuple_count=child_count, hash=child_hash)
            -- child_count may still exceed SPLIT_THRESHOLD here only if child_prefix_len == MAX_PREFIX_LEN:
            -- an accepted, terminal over-full leaf (see corner case below), not a bug
    if prefix_len == original split target:
        UPDATE merkle_node SET is_leaf = false WHERE index_oid=:index_oid AND node_id=:node_id AND prefix_len=:prefix_len
    else:
        INSERT INTO merkle_node (index_oid, node_id, prefix_len, is_leaf=false, tuple_count=|rows|, hash=XOR of rows)
    -- parent's own `hash`/`tuple_count` are untouched at the top level by the split itself (XOR of children == old aggregate, by construction)

do_merge(index_oid, node_id, prefix_len):
    require: is_leaf = false, all direct children currently is_leaf = true, sum(children.tuple_count) < MERGE_THRESHOLD
    merged_hash  = XOR of all children hashes
    merged_count = sum of children tuple_count
    DELETE FROM merkle_node WHERE index_oid=:index_oid AND prefix_len = child_prefix_len AND node_id BETWEEN :lower AND :upper
    UPDATE merkle_node SET is_leaf = true, hash = merged_hash, tuple_count = merged_count
        WHERE index_oid=:index_oid AND node_id=:node_id AND prefix_len=:prefix_len
```

Both run inside the ordered applier, gated by `enqueue_split`/`enqueue_merge_check` from Section 8 — never triggered by a user backend directly. This is what gives you Invariant D1 without extra bookkeeping: same delta log in, same sequence of splits/merges out, on every replica, regardless of transaction interleaving at execution time.

### Corner case (resolved): all rows land in one child bucket

13th July (lines 433–451) raises this and doesn't resolve it; the design doc leaves it open. Decided strategy: **bounded in-memory recursive resolution, capped at `MAX_PREFIX_LEN`** (the `resolve_children` recursion above), not unbounded recursive splitting and not a naive single-level split that leaves an over-full child for the next insert to re-trigger.

Reasoning:
- **Unbounded recursion is not just risky, it's incorrect in general.** Two rows with an *exactly identical* routing digest (extremely rare, but possible with a genuine hash collision or, more realistically, a legitimate duplicate key value) can never be separated by taking more bits, no matter how deep you go — recursion without a hard cap does not terminate in that case. A cap is required for correctness, not just for tidiness.
- **Resolving it eagerly, inside the same `do_split` call, is cheap.** The full row set for the range is already in memory from the one index-only scan; partitioning it further by additional bits costs no extra I/O. Doing this eagerly avoids leaving a known-overloaded bucket that the *next* insert has to re-discover and re-split (which would otherwise mean repeated single-step splits into the same skewed branch, each paying its own scan/applier-batch cost).
- **The cap terminates deterministically and matches Sudarshan's own stated attitude** ("that's a corner case, doesn't matter much... if you want to balance it, it becomes a different kind of structure" — 13th July line 479): once `prefix_len` hits `MAX_PREFIX_LEN`, stop recursing and accept the bucket as an over-full leaf. This is the mathematically correct terminal case for exact-duplicate digests, not a shortcut taken for engineering convenience.
- `MAX_PREFIX_LEN = 60` (a clean multiple of `BITS_PER_SPLIT = 5`, i.e. 12 levels of 5-bit splits, leaving the low 4 bits of the 64-bit prefix always zero beyond that depth so every split step has a full, uniform `BITS_PER_SPLIT`-bit chunk to consume, with no ragged final level). At `FANOUT = 32`, `SPLIT_THRESHOLD = 32`, 60 bits of address space is many orders of magnitude beyond what any realistic row count requires (a balanced tree needs roughly `log(FANOUT)(N / SPLIT_THRESHOLD)` levels; even at N = 10^12 that's under 10 levels, i.e. under 50 bits) — hitting the cap in practice should only happen under adversarial/duplicate-key conditions, which is exactly the case it exists to handle.
- This is deterministic and safe for Invariant D1: the recursion is a pure function of the (deterministically-arrived-at) row set for the range, and `MAX_PREFIX_LEN`/`BITS_PER_SPLIT`/`SPLIT_THRESHOLD` are fixed, versioned constants (Section 14), not per-replica tunables.

---

## 10. Bulk build (replaces `merkleBuild`'s in-memory partition-array accumulation)

The current `merkle_build_callback` (`merklebuild.c:118`) does a single heap scan, XORing directly into a fixed in-memory array sized by the perfect-tree geometry — that approach has no equivalent "size" to preallocate once node count is data-dependent. Recommended replacement: **top-down recursive radix partitioning**, computed once the covering index (Section 6b) exists and is fully populated by a first pass:

```
build(index_oid, node_id=0, prefix_len=0, digest_range=[full 64-bit range]):
    count = SELECT count(*) FROM base_table WHERE merkle_route_digest IN digest_range   -- index-only
    if count <= SPLIT_THRESHOLD:
        hash = XOR aggregate over the range (index-only scan)
        INSERT merkle_node(is_leaf=true, tuple_count=count, hash=hash, ...)
        return hash
    else:
        child_hash = XOR of build(...) for each of the 2^BITS_PER_SPLIT sub-ranges   -- recurse
        INSERT merkle_node(is_leaf=false, tuple_count=count, hash=child_hash, ...)
        return child_hash
```

This is a single index-only scan overall (each row visited once, at its terminal recursion level via range boundaries derivable without re-scanning) if implemented as a sort-merge over the already-ordered covering index rather than naive repeated range-count queries per node — worth spelling out as a `CREATE INDEX`-time bulk-load routine analogous to a B-tree bulk build, not a naive repeated-query recursive implementation. Get this right; a naive version re-scans overlapping ranges O(depth) times over N rows.

---

## 11. Recovery: generalizing `merkle_get_children_batch`

Current function (`merkleverify.c:2130`) takes arrays of `(partition, node_in_partition)` and returns exactly the immediate children (fixed `fanout`) of each, via `merkle_geometry_child_node` arithmetic. Generalize to:

```sql
merkle_get_descendants_batch(relid oid, node_ids bigint[], prefix_lens smallint[], depth int)
```
returning every live node (leaf or internal) within `depth` levels below each given node — a `WITH RECURSIVE` query over `merkle_node`, bounded by `depth`, is the natural implementation now that children are just rows with a longer matching prefix, not arithmetic offsets. Keep the existing 1-level call as the `depth=1` case for A/B comparison, per the design doc's explicit ask (Section 6.2).

**Recalculate the depth-vs-bandwidth tradeoff for `FANOUT = 32`, not the transcript's illustrative `fanout = 4`.** The 27th July "depth 2 is 16 nodes, depth 3 is 64 nodes" numbers assume fanout 4. At the decided `FANOUT = 32` (Section 13), depth 2 is up to 32² = 1,024 nodes and depth 3 is up to 32,768 — a single-level mismatch (depth 1, up to 32 nodes) is already a substantially larger batch per round trip than the transcript's fanout-4 baseline, so the marginal benefit of going deeper is smaller and the bandwidth cost grows far faster. **Default recovery batch depth to 1** for this fanout; treat depth 2 as an experimental A/B option and don't default to depth 3 at all without measuring actual round-trip-vs-bandwidth numbers first — the transcript's specific depth recommendation doesn't transfer across the fanout change and needs to be re-derived, not copied.

Canonical ordering requirement (design doc Section 6.3, 23rd July line 5): serialize the returned descendant set sorted by `(prefix_len, node_id)` on both sides before comparing, so logically-identical-but-differently-ordered result sets from two replicas still compare equal.

---

## 12. Concurrency and performance risk — read this given your own bottleneck history

You've already found, empirically, that this system's throughput ceiling moves between serialized Raft append → single-threaded fdatasync → commit-watermark CAS contention as you fix each bottleneck in turn. This redesign introduces a **new** candidate bottleneck that didn't exist in the static tree, and you should benchmark for it explicitly rather than discover it three profiling rounds from now:

- The custom page format never left behind dead tuple versions — an XOR into a fixed page slot is an in-place page mutation under your own WAL-safe scheme, not an MVCC `UPDATE`. The node relation in Section 6a is an ordinary heap table; every leaf hash update is now a normal `UPDATE`, producing a dead tuple version. A small number of leaf nodes absorbing a disproportionate share of writes (exactly the skew case the 13th July meeting worried about) is now a **hot-row bloat and autovacuum** problem, structurally similar in shape to a hot-counter-row problem, not just a lock-contention one. Plan for aggressive/targeted autovacuum settings on `merkle_node` (low scale factor, maybe `fillfactor` tuning) from the start, and put "bloat on `merkle_node` under skewed key distributions" on your benchmark list next to the throughput numbers you already track.
- Because splits/merges are funneled through the single ordered applier (Section 3, deliberately), they inherit whatever serialization characteristics that applier already has. This is good for correctness (Section 2/3) but means split cost is now visible on the same critical path as your commit-watermark work — a burst of splits (e.g., initial ramp-up while the tree is shallow and every partition splits repeatedly) could show up as an applier-side latency spike distinguishable from, but adjacent to, the bottlenecks you already diagnosed. Worth a dedicated micro-benchmark: sustained insert throughput during the "tree still shallow, splitting frequently" phase vs. steady-state.

---

## 13. Concrete parameters — **decided**

| Parameter | Value | Notes |
|---|---|---|
| `FANOUT` | 32 | Reloption, not a compile-time constant. Matches the existing static tree's `MERKLE_DEFAULT_FANOUT`, minimizing new tunables and reusing a value you already have benchmark history against. This is a deliberate departure from the 27th July meeting's small-fanout (2–4) suggestion, which was framed there as an optimization to try, not a requirement — see the recovery-depth consequence in Section 11. |
| `BITS_PER_SPLIT` | `log2(FANOUT)` = 5, **derived, not independently configurable** | Not its own reloption. Always computed from `FANOUT` at build/format time; storing it separately would let it drift out of sync with `FANOUT` and break the traversal/split arithmetic. |
| `SPLIT_THRESHOLD` | 32 | Reloption. Tuples per leaf before it splits. |
| `MERGE_THRESHOLD` | 8 | Reloption. A non-leaf whose children collectively fall below this, with all children currently leaves, merges back (13th July rule, Section 9). `MERGE_THRESHOLD < SPLIT_THRESHOLD` (here, 1/4) is required to avoid split/merge oscillation right at a single threshold — standard B-tree hysteresis practice, not a new idea introduced here. |
| `MAX_PREFIX_LEN` | 60 | Not a reloption — fixed per format version (Section 14). Bounds the recursive split resolution in Section 9; must be identical across all replicas for Invariant D1, so it is not user-tunable per index. |
| Node-id / prefix width | 64 bits, from the high 8 bytes of the already-computed `route_digest` | Matches `MerkleRoute.static_route_value`, which already exists — no new hashing work. |
| Tuple hash width | Unchanged, 256-bit BLAKE3 | Per design doc Section 8 ("same logical aggregate unless a later design changes it"). Do not conflate with the node-id width above — different fields, different size rationales (collision resistance for integrity vs. address space for routing). |
| Recovery descendant batch depth | Default 1; depth 2 experimental only (Section 11) | Re-derived for `FANOUT = 32`, not copied from the transcript's fanout-4 illustration. |

`FANOUT`/`SPLIT_THRESHOLD`/`MERGE_THRESHOLD` are reloptions (settable per index, benchmarkable). `BITS_PER_SPLIT` (derived) and `MAX_PREFIX_LEN` (fixed) are not — they're format-level constants that every replica must agree on for determinism, not per-index tuning knobs.

---

## 14. Format versioning

Bump `MERKLE_VERSION` (currently 7) to reflect the storage-model change — this is not a page-format tweak, it's a change from custom pages to relation-backed storage, which is a bigger jump than prior version bumps in this file's history suggest is typical. Bump `MERKLE_ROUTE_FORMAT_VERSION` (currently 2) to 3 to record that route digests are now persisted and indexed, not just computed transiently. Record the resolved Section 13 constants in the version's on-disk/schema notes explicitly: `FANOUT = 32`, `BITS_PER_SPLIT = 5` (derived), `SPLIT_THRESHOLD = 32`, `MERGE_THRESHOLD = 8`, `MAX_PREFIX_LEN = 60` (fixed, not a reloption), node-id width 64 bits, tuple hash width unchanged at 256 bits, internal nodes store combined hashes incrementally (Section 8). `FANOUT`/`SPLIT_THRESHOLD`/`MERGE_THRESHOLD` are reloptions and therefore vary per index instance within a format version — record the actual values used per index in `MerkleMetaPageData`'s relation-based successor (whatever metadata row/table tracks per-index configuration now that there's no metapage), not just in the format version's documentation.

---

## 15. Suggested implementation order (revises design doc Section 9 with the gaps above folded in)

1. **Formalize Invariant D1** (Section 2) and get Sudarshan's explicit sign-off on the "splits/merges only happen inside the ordered applier, gated on the global delta sequence" mechanism (Section 3) *before* writing split code — this is the one decision that's expensive to reverse later.
2. Add `merkle_route_digest`/`merkle_tuple_hash` columns (Section 6b) to a **non-production** test table, populated by a scratch extension of `merkleInsert`/`merkleBuild` (Section 7) — not a trigger, since that's not the final architecture and you don't want to build and then discard trigger-based plumbing. Resolve the open implementation detail in Section 7 (whether your existing executor hook fires before or after heap tuple formation) here. Prove index-only scans actually return the hash without heap fetches on your PG build (`EXPLAIN (ANALYZE, BUFFERS)`, check for "Heap Fetches: 0") — this was explicitly flagged as unverified in both meetings and the design doc's Section 10; verify it before any further design decisions depend on it.
3. Build the `merkle_node` relation (Section 6a) and the static-equivalent traversal (Section 8) with a **fixed** prefix length (no splitting yet), driven from inside `merkleBuild`/`merkleInsert` per Section 4 — this reproduces current static behavior on the new substrate and isolates "did I break the storage model" from "did I break the dynamic logic."
4. Re-point `merkledelta.c`/`merkleapply.c` to the new `MerkleDeltaKey` (route digest instead of `leaf_id`) and the new resolve-at-apply-time walk (Section 3). Test this in isolation against the fixed-prefix-length tree from step 3 — no splitting yet, just confirm the async apply model still gives identical results to the current static tree under your existing crash-failpoint harness (`ARIABC_MERKLE_FAILPOINT`).
5. Implement split (Section 9) inside the applier, using the decided `FANOUT = 32`/`SPLIT_THRESHOLD = 32`/`MAX_PREFIX_LEN = 60` constants (Section 13) and the bounded in-memory recursive resolution for the all-rows-one-bucket corner case (Section 9) — this is fully specified now, nothing left implicit.
6. Implement merge (Section 9) at `MERGE_THRESHOLD = 8`, using the same bottom-up-safe rule as 13th July.
7. Bulk build (Section 10) for `CREATE INDEX`/initial load, inside `merkleBuild`.
8. Extend recovery (Section 11): generalize `merkle_get_children_batch` to `merkle_get_descendants_batch` with depth (default 1, per Section 11's fanout-32 recalculation); add canonical-ordering serialization (Section 11) for cross-replica comparison; extend `scripts/distributed/test_merkle_consistency.sh` to assert byte-identical `merkle_node` state across replicas after the same delta log.
9. Benchmark: split I/O cost, index-only-scan confirmation, recovery round-trip count at depth 1 vs. experimental depth 2, and specifically the bloat/autovacuum behavior flagged in Section 12 — the design doc's Section 9 step 7 already asks for this, but add the bloat measurement, which it doesn't mention.
10. Rewrite `README_MERKLE.md` and `doc/merkle-static-contract.txt` once the design is implemented, not before — both currently describe the static, page-based, version-7 implementation and will actively mislead anyone reading them mid-rewrite (this is already flagged in your own recent repo state).

---

## Decisions (resolved)

All five items that were open after the first pass are now settled:

1. **`CREATE INDEX ... USING merkle` facade**: survives, as a genuine `IndexAmRoutine`-backed index AM. Column maintenance is internal to the AM's own callbacks (`merkleInsert`/`merkleBuild`), not a generic SQL/PL trigger. The one required standard-btree piece is the auxiliary covering index for index-only scans (Section 6b), owned/created by the Merkle AM's own build path, not user-managed. (Section 4)
2. **`FANOUT`/`BITS_PER_SPLIT`/`SPLIT_THRESHOLD`/`MERGE_THRESHOLD`**: `FANOUT = 32` (reloption), `BITS_PER_SPLIT = log2(FANOUT) = 5` (derived, not independently configurable), `SPLIT_THRESHOLD = 32` (reloption), `MERGE_THRESHOLD = 8` (reloption). (Section 13)
3. **All-rows-one-bucket split corner case**: bounded in-memory recursive resolution during the same `do_split` call, capped at `MAX_PREFIX_LEN = 60` (fixed per format version, not a reloption), falling back to an accepted over-full leaf only past the cap — the correct terminal case for exact-duplicate routing digests, not an engineering shortcut. (Section 9)
4. **Node relation scope**: per indexed table, not shared/global. (Section 6a)
5. **Internal-node hash maintenance**: incremental XOR propagation on every leaf update (Section 8), not lazy recomputation at recovery time — accepted along with its consequence flagged in Section 12 (more `UPDATE`-driven bloat surface on the hot path than the lazy alternative would have, in exchange for cheap recovery comparisons).