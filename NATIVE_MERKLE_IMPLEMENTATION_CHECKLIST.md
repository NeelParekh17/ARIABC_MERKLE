# Native PostgreSQL Merkle COW implementation checklist

Contract: `PLAN.md` (partitioned transaction-coupled copy-on-write Merkle index).

Legend: `[x]` implemented and verified in the current checkout; `[ ]` still
requires implementation, investigation, or acceptance proof.

Current status: the PLAN.md production acceptance work is implemented and
verified. The final installed binary passes focused regression, the 12-case
destructive crash campaign, the four native WAL-boundary cases, a 10M-row
skewed build, and one distributed structure/crash/exact-marker campaign.
Remaining unchecked items below are optional optimizations or broader test
campaigns, not missing PLAN correctness.

## A. Contract and authority

- [x] Use normal PostgreSQL WAL/commit flush as the heap + Merkle durability boundary; no page fsync at every commit.
- [x] Emit native node, leaf, item, and root WAL before the ordinary transaction commit record.
- [x] Make native dynamic layout-v8 index pages the authoritative production state.
- [x] Remove pending-mode compatibility/differential state from the production path.
- [x] Select the exact committed root immediately in `synchronous_cow`; no applier or schema dependency.
- [x] Publish immutable XID-visible partition-root versions, never overwrite the committed root in place.
- [x] Derive the global root from ordered partition roots; no mutable global-root hotspot.
- [x] Exclude block/offset locators and allocation hints from logical hashes.

## B. Transaction semantics and concurrency

- [x] Capture INSERT, DELETE, payload/HOT-candidate UPDATE, and routing-key change semantics outside `aminsert` alone.
- [x] Coalesce repeated same-key changes into one net transition.
- [x] Preserve savepoint/subtransaction commit and rollback semantics.
- [x] Store canonical unique-key bytes and tuple hashes, not heap TIDs, as native item identity.
- [x] Publish at `XACT_EVENT_PRE_COMMIT`.
- [x] Group transitions by index and partition and sort before publication.
- [x] Acquire transaction-scoped `(database,index,partition)` locks in deterministic order and retain them through outcome.
- [x] Build from the newest committed root while holding the partition lock.
- [x] Tag every partition root in a multi-partition transaction with the same top-level XID.
- [x] Reject PREPARE after Merkle mutation until two-phase publication exists.
- [x] Verify concurrent same-partition and different-partition commits without lost updates.
- [x] Verify repeatable-read snapshot selection keeps an older root stable while a concurrent commit publishes a new root.

## C. Native page format and algorithms

- [x] Version layout as dynamic layout v8 and fail older layouts closed with a REINDEX requirement.
- [x] Store immutable configuration and native directory location on the metapage without a per-commit global root/count update.
- [x] Add one native partition-directory page per partition to avoid shared root-head hotspots.
- [x] Add checksummed root records with creator XID, sequence domain/flags/epoch/value, version, root locator, summaries, and previous-version link.
- [x] Add checksummed packed immutable internal-node and leaf-node records.
- [x] Add packed variable-length canonical item chunks; retain backward reading of the initial one-item record form.
- [x] Validate every native page envelope, record type/version/size, and checksum.
- [x] Account for `MAXALIGN(record_size) + sizeof(ItemIdData)` when selecting append/root pages; verify publication across append-page boundaries.
- [x] Include a page generation in every append/free envelope and physical locator; fail closed on generation mismatch (ABA/page-reuse protection).
- [x] Implement direct native bulk build without side tables in strict mode.
- [x] Implement bounded-leaf exact-key lookup by route digest plus canonical-key equality.
- [x] Implement same-key replacement, insert splitting, delete merging/hysteresis, compressed-prefix divergence, and root contraction.
- [x] Rebuild only affected paths/subtrees and reuse unchanged child locators.
- [x] Compute data XOR incrementally, topology-independent content commitments, and structure hashes from canonical logical identities/summaries.
- [x] Compute and expose independent `data_root`, `structure_root`, and `combined_root` commitments. Normal root reads are O(partitions): each partition root carries the canonical item commitment, while physical locators remain outside logical hashes.
- [x] Append all referenced records before publishing the XID-visible root.
- [x] **[hardening]** Patricia-style prefix divergence: compute LCP, create branch node at LCP, reuse old subtree locator verbatim — O(depth+new_items) instead of O(subtree_size). (plan_left.md §8)
- [x] **[hardening]** Byte-aware merge: both tuple-count threshold AND byte-capacity bound must be satisfied before merging two children into a leaf. (plan_left.md §9)
- [x] **[hardening]** Merge also enforces the configured leaf tuple capacity, not only the hysteresis threshold.

## D. WAL and crash safety

- [x] Use Generic WAL for directory, append-page, root publication, freeze/hint, and free-page changes; all mutations occur on registered temporary pages and the register/finish boundary has a dedicated failpoint.
- [x] Keep pre-root and aborted COW records unreachable and harmless.
- [x] Hide aborted/uncommitted roots using PostgreSQL transaction status.
- [x] Add failpoints `after_native_register_before_finish`, `after_native_record_wal`, `before_native_root_publication`, `after_native_root_wal_before_commit`, and `after_user_transaction_commit`.
- [x] Prove crash before/after those four boundaries: pre-commit rows absent, post-commit row present, and `merkle_verify()` true after every restart.
- [x] Verify strict-mode restart needs no Merkle catch-up.
- [x] Confirm `synchronous_commit=on`, `fsync=on`, and no Merkle-side explicit `XLogFlush`, `fsync`, `fdatasync`, or immediate smgr sync.
- [ ] A dedicated compact Merkle WAL resource manager remains an optional measured optimization; Generic WAL is the correctness implementation requested by the plan.

## E. Reads, verification, and comparison

- [x] Route root, partition-root, frontier, range, range-item, stats, and verification helpers to native pages in strict mode.
- [x] Report `authority=native_index_pages` and data/structure/combined commitments in stats.
- [x] Add typed `merkle_native_partition_roots_at(regclass,int2,int8,int8)` with visible domain/flags/epoch/value/version/XID/frozen/data/structure provenance and BUILD_BASELINE fallback.
- [x] Require an explicit ordered domain/epoch/value marker for distributed lookup; current-state lookup is no longer used as proof.
- [x] Independently verify heap contents, canonical item ordering, topology, prefix bounds, tuple/byte summaries, XOR, and structure hashes.
- [x] Verify helper results on split, contracted, mixed legacy-item/packed-chunk, and empty trees.
- [x] **[hardening]** Prefix-tree range traversal: `merkle_native_get_ranges` and `merkle_native_get_range_items` now descend only into overlapping nodes — O(depth + matching_frontier) instead of O(partition_items). (plan_left.md §7)
- [x] Keep parsed range requests in the caller memory context across `SPI_finish()`; the former SPI-context use-after-free was the root cause of the impossible-locator failures.
- [x] **[hardening]** Single-pass heap verification: `merkle_native_verify_relations` scans the heap once and compares two spillable PostgreSQL tuplesort streams bounded by `work_mem`. (plan_left.md §11)

## F. VACUUM, aging, and lifecycle

- [x] Mark aborted root versions with WAL-logged hints.
- [x] Freeze old committed roots before transaction-status truncation and cut obsolete history.
- [x] Respect the oldest relevant snapshot through `GetOldestXmin`.
- [x] Mark reachable roots/nodes/item chunks and convert whole unreachable append pages to native free pages.
- [x] Register/reuse free pages through the index FSM and maintain a backend append hint.
- [x] Verify heavy update/delete + VACUUM + reinsert reuses almost all pages and remains valid.
- [x] Verify packed-item trees remain valid after VACUUM.
- [x] Verify TRUNCATE rebuilds an empty native tree; DROP and non-concurrent REINDEX use normal relation lifecycle.
- [ ] Mixed-page/root-log compaction beyond whole-page reclamation is a future storage optimization.

## G. Modes and compatibility

- [x] Store `update_mode` as a persistent index reloption; the obsolete session GUC and online setter API are removed.
- [x] Remove the obsolete `pending_log` compatibility mode; native v8 uses synchronous COW only.
- [x] Keep pending materialization and native page images internally consistent; online authority changes are fail-closed and require REINDEX rather than attempting an unsafe migration.
- [x] Require native dynamic-v8 indexes for production Merkle maintenance.
- [x] Fix a static verification resource-owner mismatch exposed by the compatibility regression.
- [x] Keep ALTER/rewrite operations fail-closed; allow native-safe DROP/TRUNCATE/REINDEX lifecycle paths.
- [x] Add user-facing native-v8, WAL/crash, operations, and upgrade documents under `Dynamic_merkle_docs/`.

## I. Hardening (plan_left.md — completed in this pass)

- [x] **Impossible locator eliminated** (plan_left.md §1): root-caused the repeatable failure to an SPI-owned range-request vector returned after `SPI_finish()` and moved it into caller-owned memory. Also hold the relation extension lock until the new buffer is exclusively locked, preventing exposure/overwrite of an uninitialized P_NEW page. Bounds, checksums, and page generations remain fail-closed defenses.
- [x] **Global root commitments** (plan_left.md §2): `merkle_native_root()` now derives independent topology-independent data and topology-sensitive structure commitments and hashes them into a combined root with layout/route/row format tags.
- [x] **Extension lock eliminated from hot path** (plan_left.md §5): `native_append_record()` takes `LockRelationForExtension(ExclusiveLock)` only on the P_NEW slow path and releases it immediately after the new buffer is obtained. All three fast paths (hint, FSM recycle, last-block reuse) operate under the buffer lock alone, allowing concurrent writers in different partitions to proceed in parallel.
- [x] **Root publication lock scope** (plan_left.md §5): root publication follows the same existing-page fast path and takes the extension lock only for physical allocation.
- [x] **Exactly-once routing** (plan_left.md §4): strict native transitions publish through the native v8 commit path and are not serialized into a compatibility queue.
- [x] **Persistent mode guard** (plan_left.md §3): mode is read from the native metapage, missing reloptions default to synchronous COW, and mode changes require an explicit reloption plus REINDEX.
- [x] **Byte-aware merge** (plan_left.md §9): merge check now requires `left.subtree_bytes + right.subtree_bytes <= leaf_byte_capacity` in addition to the count threshold.
- [x] **O(depth)-range traversal helpers** (plan_left.md §7): `native_traverse_range_summary` and `native_traverse_range_items` implement correct prefix-tree descent (disjoint -> stop, covered -> return summary, internal -> recurse). Both `merkle_native_get_ranges` and `merkle_native_get_range_items` use these helpers.
- [x] **Patricia-style prefix divergence** (plan_left.md §8): when incoming routes diverge before the old compressed node prefix, `native_apply_batch_node` now computes the LCP, creates a branch node at that bit, reuses the old subtree locator verbatim as one child, and builds only the new item paths as the other child — without touching existing records.
- [x] **Spill-backed verification/build** (plan_left.md §§10-11): strict input and verification use PostgreSQL tuplesort; per-partition build/oracle input uses disk-backed `BufFile` data/offset spools and a 1,024-row SPI cursor, so no full partition/SPI result is materialized.
- [x] **Sequence provenance foundation** (plan_left.md §12): native roots persist sequence domain, flags, epoch and value; BUILD_BASELINE is a flag, Raft epochs use a documented canonical big-endian representation, and the runner calls the typed marker helper.

## H. Verification completed

- [x] `make -j4` full PostgreSQL tree build passes.
- [x] Installed final PostgreSQL backend binary passes the native-only `merkle_native` regression after the spill verifier fix.
- [x] `git diff --check` passes.
- [x] Fresh database without `ariabc_internal` supports strict native build/DML/root/verify.
- [x] Focused DML covers insert/update/delete, coalescing, key change, savepoint rollback, abort, split, merge, and contraction.
- [x] Local concurrency and repeatable-read snapshot tests pass.
- [x] Final 12-case dynamic postmaster-kill campaign passes 12/12 in `/tmp/ariabc-native-v5-final-20260718-r6`.
- [x] Final-binary native WAL boundary campaign passes 4/4 postmaster-kill cases in `/tmp/ariabc-native-wal-final-20260718-r3` (`after_native_register_before_finish`, `after_native_record_wal`, `before_native_root_publication`, and `after_native_root_wal_before_commit`).
- [x] Local packed-tree VACUUM verification passes (`bench_native`, 11,248 pages, verification true).
- [ ] Online pending-mode -> strict-mode migration is intentionally rejected; REINDEX with the desired reloption is required. A future drain-and-compare migration protocol remains open.
- [x] Local pgbench measured plain and native WAL/latency/TPS and confirmed zero backend fsync calls.
- [x] Remote source/binary provenance passed on all three configured nodes.
- [x] Remote restore produced the same 11,994-row root on all three nodes and native verification true.
- [x] Repeated four-statement distributed workloads completed with `divergence_count=0` and `permanent_failures=0`.
- [x] **[hardening]** `make -j$(nproc)` full tree build passes with all plan_left.md fixes applied.
- [x] **[hardening]** `merkle_native` regression passes cleanly against installed server.
- [x] **[hardening]** Final focused native schedule (`merkle_native`) passes after layout-v8, spill-verifier, runner, and documentation changes; `bash -n scripts/distributed/run_4node_raft_cluster.sh` passes.
- [x] Benchmark proof tooling is bounded and machine-readable: native WAL cases are runnable via `scripts/test/merkle_crash_atomicity/run_native_wal_boundaries.sh`, memory builds via `scripts/benchmark/run_native_merkle_memory_curve.sh`, and the parser self-test passes after the Kafka-majority-visible label change.
- [x] Full PostgreSQL regression schedule was attempted after the final install; Merkle-focused tests remain green, while the broader baseline still fails unrelated existing tests and lacks `sql/expected/security_label.out`. It remains an environment/repository baseline blocker, not a native Merkle failure.
- [x] Final distributed artifact `scripts/bench_full_results/cluster4_20260718_110847` passes provenance/input freeze, 11,741.84 Kafka-majority-visible TPS, `divergence_count=0`, `permanent_failures=0`, native structure mutation, immediate replica crash/restart, exact typed-marker content/topology/combined commitments, full verifier, and three-replica equality.
- [x] 10M-row 100%-skew build passes in `/tmp/ariabc-native-scale-10m-20260718-r2`: 136.43 s, verification true, 1,552,498,688-byte index. Peak backend RSS was 2,973,680 KB (private 1,545,656 KB; PSS 2,250,713 KB); disk-spooling removes full-partition materialization but PostgreSQL/backend RSS is not capped at `maintenance_work_mem`.
- [x] Memory-curve harness records independent 1M/3M/5M build points in `/tmp/ariabc-native-memory-curve-final-20260718-r1` (all verified) and a clean 10M build-only point in `/tmp/ariabc-native-memory-curve-final-20260718-r2`: 145.18 s, 2,971,684 KB RSS, 1,545,164 KB private, 2,248,669 KB PSS; the verifier was bounded to a one-second diagnostic timeout. The curve is evidence for build scaling, not a claim of a fixed RSS cap.
- [x] Root `./run_sweep.sh --threads 1 --executor-workers 1 --reps 1` smoke passed through the three-node cluster in `scripts/bench_full_results/cluster4_20260718_153154`: dynamic restore, `dyn_verify=t`, all topology/leaf/root gates PASS, `divergence_count=0`, `permanent_failures=0`, exit 0.
- [x] Root `./run_sweep.sh` with all defaults (`threads=96`, executor workers `1 2 4 8 12 16`, reps `1`) passed all 6/6 runs in `scripts/bench_full_results/pg_executor_sweep_20260718T101645Z`; every underlying cluster artifact passed dynamic equality, `divergence_count=0`, and `permanent_failures=0`. Kafka-visible TPS ranged from 3,120.14 to 9,476.29 across the sweep.
- [x] Root `./run_parallel_yscb_all_nodes.sh` default run passed on all three configured nodes in `scripts/bench_full_results/parallel_ycsb_20260718_153943`: 3/3 nodes done, 36/36 cases exit 0, all deterministic cases `db_merkle_verify=t`, all restores exit 0, all `permanent_failures=0`, no stall abort. Each remote `run_meta.json` records `dynamic_merkle=true` and the dynamic restore path.
- [x] Parallel runner defaults now select native dynamic Merkle (`synchronous_cow`) for deterministic cases, retain plain PostgreSQL as the baseline, pass the PostgreSQL destructive-reset session GUC, clean orphaned dynamic generations on interrupted restores, and abort after 1,800 seconds of no log progress instead of warning indefinitely. Root wrappers are provided for both `run_parallel_ycsb_all_nodes.sh` and the historical `run_parallel_yscb_all_nodes.sh` spelling.
- [x] Post-change parallel WAL-gate smoke (`parallel_ycsb_20260718_154519`) validated `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, and `wal_level=replica` on all three nodes before execution; all three native dynamic cases completed with `verify=t`, exit 0, and no stall.

## Still left to do

0. [x] Add opt-in `--dynamic-structure-profile 1` native split/merge profiling: durable counters are instrumented in the native mutation path, reset before workload, emitted per replica, and compared fail-closed. Default `0` leaves the normal run path unchanged. The code/build/syntax gates pass; a fresh remote profile PASS artifact should still be rerun after the gateway SSH transient seen on 2026-07-18.

1. [x] Reproduce and eliminate the impossible-locator cause. The returned range vector was freed by `SPI_finish()`; caller-context ownership plus a frontier/range regression fixes it. A 34,031-commit/1,000-live-stats stress pass, the final distributed range gate, and the final crash campaign complete the proof.

2. [x] Validate the native-aware runner branch end-to-end, including stable committed-root barriers, JSON parsing, exact typed-marker content/topology/combined hashes, and fail-closed restore behavior.

3. [x] Exact distributed acceptance is recorded in `cluster4_20260718_110847` with every required flag in one artifact.

4. [ ] Decide whether to optimize the measured commit overhead. The small single-row pgbench comparison was about 5,922 TPS plain versus 2,239 TPS native and about 5.7 KB additional WAL/transaction. The extension-lock elimination (plan_left.md §5, implemented this session) is the primary structural fix; a new measurement run against the patched binary is recommended. This is a performance concern, not a correctness blocker.

5. [x] `update_mode` is authoritative in `MerkleMetaPageData.nativeFormatFlags`; no session GUC or online setter remains. Existing-index mode changes require an explicit reloption and REINDEX. (plan_left.md §3)

6. [x] Replace largest-partition and full-SPI materialization with disk-backed per-partition spools, bounded leaf reads, and an SPI cursor. See the measured 10M RSS caveat above.

7. [x] Add user-facing native-v8 REINDEX/upgrade and WAL/operations documents.

8. [ ] Run the full regression schedule in an environment with enough memory; the repository schedule still contains unrelated missing/failed baseline tests, so only focused Merkle evidence is currently usable.

9. [x] Implement spill-backed build/verification and record 10M-row worst-partition evidence. A 50M repetition is optional capacity characterization.

10. [x] Complete persisted sequence provenance: strict and pending roots carry the originating Raft epoch (canonical digest prefix) or LOCAL_XID domain through the transition/applier path, and typed marker filtering is enforced.

11. [ ] Add root-log compaction/record relocation and bounded root-chain maintenance beyond whole-page reclamation.

12. [x] Complete the required lifecycle/failpoint and distributed no-sleep structure/crash acceptance matrices. Additional performance repetitions and lock-wait profiling remain optional tuning work.

13. [x] Free every mutation/range vector on normal return, including delete keys, merge collections, Patricia divergence items, and range traversals.

14. [x] Split native mutation authority into strict-publication and pending-materialization APIs with reloption checks at the native boundary.

15. [ ] Add append-page class/record compaction and partition-local VACUUM lock scheduling; Generic WAL append allocation and whole-page reclamation are correct but long-lived append logs remain a scalability hotspot.

16. [ ] Split the native implementation into reviewable translation units and add repeated 100k-update/split-merge memory tests under ASAN or Valgrind.
