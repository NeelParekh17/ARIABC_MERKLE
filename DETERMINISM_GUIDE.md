# Determinism in ProtectDB and the updated AriaBC implementation

Reviewed 9 October 2026 against source commit c165637f21cc25411af302a56f83717fd340c303 and the current local ProtectDB_arxiv-3.pdf. Open [the animation](http://127.0.0.1:8765/DETERMINISM_EXPLORER.html). The page contains 17 chapters, moving transaction tokens, dependency arrows, discard/retry and publish/commit cues, a per-event change summary, private write queues, visible database state, completion holes, dependency diagrams, source excerpts, and slow playback down to 0.125×.

The [source manifest](docs/determinism/source_manifest.json) records exact file hashes, anchored excerpts, and the reviewed commit. Timings and schedules are illustrative. The diagrams explain reviewed source behavior; they are not traces captured from PostgreSQL. This refresh ran browser checks on the lab machine, not a new database correctness or TPS campaign. Latest Claude/Codex handoffs were used to locate changes and then checked against source.

## The intended property

Given the same initial logical database state and agreed sequence T0,T1,…, each healthy replica should produce the logical state and specified outcomes of that sequence executed serially. Ordinary serializability permits some serial order; this system must implement the agreed order. For example, starting from 10, adding five and then doubling gives 30; reversing those two transactions gives 25.

The sequence ID is not a PostgreSQL XID. The diagrams use zero-based source IDs and initial prefixes −1. The draft uses its own numbering convention.

The merged changes strengthen the implemented mechanisms: conservative relation read coverage, old chosen-key coverage, own-write visibility, ordered physical execution where deferral is insufficient, sequence-derived random streams, intact long SQL, finalized error propagation, and accurate coalesced Merkle counts. This does not turn every possible PostgreSQL statement, external input, failure, or configuration into a universally proved deterministic program.

## Three separate boundaries

| Boundary | Meaning |
|---|---|
| P: published_max_tx_id | Contiguous prefix with ordered footprint metadata released to successors |
| C: last_committed_tx_id | Contiguous finalized-item prefix; successful data effects follow PostgreSQL commit, supported terminal errors also finalize slots |
| Result/delivery/client acceptance | Selection of result or receipt payload, Kafka send/delivery, and downstream acceptance; these are separate from P and C |

P can be greater than C. Data effects are not visible merely because their writer tags were published. If slots 1 and 2 finish before slot 0, C stays −1; after 0 finishes, the prefix scan can advance through all three. “Committed prefix” is shorthand in variable names: C also includes ordered terminal error outcomes.

The page derives P, C, queues and visible data from one event reducer. Its guards reject publication out of sequence, physical SQL before predecessor readiness, changes to a published queue, and business SQL after publication. These checks prevent contradictory educational scenes; they do not execute or certify the database implementation.

## What the paper actually specifies

Read §4 on printed pages 6–7 and Algorithm 1 on page 7 of [ProtectDB_arxiv-3.pdf](ProtectDB_arxiv-3.pdf).

Run_Tx records the committed baseline and executes SQL on a snapshot to discover read/write sets. The prose describes snapshot and unfinished-predecessor boundary acquisition as atomic. While the recorded committed boundary is not k−1, a worker waits for commit signals and checks newly committed predecessors. A conflict causes SQL execution on a fresh snapshot; a clean interval extends the checked boundary. Commit commits the changes, stores dependency metadata, advances the prefix and broadcasts.

Two details matter when comparing the paper with this source:

- Independent SQL still waits for committed predecessors in Algorithm 1. Avoiding a retry is not permission for early commit.
- Incremental checking already exists in the paper. The implementation adds a separate publication frontier and optimized digest representation, among other refinements.

Algorithm 1 abstracts commit and dependency publication together. Its Commit pseudocode displays footprint insertion and watermark movement; the prose supplies the data-commit semantics. The paper does not specify today's separate P frontier or the frozen-queue post-publication settlement branch.

The draft's blanket discussion of index-leaf phantom detection is not a substitute for inspecting current hooks. The merged source explicitly adds relation-level read coverage to cases that cannot obtain a chosen-key prefix. The page now shows that coverage instead of retaining the previous empty-range gap.

## Fast optimistic execution in the reviewed source

This explanation selects DT/OEP with conflict tracking, publication gating, and the default early-validation, tag-dedup and post-publication-settle settings. It is not a claim about every legacy, safe-ledger, direct-SQL or completion-only branch. The compiled globals initialize OEP and conflict tracking to false; a real run must enable the intended mode and record effective settings.

1. **Baseline and snapshot.** bcdb_worker_process_tx_dt reads bcdb_dt_snapshot_baseline before starting the SQL transaction and obtaining GetTransactionSnapshot. Baseline-before-snapshot accounting is conservative; it is not the draft's single atomic operation.
2. **Private simulation.** Business SQL runs and discovers reads, checked writes and publish-only tags. Deferred writes carry the computed values and decisions. Dependencies are discovered through execution, not entirely declared before scheduling.
3. **Validation while waiting.** bcdb_wait_for_serial_slot checks publication readiness. A scheduled waiter can validate newly published predecessors before owning its turn. If a missing predecessor writer conflicts, the attempt is discarded, the remembered writer is awaited, and a fresh baseline/snapshot and SQL attempt follow. Publication does not asynchronously abort all readers by itself.
4. **Final validation.** At the turn, validate through s−1. A conflict keeps the item unpublished while it waits/resimulates. Some block shapes also have a parse barrier.
5. **Publish and release.** publish_ws_tableDT publishes writer metadata and digest summaries. set_published_max_txid releases the successor before deferred apply on the fast path. Reads remain primarily local; this is not a literal shared table of all predecessor read/write sets.
6. **Apply and commit.** The accepted deferred queue is physically applied. finish_xact_command precedes successful slot finalization. Contiguous C advancement then crosses any already-ready successors.

Independent effects can physically finish out of order. For example, T0 writes A, T1 writes B and T2 writes D: P may reach 2 while C remains −1, with B and D already visible and A still applying. Correctness requires complete represented dependencies and faithful application of the accepted execution.

Source: worker.c anchors baseline, gate, early, turn, publish and commit; shm_block.c prefix. Exact excerpts are embedded in the page.

## New relation coverage: empty, broad and secondary scans

bcdb_reserve_read_key_tag_scan attempts to bind a leading equality prefix of the chosen logical key. If no prefix is bound, it reserves a relation read tag. This registration occurs even when there are no returned tuples; B-tree registration is before the zero-boundary early return. Bitmap paths can resolve/open the heap relation when it was not already supplied. Heap and non-B-tree scan hooks also reserve relation reads.

Every tracked write reserves matching relation membership through bcdb_reserve_write_relation_tag. That writer membership is publish-only, distinct from checked-write membership. Consequently, two narrow writers do not automatically conflict just because they publish the same relation summary. A relation reader does check that summary against missing predecessor writers.

Example: T1 executes an empty secondary-range COUNT and records zero; T0 subsequently publishes an INSERT into that relation. T1's relation read detects T0's relation writer tag, discards its zero-based decision, waits for T0 to finish and reruns. The retry can see one row.

This is deliberately conservative: an unrelated row in the same relation may also cause a retry. Narrow chosen-key equality reads can retain finer prefix tags. Relation tags participate in digest checks and full writer-map fallback; they are not merely telemetry. The mechanism depends on tracked user-relation scans and writes, not an assumption that ordinary SSI predicate locks cover everything with pred_lock disabled.

Source: shm_transaction.c relationread/relationwrite, nbtsearch.c btfirst, heapam.c heapscan.

## Old chosen-key tags on UPDATE

When UPDATE targets a chosen-key column, bcdb_reserve_old_write_key_tags fetches the old version and reserves its logical write tags. The ModifyTable hook also reserves new-slot tags and passes updatedCols/extraUpdatedCols to the old-key helper.

For a primary-key move 7→8, the writer therefore announces both the disappearing old key 7 and the new key 8. A reader of key 7 from an earlier snapshot detects that predecessor even though the new row no longer carries 7.

The chosen key is selected from the primary key, replica identity, or a suitable plain unique index. This example specifically concerns that chosen key. It does not imply that every arbitrary expression or every secondary-index change independently passes the helper's chosen-column guard. Extra unique-index coverage and conservative relation summaries are separate parts of the tagging design.

Source: shm_transaction.c oldkey, nodeModifyTable.c modifyold and relationwrite.

## Own-write overlay and composition

The fast path now has backend-local maps keyed by relation/TID and per-relation pending-write state. After fetching a physical tuple, bcdb_overlay_slot_slow can substitute an earlier deferred UPDATE or hide a deferred DELETE.

The command counter is essential: an entry whose cid is at least the current command ID is not visible through the overlay. Earlier-command writes become visible after command-counter advancement. It is not a rule that every SQL expression instantly sees every same-command queued write.

For a non-indexed value starting at 10:

- A command queues value←11 while physical PostgreSQL still stores 10.
- A later command fetches 10, overlays 11, and can compute a second update to 12.
- Repeated updates to the same TID compose into one latest queue entry.
- A later deferred DELETE replaces the entry; after the next command boundary, reads hide the row.
- Validation, publication, physical apply and PostgreSQL commit make the composed result visible outside the transaction.

Heap, tuple-fetch, bitmap and related fetch hooks consult the overlay. Index-only scans force heap fetch when pending writes exist, even when the visibility map would otherwise avoid it. SELECT FOR UPDATE simulation records tuple dependencies and avoids a speculative physical row lock; the normal physical route retains PostgreSQL's ordinary behavior.

An overlay cannot invent physical heap/index entries for pending INSERTs, nor safely discover every row added by an indexed UPDATE or partial-index predicate change. Those cases request ordered physical fallback.

Source: shm_transaction.c overlay/compose/scanpending/indexpending and nodeIndexonlyscan.c indexonly.

## Ordered physical fallback: the precise order

bcdb_request_opf sets needs_opf and raises the internal SQLSTATE BC010 only during simulation. The worker's catch restarts this route only before publication. It aborts/discards the original attempt and deferred queue; it does not switch to physical execution halfway through a speculative transaction.

| Guard | Why ordinary physical semantics are needed |
|---|---|
| Trigger-bearing DML target, including foreign-key trigger behavior | Invoke PostgreSQL trigger/check behavior at the ordered position |
| INSERT ON CONFLICT | Resolve DO NOTHING/DO UPDATE using the normal PostgreSQL conflict path |
| nextval/setval | Avoid mutating the sequence during discarded speculative attempts |
| Scan after pending own INSERT | The inserted row is absent from the physical scan structures |
| Scan affected by own indexed UPDATE | A new indexed value or partial-index membership can have no old index entry to fetch |

The retry order is:

1. Preserve needs_opf across the internal error restart, while clearing abandoned transaction/context state.
2. Observe the parse barrier where applicable; obtain the publication turn under source 0 and wait for C≥s−1. Under committed-prefix gating, the predecessor-completion wait supplies ordering.
3. Record the fresh baseline and take a new transaction snapshot.
4. Seed random and execute business SQL physically with simulation disabled. bcdb_should_defer_dml returns false on this fallback route.
5. Physical DML records its write tags; SQL completes while the item retains its publication turn.
6. Publish the footprint and advance P after physical SQL returns. Skip deferred apply.
7. Finish PostgreSQL commit, then finalize the successful slot.

P is released after physical SQL but before final PostgreSQL commit. Describing fallback as releasing P before physical SQL would be wrong. Describing it as necessarily holding P through final commit would also be wrong.

A later optimistic transaction can already be simulating; after the fallback writer publishes, dependency checks protect missing relevant effects. No later ordered publisher can overtake the unpublished fallback item. External writes bypassing this protocol require a separate contract.

A PL/pgSQL exception handler cannot silently absorb the fallback requirement. If it catches BC010 and SQL returns while bcdb_dt_simulating and needs_opf are both true, the worker raises BC010 again. The whole unpublished attempt is still rejected. This is why the page includes a dedicated caught-error chapter.

Source: shm_transaction.c opf/scanpending/indexpending; worker.c opfwait/opfretry/caught; nodeModifyTable.c physical/trigger/upsert; sequence.c sequence.

## Freeze the accepted decision after publication

With source 0 and default BCDB_DT_POST_PUBLISH_SETTLE enabled, eligible failed application attempts settle by reapplying the same deferred queue. Internal subtransactions roll back failed apply effects. Business SQL is not rerun after publication has released successors.

Let T1 read A=0 and compute B←0; T2 later writes A←1. They have different writer keys. After T1 publishes B, T2 can progress while T1 settles. Rerunning T1's SQL against successor-visible A=1 would incorrectly compute B←1. Keeping the original B←0 preserves this example's agreed-order result: A=1,B=0.

Settlement can wait for the predecessor committed prefix before retrying the frozen queue. Supported persistent errors become specified terminal outcomes; an unmodeled post-publication failure can raise the BC001 invariant error. Error/no-op serial equivalence and constraint coverage still require their own argument for the admitted SQL class. This page does not claim that every failed application, top-level commit failure or configuration override has a general recovery proof.

Source: worker.c applyretry/settle/terminal. Historical chat descriptions of full SQL restarts after publication do not describe this selected default branch.

## Digest checks and retained writer history

The publication digest ring has 8,192 slots with capacity for 384 hashes per slot. A slot records its owning deterministic ID. Validation checks identity around payload reads and falls back to full writer maps on unavailable/reused/overflowed history or an excessive interval.

The initial full-map check anchors validated_through at the captured publication boundary. Subsequent checks inspect only the newly published suffix, capped at s−1 at the turn. The animation uses a consistent sequence: T3 publishes, then T4 publishes, then T5 can finish validation through 4 and publish.

Local dedup preserves distinct read, checked-write and publish-only membership. Relation tags now participate in this path too. Digest hashes summarize dependencies; they are not cryptographic database roots.

Fallback maps must still retain every required predecessor writer. The alternating-map retirement policy, actual worker/pool bounds, and stalled snapshots matter. Finite ring capacity is not an excuse to silently drop a dependency.

Source: shm_transaction.c ring/dedup/digest.

## Random streams and complete SQL

Before every DT business-SQL attempt, including the physical fallback attempt, the worker calls bcdb_seed_random(tx_id). PostgreSQL's random state is initialized from the ID XOR 0xBCDB13579BDF, split into three 16-bit seed words.

Same sequence ID plus the same call order yields the same stream across replicas and retries. The animation uses symbolic positions r₁[0], r₁[1], not invented numeric samples. A changed branch can change how many random calls are made; determining the correct branch is still the job of dependency/snapshot correctness.

This change does not normalize clock_timestamp, external inputs, all UUID sources or arbitrary side effects. Sequence mutation uses the physical route, which prevents speculative consumption but still follows ordinary PostgreSQL sequence behavior on the physical attempt.

Long SQL now uses dynamic length-aware signature copying and DSA storage beyond the 1,024-byte inline transaction buffer. bcdb_tx_sql chooses the correct storage and deletion frees the DSA allocation. This protects the executed program from the old fixed-buffer boundary; it does not imply unbounded result-buffer capacity.

Source: worker.c seed, float.c random, shm_transaction.c longsql; postgres.c length-aware signature path.

## Finalized SQL errors retain their outcome

Where the error is raised matters. CHECK and NOT NULL constraints run in ExecInsert/ExecUpdate (ExecConstraints) before a write is deferred, so they fail inside the speculative attempt. Outside the safe ledger, that error reaches the worker's outer PG_CATCH: bcdb_publish_error_result writes ERROR <sqlstate> into the item's result slot, mark_published_ready_txid marks the slot publication-ready and advance_last_committed_txid tries to advance C. The slot does not wait for its turn; P and C scan forward over it once its predecessors finish, so log order is kept.

**Stale-snapshot errors (bug found and fixed 2026-10-09, uncommitted in the working tree).** Before the fix, that catch path finalized the error without validating the reads that produced it. On the ledger path, terminal outcomes also skipped the turn conflict check. So an error that depended on a stale snapshot became final: at W=8, a slow v←v+1 alternating with v←v−1 under CHECK(v≥0) returned 23514 for 32/32 UPDATEs that succeed serially.

The fix is in worker.c, both catch paths, plus the dt_opf_stale_error trace counter. An error raised while simulating with baseline < s−1 sets needs_opf and restarts through the ordered physical route (BC010): the transaction waits for its turn and for every predecessor to commit, then re-runs. An error on that run, or one raised with baseline = s−1, is the serial outcome and is finalized as before. The TPC-C TP001 business abort keeps its own path.

Verified on .247 against the serial oracle:
- 5 stale-error families (CHECK, division by zero, NOT NULL, PL/pgSQL RAISE, and a mixed case with genuine serial errors) fail 5/5 on the old build and pass 15/15 at W=8 on the fixed build. The mixed case returns exactly the 21 genuine errors.
- All 62 existing detfix families pass at W=8. `volatile_random` needs the audit_A.py oracle, which seeds random() correctly; the other script mismatches on the old build too.
- The safe-ledger branch has not been exercised by these native tests.

Chapter 14 animates the fixed behaviour.

The backend now retains the real terminal SQLSTATE, finalizes the item at its ordered position, and returns a marked error. Inline terminal errors are raised after PG_TRY so the catch cannot revisit a deleted transaction. The catch-path finalization also attaches bcdb_finalized=1 when it already finalized an error item.

The executor parses that detail and preserves ERROR sqlstate=… with a finalized=1 suffix. is_finalized_error_result recognizes marked outcomes. The completion paths exclude those outcomes from infrastructure/task failure classification, allowing later items to progress.

A completed 23514 or 23505 is still a failed SQL operation. It is not a successful UPDATE and no failed transaction effects should be shown in visible database state. Unmarked infrastructure errors still belong to failure handling. Direct-completion success_count bookkeeping must not be interpreted as a count of successful business statements.

Source: worker.c terminalerror/finalized, pg_error_result.hxx errorhelper, pg_executor.cxx errorcomplete.

## The newest Merkle fix: hash XOR and count sum

A staged delta is keyed by index identity, event type and relevant old/new key hashes. Multiple changes sharing that key can coalesce. The hash contribution combines with XOR; count contributions add. INSERT contributes +1, DELETE −1 and UPDATE's direct delta has no net count change.

The newest commit carries count_delta in each entry through staging, subtransaction merge and apply. An entry is removed only when both its XOR hash and its net count are zero. Ordinary and batched application use the accumulated count instead of deriving one row per retained entry.

Example on an initially empty leaf: insert row h, delete it, reinsert identical row h. The two INSERT contributions coalesce to hash 0 but count +2. The DELETE contribution has hash h and count −1. The combined effect must be hash h and count +1. Removing the zero-hash INSERT entry would lose its +2 count and can lead to incorrect zero-count/hash treatment. Keeping both components gives the final one-row leaf.

Only changes with the same delta key coalesce; arbitrary distinct rows are not automatically one entry. Failed subtransactions discard their staged frames with both components. The animation shows symbolic algebra and state, not freshly measured root values.

Source: merkledelta.c merklestage/merklemerge, merkleapply.c merkleapply and batched route count assignment.

## Connection lanes and outcomes

push_task_ordered maps an agreed sequence to s modulo W. An idle event connection takes the front of its FIFO lane. With consecutive ordered admission, FIFO execution, fairness, one active item per lane and finite predecessor work, ordering/lane waits point to smaller IDs; a cycle of only those waits cannot strictly decrease IDs all the way around.

Modulo affinity alone does not prove this. The argument does not rule out ordinary database-lock deadlocks, missing IDs, crashed workers, starvation or retry livelock.

The draft discusses comparing actual results/hashes. Ordinary non-safe block mode defaults to completion receipts unless BCDB_BLOCK_RETURN_ACTUAL_RESULTS is enabled. Safe-ledger and reserved business-abort handling have separate payload behavior. Receipt agreement cannot certify passive SELECT-result equality.

Ordinary Kafka sends enqueue asynchronous work; safe paths explicitly wait for delivery. Result generation, data commit, state maintenance, delivery callback and client majority acceptance are distinct. Existing Merkle maintenance does not prove that every mode compares every changed root per transaction.

Source: pg_executor.cxx lanes/fifo/delivery, middleware.c payload/actual.

## Read a real guarantee with its configuration and evidence

| Setting/evidence | Interpretation |
|---|---|
| bcdb_serial_gate_source=0 | Publication-frontier ordering |
| bcdb_serial_gate_source=1 | Committed/finalized predecessor gating |
| bcdb_serial_gate_mode | Waiting mechanism, separate from gate source |
| bcdb_dt_conflict_tracking | Must be enabled for the described coverage path; compiled default is false |
| BCDB_DT_EARLY_VALIDATE / BCDB_DT_TAG_DEDUP / BCDB_DT_POST_PUBLISH_SETTLE | Reviewed defaults are enabled; overrides change execution |
| Actual responses plus a serial oracle | Required to assess prescribed outcomes, not just replica agreement |
| Final state and Merkle verification | Observed state evidence, distinct from response equality |
| Distributed post-marker PASS, divergence_count=0 and permanent_failures=0 | Workload-specific evidence, not universal correctness |
| Source, binary and effective-setting provenance | Identifies what implementation actually ran |

For an admitted SQL class, the proof obligations remain: same input/state, conservative snapshots, complete dependency and constraint coverage, retained history, validation through s−1 or ordered physical execution, no accepted business rerun after P release, faithful atomic application/finalization, and specified error/recovery semantics. The merged source addresses several concrete gaps in these mechanisms. The page explains those improvements without presenting historical chat audit counts as newly rerun measurements.

## Maintenance and verification

The standalone page is built from docs/determinism/model.js, view.js, style.css and template.html by scripts/build_determinism_evidence.py. Source references use reviewed literal anchors rather than hard-coded old line numbers. The builder fails when an anchor is missing or ambiguous; repeated call sites have explicitly selected occurrences. Review semantics before rebuilding after a source change.

scripts/check_determinism_explorer.py runs only on lab 10.129.148.247 using an isolated Firefox profile and temporary HTTP server. It exercises all scene events, expected illustrative outcomes, P/C holes, SVG motion, slower speeds, scrubbing/reverse/replay, five fallback reasons, source bytes/excerpts, theme, reduced motion and exact mobile viewports. It starts no database or gateway. Screenshots and result JSON are preserved under .bench_tmp/determinism_refresh_20261009_*/.

Historical context consulted: Claude 09a68b55-60dd-43e6-b156-e08c0b922875 (merged fixes and newest count fix), Codex 01a11ce2-b167-7bb2-ab2c-57a8cd98e4cd (coverage/seed/program fidelity audit), and the previous paper/settle analysis. These chats guide source review; the current code and current PDF determine this explanation.
