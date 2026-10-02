# Online recovery with current synchronous Merkle code and S1024

Task I prepared the source analysis and conditional models below. Task J
completed the eight requested remote trials on 2026-10-01. **Measured new
F32/S1024/M256 baseline: 8,609.56 TPS; same-current-binary F4/S32/M8 control:
8,848.09 TPS.** New C / M_mixed / L_mix_prio repair took **60 / 93 / 95 ms**.
All eight final all-three audits passed. These are single trials, with no
consistent measured throughput gain. The conditional estimates below are
retained as pre-rerun models; the measured results at the end supersede them.

The existing fault injector/NodeConnection force READ COMMITTED
(`scripts/distributed/recovery/fault_injector.py:91`, `remote_db.py:58`).
The rerun generator makes isolated SERIALIZABLE copies with the existing
whole-transaction retry loop retained. No original recovery script is edited.
This injector isolation change is another published-to-new difference; it is
held fixed in the same-new-binary geometry controls.

## Published evidence and geometry

The 12 archived run_summary.env/CSV, phase_markers.env, runner.log,
gateway_test.log, server logs, fault logs and all-three post-marker readbacks
support the summary.csv. A read-only re-extraction with the new comparison tool
matched **156/156** selected fields across all twelve runs, including phase
times, repair counts, gaps and roots. All archived Phase-8 readbacks have
12,001 rows and matching verify=t/root/data digest; the extra row is the
verification marker. The setup logs explicitly show **12,000** initial rows.
The restore dump has keys 1..11994 and deterministic fill for 11995..12000.
160,000 is the transaction count, not the table size.

| Scenario | Majority TPS | TPS delta vs A | Cut ms | Repair ms | Catchup ms | Detection-to-event total ms | Mismatched partitions | Differing leaves | Candidate rows fetched | Deleted / upserted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| A baseline | 8850.05 | 0% | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 0 |
| B no fault | 8829.05 | -0.24% | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 0 |
| C follower update | 8692.82 | -1.78% | 2741 | 59 | 7970 | 10824 | 30 | 31 | 473 | 0 / 31 |
| M_mixed follower | 8686.21 | -1.85% | 2759 | 88 | 8094 | 10997 | 66 | 77 | 1195 | 34 / 43 |
| L_mix_prio leader | 8673.50 | -1.99% | 2777 | 115 | 7413 | 10363 | 132 | 185 | 2881 | 34 / 167 |
| C deep telemetry | 8793.14 | -0.64% | 2622 | 54 | 45 | 2816 | 19 | 20 | 324 | 0 / 20 |
| Leader mixed, slow donor | 4440.00 | -49.83% | 14559 | 324 | 2 | 14953 | 51 | 65 | 990 | 34 / 33 |

Sources: `summary.csv`; `runs/cluster4_final_C_fault_180113/server_node4_utkarsh.log:17`;
`runs/cluster4_test_mixed_utkarsh_184953/server_node4_utkarsh.log:19`;
`runs/cluster4_test_leader_mixed_192316/server_node1_admin123.log:2520`;
`runs/cluster4_recov_C_fault_140220/server_node4_utkarsh.log:19`.
Candidate rows are server telemetry omitted from the published summary.

The historical online configuration is **consistent with P=200, F=4,
split=32, merge=8**, single-column `USING merkle(ycsb_key)` with hash(key)
partition routing and a companion `(partition_for_hash(key_hash), key_hash,
ycsb_key)` B-tree. The runner and restore defaults select that configuration;
candidate_rows/differing_leaves are 15.3, 15.5 and 15.6 in C/M/L, matching the
approximately 15-row F4 children of 60-row partitions. The archived metadata
does **not** record pg_get_indexdef, reloptions or native node counts. Therefore
F4/S32 and the historical counts below are evidence-backed inference, not a
catalog measurement or proof that no unrecorded flags were used. Preserve this
distinction; a historical-binary restore on the lab can establish exact counts.

Current paths: `scripts/restore_usertable_small.sql:12116` onward and
`scripts/distributed/run_4node_raft_cluster.sh:410`, `:922`, `:3531`. Both still
default this workload to **F=4**. The recovery wrapper does not override fanout.
Changing the backend's fanout-32 thresholds therefore does **not automatically
change an unflagged recovery run**. Explicit flags for the new geometry are
`--merkle-partitions 200 --merkle-fanout 32 --merkle-split-threshold 1024
--merkle-merge-threshold 256`; restore rebuilds the index with these options.
Existing indexes retain their stored geometry until recreated.

| Geometry, same 12k rows / 200 partitions | Expected total nodes | Expected leaf nodes | Mean rows/leaf | Maximum prefix bits / height | Node updates per changed key |
|---|---:|---:|---:|---|---|
| historical online F4/S32/M8 | about 1000 | about 800 | about 15 | usually 2 / 2 | leaf + partition root, usually 2 |
| current F32/S1024/M256 | about 200 | about 200 | about 60 | 0 / 1 | partition root is the leaf, 1 |
| same-binary F32/S32/M8 control | about 6600 | about 6400 | 1.875 including empty leaves | usually 5 / 2 | leaf + root, usually 2 |

These follow native bulk construction (`merklebuild.c:125`, `:1831`): a
partition at or below split_threshold remains one root/leaf; otherwise it
emits all F children, including zero-occupancy children. F32/S32 consequently
has about 982 empty child leaves under a uniform hash model
(`6400 * exp(-12000/6400)`). S1024 does **not** yield 1024-row leaves in this
dataset: each partition has only about 60 rows. Model node count reduction is
5x against historical online, 33x against F32/S32. Measure native tuple_count,
prefix_len and reloptions using the new geometry.sql on all nodes; no exact
new geometry has been observed in a running DB here.

`Final_Results/Recovery/Report.md:3` describes a separate ranking-host,
single-node 1M–50M **F32/S32/M8** scaling experiment (110 trials; K=75 bad
leaves, C=300 corruptions). Its measured heights and repair times do not
describe the 12k-row online workload. It has no network cut or live Raft
catchup component. Do not extrapolate its 1024-row threshold gain or published
height table into online TPS.

## How geometry changes repair work

The actual C++ repair implementation is a partition-root comparison followed
by **complete leaf-map fetches in mismatched partitions**, not recursive
child-by-child Merkle descent:

1. `replica_repair.cxx:175` reads each dedicated node table's prefix_len=0 roots.
   `:376` compares maps and builds the mismatched-partition set P_bad.
2. `:201` fetches all `is_leaf` entries on both donor and target for P_bad,
   then `:418` unions differing/missing leaf keys (partition, node_id, prefix_len).
   Cost is proportional to all leaves in those partitions, including empty ones.
3. `:222` builds bounds using `merkle_node_upper_bound`; `:460` joins the
   companion lookup B-tree by partition and inclusive hash interval, streams
   **all donor rows in those ranges** through COPY into a temporary table, and
   deduplicates overlapping ranges by PK. Candidate_rows is the COPY row count.
4. `:470` deletes extras, then conditional ON CONFLICT updates only rows whose
   data is distinct. Unchanged candidate rows are read/transferred/compared but
   are not rewritten. Rows_upserted is therefore not COPY volume.
5. `:514` confirms roots and normally heap-verifies; unsuccessful confirmation
   or a heap/root inconsistency can trigger a full-table-copy fallback. Count
   full_copies explicitly; a nominal PASS with fallback is not sparse repair.

`merkle_get_partition_root_hashes` (`merkleverify.c:733`) reads root rows and
materializes one result for each configured partition. It is used by SQL
helper consumers; C++ localization above reads the native node table directly.
`merkle_node_upper_bound_sql` (`merkleutil.c:1061`) produces an inclusive 64-bit
prefix bound, and prefix_len=0 spans the entire key-hash range. P stays 200,
so root-vector cardinality is unchanged. A smaller compact node heap can make
these reads cheaper, but not 32x fewer root results.

Under S1024, one differing leaf = one mismatched partition. At **fixed P_bad**,
leaf-map rows fall about 4x versus F4/S32 (32x versus F32/S32), but fetched heap
rows rise from about 15 per differing old leaf to about 60 per bad partition.
For K independent persistent bad keys, expected bad partitions are
`200 * (1 - (199/200)^K)`: K=20 gives 19.1, K=100 gives 78.8. 100 injected
corruptions do not imply 100 persistent bad rows: subsequent workload updates
overwrite some, while a cut ahead of the quarantined replica creates additional
legitimate state differences. L_mix_prio upserts **167** rows, exceeding the
injected update count. Keep L, B, fault timestamps, replay targets and donor
selection when comparing these quantities.

| Same bad partitions as published | New differing leaves, model | New candidate rows, model | Candidate multiplier | Approx text COPY payload at 216 B/row |
|---|---:|---:|---:|---:|
| C: 30 | 30 | 1800 | 3.81x | 0.37 MiB (was about 0.10 MiB) |
| M_mixed: 66 | 66 | 3960 | 3.31x | 0.82 MiB (was about 0.25 MiB) |
| L_mix_prio: 132 | 132 | 7920 | 2.75x | 1.63 MiB (was about 0.59 MiB) |
| deep C: 19 | 19 | 1140 | 3.52x | 0.23 MiB |

216 bytes approximates ten 20-byte fields, delimiters and a key; corruption
strings, nulls and escaping alter it. This is an estimated payload, not measured
wire traffic. Worst-case P_bad=200 fetches almost the whole 12k-row table via
partition ranges without incrementing full_copies. Larger leaves improve node
maintenance but **reduce repair selectivity**.

Partition/root hashes remain XOR aggregates of canonical row hashes
(`merklebuild.c:168`, `:194`, `:1837`); grouping changes do not alter the
canonical row-hash bytes or expected partition/table root for identical rows.
Keep exact commit-time maintenance of every leaf/ancestor; do not turn off
merkle_apply_synchronous_direct or defer ancestor folding.

## Quantitative predictions and their limits

The current path uses cached tuple send functions, route TID hints, one
traversal snapshot and lazily initialized executor index state, plus compact
rebuild storage. S1024 additionally removes the second node update in this
small table. Root row locking remains shared among updates to the same
partition in both geometries. Reduced catalog work cannot remove that
contention, nor Kafka/quorum, WAL, heap work or gateway overhead.

An explicit **uncalibrated sensitivity model**: if 10–30% of the old wall-time
critical path is the portion of Merkle work halved by geometry, then Amdahl
speedup `1 / (1 - f/2)` is 1.053–1.176. Baseline TPS would be **9,316–10,412**.
If Kafka/quorum or another resource caps throughput, the gain can be zero
(about 8,850 TPS), and a regression remains possible. Holding published fault
penalties constant gives these model values, not measured predictions:

| Scenario | Conditional TPS for that 5.3–17.6% baseline gain |
|---|---:|
| A | 9316–10412 |
| B | 9294–10388 |
| C | 9156–10227 |
| M_mixed | 9149–10219 |
| L_mix_prio | 9130–10204 |

Synchronous path caching might add gains or simply move the bottleneck; the
pre-rerun model had no matched cluster profiles to calibrate f. The completed
measurements below did not establish this modeled gain.

For repair, use the published component telemetry and an explicit fixed-P_bad
model: retain uninstrumented/fixed repair time and verify_us, take localization
as 3 ms, scale transfer_us by candidate-row growth, and halve apply_us for
the shallower synchronous tree. This gives:

| Scenario | Old localise / transfer / apply / verify ms | Model repair ms | Planning range ms |
|---|---|---:|---:|
| C | 7.55 / 6.57 / 5.42 / 24.81 | 70 | 50–100 |
| M_mixed | 8.28 / 21.41 / 20.30 / 23.76 | 122 | 90–180 |
| L_mix_prio | 11.74 / 30.64 / 32.38 / 24.24 | 144 | 110–220 |
| deep C | 5.91 / 6.01 / 4.56 / 25.55 | 64 | 45–95 |

These ranges are planning allowances, **not statistical confidence intervals**.
The linear-transfer assumption may overestimate extra cost if the old transfer
time is dominated by fixed SQL/network latency, or underestimate it under
swapping/contention. Extra candidates still require temp-table insertion and
PK probes. Persistent changed-row counts and node contention may also change.
Repair could stay near published values or be slower despite faster workload
execution; there is no defensible unconditional claim that 59 ms becomes 20 ms.
The slow-donor run transferred 990 rows in 142 ms: extrapolating to about 3060
rows can approach 0.4–0.7 s total repair if that host remains constrained.

Cut measures commit-progress wait plus CUT RPC, not merely root comparison
(`gateway_recovery_manager.hxx:450`); old ordinary cuts are 2.62–2.79 s. With
the same queue/boundary behavior, allow **2.2–2.8 s**, rather than scaling it
by leaf-map reduction. A slow donor can still give a 14.56 s cut or timeout.
Reference STATUS ranking (`:432`) is already present in L_mix_prio, so do not
attribute its published improvement over the old ref=2 trial to new geometry.

Catchup follows a moving Raft target with a bounded executor window (64 entries
by default; `pg_state_machine_recovery.cxx:162`, `:644`). The recorded event
can become LIVE after replay entries are handed to the executor; it is not
the final all-three execution/Merkle audit. Same-prefix faster execution may
shorten backpressure, but a faster healthy cluster also advances the target
more quickly. There is no constant 8-second timer implied by these logs.
Keep replay_from/target, LIVE entries/time and final audit when attributing
catchup improvements. The 45 ms deep-C result versus 7.97 s C result is a
different operating point, not a geometry experiment.

If catchup is unchanged, predicted totals remain roughly **10–11.2 s** in
C/M/L, because cut and replay dominate. If cut/replay both improve by a
conditional 5–18%, their combined ~10–11 seconds yield roughly **8.5–10.9 s**,
including the larger repair. Deep-C would remain roughly **2.3–2.9 s** if its
small replay backlog persists. These are conditional regimes; which one
occurs requires matched runs. Even eliminating C's 59 ms repair entirely
would remove only **0.55%** of its 10,824 ms total.

Total_ms is detection-to-gateway-success, and contains quarantine, status
ranking, donor attempts, release and RPC work. It is not cut+repair+catchup.
Rebase_ms is separately visible: normally 25–28 ms, but about **1511–1518 ms**
in the leader-update runs. Include that omitted phase rather than attributing
the residual time to Merkle repair. The current manager records live=0 if its
catchup wait ends and then waits separately; such an event total is not time
to fully recovered LIVE. The comparison keeps event_live and rejects that
as complete fault evidence until separately investigated.

## Completed rerun: scope and correctness limits

Task J followed the user override to the Task I runbook: native U24 builds on
10.129.27.111 and native U22 builds on 10.129.148.246, rather than builds on
.247 or container builds. No local compilation, database or benchmark ran.
The frozen current working tree, ABI-specific installations, libraries,
PGDATA, Raft directories and generated runners all live within the same
fresh remote base, `/home/neel/Desktop/recovery_s1024_20261001T060514ZJ`.
PG used 5448, Raft 9018 and client listeners 8018/8019. Existing Kafka brokers
were used at the published cluster addresses (.247/.246/.248:9092); only
experiment topics ending `20261001T060514ZJ` were created. No broker was
restarted or reformatted. Heavy work on .247 began only after the local OOM
status contained `CHAIN_DONE`: gate observed 07:00:34 UTC, deployment began
07:00:49 UTC. Other-host preparation proceeded while waiting.

One timed trial per requested scenario/geometry ran sequentially, with
160,000 YCSB transactions, 96 lanes, eight workers, Kafka majority_async_all3,
32MB shared_buffers, and the published workload/ordering/pipeline flags.
The generated runner and exact command are retained in every run directory.
Workload and generated fault injector transactions used SERIALIZABLE;
whole-transaction serialization retries were retained. All three final GUC
readbacks show SERIALIZABLE default isolation, synchronous Merkle maintenance,
fsync=on, synchronous_commit=on and full_page_writes=on. Canonical row hashing
and recovery source were not changed. F32/S32/M8 was not requested and was
not run; control B and L_mix_prio were not requested either.

Correctness limits in the current code deserve explicit validation:

- merkleapply.c:2734 temporarily sets XactIsoLevel to READ COMMITTED only around
  internal aggregate maintenance, restoring the enclosing isolation/snapshot.
  Benchmarks still require SERIALIZABLE transactions; this pre-existing SSI
  suppression is not proof of serializable safety and must not be treated as
  a performance knob. No isolation or recovery source was edited in Task I or J.
- Snapshot import uses REPEATABLE READ READ ONLY (`replica_repair.cxx:565`);
  that preserves existing recovery snapshot semantics. Do not change it to
  force benchmark isolation settings onto the recovery protocol.
- Current QUARANTINE failure logs and continues (`gateway_recovery_manager.hxx:422`),
  but cmd_recover quarantines again and drains (`pg_state_machine_recovery.cxx:527`).
  Validate those actual paths, donor prefix and drain failures, rather than
  equating a normal recovery-enabled run with failure-path safety evidence.
- Compact node storage rebuild received remote initial restore/REINDEX and
  final heap/hash/root checks in these runs. This is bounded evidence for
  this 12k-row workload, rather than exhaustive split/merge/recovery safety.
  Per-run source fingerprint and per-platform binaries must match the frozen
  source snapshot. Binary SHA equality across U22/U24 is not required.

## Measured results (Task J)

Signed delta follows the published convention: negative means lower TPS.
Every row is one timed trial, not a multi-trial median. A_r1 and A_r2 stopped
before any database startup/workload due to preparation errors (missing
output parent, then incompatible skip-cleanup/fresh Raft storage). Both are
preserved; A_r3 was the first measured new baseline. This was not selection
among baseline samples.

| Geometry | Scenario | TPS | Signed delta vs A | Cut ms | Repair ms | Catchup ms | Total ms | Nodes / leaves | Phase-8 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| F32/S1024/M256 | A | 8609.56 | +0.00% | 0 | 0 | 0 | 0 | 200 / 200 | PASS |
| F32/S1024/M256 | B | 8868.20 | +3.00% | 0 | 0 | 0 | 0 | 200 / 200 | PASS |
| F32/S1024/M256 | C | 8740.30 | +1.52% | 3129 | 60 | 8152 | 11367 | 200 / 200 | PASS |
| F32/S1024/M256 | M_mixed | 8675.85 | +0.77% | 3153 | 93 | 8187 | 11484 | 200 / 200 | PASS |
| F32/S1024/M256 | L_mix_prio | 8853.96 | +2.84% | 2731 | 95 | 7471 | 10343 | 200 / 200 | PASS |
| F4/S32/M8 | A | 8848.09 | +0.00% | 0 | 0 | 0 | 0 | 1000 / 800 | PASS |
| F4/S32/M8 | C | 8610.48 | -2.69% | 3154 | 67 | 7641 | 10940 | 1000 / 800 | PASS |
| F4/S32/M8 | M_mixed | 8378.72 | -5.30% | 3661 | 91 | 445 | 4266 | 1000 / 800 | PASS |

Fault-work and availability counters:

| Geometry | Scenario | Bad partitions | Differing leaves | Candidate rows | Upserted | Deleted | Full copies | Empty 100ms buckets | Max gap ms | Donor | Injector retries |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| F32/S1024/M256 | A | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 34.92 | — | 0 |
| F32/S1024/M256 | B | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 18.66 | — | 0 |
| F32/S1024/M256 | C | 30 | 30 | 1835 | 35 | 0 | 0 | 1 | 212.30 | 2 | 0 |
| F32/S1024/M256 | M_mixed | 62 | 62 | 3723 | 45 | 34 | 0 | 0 | 102.56 | 2 | 0 |
| F32/S1024/M256 | L_mix_prio | 65 | 65 | 4015 | 45 | 34 | 0 | 0 | 64.60 | 4 | 2 |
| F4/S32/M8 | A | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 28.71 | — | 0 |
| F4/S32/M8 | C | 38 | 39 | 654 | 39 | 0 | 0 | 0 | 139.62 | 2 | 2 |
| F4/S32/M8 | M_mixed | 66 | 73 | 1129 | 46 | 34 | 0 | 1 | 139.20 | 2 | 8 |

The [complete comparison CSV](runs_s1024/comparison_with_telemetry.csv) includes
all twelve published rows and all eight new rows, signed baseline overhead,
published-to-new TPS changes, and recovery/audit evidence. The original
summary.csv lacks candidate-row and full-copy columns. A separate copy was
supplemented from archived server/gateway telemetry and fed to the unchanged
comparison tool; these columns are now present for every published run. The
[direct summary comparison](runs_s1024/comparison.csv), with those historical
columns UNKNOWN, is also retained. Original published files were not edited.
The [comparison figure](graphs/s1024_comparison.png) shows TPS, cut, repair
and catchup against the published results.

### What these measurements support

- New baseline TPS was 2.72% below published A and 2.70% below the current-code
  F4 control. New C and M_mixed TPS were 1.51% and 3.55% above their current-code
  controls, respectively. B and fault scenarios exceeding the new A sample
  do not establish negative recovery overhead. Single trials in fixed order
  cannot separate geometry effects from resource variation or noise.
- Measured native geometry matches the model: new has 200 root/leaves, no
  internal nodes, about 60 rows/leaf and 106,496 node-storage bytes; control
  has 1,000 nodes / 800 leaves / 200 internal nodes, about 15 rows/leaf and
  270,336 bytes. At the final 12,001 rows, new occupancy is 42–80 rows/leaf;
  control is 4–27. Larger leaves expanded candidate transfer: C 1,835 vs
  control 654 (2.81x), mixed 3,723 vs control 1,129 (3.30x). This confirms
  reduced tree size together with greater repair transfer work.
- Published C / M / L repair was 59 / 88 / 115 ms; new was 60 / 93 / 95 ms.
  New leader repair touched 45 upserts versus published 167, and 65 mismatched
  partitions versus 132. Its lower repair latency cannot be assigned to
  geometry alone. Random selected fault keys and injection timing differ.
- All new/control follower repairs selected donor node 2; published follower
  C/M selected donor node 1. Prioritized leader repair selected donor node 4.
  The donor difference further limits published-to-new phase comparisons.
- Control mixed injection committed on attempt nine after eight SERIALIZABLE
  failures, about 7.41 seconds after its first scheduled 5s attempt. Its cut
  was at L=546 / B=139,519 versus new mixed L=272 / B=69,375. Actual replay
  was 80 entries (547..626), 444 ms, versus 354 entries (273..626), 8,186 ms
  for new mixed. Control's 445 ms catchup is therefore a different recovery
  window after a late fault; it is not evidence of faster geometry replay.
  Control C and new L each needed two injector retries; new C/M succeeded
  immediately. Every successful fault case returned LIVE with one successful
  recovery event and no full copy.
- New C had one empty 100ms completion bucket and a 212.30 ms maximum gap,
  against published 0 / 91.89 ms and control 0 / 139.62 ms. Control mixed
  had one empty bucket / 139.20 ms gap. These stalls remain in the results.
- User4 had 6,935–7,186 MiB available RAM at the before/after samples, with
  pre-existing swap use of 1,783–1,785 MiB. System-wide swap-in deltas per
  new A/B/C/M/L were 430/94/214/294/81 pages; control A/C/M were 12/110/7.
  Swap-out was zero except three pages during control C. These samples span
  setup/run/cleanup and cannot attribute all swap activity to this workload.
  No experiment OOM or severe new swapping was observed, but shared-host
  memory activity is a remaining performance confound.

### Acceptance and preservation evidence

All eight runs completed 160,000 client transactions with divergence_count=0,
permanent_failures=0, no missing completions/timeouts, provenance PASS and
Phase-8 all-three Merkle/root/heap PASS. Async verified plus recovery-attributed
completions account for the 160,000 transactions. Final state on every node
was 12,001 rows, root
`80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5`,
heap digest `9a15b0794b52e087376bc7190d48cc58`, verify=t, matching the
published final state. Fresh initial restore/REINDEX audits gave 12,000 rows,
root `0df84e84229328afb8f70564f63a5ed45fd1cd78287b1c97ff35be39baa8080f`
and heap digest `99091c724b1ff4908f315b65614f378b`, matching published initial
state. Each fault case has an actual nonzero trigger, incremental repair,
catchup and return-to-LIVE trace. These successful cases do not validate all
failure paths listed above.

Frozen source fingerprint on all four hosts:
`60cae0f48ccd4cb4797c671757e6af640c0512ec827598fca234a338c6390490`.
Git status, full working-tree diff and diff SHA, source manifests, ABI-specific
binary manifests, native build logs and ldd checks are retained. No missing
runtime libraries were found. A staging exclusion initially omitted
src/benchmark/requirements.txt; the exact frozen file was restored before
runs and source/binary manifests revalidated without a fingerprint override.

The generated adapter removed shared-port kills, scoped server shutdown to
recorded executable-validated PIDs, enabled isolated Raft cleanup, and placed
server/gateway logs within RBASE. Original runners and shared source edits
were preserved. At completion all experiment servers/PG instances were
stopped and ports 5448/9018/8018/8019 were clear on all four hosts. Canonical
before/after hash/PID/listener inventories were identical on all hosts;
SHA-256 verification found zero changes among 582 pre-existing files under
published runs, summary.csv, graphs and Report.md. Only the new comparison
figure was added under graphs.

Accepted run directories are in [runs_s1024](runs_s1024/), with separate
failed_setup archives and provenance. The timestamped remote-command log,
acceptance checks, prepared runner, failed-run notes and helper scripts are in
[runs_s1024/launch_evidence](runs_s1024/launch_evidence/). The bulk local
artifacts (a 4.1 GB frozen source copy and per-host build trees) were deleted on
2026-10-02 to free disk space; the frozen source is identified by
`frozen_source_manifest.txt` and `uncommitted_diff.patch` in launch_evidence.
The remote RBASE and ten suffixed Kafka topics remain for retention. Exact
execution and retirement commands are in
[Task J report](../../.bench_tmp/codex_tasks/report_J.md).
