# Task H — remaining OOM Merkle overhead and next measurements

The remaining penalty is primarily update-path work: the large lookup B-tree
still gets a new entry on almost every non-HOT user UPDATE, while each update
also hashes both row images and synchronously writes three MVCC node tuples.
The S1024 tree removes much of the old node I/O, not those costs. The strongest
structural lever is a HOT-friendly **common** heap baseline; the cheapest
no-rebuild experiment is WAL FPI compression. I supplied procedures for both,
and made **no Merkle C changes**.

## Evidence and quantitative reconciliation

Read Graphify before exact source. Read both campaigns' raw result/setup/io/
checkpoint JSON and workload bytes, not just comparison CSV. Local data analysis
completed for all **24 new pairs**, validating completion, failures, isolation,
Merkle PASS, SQL SHA256 and raw buffer/device counter deltas. Artifacts:

- `.bench_tmp/codex_tasks/oom_h_analysis_20261001_v3/decomposition.csv`: complete
  det/S32/S1024 measurements and mutation-normalized deltas.
- Same directory `evidence.json`: effective settings, fresh det controls,
  archived read-only C and limitations; `table.md`: all 24 points.
- The first analysis attempt stopped on an older result schema lacking
  `row.isolation`; the final reader uses recorded effective settings for that
  schema. The empty first directory is retained, not overwritten.

For a workload with N statements and U updates, the net mixed-workload
equivalent is `(new_wall_ms - det_wall_ms)*1000/U`. This is not a separately
measured UPDATE latency. Pure reads do no Merkle DML; mutations can still
interfere with their cache, waits and scheduling. Rounded gateway wall_ms and
TPS-derived denominators differ by fractions of a microsecond.

| A θ0, w1, 20,000 statements, 9,983 updates | det | old S32 | new S1024 |
|---|---:|---:|---:|
| TPS | 1,427 | 907 | 1,170 |
| Device workload reads MiB | 328.79 | 676.39 | 444.36 |
| Device workload writes MiB | 279.03 | 487.97 | 387.36 |
| Separate checkpoint writes MiB | 106.29 | 285.38 | 201.73 |
| PostgreSQL blocks read | 43,964 | 113,970 | 83,328 |
| PostgreSQL blocks hit | 135,201 | 503,748 | 336,719 |
| PostgreSQL read time ms (aggregate) | 6,531 | 12,976 | 8,644 |

New minus det is **153.9 µs/statement ≈ 308.4 µs/UPDATE-equivalent**.
Its recorded PG read-time difference is **105.7 µs/statement ≈ 211.7
µs/UPDATE-equivalent**. At w1, the remaining **48.3 µs/statement ≈ 96.7
µs/UPDATE-equivalent** is an *unattributed net residual*, including CPU,
WAL/flush waits, locks, scheduling, and changes to shared/common work. PG
read time can include OS-cache reads and waiting, and some housekeeping is
outside gateway time; even w1 is not a perfect exclusive timer decomposition.

The corresponding excess per UPDATE-equivalent is **11.85 KiB device reads**,
**11.11 KiB workload writes**, **9.79 KiB separate checkpoint writes**,
**3.94 PG buffer reads**, and **20.19 PG buffer hits**. PG buffer reads represent
31.5 KiB of buffer loading, much more than 11.85 KiB physically read from the
SSD: repeated loads from Linux cache and attribution-window differences matter.
Never add PG buffer bytes to device bytes. Continuous device writes increase
by **21.00 KiB/update**, including workload, interphase and checkpoint windows.
Do not call this WAL volume: data pages and other filesystem writes are included.
Checkpoint time/writeback is outside TPS.


The matching PostgreSQL checkpoint log reports WAL distances of **154,249 KiB
(det), 333,232 KiB (old), 249,207 KiB (new)** at A θ0 w1. New minus det is
**9.51 KiB/update-equivalent** of checkpoint-to-checkpoint WAL, versus old's
17.93 KiB. At A θ0.99 and F θ0.99 w1 the new increment is **5.93 KiB/update**;
at A θ1.2 it is **2.74 KiB/update**. These proxies include work between the
previous checkpoint and the timed checkpoint, not only gateway SQL; use start/
end LSNs for an exact workload denominator. The analyzer matches checkpoint
write/sync/total durations against checkpoint.json, leaving ambiguous matches
blank (D det w1 has two matches). FPI reuse in a 10k-update mixed run versus a
3k-update probe helps explain why this WAL increment is lower than the probe's
~12.07 KiB/update. It cannot establish the relation split without pg_waldump.

`PROFILE_SERVER` reports A θ0 w1 PostgreSQL query time **13,793.7→16,917.1 ms**,
an increase of **156.2 µs/statement**, closely tracking the 153.9 µs gateway
wall increase. Result-format time increases only **12.41→14.54 ms** for the
entire run; ordered-apply wait **8.36→9.25 ms**. The logs report 20,000 exec
calls, zero retry attempts/exhaustion and **zero Kafka sends**. This single-node
bypass/direct experiment therefore locates the penalty inside PostgreSQL call
duration, not Kafka delivery. The enormous aggregate `queue_delay_ms` counters
sum waiting across already-submitted requests and cannot be added to wall time.

At A θ0 w16, the excess remains about **11.97 KiB SSD reads**, **3.90 PG
buffer reads**, and **10.19 KiB checkpoint writes/update**, but the gateway
penalty is only **51.4 µs/statement**; aggregate PG read-time excess is **220.8
µs/statement**. Their overlap makes additive time accounting invalid at w>1.
The workloads load more concurrent I/O; det and new device read await rise
from ~0.150/0.145 ms at w1 to ~0.719/0.610 ms at w16. Do not divide aggregate
time by nominal workers or treat `workers/TPS` as measured service latency.

### Relation-level reads and WAL: separate probe, not campaign instrumentation

The user-supplied 3,000-update cold/plain-SQL probe reports:

| Incremental relation work per UPDATE | Cold PG misses/update | Warm observation | FPI/record bytes/update |
|---|---:|---|---|
| Merkle node heap | 0.83 | Node heap+PK ~0.17 combined | Heap2/prune FPI ~4.4 KiB |
| Merkle node PK | 0.33 | Included above | Normally no insertion for HOT node update |
| User lookup B-tree | 1.78 | ~1.14 misses/update | B-tree FPI ~7.3 KiB |
| Node heap updates | Covered by heap row above | Three successful tuple updates | Update records ~0.37 KiB |
| User heap+PK shared with det | Not separately supplied | ~99.8% user updates non-HOT | ~15.6 KiB shared WAL/update |

Thus the probe attributes **2.94 additional buffer misses/update ≈23.52 KiB**
and roughly **12.07 KiB extra WAL/update** to lookup+nodes. Lookup is about
**61% of these misses** and **62% of the extra FPI bytes**. This establishes a
credible priority; it does not establish identical percentages for gateway DET.
Its 23.52 KiB buffer-loading estimate is below the campaign's 31.5 KiB
increment; the **~1.00 miss/update gap** includes DET versus plain-SQL staging/
visibility access, workload mixture/cache competition, and physical layout.
The provided node and lookup miss numbers are not actual SSD reads by relation.
The archived campaign does not contain per-relation WAL/CPU measurements, so
an exact relation-by-relation campaign decomposition cannot be recovered.

The **12.07 KiB probe WAL** is close in scale to the **11.11 KiB campaign
workload-write increment**, but these are different denominators/windows and
plain-SQL versus DET paths. Node/lookup pages can be written during the
workload or at its checkpoint, and some WAL may be included with FPIs or
buffered/recycled differently. There is no independent second addition of
FPIs to device bytes. The extra checkpoint ~95.44 MiB is dirty-page writeback,
not proof of an additional 95.44 MiB of WAL. Repeated checkpoints change
first-touch FPI frequency; both campaigns record WAL compression off,
full-page writes on, fsync on and synchronous commit on.

### CPU and in-memory work per same-key UPDATE

Current exact source confirms the following successful no-topology-change
operation counts; time is not recorded per component:

| Component | Successful work/update | Source |
|---|---|---|
| Row hashing | Two canonical whole-row hashes, old and actual new heap images; 22 send-function calls for key+10 fields | `shm_transaction.c:2375`, `shm_transaction.c:2535`, `merkleutil.c:390` |
| Merkle routing | Two full-key BLAKE3 computations; P200 full-key mode has no leading-column extra digest | `shm_transaction.c:2414`, `shm_transaction.c:2554`, `merkleutil.c:634` |
| Lookup expression | Two independent `merkle_key_hash` evaluations per non-HOT insertion, plus partition modulo and bytea results; expression EState/BuildIndexInfo | `heapam.c:1922`, `merkleutil.c:1370`; setup index definition |
| Staging | One SAME_LEAF XOR event in DET, a subtransaction frame/map and a second combined map before apply | `merkledelta.c:95`, `merkledelta.c:170`, `merkledelta.c:375` |
| Route resolution | Prefix-cache lookup; on miss traverse root/internal/leaf with a traversal snapshot, collect validated TID hints | `merkleapply.c:1447` |
| Node writes | Three heap tuple updates at prefix lengths 10,5,0, old/new hash XOR, MVCC tuple formation, normal locks/WAL/prune | `merkleapply.c:1992`, `merkleapply.c:2170`, `merkleapply.c:2541` |
| Per-apply setup | SPI connect/finish, security/isolation save/restore, sorting/slots/relation/plan setup; normally one CCI after an index's nodes | `merkleapply.c:2331`, `merkleapply.c:2594`, `merkleapply.c:2691` |

For integer keys and ten non-null 20-byte text fields, the canonical row stream
is **407 bytes**: 16-byte header + eleven 17-byte field headers + 4-byte key +
200 bytes of text. One route stream is **37 bytes**. A non-HOT UPDATE therefore
hashes approximately **2×407 + 4×37 = 962 bytes**, with 26 scalar binary-send
calls/temporary results before node tuple allocations. Bulk BLAKE3 throughput
alone is a poor model: many incremental calls, tuple deformation, catalog/type
cache work, send calls, allocations and executor construction dominate this
small-byte workload. The existing send-function cache already removes repeated
type-send resolution; it does not eliminate binary-send allocation/serialization.

There is **no BLAKE3 rehash of children** in normal ancestor propagation: it is
XOR/count maintenance. The new node path uses route TID hints, fresh lookup
snapshots per node, and opens executor node indexes lazily only for non-HOT
fallback. SPI connect is once per apply; direct steady-state node writes do not
execute SQL per node. `GetCurrentCommandId(true)` is not another CCI. The prior
TPC-C ~40 µs/HOT-node observation suggests a ~120 µs three-node component **in
that environment**, but cannot be added to this OOM campaign's 211.7 µs PG-read
increment: that would exceed its 308.4 µs net update penalty even before hashing.
Different binaries/paths, cache states, instrumentation, and common-work
changes prevent transferring that CPU timer as an exclusive OOM measurement.
The observable ~97 µs net residual is the honest current CPU/other budget.

The inherited applier temporarily sets internal `XactIsoLevel` to READ COMMITTED
at `merkleapply.c:2735` while preserving/restoring the outer SERIALIZABLE
transaction; I did not introduce or change this behavior. All benchmark
sessions/settings remain SERIALIZABLE. This report does not independently prove
that existing internal SSI bypass or recovery safety; no relaxation is proposed.

### Skew, workers, and read-heavy behavior

- A θ0→0.99→1.2 w1 incremental device reads fall **11.85→8.03→4.04
  KiB/update**; PG misses fall **3.94→2.17→0.64**, and checkpoint excess
  **9.79→5.77→0.42 KiB/update**. The ~20→18 extra buffer hits remain.
  Skew improves page reuse/FPI reuse but does not remove the three node writes,
  row hashes or staged apply. Net per-update equivalents fall **308→184→125 µs**.
  At high workers hot keys and ancestor tuple locks/ordered commit can matter;
  the existing artifacts do not isolate contention from device variance.
- F θ0.99 w1 adds **297 µs/update-equivalent**, with **217 µs PG read-time
  excess** and **8.03 KiB device reads/update**. Its read-modify-write is a single
  materialized-CTE statement and still roughly 50% mutations; do not count F as
  two gateway statements. The worse F wall delta versus A despite similar byte
  counts includes higher observed device await (0.154 vs det 0.119 ms) and
  additional common SQL/execution/scheduling work, not a different node depth.
- Archived C θ0.99 Merkle/det ratios are **0.931,1.028,1.006,1.040** at
  w1,4,8,16, with device-read differences **+0.816,+0.129,-0.090,+0.031 MiB**
  over ~147 MiB. No row hashes/deltas/node writes/lookup inserts run for SELECT.
  There is no new S1024 C point; read-only equivalence is observed in the archived
  campaign and expected for the same SELECT access path, not newly measured.
  Per archived read statement, det loads about **0.982–0.986 PG blocks** and
  **7.55 KiB from the SSD** at θ0.99. Merkle's incremental SSD bytes are only
  **+0.0418,+0.0066,−0.0046,+0.0016 KiB/read** at w1,4,8,16; corresponding wall
  differences are **+16.35,−2.75,−0.40,−2.10 µs/read**. Direct Merkle row/route
  hashing, lookup maintenance, node writes and staged apply are all zero for
  these SELECTs; the small signed differences reflect ordinary access-path and
  run variation, not a measured per-read Merkle maintenance tax.
- B has only **969 updates/20,000 statements (4.845%)**, and D has **1,024
  inserts (5.12%)**, versus A/F's ~50%. Thus removing a level saves much less
  per gateway statement. Rare mutations have poorer amortization/reuse: B
  w1 has **4.18 extra PG misses and 18.89 KiB SSD reads/update-equivalent**, D
  **5.38 misses and 26.38 KiB/insert-equivalent**. D inserts must add both
  B-trees; HOT cannot eliminate index maintenance for INSERT. Initial node/
  lookup page touches, sparse apply setup and cache interference are diluted
  across mostly reads. B/D new/old w1 TPS gains are only ~2.2%/~9.0%, while
  A θ0 is ~29.0%. These are single trials, not fixed per-update service costs.

Fresh canonical-det drift is material. A θ0.99 det is +0.39% at w1 and
**+13.88% at w16**; F is **−6.60% at w1, +5.62% at w16**. New/fresh-det ratios
are **0.832/0.714 for A** and **0.824/0.747 for F** at w1/w16. Historical-det
ratios alone therefore exaggerate or understate some remaining gaps.

## Ranked synchronous optimizations

All gains below are **hypotheses for planning, not measured improvements**.
They are not additive. Node+lookup I/O remains visible under full synchronous
roots; SERIALIZABLE/retries, canonical hash bytes and recovery APIs remain.

| Priority | Lever | Expected A/F benefit and limits | Cost/risk |
|---|---|---|---|
| 1, cheapest experiment | `wal_compression=on` on every compared mode | Planning 0–15% TPS uplift; reduce FPI WAL/write volume, not SSD reads or dirty data pages. PGLZ CPU can regress TPS. At 50% compression of the probe's 11.7 KiB extra FPIs, save ~5.85 KiB/update (~57 MiB/10k updates), plus shared baseline FPIs | No rebuild, no C changes; paired off/on det and pg required. Existing published config is off, so cannot relabel on results as that campaign |
| 2, strongest structural lever | Physically rewrite usertable with fillfactor90 for **all modes** | Planning ~5–25% new-Merkle gain if HOT fraction rises sharply; eliminates lookup and PK insert/expression work, some common heap/PK WAL and new-page work. det/pg also improve; Merkle/det gap should be assessed anew. Does not remove hashing or three node updates | Heap +11–14%, ~2.6–3.4 GiB; rebuild/copy/sort/Merkle verification hours. Current 62GB free insufficient for preserving baselines+test copies. Hot-key/long-snapshot chains may exhaust per-page reserve |
| 3 | Reduce node prune/FPI churn via separately measured node fillfactor/layout | More reserve can improve HOT chain reuse and pruning amortization; saving half the 4.4 KiB prune FPIs is ~2.2 KiB/update. Likely a small TPS gain after S1024; fewer resident pages may counteract it | Node DDL already fillfactor80 (`merkleutil.c:1629`). Lowering to70 grows heap ~14%; must rewrite to realize space. Do not disable pruning: it can cause more non-HOT writes, bloat and lock/WAL cost |
| 4 | Share lookup route hash results | At most remove one duplicate route hash of two lookup expressions, plus two old/new routes if a safe wider key cache is designed. Planning 0–3% TPS; no disk/FPI saving | `fn_extra` is per expression call site; DET constructs/frees EState per insertion, so ordinary fn_extra caching does not share expressions. Cross-call cache must own canonical bytes, include type/typmod/format identity, and preserve user-defined send behavior; never key on transient Datum pointers |
| 5 | Eliminate small per-apply map/sort/SPI setup, specialize exact binary-send hot cases | Planning 0–5%; profile first. One-frame map reuse could avoid a dynahash clone; one-entry sort is unnecessary. Preserve arbitrary subtransactions/abort and all synchronous write/CCI visibility paths | Tiny likely gain versus I/O; existing concurrent C work includes TID/snapshot/index/send caches. Skipping SPI/security lifetime, CCI or send semantics without matched tests has correctness risk |
| Rejected | Skip identical lookup-key insertion on non-HOT UPDATE | Would appear to remove dominant B-tree work | **Incorrect as a local optimization:** old entry points to an old TID; a cross-page successor is not reachable via a HOT chain. After visibility/prune/VACUUM, index scans can lose the live row for split/repair. Logical-key equality is not physical row-version reachability |

Node update **record** bytes (~0.37 KiB/update in the supplied probe) are only
about 3% of its extra WAL; eliminating all of them would still leave the two
FPI components. A custom compact WAL/storage format is therefore a poor first
lever and introduces redo/recovery compatibility risk. Adjust checkpoint/FPI
frequency or compression through paired durable settings instead; disabling
full-page writes or delaying ancestor folding is excluded.

Keeping one persistent lookup key per row would require a different stable
locator/key directory, row-version resolution and atomic maintenance for
insert/delete/key change, and adapted split/repair scans. That is a schema/API/
recovery redesign requiring rebuilds and long-lived snapshots/crash/vacuum
validation. It is not a safe small hunk in the Merkle directory. Removing the
lookup index completely retains scan correctness only if consumers deliberately
fall back to full heap scans; that radically worsens split/repair behavior and
must not be hidden in a throughput-only result.

## Changes, ownership, and exact next commands

Only NEW files under `scripts/distributed/oom_opt/` plus this required report
and analysis outputs were authored. **Every C hunk changed by Task H: none.**
No existing runner, defaults, node apply, send cache, delta staging, rebuild,
baseline or user artifact was edited. Other agents' Merkle diffs remain theirs.

- `oom_opt/analyze.py:13` reads/validates raw evidence; `:54` produces all-point
  decomposition and settings/control evidence. It executed locally as permitted
  **data analysis**, with 24 accepted pairs.
- `oom_opt/run_wal_compression.py:18` is a scoped configuration wrapper around
  the canonical runner; `:37` records treatment identity for resume; `:46`
  writes only stopped disposable config; `:66` validates effective treatment.
- `oom_opt/prepare_fillfactor90.sh:6` guards host/user/root/storage/memory;
  `:66` rewrites a new common heap and explicitly rebuilds both Merkle indexes;
  `:90` verifies keyspace, geometry, full aggregate and per-partition roots.
- `oom_opt/relation_snapshot.sql:5` provides relation/HOT/LSN snapshots for a
  separate attribution run.
- `oom_opt/RUNBOOK.md:40` has **copy/paste exact controller commands** for
  dry-run, preflight and 16 exploratory measured cases (A θ0 and F θ0.99,
  w1/w16, det/Merkle, WAL off/on). `:118` has remote fillfactor preparation and
  fair all-mode baseline measurement commands; `:184` describes profiling.

Local analysis command already executed:

```bash
python3 scripts/distributed/oom_opt/analyze.py \
  --comparison .bench_tmp/oom_s1024_20261001/comparison/comparison.csv \
  --published Final_Results/OOM_100M/summary.csv \
  --out-dir .bench_tmp/codex_tasks/oom_h_analysis_20261001_v3
```

The orchestrator should run `RUNBOOK.md:40` first, preserving the old roots.
After those arrays are defined, its first small accepted gate can use:

```bash
"${OOM_H_WRAPPER[@]}" --wal-compression off "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --combos a:0.0 --workers 1 --out-dir "$OOM_H_OUT/gate_merkle_off"
"${OOM_H_WRAPPER[@]}" --wal-compression on "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --combos a:0.0 --workers 1 --out-dir "$OOM_H_OUT/gate_merkle_on"
```

Run the corresponding det off/on gate as well, then remaining points if the
setting is accepted. Preserve full query terminal/completion evidence, zero
divergence/permanent failures, no failed cleanup and Merkle PASS. Repeat any
winner three cold trials before making a performance claim. Confirm hashes
select the intended frozen remote binary; graph/source continuity alone does
not prove a remote build. No builds, database instances, tests, benchmarks or
SSH ran locally in this task. These new remote procedures remain **unexecuted**;
there is no claimed TPS improvement or recovery/crash validation.

Static whitespace review produced no diagnostics for all new files (git
no-index diff returns 1 because these files are additions). The final data
analysis completed for all 24 accepted pairs. `graphify update .` completed
with exit 0 after the last Python edit: 60,937 nodes, 201,272 edges; log is
`.bench_tmp/codex_tasks/graphify_h_update_final_20261001.log`. Graphify also
reported its missing SQL parser and partial extraction in 641 existing files;
no dependencies or other source files were changed. The wrapper, preparation
script and SQL snapshot remain runtime-unvalidated; remote syntax checks and
then dry-run/preflight/accepted points are specified in the runbook.

The 24-point table follows, using historical det as its denominator; fresh det
controls and the associated drift are reported above.


| Workload | Skew | Workers | New/det | Δ wall µs/stmt | Δ PG read µs/stmt (overlaps) | Δ device KiB/mutation | Δ buffers/mutation |
|---|---:|---:|---:|---:|---:|---:|---:|
| A | 0.0 | 1 | 0.820 | 153.9 | 105.7 | 11.85 | 3.94 |
| A | 0.0 | 4 | 0.748 | 109.5 | 181.9 | 12.01 | 3.91 |
| A | 0.0 | 8 | 0.799 | 67.4 | 143.9 | 12.05 | 3.89 |
| A | 0.0 | 16 | 0.796 | 51.4 | 220.8 | 11.97 | 3.90 |
| A | 0.99 | 1 | 0.835 | 92.1 | 49.2 | 8.03 | 2.17 |
| A | 0.99 | 4 | 0.750 | 85.5 | 135.1 | 8.11 | 2.15 |
| A | 0.99 | 8 | 0.873 | 28.2 | 70.5 | 8.03 | 2.14 |
| A | 0.99 | 16 | 0.813 | 31.4 | 169.6 | 8.09 | 2.14 |
| A | 1.2 | 1 | 0.858 | 62.6 | 31.8 | 4.04 | 0.64 |
| A | 1.2 | 4 | 0.804 | 52.1 | 73.4 | 4.02 | 0.65 |
| A | 1.2 | 8 | 0.761 | 47.1 | 92.4 | 4.07 | 0.65 |
| A | 1.2 | 16 | 0.782 | 35.4 | 82.0 | 4.09 | 0.66 |
| B | 0.99 | 1 | 0.863 | 39.4 | 26.1 | 18.89 | 4.18 |
| B | 0.99 | 4 | 0.894 | 14.2 | -8.4 | 18.82 | 4.13 |
| B | 0.99 | 8 | 0.894 | 10.9 | -23.1 | 18.97 | 4.09 |
| B | 0.99 | 16 | 0.811 | 16.9 | -4.0 | 19.04 | 4.11 |
| D | 0.99 | 1 | 0.876 | 36.1 | 29.3 | 26.38 | 5.38 |
| D | 0.99 | 4 | 0.849 | 20.9 | 11.0 | 26.61 | 5.27 |
| D | 0.99 | 8 | 0.834 | 17.8 | -5.5 | 26.15 | 5.27 |
| D | 0.99 | 16 | 0.838 | 14.3 | -37.8 | 26.03 | 5.31 |
| F | 0.99 | 1 | 0.770 | 148.7 | 108.8 | 8.03 | 2.19 |
| F | 0.99 | 4 | 0.755 | 84.0 | 135.9 | 8.08 | 2.17 |
| F | 0.99 | 8 | 0.750 | 63.4 | 188.8 | 8.13 | 2.15 |
| F | 0.99 | 16 | 0.789 | 35.7 | 203.2 | 8.09 | 2.16 |
