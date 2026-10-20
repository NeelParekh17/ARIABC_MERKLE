# TPC-C-derived, unaudited campaign, 2026-10-12

## main_20261020 changes

The deck methodology below now runs on main's committed engine 1105ea88
(det optimizations/settle, audit fixes, AdHash, confirm-before-repair,
`merkle_lsm` lookup indexes) in `~/claude_checks/tpcc_20261020`, tag
`main_20261020`, prepared images in `prepared/`. The deck_20261012 campaign
(engine 16577072) stopped on 2026-10-12 after 3 of 111 runs and resumes only from
its own checkout. Changes, all outside measured code paths:

- WAL collector: sampled LSNs are the WAL write frontier, so `pg_waldump -n1` on
  one can find no complete record yet (pg W5 attempt1: "could not find a valid
  record after 1/1598FFA8", which failed wal_stats/wal_incremental_ranges/
  wal_denominator). Boundary lookups retry for up to 10 s. A physical slot
  `tpcc_wal_collector`, created at collector start and advanced after every
  chunk, keeps undumped WAL from recycling; both appear in `wal_collection.json`.
- Checkpoint phase: two `CHECKPOINT`s just before the first gateway submit (the
  first flushes setup dirt, the second sets the timer; `checkpoint_align.txt`).
  checkpoint_timeout counts from the last checkpoint start, so the 300 s ramp
  ends at a checkpoint start and every 600 s window holds exactly two identical
  5-minute cycles. Before, the window started at a run-dependent point of the
  cycle; the pg W5 rejection (fitted change 0.50) was a sync stall in the first
  two window minutes, while TPS dipped to ~450 for ~90 s every cycle all run long.
  In-window checkpoints are still time-driven; the gates are unchanged.
- merkle_lsm evidence (Merkle with `MERKLE_LOOKUP_AM=merkle_lsm`):
  `lsm_sampler.py` samples `merkle_lsm_stats` for all nine lookup indexes at
  1 Hz in its own process (`lsm_samples.jsonl`), through the post-run drain.
  `lsm_summary.{json,md}` gives merges per index/level in ramp, window, tail and
  drain, seconds with a merge in progress, window TPS with and without a merge,
  and level/base sizes. Evidence only, not a gate.

The B-tree lookup remains available as `MERKLE_LOOKUP_AM=btree`.

## Frozen deck_20261012 methodology

The authoritative paper methodology uses frozen engine/server/gateway commit
16577072. Only harness and analysis code changes. All execution is in ranking's
isolated `~/claude_checks/paper_20261010_tpcc`, ports 55479/18140/19140,
CPU affinity 32-95, SERIALIZABLE, logged tables, 32GB buffers, durability and
autovacuum on. Merkle: fanout32, partitions16384, key columns1, subpartitions16,
split1024/merge256, including nine expression lookup b-tree indexes; this is the
geometry validated in the Oct3 v3 results. Existing v2 evidence remains archived.
The old common-N full plan is disabled.

## Metrics and windows

TPC-C v5.11 Clauses 5.4.2-5.4.4 define completed New-Orders including expected
rollbacks, per minute, truncated. We call this **TPC-C-derived, unaudited
tpmC-equivalent**. Every count comes directly from gateway `txLatencyCsv`
completion records, joined by zero-based transaction index to the exact seeded
SQL input. Count completions in the half-open gateway steady-clock interval
[300s, 300s + duration); exclude the five-minute ramp. There is no committed-count
scaling, adaptive endpoint, or drain inclusion. Report direct total business tx/s,
committed-only NOPM, realized mix, and per-type submit-to-gateway-completion
p50/p90/p99 (nearest rank). Expected rollback identification is checked against
the actual gateway abort total; independent whole-run district-ID growth plus
actual rollbacks must equal directly counted New-Orders. Initial and final
consistency conditions1-4 and all nine table state checks remain required.

The generator now uses the Cl.5.2.4.2 deck method: one shared deck of96 sets,
2208 individually identified cards, each set containing10NO,10P,1OS,1D,1SL.
The gateway's96 submission lanes share it at every DB-worker count. An
independent seeded RNG freshly shuffles the whole deck after exhaustion; every
card is consumed once per pass. Nominal mix is10/23,10/23,1/23,1/23,1/23,
replacing the earlier independent45/43/4/4/4 draws. Each actual completion window
must HARD-pass all Cl.5.2.3 minimums: Payment43%; OS/D/SL4% each. There is no
New-Order minimum. Every failed mix remains rejected evidence and receives a
retained extra attempt. A complete balanced deck cannot guarantee every arbitrary
time window because boundaries and in-flight completions differ; self-tests
check deck properties and50001 sliding generated windows, while acceptance
checks every actual window. A single failing minimum prevents qualification.
Response flags (NO/P/OS/D5s; SL20s, Cl.5.2.5.3/.7) remain disclosure columns.
Delivery is synchronous, including all ten districts of the selected warehouse in its business
transaction, rather than deferred. Completion includes gateway batch-ack wait.
No keying/think time or 10-terminals-per-warehouse pacing is used (research
convention); no audit, eight-hour sustainability, pricing/space certification,
or spec throughput-per-warehouse qualification is claimed.

## Predeclared steady-state and exclusion rules

For every real run, divide the entire fixed window into complete 60-second bins.
Use directly counted New-Orders including expected rollbacks in each minute.
Require sample CV <=0.40 and absolute fitted end-to-start change divided by
mean <=0.20, AND a95% slope confidence interval containing zero. The interval
uses Newey-West HAC standard errors with lag5 minute bins (or n-3 if fewer),
n/(n-2) small-sample scaling and a Student-t quantile approximation. This
accounts for the configured five-minute checkpoint cadence. Even a small
perfect linear drift is rejected. At least four minute bins are needed. OLS uses minute-bin centers; report signed slope in tpmC-equivalent
per minute and the normalized fitted change. The CV bound allows the repeatable checkpoint cycle observed in the matched
W5/32 PG calibration; the initial provisional0.20 bound was revised before any
publication run. See calibration_threshold_basis.csv for its direct minute
series statistics. These bounded variation and trend
thresholds apply identically to all modes and both durations; they are a research
steady-state test, not an official audit. No bin is removed. Save `per_minute.csv`,
including direct total-business counts and the five transaction counts.

A >=10-second zero-business-completion gap anywhere in the window, or a failed
sampler-quality gate, triggers an extra attempt. Preserve and flag all original
attempts. Host load and other-process CPU flags are disclosed without automatic
exclusion. The first chronological run passing all gates and the common stall
rule qualifies, never the fastest. Correctness/settings/lifecycle failures stop
sweeps/pilots for inspection. Up to three matched attempts per sweep/pilot are automatic; preliminary
calibrations and corrected setup failures without submission do not consume that
budget, and remain raw evidence;
headline failures, including acceptance/steady-state failures, automatically
rerun with the failed evidence retained. Fixed durations are never shortened.
Checkpoint timeout is5min; require time-driven starts inside the measurement
window and reject requested/WAL-driven starts (Cl.5.5.2.2). Environmental/WAL
samples use a conservative inner interval including recorded progress age and timestamp rounding, based on a <=1s measured bound between
gateway progress origin and first submit. Exact completion metrics use the full
fixed interval. WAL-per-transaction diagnostics therefore have an inner-sample
limitation, disclosed in `window.json`.

## Matrix, sizing and reporting

Sweeps: W5/10/20/30/50/75/100 at32 workers; W100 at8/16/24/32/48/64 workers.
W100/32 is shared between curves. Three trials per point/mode, rotating pg/det/
Merkle order per configuration and trial, 600-second measurement after300s ramp:
108 minimum runs. The first sweep block is det/Merkle/pg, then orders rotate
cyclically by point and trial; this is fixed before launch. Median is primary with min-max and n; publish no best-of result.
Headline W100/32: one accepted7200-second measurement per mode after300s ramp
(Cl.5.5.2.1). A separate reproducibility measurement is not required by
Cl.5.5.1.4. These three accepted headline runs supplement the sweeps. Every
headline attempt gets a per-minute matplotlib PDF/PNG, including rejected ones.

Matched pilots use the same W/workers/settings with300s ramp+60s measurement;
they are not publication runs. An initial60s-ramp preliminary calibration is
retained and excluded from matched sizing. Choose N individually per mode and point using
1.20 times the maximum complete post-ramp pilot minute's direct business rate, then a1.15 sweep margin
or1.10 headline margin plus65536 submission allowance. Acceptance requires the
last submission to occur strictly after the fixed interval ends. Insufficient
coverage retains the attempt and increases that mode's N by at least50%, also
using the failed attempt's observed submission span and any complete post-ramp
minute maximum to cover the entire ramp plus window with a20% margin and65536
submission allowance. An invalid/incomplete timeline uses the50% fallback.
This rate is used only for sizing the extra attempt, never for performance.
The capacity envelope includes every complete post-ramp60s bin ending before
submission finishes, saved in capacity_series.csv; it is used only for N/ETA,
never to replace the fixed pilot or publication metric window. ETA uses the
mean of those same bins, avoiding dependence on a single checkpoint phase.
Pilot-based ETA
includes observed setup/check time, a20% noise allowance, and elapsed preparation;
if total exceeds120h, write BLOCKED.md and do not launch the publication campaign.

Separate identical20000-transaction det/Merkle runs at every swept W verify
initial/final state parity and workload SHA. The commutative all-column SQL
state checksum is diagnostic, not a cryptographic equality proof. Duration runs do different amounts
of work and are not compared by final state. `standard_parity.json` records all
seven correctness pairs. Consistency, merkle_verify, zero permanent failures/
divergence, gateway success, isolation, relation settings, affinity, sampler,
WAL and lifecycle gates remain unchanged.

Preparation caches are campaign-owned initial databases stopped cleanly before
any workload. Their manifest records binary/schema/loader/procedure/geometry
identity, archive SHA and initial all-table state checksum. Restores are copies,
never hard links; initial state and consistency are verified again each run.
Only obsolete pre-REDO WAL in the stopped backup is omitted. No active database
copy occurs. Restart/prewarm/cache creation is excluded by the subsequent ramp.
Validation-only sessions use sequential scans, no nested loops, and128MB work_mem
to avoid quadratic consistency plans; workload GUCs remain unchanged.

The detached supervisor automatically rejoins an orphaned owned shell after a
controller crash. Completed acceptance is processed once; incomplete attempts
remain evidence and receive a fresh attempt. Atomic status/state and ownership
checks prevent reuse of attempt directories or interference with others.
After shutdown, disposable campaign pgdata is retired. Large workload/timeline/
server/trace files are losslessly gzipped, SHA-verified before removing the
uncompressed copy. `LOSSLESS_ARCHIVES.json` records both sizes and original SHA;
all raw evidence survives. Curated checksummed copies exclude bulk traces and
pgdata. `all_runs.csv` contains every attempt; `qualified_runs.csv` contains the
chronological accepted logical trials; `summary.csv` reports median/min/best/n.


The stopped B_standard_campaign_20261011 is rejected evidence; its original
acceptance JSON is preserved with REJECTED_CAMPAIGN.json overriding publication
eligibility. The fresh deck campaign is under deck_20261012. Engine binaries and
initial prepared images are reused only after exact provenance/state checks;
all36 mode/point pilots and all seven fixed-N pairs are measured again.


WAL accounting for the fresh deck campaign is incremental. The former120GB
retention could recycle the beginning of a two-hour interval before end-of-run
analysis: the old W100 pilots implied248–298GB over some oversized headline
inputs. wal_collector.py uses the same conservative inner sampled interval as
window.json, with a fixed one-second progress-origin bound (the measured bound
must still be≤1s). Every60s it calls the frozen pg_waldump; intermediate endpoints
are native WAL-record starts found by pg_waldump -n1, so a spanning record belongs
to exactly one disjoint chunk. The final endpoint remains the original sampled
LSN. All commands/LSN endpoints/native outputs/SHA are retained; acceptance
requires complete contiguous ranges, matching sample endpoints, each raw SHA,
and summed native count/record/FPI/combined bytes. A five-chunk real WAL test
matches a single native monolithic dump exactly. Primary completion counts and
measurement endpoints are unchanged. All fresh matched pilots and publication
modes use the same collector and normal max_wal_size64GB, wal_keep_segments0,
with no archive copy or extra checkpoint. The analysis reader inherits32–95
CPU affinity. Raw statistics survive ordinary recycling of disposable WAL.
Three additional short integrated smoke runs exercise this final accounting
path before matched pilots. Test/data generation and collector qualification
remain separate from publication results.


Response diagnostics additionally report mean/max and p90+100ms>mean
(Cl.5.2.5.6); this is a gateway diagnostic, not a screen-pacing certification or
new exclusion. The frozen transaction procedures omit some required display
field reads (New-Order item/stock brand fields; Payment warehouse/district
addresses), in addition to bounded return summaries. These Cl.2.x departures
are disclosed in COMPLIANCE.md; full transaction-profile compliance is not
claimed. Their source remains unchanged under the analysis-only task scope.
