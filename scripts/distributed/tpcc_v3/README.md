# TPC-C-derived v3 harness

This harness runs only as `protectdr` on ranking (10.129.7.57). All PostgreSQL,
server, gateway, loading and sampling work happens there. It creates a new
`~/claude_checks/v3/B_*` campaign and a fresh database directory for every attempt.
It never reuses an attempt, replaces a failed trial, selects the best trial, or
changes the v2 scripts. See `VALIDATION_B.md` for integrator commands.

`run.sh <pg|det|merkle> <W> <N> <workers> <trial>` requires `INST BINDIR SRC RUNROOT
CPUSET`. Ports default to 55439/18100/19100. `campaign.sh W:workers:N ...` runs
trial-outer order across `MODES='pg det merkle'`, with fixed `TRIALS=3` by default.
The requested final campaign explicitly sets `TRIALS=1`. With one trial the median,
minimum and maximum coincide; that does not establish a stable performance ranking.
Failed/noisy trials remain listed. Campaign locks are held only by the parent;
owned daemon children close the lock descriptor. Port checks never kill listeners.

Each attempt uses initdb, Agent A's seeded UNLOGGED loader, fillfactor 90 and
LOGGED conversion, Agent A's indexes/procedures and optional Merkle setup, ANALYZE,
and prewarm. All nine tables must be LOGGED with the expected fillfactor. All modes
use SERIALIZABLE, `shared_buffers=32GB`, synchronous_commit/fsync/full_page_writes
on, autovacuum on, `checkpoint_timeout=5min`, `max_wal_size=64GB`, and the v2 BCDB
GUCs. `enable_seqscan=off` limits coarse relation SIREAD locks and deterministic
read tags. PostgreSQL (including inherited workers/autovacuum), server and gateway
are pinned by `taskset -c "$CPUSET"`. The sampler saves process affinity evidence.
The ranking host has one NUMA node; the harness saves `lscpu` and makes no claim
that NUMA placement causes stalls. CPUSET must be fixed across comparable points
and be a subset of the launching shell's allowed CPUs.

## Window and metrics

The gateway retains its default 5000 ms progress interval. The harness requests
`--progressIntervalMs 1000`; progress lines now also contain `wall_time_unix_ms`.
`gateway.log` preserves every line. `sampler.py` runs once per monotonic second,
reads the latest complete gateway line before its SQL query, then records database
wall time, cumulative completions/abort outcomes, `sum(d_next_o_id)`, current WAL
LSN, load averages, query latency and progress age in `samples.csv`. The sampler's
read-only transaction uses READ COMMITTED to avoid adding SERIALIZABLE predicate
locks to the workload. This observational query does not change workload isolation.
Every ten seconds `/proc` tick deltas identify the ten busiest processes outside
the owned PostgreSQL/server/gateway process trees, including unrelated processes
of the same Unix user. User names, PIDs, CPU percentages and command lines are saved.
CPU percentage is relative to one core and can exceed 100 for multithreaded tasks.

The window begins at the first valid sample whose gateway `elapsed_s >= WARMUP_S`
(default 60). It ends at the last sample **strictly before** the first observation
with `sent >= total`. It ends earlier if five consecutive per-second completion
rates fall below half the preceding twenty-interval median: the endpoint is the
sample immediately before the first falling interval. A genuine mid-run stall can
trigger this conservative rule; the endpoint reason and a noise flag disclose it.
If submission completion is not observed, the final observation is also excluded,
and acceptance still requires a final progress line and all N outcomes.
No interpolation or final/drain counter substitution is used. `window.json` saves
both complete boundary rows and every arithmetic input. Duration uses the database
wall-clock delta; the gateway and database observations can be offset by up to
roughly one sampling interval. Acceptance rejects stale progress (>2.5 s), sample
gaps (>2.5 s), query latency >1 s, missing rows/counters and affinity mismatches.

* Total TPS = business outcomes in the window / window seconds. Expected abort
  outcomes are included; their window count and whole-attempt count are separate.
* NOPM = `(sum(d_next_o_id)_end - sum(d_next_o_id)_start) * 60 / seconds`.
  This counts committed NewOrders, excluding their expected rollbacks.
* NOPM is **tpmC-equivalent, unaudited (no keying/think time)**. These workloads
  have no terminal binding, keying or think time. Do not publish unqualified tpmC.
* The parser tolerates absent `user_aborts=` fields, reports unknown counters, and
  rejects acceptance rather than treating them as zero. It supports either a
  `completed` counter that includes aborts (`completed=N`) or excludes them
  (`completed+user_aborts=N`); it records the convention and computes committed
  transactions accordingly. The abort total must exactly match workload metadata.
  The current Agent A counter is inclusive; its supported-counter flag also
  enforces `completed=N`. Whole-attempt district ID growth must equal generated
  NewOrders minus expected rollbacks.
  Agent A documents the actual counter in `scripts/tpcc_v3/ROLLBACK.md`.
* WAL comes from `pg_waldump --stats=record -s WINDOW_START_LSN -e WINDOW_END_LSN`.
  Record and FPI bytes are separate, divided by committed window transactions
  (business outcomes minus expected aborts). Combined bytes are their sum; segment
  headers/alignment mean they need not equal `pg_wal_lsn_diff`. Sampled LSNs mark
  insertion positions; pg_waldump aligns to complete records at those boundaries.
  `walstats.txt` preserves the raw output. Archiving into the attempt keeps older
  segments available if WAL is recycled; `wal_view` links archived/live segments.
  Archive copy I/O is enabled identically in all modes and is part of measured cost.
* There is no explicit pre-measurement CHECKPOINT, PostgreSQL restart, or sync.
  Checkpoints run on time inside the workload. UTC checkpoint start/completion
  log lines inside the sampled window are saved in acceptance JSON. At least one
  time-driven checkpoint start is required; a requested or WAL-driven start in
  the window rejects acceptance. Raise max_wal_size if WAL-driven starts occur.
* Serialization failures (`retryable_sqlstate_40001`), executor retry attempts
  and phase-trace restarts are whole-attempt diagnostics, not window rates.
  Missing diagnostics are null, never fabricated zero. Trace restarts and SQL
  retry counts are different counters. PG exponential backoff/jitter is unchanged.

## Reporting and acceptance

`summary.py RUNROOT` writes every attempt to `results.csv` and `summary.md`, with
median/min/max per (mode, W, workers, N). All noisy/rejected attempts with computable
windows contribute to those descriptive statistics. Missing windows, incomplete
trial counts and acceptance failures prevent campaign acceptance. Load above the
host's logical CPU count or another process consuming >=100% of one core flags a
noisy attempt; flags do not silently drop it. Inspect saved affinity and top CPU
records to distinguish shared-host noise from stack activity.

Every attempt gets `acceptance.json`: gateway rc zero, all N outcomes accounted,
exact expected aborts, explicit zero permanent failures/divergence, final progress,
consistency SQL rc zero and `consistency_ok=t` plus conditions 1–4 true, all nine
Merkle verifications true in Merkle mode, settings and relation/index evidence,
all-table final state hash, completed lifecycle, >=300 s measured window, a
time-driven checkpoint, valid sampler/affinity evidence and valid WAL statistics.
The campaign additionally requires every requested attempt and identical initial
state, workload SHA256 and final all-column state hash for matched det/Merkle runs.
`state_parity.json` and `campaign_acceptance.json` expose those gates. The all-column
commutative SQL state checksum includes timestamps and all nine tables; it is a
diagnostic checksum rather than a cryptographic proof of equality.

Merkle mode includes **nine additional expression lookup b-tree indexes**, one
per TPC-C table, as well as Merkle indexes. Their storage, update work, WAL and cache
footprint are part of the measured Merkle cost. `indexes.csv` and
`merkle_options.csv` disclose the actual definitions/options; acceptance checks the
nine lookup indexes. They are intentionally retained.

`SMOKE=1` permits reduced buffers/warmup/window/checkpoint period and submission
window. It relaxes only the 300 s and checkpoint requirements for the shell exit
status; JSON keeps the strict gate and marks smoke. Smoke is never publishable.
Autovacuum remains on. No automatic fallback disables it: runtime failures retain
logs for an evidence-backed integrator decision.

`recommend_n.py --rates pg=TPS det=TPS merkle=TPS` recommends a common N using the
fastest measured estimate, warmup + 300 s + a default 60 s drain allowance, 25%
margin and a 65536-transaction submission-lead allowance. This is an estimate,
not a waiver of observed-window acceptance. Use matched W/workers pilot rates.
Source HEAD, working diff hash, contract file hashes and binary SHA256s are saved.
Runtime file hashes need an integrator build log linking the binaries to that
source. Never infer successful execution or performance from these static files.
