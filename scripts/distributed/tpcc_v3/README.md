# TPC-C-derived, unaudited v3 harness

The 2026-10-12 deck paper campaign uses direct gateway completion metrics and fixed
measurement durations. See [PAPER_CAMPAIGN.md](PAPER_CAMPAIGN.md) for the binding
methodology, predeclared gates, exact matrix, per-mode pilot sizing, and restart
behavior. Engine/server/gateway remain at commit16577072; changes are confined
to harness/analysis. Execute only on ranking as protectdr in the isolated root.

Primary metric: **TPC-C-derived, unaudited tpmC-equivalent**, directly counted
completed New-Orders including expected rollbacks, per minute, truncated
(TPC-C v5.11 Cl.5.4.2-5.4.4). Also report directly counted business tx/s,
committed-only NOPM, realized mix and per-type response percentiles (Cl.5.2.5.7).
Delivery is synchronous; no keying/think time is used. This is not an audited
TPC-C result and does not establish all standard requirements.

Sweeps have three trials per point/mode, median primary and min-max, each with a
600s fixed interval after at least300s ramp. Headline W100/32 has one accepted
7200s interval per mode after ramp (Cl.5.5.2.1); a separate reproducibility
measurement is not required (Cl.5.5.1.4). All runs require whole-window CV<=0.40
and absolute normalized fitted change<=0.20, plus a95% Newey-West slope
interval containing zero (no significant linear trend). Checkpoint timeout5min satisfies
the interval bound in Cl.5.5.2.2. Retain and flag failed/noisy attempts; headline
failures automatically rerun. Never select the fastest or shorten a window.

`standard_campaign.py prepare` performs matched pilots and seven fixed-N
correctness pairs. `standard_campaign.py campaign` consumes its ETA-qualified
plan:108 sweep runs plus3 accepted headlines. The detached campaign supervisor
and exact monitoring/resume commands live in agents/tpcc/MONITOR.md.
`run.sh` refuses the superseded common-N full settings. `paper_campaign.py` and
legacy selection remain for archived smoke/parity checks only. v2 is untouched.

Historical integrator instructions in [VALIDATION_B.md](VALIDATION_B.md) and
[B_NOTES.md](B_NOTES.md) document initial v3 development; their older metrics,
paths, trial counts and windows do not govern this paper campaign.


Mix regulation follows Cl.5.2.4.2: a shared96-set,2208-card seeded deck, freshly
shuffled after exhaustion (10NO/10P/1OS/1D/1SL per set). The96 sets correspond to
the gateway's96 lanes, independently of database workers. Nominal mix changes
from45/43/4/4/4 to43.48/43.48/4.35/4.35/4.35. All actual completion-window
Cl.5.2.3 mix minimums are HARD acceptance gates. Earlier20261011 campaign results
are rejected evidence. See agents/tpcc/COMPLIANCE.md for checked clauses and
remaining research departures; no full TPC-C compliance is claimed.


Long intervals use incremental native pg_waldump statistics before WAL recycling;
record-aligned disjoint chunks and their raw outputs are hard-verified. Normal
max_wal_size64GB replaces oversized retention; no engine change. The primary
direct completion window is unchanged. See methodology for exact boundaries,
collector/native equality validation and uniform analysis overhead.
