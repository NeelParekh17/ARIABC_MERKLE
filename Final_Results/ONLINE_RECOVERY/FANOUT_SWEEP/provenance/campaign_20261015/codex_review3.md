All three review-2 findings are resolved:

- **COPY cleanup:** [replica_repair.cxx:289](/work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx:289) stops draining on active COPY status; failed `PQputCopyEnd` returns without collecting results, and setup-failure cleanup drains only after success. **Source discrepancy:** `-2` still reaches the guarded drain rather than returning immediately, but the reported infinite loop is eliminated.
- **Percentage denominator:** [analyze.py:293](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:293) skips and annotates overflow beyond its 0.1% tolerance; otherwise it divides by the selected run’s total.
- **Missing totals:** [analyze.py:283](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:283) now marks `n/a` when runs exist without an available total.

**New NIT — [analyze.py:304](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:304): legend disappears if the first representative run overflows.** Labels remain assigned to that skipped fanout, so subsequent valid bars receive none. Assign labels to the first bar actually drawn, or use explicit legend handles.

No new **BLOCKER/SHOULD-FIX** found. Read-only inspection and `git diff --check` passed; no builds or runtime tests were rerun.

Building is reasonable, and the remaining legend nit does not block the 60-run sweep. Before launching unattended, qualify online repair at 512/1024 remotely with final Merkle checks, because the supplied smoke plan covers only 16/256 and engine-only checks do not exercise that integration.

