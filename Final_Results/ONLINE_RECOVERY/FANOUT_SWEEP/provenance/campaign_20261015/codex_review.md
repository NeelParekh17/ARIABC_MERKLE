**Build on .247: reasonable. Launch the full 60-run sweep: hold until the DELETE plan is qualified and the COPY failure path is fixed.** I found no fanout-specific correctness blocker in the inspected paths.

1. **SHOULD-FIX — [replica_repair.cxx:568](/work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx:568): disabling hash joins also penalizes the anti-join.**  
   The DELETE includes `NOT EXISTS` against `ariabc_repair_rows_*`. That temp table is created with `LIKE`, without indexes. Disabling both hash and merge joins can therefore replace the heap walk with quadratic nested-loop comparisons against the temp table. The supplied sample transfers **31,372 candidates**, so this is a material risk, especially where smaller fanouts produce larger candidate sets.  
   **Fix:** index the temp table’s primary-key columns after COPY and analyze it, or constrain only the range-selection join. Confirm the resulting DELETE plan and timing remotely before the sweep. This performance concern is source-derived; I did not execute EXPLAIN.

2. **SHOULD-FIX — [replica_repair.cxx:318](/work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx:318): COPY failure can prevent cleanup indefinitely.**  
   If `PQputCopyData(local, …)` fails mid-transfer, the loop exits while the healthy reference connection remains in COPY OUT. The following `while (PQgetResult(ref))` cannot drain that stream: libpq continues returning `PGRES_COPY_OUT` until COPY data is consumed. Recovery can spin while retaining the reference snapshot and never reach rollback. **This is pre-existing, not introduced by the planner settings.**  
   **Fix:** drain or cancel reference COPY before collecting final results, abort local COPY, and ensure both connections reach rollback/close on failure.

3. **SHOULD-FIX — [analyze.py:93](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:93), [analyze.py:268](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:268): missing measurements become zero-duration phases.**  
   Missing fields inside matched records default to zero; completely missing records also become zero through `values(...) or [0]`. Consequently, an absent held CUT or timing field can appear as a measured zero, with its duration attributed to “Other.”  
   **Fix:** retain unavailable values, omit or mark incomplete bars, and report the number of measured repetitions for each phase.

4. **SHOULD-FIX — [analyze.py:275](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:275): stacked medians need an explicit overflow policy.**  
   Independent phase medians can sum above the median total. Clipping “Other” to zero then produces a stack taller than the total marker; the percentage chart divides by the stack sum while claiming “% of median total.” For example, phase pairs `(100,0), (0,100), (100,100)` have median total 100 but phase medians summing to 200.  
   **Fix:** flag this condition and label the percentage denominator accurately, or use an additive aggregation/representative run for stacked breakdowns. The supplied accepted historical groups did not exhibit total-stack overflow.

5. **NIT — [analyze.py:214](/work/ARIABC/AriaBC/scripts/distributed/recovery_fanout/analyze.py:214): verification legend promises a heap audit.**  
   “Verify (roots + merkle_verify)” is inaccurate for ordinary recovery with the new default.  
   **Fix:** use “Verify (partition roots; optional heap audit).”

Verified and found correct:

- **Fanout paths:** widened child values in bulk build, incremental splitting, traversal and descendant enumeration; dynamic bucket allocation and loop bounds; merge ranges; parent clearing; and the SQL `bit(64)` parent calculation. Terminal prefixes are **63 bits for fanout 512** and **60 bits for 1024**, within the 8-byte node ID.
- **Routing and metadata:** inspected key-hash, partition, upper-bound, lookup predicates and cache representations without finding a remaining 256-child limitation.
- **Planner-setting scope:** both connections are inside transactions; successful COPY finishes and collects its results before restoration; later statements receive default join planning. Ordinary SQL failures exit repair and close the connections.
- **Recovery default:** the gateway omits `heap_verify`, so it adopts zero. The standalone repair tool retains its explicit default of one. Final harness heap verification remains required.
- **Sample parsing:** correctly selects reference node 2’s held CUT at `L=290`: prepare **0.316 ms**, export **3189.227 ms**, digest **11.515 ms**. Recovery phases, **336 replay entries**, backlog `626−291+1=336`, and unit conversions agree with the logs.
- **Harness:** tag/run IDs agree; the allowlist includes 512/1024; matrix generation covers **60 unique targets**; rotation, ten-position plot axes and depth normalization are sound. Allowlist normalization matches exactly one line.
- **Finish fallback:** venv and `pip --target`/`PYTHONPATH` branches are structurally consistent; the supplied finish copy matches the repository version.

No files were edited, dependencies installed, or builds/databases/benchmarks started. `analyze.py --help` succeeded. After addressing the findings, qualify **512 and 1024 remotely with repair and final Merkle checks** before committing to all 60 runs; the existing smoke plan only exercises 16 and 256.

