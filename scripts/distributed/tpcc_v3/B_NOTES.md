# Agent B integration notes

Implemented against the complete shared tpcc_v3_spec.md and sandbox override.
No SSH, build, database/server launch, or performance run was attempted locally.
No commits. Legacy v2 files and Final_Results were not edited.

The C++ diff touches only gateway progress configuration and emission: positive
`--progressIntervalMs`, default 5000, and `wall_time_unix_ms` on progress lines.
Agent A can independently add user_aborts near the progress/final counters;
retain the timestamp and configured timer when combining edits.

Agent A SQL contracts were inspected: consistency_check.sql emits named
condition_1..4 rows with `true` values and a final `consistency_ok=t`, and
state_hash.sql emits all nine tables as `table=count:hashsum`. The harness checks
those markers plus SQL exit codes. Workload metadata has top-level
`expected_rollbacks`. Completion counters may include or exclude user aborts;
the summary records which convention matches N and applies it to window math.
Missing abort counters fail acceptance, while parsing remains tolerant.
Agent A's final ROLLBACK.md confirms inclusive completed=N. Supported-counter
flags require that convention and zero polling failures. The whole-attempt
district ID delta is also checked against generated NewOrders minus rollbacks.

Server profile diagnostics use existing `retryable_sqlstate_40001`,
`retry_attempts_total`, `retry_exhausted_total`. They are whole-attempt counters.
PG/det restart diagnostics also retain BCDB_PHASE_TRACE records. No executor,
server or PostgreSQL C edits were made by B.

Autovacuum stays enabled, and all failure artifacts are retained. Runtime
compatibility, deterministic expected aborts, Merkle behavior with autovacuum,
pg_waldump against real LSN ranges and source/binary build parity await integrator
validation. See VALIDATION_B.md for exact ranking commands and required evidence.
