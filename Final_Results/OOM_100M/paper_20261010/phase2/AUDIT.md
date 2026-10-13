# Independent read-only audit

Audit time: 2026-10-11T19:21:03.956014+00:00. All campaign/smoke raw gateway and server logs were read remotely through stdin-only Python. Helpers set `PYTHONDONTWRITEBYTECODE=1`; all output was redirected locally. No remote files, DB state, observer or benchmark process was created or modified.

| Dataset | Accepted | Rejected | Excluded | Terminal results | Merkle PASS | Content checks |
| --- | --- | --- | --- | --- | --- | --- |
| smoke | 3 | 0 | 0 | 60000 | 1 | 2 |
| campaign | 252 | 0 | 0 | 5040000 | 84 | 27 |

Acceptance checks: exact planned order and unique full grid; terminal protocol version and all-request successes; IDs 1–20000 exactly once with valid SQL row counts; gateway exit and nonterminal/error/divergence counters; retry totals; Merkle `t` and geometry; SERIALIZABLE/durability/32MB settings; actual PG/DET command and worker configuration; frozen build/hash manifests and actual gateway workload copies. Summary TPS was recalculated from raw elapsed milliseconds and every saved row compared to JSON. Every Merkle case had post-run `t`.

No accepted case should have been rejected by these checks. There were no empty SQL results, exhausted retries, failure markers, noise exclusions or extra completed attempts. A second parser independent of benchmark validators checked complete request IDs, INSERT/SELECT/UPDATE terminal tags, the canonical SQL operation mix (F's CTE ends in UPDATE), and every reported gateway error counter. All 255 cases pass; independent_terminal_check.json preserves per-case command counts. Detailed det counters were re-extracted from server/stdout, server/stderr and PostgreSQL logs; none of the three labels was emitted. Null is preserved in CSV and JSON.

Noise was independently recomputed from every telemetry record: JSON errors, gaps, and rolling PSI windows. Both hosts kept stable boot IDs and the gateway clocksource stayed tsc. Every settle history's last three entries was checked against the actual rounded thresholds and idle-write bound. Readiness and administrative fatal messages were classified rather than falsely reporting error-free logs. All PG serialization errors match successful retries. Category counts:

| Dataset | Message | Count |
| --- | --- | --- |
| smoke | FATAL: role "neel" does not exist | 9 |
| smoke | FATAL: terminating connection due to administrator command | 2 |
| campaign | FATAL: role "neel" does not exist | 756 |
| campaign | FATAL: terminating connection due to administrator command | 1218 |
| campaign | ERROR: could not serialize access due to concurrent update | 39279 |
| campaign | ERROR: could not serialize access due to read/write dependencies among transactions | 382 |

Limits: row-count/terminal and Merkle-consistency checks are not an independent serial oracle or cross-mode final-state equality proof. Most timed cases are too short for a ten-second pressure test. Host/device background traffic is retained; no dedicated-host claim. Five-repeat qualification is not met by this intentionally three-trial campaign. Failure-free completion and low CV do not supply confidence intervals or general correctness guarantees.

The source archive matches frozen git file-by-file, and engine manifest entries match its archived contents. Read-only current executable/dependency checks match launch provenance. Every fetched case file was checked against remotely computed SHA-256. Bulk server traces and pgdata remain remote. `REMOTE_RAW_SHA256SUMS` is a remote-path manifest; `SHA256SUMS` checks local curated publication files.

Reproduce publication with `analyze.py` and `publish_readme.py` locally. Do not invoke the remote audit helpers as local scripts: they expect .111 paths and must be streamed to a read-only Python interpreter only if re-fetching is authorized. `verify_source_and_fetch.py` is the one-time agent-directory source-archive verification script and expects the agent's full source tarball, which is omitted from the curated publication.
