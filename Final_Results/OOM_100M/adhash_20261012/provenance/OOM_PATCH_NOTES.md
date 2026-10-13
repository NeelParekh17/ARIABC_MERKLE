# F15 OOM harness adaptations

The accepted engine commit remains 6a1c66d6; no engine changes or rebuilds were
made for Step 2. The source harness was copied from the exact paper OOM clone.
`oom_harness.patch` records adaptations to that copy only:

- Explicit approved physical baseline and working paths, with all logs,
  manifests, probe writes, locks, binaries and gateway workload copies in F15
  directories. The baseline stays stopped; PostgreSQL starts only on approved
  working paths during the sweep.
- Cached verified AdHash manifest required; missing/stale baseline metadata
  fails rather than creating a 34GB verification copy. Plain baseline is
  reused and identity-checked read only, not derived or rebuilt. Generation
  is disabled in this campaign.
- Port 5478 / client 8078 / raft 9078; own accepted PG/server/gateway binaries.
- Existing copies required; preserve paper delta rsync, metadata checks,
  full first/10th content check, idle calibration, settle probe, cold drop,
  terminal parser, post-run Merkle verification, telemetry and noise rules.
- Maintenance memory capped at 256MB instead of the paper's declared 2GB,
  to satisfy the binding memory limit. Autovacuum is off and no maintenance
  build occurs in the timed workload. Shared buffers remain 32MB; work_mem
  remains 4MB; timed SQL and all gateway settings are unchanged.
- Before every PG start record free -g and enforce 5 GiB free. REINDEX alone
  had the approved transient 3 GiB limit, which was never approached.
- 84 Merkle trials + 28 det controls. The paper's seeded shuffle and mode
  rotation are retained; one det control per point is distributed over the
  three trial blocks, with its reported trial numbered 1. The 112-case order
  is saved and independently checked; all seven workloads match paper hashes.
- TMPDIR and tmux socket are under F15 home scratch directories.

Preparation corrections before any measured case: created the nested server
binary destination after the initial rsync found it absent; copied the
required benchmark_telemetry.py dependency before the first gateway measurement.
The preflight/smoke source manifests initially omitted that file; their hash
snapshots were corrected before any gateway.log existed, retaining the original
JSON snapshots. All other source hashes were unchanged. The correction record
is results/provenance/telemetry_dependency_snapshot_fix.json on .111.
The optional restore_usertable_small.sql file is absent in the paper clone
and unused by this OOM physical-restore harness; it was not fabricated.

Baseline timing note: `verification.json`'s raw field named reindex_seconds
includes verification too (365.25s). REINDEX alone is correctly recorded in
`reindex_elapsed.json` as 115.04s. The profiler reports 111.32s for the engine
build itself. These are setup durations, not TPS intervals.
