Task O: launched unattended on ranking; matrix completion is pending.

Output root: /home/protectdr/claude_checks/tpcc_sweep_v2_20261002_130000
Session: tpccsweep (tmux + nohup), started 2026-10-02 12:57:47 +05:30.
Confirmed first matrix run: pg, W=5, workers=32, tx=20000; config.txt,
host_before.txt and owned postmaster.pid present. No local build/DB/test/benchmark.
At 12:59:09 +05:30 the first pg matrix run passed 20000/20000, zero
divergence/failures (764.37 TPS); det W=5/32 then started.
Expected duration: approximately 2–4 hours; shared-host stalls and conditional
reruns can extend this estimate.

Changes and findings:
- scripts/distributed/tpcc_v2/sweep_v2.sh:9: exclusive existing tpcc_v2 lock;
  :23: fixed read-only install_v2/src_v2/jitter-binary paths and FF90/1024/256;
  :26: binary/input verification, port/free-space checks, remote syntax checks.
- scripts/distributed/tpcc_v2/sweep_run.sh:26: campaign-owned run directory;
  :51: continuation's pg_ctl stop -t 600 preserved;
  :114: FF90 on all nine tables (continuation excluded item);
  :156: pg dbType=0/safedb=0/pool=workers, plus bcdbInitBlockSize=workers;
  BCDB workers=1 and ARIABC_PG_MAX_RETRIES=100 preserved.
- scripts/distributed/tpcc_v2/sweep_summary.py:22: exact interleaved points;
  :53: strict settings/completion/Merkle acceptance and det/Merkle published
  state comparison at the same W; :142: published-shape CSVs plus metrics;
  :174: isolated failures retained and accepted stopped PGDATA cleanup;
  :217: neighbour threshold; :261: matrix/failure/stall retry lifecycle.
- scripts/distributed/tpcc_v2/SWEEP_V2.md:1: configuration, policies and commands.
- The requested sets contain 36 unique runs, not 38: (7+6-1)*3.
  W=100/32 is executed once and included in both sweep summaries.
- All six binaries match the headline provenance. Jitter server SHA-256:
  cc02e6df017f5d1dbbb181ac39ecf2b8a29b971a4c53c72d17cdf52695ca2a17.
  No rebuild or edits to the protected remote trees.

Validation on ranking:
- bash -n sweep_v2.sh sweep_run.sh; py_compile sweep_summary.py summary.py PASS.
- Remote logic checks PASS: point coverage/uniqueness, endpoint and interior
  stall detection, strict less-than 0.65 boundary, shared-point retry dedup.
- pg smoke W=5/32 FF90: 2000/2000, divergence=0, permanent failures=0,
  no workload FATAL/PANIC, TPS=1708.21, WAL/tx=44123.92, serialization failures=5020.
- Merkle smoke W=5/32 FF90: 2000/2000, divergence=0, permanent failures=0,
  merkle_verify=9:true, TPS=591.83, WAL/tx=78552.188, final traced restarts=1523.
- Both smoke runs validated SERIALIZABLE, durability, synchronous maintenance,
  nine logged FF90 heaps, positive WAL evidence and final HOT statistics.
- git diff --check (task folder) PASS; graphify update . completed after edits.

Remote artifacts: scripts/, provenance/{binaries.sha256,inputs.sha256,
continuation_original.sh,continuation_changes.diff,campaign_scripts.sha256,
smoke_scripts.sha256,validation.txt}, smoke.ok, smoke.log, campaign.log,
status.txt, status_history.txt, commands.log, workload and per-run evidence.
Accepted attempts write root all_runs.csv/summary.csv and each sweep's
warehouses_w32/{all_runs,summary}.csv / workers_w100/{all_runs,summary}.csv.
Each accepted.json retains all table HOT fractions. Failures are separately
retained in failures.jsonl and run logs. Initial and rerun accepted values stay
in CSVs; summary selects best and records rerun. Failure retry consumes the
same one-rerun budget as stall retry. Hard failures stop immediately.
Local launch commands: .bench_tmp/tpcc_sweep_launch/commands.log.
Local smoke and launch snapshots: .bench_tmp/tpcc_sweep_launch/{smoke_validation.txt,status_history_at_launch.txt}.

Exact monitor command (no further launch needed):
ssh -o BatchMode=yes protectdr@10.129.7.57 'ROOT=$HOME/claude_checks/tpcc_sweep_v2_20261002_130000; cat "$ROOT/status.txt"; tail -20 "$ROOT/campaign.log"'

Exact live log command:
ssh -t protectdr@10.129.7.57 'tail -f $HOME/claude_checks/tpcc_sweep_v2_20261002_130000/campaign.log'

Exact summary regeneration command, if needed after termination:
ssh -o BatchMode=yes protectdr@10.129.7.57 'ROOT=$HOME/claude_checks/tpcc_sweep_v2_20261002_130000; python3 "$ROOT/scripts/sweep_summary.py" summarize "$ROOT"'

Risks/limits: one trial with conditional reruns retains host-noise sensitivity.
Smoke TPS is validation only. Matrix det/Merkle state comparison remains pending;
its published eight-table row-count/hash-sum projection excludes timestamps and
is weaker than a complete cryptographic contents hash. Graph publication should
wait for COMPLETE and use accepted CSVs. This task intentionally exits after
launch and does not claim completed sweeps or updated final graphs.
