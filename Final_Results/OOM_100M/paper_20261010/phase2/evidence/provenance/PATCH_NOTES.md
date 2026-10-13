# OOM preparation harness changes

Engine, PostgreSQL and C++ source remain exactly 16577072. Changes are harness-only and are retained as patches. No commits.

- Dedicated home paths on .247/.111, ports 5468/8068, 3 trials (252 cases), full 20K-statement smoke per mode, and 252-case final summary.
- PG builder: isolated source/build/install paths, --enable-depend, hashes of every PG executable the runner uses and pg_prewarm.so/libpq.
- Generation guard: 200 GiB available instead of 240 GB. Computation is in SPACE_BUDGET.md; original per-case 20 GiB reserve remains unchanged.
- Baseline verifier: campaign-specific path assertions and interpolated DB port; full count/bounds, fillfactor, geometry and merkle_verify checks retained.
- Gateway workload copies scoped to own campaign checkout; tsc rechecked at preflight and every gateway launch.
- Add .111 host telemetry alongside .247 telemetry. Extract all observed dt_opf/dt_opf_stale_error/dt_rel_read_tags samples to det_counters.json and report max observed cumulative values, null when absent; never sum periodic cumulative counters or silently convert absence to zero.
- Fresh baseline size/space gate before smoke: two new working copies plus 20 GiB WAL and 8 GiB slack must fit measured free bytes.
- Predeclared severe-stall exclusions: >=10s gap between host telemetry samples, >=95% full IO PSI or >=50% full memory PSI over >=10s, or telemetry error. The same rule applies to every mode. Keep original result/logs plus all_attempts.csv, mark NOISE_EXCLUDED.txt, and make up to two EXTRA attempts without deleting originals. Severe noise after three attempts stops for investigation. Short runs/windows have limited detection; all host load/diskstats/process data remain for phase-2 analysis.

Preparation check found that generate_ycsb_workloads.py is ignored by .gitignore and absent from the 16577072 clone/archive, although the tracked OOM runner imports it. Restored the existing canonical harness file from /work/ARIABC/AriaBC/scripts/generate_ycsb_workloads.py; its SHA-256 e239de1ec2b5800017951a042bc8bec3ce871f65d29f039b65cc1a6874249ed3 also exactly matches .111 AriaBC_v2/scripts/generate_ycsb_workloads.py. This is a separately recorded harness overlay, not an engine change. The seven generated SQL hashes must still match the published inputs before any measurement.

PG compile/install completed, but the final SHA-256 stage failed because pg_prewarm.so installs under lib/postgresql, not lib. Corrected that hash path and preserved first-attempt logs/status. The immutable extracted source copy is verified on resume rather than generated build files. No data had been generated at this failure.

Both hosts additionally record timestamped iostat -xz at 5-second intervals, started outside the measured workloads, in controller_iostat.txt / host_iostat.txt. Their timeout/PID records permit stopping only these campaign-owned observers in phase 2. Per-case telemetry and baseline settle probes remain the exclusion evidence.

Before any smoke/full case started, include stderr in observed log-counter extraction and bind paper_noise.py into the shared source/resume hash contract, so the exclusion rule cannot change silently on resume. Phase tracing remains disabled as in v2; absent counters are explicitly null, not asserted zero.

The first fresh generation was interrupted by a .247 host reboot at 2026-10-11 00:04 IST (new boot ID 9a6f5f9b-1487-4cc1-9861-48d6954ff623, kernel changed 7.0.0-34-generic to 7.0.0-38-generic). Cause is not established; no contemporaneous kernel OOM record was found. Preserve generation log, PG init log/config/pid/control output and pre-reboot iostat. Because fsync/full_page_writes were off for bulk load and no clean baseline/archive existed, discard only this campaign-owned incomplete pgdata and regenerate from scratch. The already-built PG binaries still verify against all saved hashes. Add kernel/boot ID/clocksource to both per-case host telemetry streams, so future environment changes remain visible. No benchmark cases had run.

Before smoke/full measurements, persist noise_excluded in result.json as well as the CSV. Resume now requires clean noise_qualification.json and rejects NOISE_EXCLUDED markers; it no longer relies solely on absence from the accepted CSV. This closes a saved-row consistency/qualification gap found by inspecting the actual resume path. It does not change workload execution or exclusion thresholds.

Second fresh generation completed at 2026-10-10T19:24:54Z in 44.85 minutes (COPY 859.9s). Stopped Merkle baseline apparent size 35,866,524,242 bytes; golden archive 5,359,018,724 bytes, SHA-256 a31d740e984cad9af29f32483e249921340c1d8a5967cbb2513edb5f5e1c7142. The preparation observer's partial coverage records 452 samples, minimum MemAvailable 2,480,160 KiB, minimum SwapFree 0, maximum single PostgreSQL backend RssAnon 7,845,724 KiB. Build completed; this is not evidence of an OOM-killer event. No timed measurement has started at this note.

Before smoke/full cases, replace the remaining hardcoded Raft endpoint/preflight port 9000 with --raft-port 9068. DB 5468, client 8068, and Raft 9068 are distinct; 9068 was checked unused. Raft is bypassed exactly as in v2, so this is port isolation rather than an execution-method change. This paper clone's default is also 9068, so the already-running controller (whose argument array was sourced earlier) uses the dedicated port on subsequent stages; saved benchmark manifests record the effective value.
