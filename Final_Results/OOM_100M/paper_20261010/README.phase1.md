# OOM 100M remeasurement — phase 1 launched

Latest controller CSV snapshot at 2026-10-10T21:11:23.234703+00:00 ([live_status.csv](live_status.csv)): 6/252 full cases accepted, 0 failed attempts, 0 noise exclusions, and 2 full-run Merkle PASS results. The independent raw audit covers the first three full cases; the additional accepted rows are preserved in [live_summary.csv](live_summary.csv) and all raw case directories remain on .111.

The full campaign is running detached on .111 in tmux `oom_paper_20261010`, using frozen engine commit `16577072ecf888fbbbb5d80d0e90fc340c8ca342` and `--auto-resume`. This is the launch qualification snapshot at 2026-10-10T21:03:11.627693+00:00.

Smoke: **3/3 accepted**. Full campaign: **3/252 accepted in this audited snapshot**, 0 failed attempts, 0 noise exclusions. Each dataset validated 60,000 terminal results; the full block and smoke each contain one post-workload Merkle PASS. Counts and row-result totals are in [counts.csv](counts.csv), and every observation below comes from [smoke_summary.csv](smoke_summary.csv) or [campaign_summary_phase1.csv](campaign_summary_phase1.csv).

| Dataset | Workload | Workers | Mode | Trial | TPS | Merkle verify |
|---|---|---:|---|---:|---:|---|
| smoke | A θ0.99 | 1 | bcdb_det | 1 | 1953.507 | N/A |
| smoke | A θ0.99 | 1 | pg | 1 | 2092.050 | N/A |
| smoke | A θ0.99 | 1 | bcdb_merkle | 1 | 1757.469 | PASS |
| campaign | C θ0.99 | 16 | pg | 1 | 26041.667 | N/A |
| campaign | C θ0.99 | 16 | bcdb_merkle | 1 | 16625.104 | PASS |
| campaign | C θ0.99 | 16 | bcdb_det | 1 | 16077.170 | N/A |

These are qualification observations with one recorded trial per displayed configuration. The planned three trials per case are in [planned_case_order.csv](planned_case_order.csv). Median, best, minimum, n and new paper figures belong to phase 2 after the grid completes.

Fresh COPY/initdb and Merkle construction used the new binaries. Full keyspace scans, fillfactor 90, geometry 200/32/1024/256, compact node storage and baseline `merkle_verify` passed. Plain and Merkle variants share the same heap. Settings, sizes and the passed fresh-space gate are traceable to [preparation_metrics.csv](preparation_metrics.csv) and the original [baseline records](evidence/baseline_evidence/verification.json). Per-case cold start, delta restore, settle probes, SERIALIZABLE, PG jitter, 32MB shared buffers and synchronous Merkle were checked against saved settings and raw terminal logs.

The PG mode is this PostgreSQL fork with deterministic/Merkle execution disabled. Shared buffers do not bound other fork shared memory, OS page cache or storage-controller cache. Device I/O includes shared host traffic; TPS excludes restore, settle, checkpoint and verification. Short timed runs limit stall detection. The requested det counter traces are unavailable in this snapshot and remain null, never asserted zero.

The earlier storage block was resolved by user-authorized deletion performed by Claude. No older heap/index was reused. One preparation attempt was interrupted by a .247 reboot of unknown cause; its evidence is retained and only this campaign's incomplete data were regenerated. Another preparation failure was a wrong pg_prewarm hash path, corrected before generation. See [stage history](stage_history.csv) and [patch notes](../../../../agents/oom/PATCH_NOTES.md).

Raw full run: `.111:/home/neel/claude_checks/paper_20261010_oom/results/campaign/run_20261011_021231_3e7625c2`. Raw smoke: `.111:/home/neel/claude_checks/paper_20261010_oom/results/smoke/run_20261011_013308_3b13566e`. New .247 data/build/install: `/home/neel/ariabc_data/paper_20261010_oom/`. Curated files and remote raw hash manifests are in [evidence/](evidence/), with [SHA256SUMS](evidence/SHA256SUMS) verified after transfer. No pgdata or bulk server logs were fetched. The source snapshot remains in the agent evidence directory and on both lab hosts.

Use [MONITOR.md](../../../../agents/oom/MONITOR.md) for live status, the rough ETA and resume procedure. Historical v2 publication is preserved in [previous/](../previous/v2_20261001_publication/); its original raw runs remain at their existing paths.
