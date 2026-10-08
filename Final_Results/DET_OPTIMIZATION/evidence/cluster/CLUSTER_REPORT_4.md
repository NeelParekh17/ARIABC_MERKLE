# Gateway audit-consumer fix: canonical recovery C validation

Date: 2026-10-08. Local main: `025c9be59acdc2566147e2fa009a636e6c6bf33a`.

**The gateway idle-wait fix is validated in both C trials.** All four measured runs exit 0, finalize their all-three audits, report terminal divergence=0 and permanent_failures=0, and pass Phase 8 on admin123, user4 and utkarsh. Both C trials record `audit_idle_waits=1` across a 54–55-second repair; neither reports the consumer-exited error or triggers the watchdog.

**Recovery-aware accounting is explicit:** C1 has 159,993 directly verified + 7 recovery-attributed = 160,000/160,000; C2 has 159,996 + 4 = 160,000/160,000. Neither has a literal `async_all3_verified_count=160000`. The unchanged `drain_async_all3_audit` includes recovery-attributed entries in its processed total (`ariabc_pg/src/ariabc_pg_gateway.cxx`, lines 6373–6378). Both runs have zero audit failures, timeouts, missing entries and capacity exhaustion, with `all3_audit_valid=yes` and `recovery_final_check=PASS`. The attributed counts are not missing results or timeouts.

## Per-run measurements

Empty buckets are the canonical TPS_TIMELINE steady 100 ms buckets. TPS is the run-summary majority-visible rate; the timeline rate and the all-three drained rate are different measurements.

| Order | Case | W | TPS_majority_visible | Empty buckets | Max completion gap ms | Gateway / runner exit | Divergence | Permanent failures |
|---:|---|---:|---:|---:|---:|---|---:|---:|
| 1 | C 1 | 8 | 7802.21 | 0 | 19.54 | 0 / 0 | 0 | 0 |
| 2 | C 2 | 8 | 4893.27 | 11 | 579.42 | 0 / 0 | 0 | 0 |
| 3 | A reference | 8 | 8774.82 | 0 | 25.98 | 0 / 0 | 0 | 0 |
| 4 | Normal YCSB-A theta 0 | 16 | 13679.89 | 0 | 15.11 | 0 / 0 | 0 | 0 |

| Order | Directly verified | Recovery-attributed | Accounted / clients | Failure / timeout / missing | Finalized / valid | audit_idle_waits | audit_drain_ms |
|---:|---:|---:|---|---|---|---:|---:|
| 1 | 159993 | 7 | 160000 / 160000 | 0 / 0 / 0 | Yes / yes | 1 | 53122.8 |
| 2 | 159996 | 4 | 160000 / 160000 | 0 / 0 / 0 | Yes / yes | 1 | 40180.2 |
| 3 | 160000 | 0 | 160000 / 160000 | 0 / 0 / 0 | Yes / yes | 0 | 502.891 |
| 4 | 20000 | 0 | 20000 / 20000 | 0 / 0 / 0 | Yes / yes | 0 | 40.2527 |

`audit_idle_waits` is emitted in the terminal `majority_async_all3` audit line. This commit does not repeat it in `PROFILE_GATEWAY`; that line contains `audit_drain_ms` and the audit counters. Exact terminal lines and timelines follow.

```text
# Run 1
majority_async_all3 client_quorum_complete_count=160000 async_all3_verified_count=159993 async_all3_failure_count=0 async_all3_timeout_count=0 async_all3_missing_count=0 async_all3_capacity_exhausted_count=0 audit_drain_ms=53122.8 audit_idle_waits=1
[13:31:52]   TPS_TIMELINE completions=160000 duration_ms=20475 bucket_ms=100 mean_tps=7814.4 steady_min_tps=3800.0 steady_max_tps=10000.0 empty_buckets=0 max_completion_gap_ms=19.54 max_gap_at_ms=10789
# Run 2
majority_async_all3 client_quorum_complete_count=160000 async_all3_verified_count=159996 async_all3_failure_count=0 async_all3_timeout_count=0 async_all3_missing_count=0 async_all3_capacity_exhausted_count=0 audit_drain_ms=40180.2 audit_idle_waits=1
[13:33:53]   TPS_TIMELINE completions=160000 duration_ms=32667 bucket_ms=100 mean_tps=4898.0 steady_min_tps=0.0 steady_max_tps=9780.0 empty_buckets=11 max_completion_gap_ms=579.42 max_gap_at_ms=17735
# Run 3
majority_async_all3 client_quorum_complete_count=160000 async_all3_verified_count=160000 async_all3_failure_count=0 async_all3_timeout_count=0 async_all3_missing_count=0 async_all3_capacity_exhausted_count=0 audit_drain_ms=502.891 audit_idle_waits=0
[13:35:01]   TPS_TIMELINE completions=160000 duration_ms=18203 bucket_ms=100 mean_tps=8789.8 steady_min_tps=6510.0 steady_max_tps=10080.0 empty_buckets=0 max_completion_gap_ms=25.98 max_gap_at_ms=17414
# Run 4
majority_async_all3 client_quorum_complete_count=20000 async_all3_verified_count=20000 async_all3_failure_count=0 async_all3_timeout_count=0 async_all3_missing_count=0 async_all3_capacity_exhausted_count=0 audit_drain_ms=40.2527 audit_idle_waits=0
[13:37:30]   TPS_TIMELINE completions=20000 duration_ms=1417 bucket_ms=100 mean_tps=14118.3 steady_min_tps=12080.0 steady_max_tps=15320.0 empty_buckets=0 max_completion_gap_ms=15.11 max_gap_at_ms=1031
```

Published C reference: 8,693 TPS, 0 empty buckets. C1 is -10.25% below it; C2 is -43.71% below it and has 11 empty buckets. Recovery remains slow. The two trials show substantial client-throughput variation; this validation does not identify its cause or claim a recovery throughput improvement.

A reference is 0.55% below report 3's 8,823.69 TPS, and 0.85% below the published A's 8,850.05 TPS. Normal W=16 is 0.34% above report 1's same-day W=16 median of 13,633.27 TPS, and 2.94% below the historical 14,094.43 TPS reference. Its audit and replica checks pass, with no observed normal-run regression relative to the same-day reference. One trial is not a statistical proof that performance is unchanged.

## Recovery events and Phase 8

C1/C2 recovery aggregate triggered/succeeded/failed counters are 1/1/0. A and normal counters are 0/0/0 and produce no RECOVERY_EVENT. Both C repairs use donor ref=2/user4, damaged node=4/utkarsh, live=1 and full_copies=0.

| C trial | cut_ms | repair_ms | catchup_ms | total_ms | Raw divergence | Attributed audit entries |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 87 | 55182 | 11431 | 66904 | 2 | 7 |
| 2 | 72 | 54332 | 11435 | 66045 | 1 | 4 |

Complete original gateway events (no wrapper duplicates):

```text
# Run 1
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=171 B=43519 detect_to_quarantine_ms=0 cut_ms=87 recover_call_ms=66798 repair_ms=55182 drain_ms=149 catchup_ms=11431 live=1 mismatched_partitions=77 differing_leaves=84 rows_deleted=0 rows_upserted=90 full_copies=0 replay_from=172 replay_target=626 total_ms=66904 digest=usertable_small:e7c8cb886dd411ced79dd4be0126c059ca61bf480b1351594bb2c78564aa0faa
# Run 2
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=174 B=44031 detect_to_quarantine_ms=0 cut_ms=72 recover_call_ms=65949 repair_ms=54332 drain_ms=148 catchup_ms=11435 live=1 mismatched_partitions=71 differing_leaves=80 rows_deleted=0 rows_upserted=88 full_copies=0 replay_from=175 replay_target=627 total_ms=66045 digest=usertable_small:58840ff1cf14bae509e08db063cfe20c3e1f901befe0107f55c7739da7693462
# Run 3
RECOVERY_EVENT: NONE (recovery-mode off)
# Run 4
RECOVERY_EVENT: NONE (recovery-mode off)
```

Phase 8 independently observes the replicated post-workload marker. Every row below has 12,001 rows and native `merkle_verify=t`; the canonical consistency check PASSes for every run.

| Order | Node | Phase 8 root | Data MD5 |
|---:|---|---|---|
| 1 | admin123 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 1 | user4 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 1 | utkarsh | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 2 | admin123 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 2 | user4 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 2 | utkarsh | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 3 | admin123 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 3 | user4 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 3 | utkarsh | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | `9a15b0794b52e087376bc7190d48cc58` |
| 4 | admin123 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | `73f2cd865c4eb8fefd07697ccfd21160` |
| 4 | user4 | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | `73f2cd865c4eb8fefd07697ccfd21160` |
| 4 | utkarsh | `d1b0ffe0b7322cdc2e920e7e32a0712c4c66e399e5b34c733442c2619af7a157` | `73f2cd865c4eb8fefd07697ccfd21160` |

## Build and source proof

Before sync, the gateway source fingerprint matched report 3 exactly, `a1ea2435504a5eda1dab187c1bc5bafc56f0fcf7f572f282d0760f611286f7d3`. HEAD changes only `ariabc_pg/src/ariabc_pg_gateway.cxx`; that exact file was backed up remotely and synced from current clean local main. The resulting gateway fingerprint matched local HEAD before the first run. The first canonical invocation then forced a full build and synced source/binaries to the replicas, including user4's own Ubuntu 22.04 build. All later runs used `--skip-sync --skip-build`.

| Evidence | Value |
|---|---|
| Local HEAD | `025c9be59acdc2566147e2fa009a636e6c6bf33a` |
| Current portable source fingerprint | `d6ac759c79abb43734633c2eedd35f5278408d629fd53938d267ba9f0e506a81` |
| Report 3 gateway SHA256 | `3a135a707c4ef5c76f079f9ead2106847c5b4745cc15e5246cdb91bde1f078f0` |
| New .111 gateway SHA256 | `44defaf401283f1e63158ec40ccb6b83ee37875f697ead675e99932dec866074` |
| strings proof | `async_all3_audit_consumer_exited processed=` |

Every run's `build_provenance.env` records the new gateway hash, the current fingerprint, and all three valid replica manifests/live fingerprints. The PostgreSQL and server hashes equal report 3's hashes; the gateway hash changes as expected. Remote HEAD can be `unknown` because canonical sync excludes `.git`; executable hashes and manifest/live source checks provide identity evidence. Raw proof: `4_binary_proof.txt`, `4_binary_proof.json`, `4_local_provenance.txt`, `4_final_gateway_provenance.txt`, and each run's provenance files. PostgreSQL compiler logs are `4_build_pg_gateway_full.log` and `4_build_pg_user4_full.log`; C++ build output is in run 1's `build_local_gateway.log` and `build_u22_user4.log`. Builds passed; no source repair or alternate build retry was needed.

Canonical script hashes on every replica and the actual gateway checkout:

```text
a8af15f392deb3bafdf7edafe1e9e8ebad63c7d731d59ff75c1e609fb4d5c97a  scripts/distributed/run_4node_raft_cluster.sh
b40be923e5c97f6877febee397cb8f5b3fcd099314cafae0e380624d7d927d61  scripts/distributed/recovery/run_recovery_cluster_test.sh
```

## Commands and execution controls

All builds, database/server instances and benchmarks ran on lab hosts. Gateway/build/orchestration host: 10.129.27.111. Replica hosts: admin123 10.129.148.247, user4 10.129.148.246, utkarsh 10.129.148.248, user neel. Canonical recovery path and pgdata `~/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata`, port 5438, normal restore; no isolated pgdata, substituted recovery scripts or optimization environment overrides.

Recovery wrapper settings: YCSB-A theta 0, 160k transactions, W=8 executor/connection/pool/BCDB workers, 96 client lanes, det window 1024, 32MB shared buffers, raft-kafka, majority_async_all3. Each C injects exactly 100 tuples on utkarsh approximately 5 seconds after gateway startup; fault-injection logs and timelines are retained in each run.

```bash
cd /home/neel/ARIABC/AriaBC
export TMPDIR=$HOME/ariabc_data/detopt_cluster_20261008/scratch
export BYPASS_DELEGATION=1 CLUSTER_STOP_POSTGRES_ON_EXIT=1
export CALLER_GIT_HEAD=025c9be59acdc2566147e2fa009a636e6c6bf33a
export LOCAL_INSTALL_DIR=/home/neel/ARIABC/install GATEWAY_STALL_WATCHDOG=0
unset BCDB_PG_EXTRA_ENV
R=scripts/distributed/recovery/run_recovery_cluster_test.sh
# Unique CLUSTER_RUN_ID for each invocation; exact commands/env in 4_run*.json.
FORCE_BUILD=1 bash "$R" --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5
bash "$R" --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --skip-sync --skip-build
bash "$R" --recovery-mode off --skip-sync --skip-build
```

Normal W=16: report 1 section-1 sweep flags, one trial, same build, and verified cold-cache policy on all three replicas. The unchanged sweep is executed by `4_normal_sweep_driver.py` using runpy. Its child-process adapter sets the existing watchdog-off environment switch and stages byte-identical copies before same-host scp uploads; it does not modify repository code or workload behavior.

```bash
cd /home/neel/ARIABC/AriaBC
SKIP_SYNC=1 SKIP_BUILD=1 CLUSTER_DET_WINDOW=1024 python3 -u \
  scripts/distributed/run_all_modes_gateway_sweep.py --benchmark ycsb \
  --gateway-host 10.129.27.111 --gateway-user neel \
  --gateway-repo /home/neel/ARIABC/AriaBC --db-host 10.129.148.247 \
  --db-user neel --db-port 5438 --server-port 8000 \
  --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt \
  --workers 16 --modes cluster --run-cluster --db-shared-buffers 32MB \
  --cold-runs --order-seed 42 --trials 1 \
  --out-dir /home/neel/ARIABC/AriaBC/.bench_tmp/detopt_cluster_20261008/4_normal_sweep
```

The canonical self-matching watchdog is disabled using its existing switch. `4_campaign_driver.py` and `4_normal_supervisor.py` retain report 3's external 5-second/12-no-client-progress-cycle collector, with exact executable/argument matching in /proc, SQL diagnostics before any gateway SIGTERM, and post-run cleanup. SSH polling time makes the 12-cycle threshold longer than an exact 60-second deadline. No watchdog fires in any measured run. Runtime gateway logs contain no `async_all3_audit_consumer_exited`; the marker exists in the binary as required. Gateway stdout and stderr are both preserved in each `gateway_test.log`.

Postmaster `/proc/<pid>/environ` observations on every node in every run show the scratch TMPDIR and no early-validation/tag-dedup/lookahead/settle overrides. Every run-meta file records empty `bcdb_pg_extra_env`. See `4_run*_postmaster_environ.jsonl` and `4_normal_postmaster_environ.jsonl`.

## Artifacts, deviations and final state

| Order | Run ID | Local artifact directory |
|---:|---|---|
| 1 | `cluster4_4_C_default_t1_20261008_132431` | `4_runs/cluster4_4_C_default_t1_20261008_132431/` |
| 2 | `cluster4_4_C_default_t2_20261008_133200` | `4_runs/cluster4_4_C_default_t2_20261008_133200/` |
| 3 | `cluster4_4_A_default_t1_20261008_133403` | `4_runs/cluster4_4_A_default_t1_20261008_133403/` |
| 4 | `cluster4_20261008_133633_e5b4e840` | `4_runs/cluster4_20261008_133633_e5b4e840/` |

Canonical originals remain on .111 under `/home/neel/ARIABC/AriaBC/scripts/bench_full_results/<run_id>/`. Local copies are under this report's `4_runs/`. `4_evidence.json` contains terminal summaries, exact profiles/timelines/events, per-node readbacks, environment metadata and warning scans. `cluster_results_4.csv` preserves exactly the ten columns from `cluster_results_3.csv` and has four measured rows.

The first normal-sweep orchestration attempt on .247 failed before any database/benchmark run: `Host key verification failed` for self-SSH, and a separate connection check showed no gateway SSH credentials there. It was moved to the authenticated .111 host; failed-preflight output is preserved as `4_normal_preflight_failed.log`, `4_normal_preflight_failed_supervisor.log`, `4_normal_preflight_failed.exit`, and `4_normal_preflight_failed/`. There was one actual normal benchmark trial, not a discarded measured trial.

**Canonical socket exception:** unchanged replica configs require Unix socket and lock files in `/tmp`. This was disclosed before startup. Managed compiler/tool scratch files and postmaster TMPDIR use `~/ariabc_data/detopt_cluster_20261008/scratch`. Temporary .bashrc/.zshenv TMPDIR exports on lab hosts were restored byte-for-byte afterward, and previously absent .zshenv files were removed.

All three replica PostgreSQL instances and AriaBC servers are cleanly stopped. No neel-owned benchmark gateway/server/PostgreSQL process remains on the gateway or replicas. All replica postmaster.pid files are absent; benchmark ports 5438, 9000 and the configured client ports are stopped. Utkarsh's pre-existing unrelated port-8000 service and Kafka brokers remain. The unused Desktop pgdata on .111 had a stale postmaster.pid before this campaign; its absent process was verified and that pre-existing file was preserved, not removed. A cleanup attempt initially encountered that stale PID; final restoration handles the unused directory without changing it.

Every canonical postgresql.conf/postgresql.auto.conf SHA256 equals its pre-campaign backup; all shell startup-file hashes also match. Evidence: `4_config_backup_<IP>.txt` and `4_final_config_and_stop_<IP>.txt`, plus per-run `4_cleanup_<node>.json`. Saved PostgreSQL logs have no BCDB_HANG, conflict_commit_stuck, serial_gate_stuck, BCDB_INVARIANT_POST_PUBLISH_APPLY or ERROR matches; file coverage/line counts are explicit in `4_evidence.json`.

No workstation build, database, server or benchmark was started. No local source or Final_Results file was edited. Local main remains at the requested commit, with no source diff. The recovery paths are unchanged and the cluster is stopped.
