# Canonical online-recovery C: merged defaults versus noev

Date: 2026-10-08. Local main: `3bde2fdcd7a88a41a4bbe7c127efdd844192481c`.

**The failure happens in both configurations: default 2/2 hangs; noev 2/2 hangs. A default passes at 8,823.69 TPS.** Every C repair reports PASS, every replica reaches det txid 159999, and every transaction has a durable result from all three nodes. The gateway nevertheless fails to finalize its audit and exits 143 after the watchdog sends SIGTERM. Thus the failure does **not** follow the early-validation/tag-dedup/lookahead control. Isolated pgdata and the earlier modified recovery scripts are not necessary to reproduce it.

The source contains an older audit-thread idle-timeout failure path that explains the observations: an empty ready queue for 30 seconds ends the audit consumer thread, while the main gateway subsequently waits indefinitely for its audit counters. The 53.8–55.3-second repair intervals exceed that timeout; replay later supplies every result. This is a source-backed diagnosis consistent with the runtime evidence, rather than a captured stack trace of the audit thread. The experiment does not isolate `BCDB_DT_POST_PUBLISH_SETTLE`, which remains default-on in both configurations, or establish why repair is much slower than the published reference.

## Execution and provenance

Read first: `CLUSTER_REPORT_2.md`, section C and abnormalities; `Final_Results/COMMANDS.md`, section 6; and the canonical wrapper's help (`3_recovery_help.txt`). All database, build and benchmark processes ran on lab hosts: gateway/build host `10.129.27.111`, replicas `10.129.148.247/.246/.248`. No TPC-C work was performed.

The first C run synced current local source to the gateway and replicas, forced a fresh canonical PostgreSQL/gateway/server build, and rebuilt user4's Ubuntu 22.04 binaries on user4. All five runs passed the runner's source/binary provenance checks. Expected and live source fingerprint on gateway and all replicas:

```
a1ea2435504a5eda1dab187c1bc5bafc56f0fcf7f572f282d0760f611286f7d3
```

The source/run caller HEAD is recorded as `3bde2fdc`; remote checkout manifests can say `git_head=unknown` because canonical sync excludes `.git`. They are not the sole identity evidence: the local HEAD, exact source fingerprint, forced build, executable hashes and successful manifest checks establish the source/binary relationship. Every run's `build_provenance.env` records the identities. Full PostgreSQL compiler logs are `3_build_pg_gateway_full.log` and `3_build_pg_user4_full.log`; gateway/server build output is retained in the first run's build logs.

The recovery scripts were never edited. Their final hashes on all replicas match local HEAD:

| Script | SHA256 |
|---|---|
| `scripts/distributed/run_4node_raft_cluster.sh` | `a8af15f392deb3bafdf7edafe1e9e8ebad63c7d731d59ff75c1e609fb4d5c97a` |
| `scripts/distributed/recovery/run_recovery_cluster_test.sh` | `b40be923e5c97f6877febee397cb8f5b3fcd099314cafae0e380624d7d927d61` |

All trials used canonical `~/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata`, port 5438, normal restore, the wrapper's YCSB-A theta-0 160k workload, 8 executor/connection/pool/BCDB workers, 96 client lanes, det window 1024, 32MB shared buffers, Raft-Kafka and `majority_async_all3`. There were no isolated copies, cold-cache overrides, native source edits, or substituted recovery/startup scripts. Each fault injection returned rc=0, changed exactly 100 tuples, and changed the root; injection started approximately 5 seconds after gateway startup (`fault_timeline.env`, `fault_injection.log` in each C run).

The sole supervision change used the existing `GATEWAY_STALL_WATCHDOG=0` switch and an external collector with the same 5-second/12-no-progress-cycle threshold. This avoids the canonical watchdog's self-matching `pgrep -f 'ariabc_pg_server'`, without editing it. The collector reads exact executable/argument identities in `/proc`, captures committed txids, gate diagnostics, activity and locks on every replica before SIGTERM, and retains full PostgreSQL logs. Polling includes SSH time, so 12 cycles are not an exact 60-second wall-clock deadline. Recovery and workload code paths are unchanged. Collector: `3_campaign_driver.py`; per-run traces: `3_watchdog_progress.jsonl`, `3_watchdog_<IP>.json`, `3_WATCHDOG_TRIGGERED`.

Exact invocation order is preserved in `3_run1_C_default.json` through `3_run5_A_default.json`. On the gateway:

```bash
export TMPDIR=$HOME/ariabc_data/detopt_cluster_20261008/scratch
export BYPASS_DELEGATION=1 CLUSTER_STOP_POSTGRES_ON_EXIT=1
export CALLER_GIT_HEAD=3bde2fdcd7a88a41a4bbe7c127efdd844192481c
export LOCAL_INSTALL_DIR=/home/neel/ARIABC/install
export GATEWAY_STALL_WATCHDOG=0  # same-threshold external watchdog
R=scripts/distributed/recovery/run_recovery_cluster_test.sh

# 1. C default, FORCE_BUILD=1; CLUSTER_RUN_ID unique for each command.
unset BCDB_PG_EXTRA_ENV
FORCE_BUILD=1 bash "$R" --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5

# 2. C noev
BCDB_PG_EXTRA_ENV="BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0" \
  bash "$R" --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --skip-sync --skip-build

# 3. C default
unset BCDB_PG_EXTRA_ENV
bash "$R" --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --skip-sync --skip-build

# 4. C noev: same command/environment as #2.
# 5. A default
unset BCDB_PG_EXTRA_ENV
bash "$R" --recovery-mode off --skip-sync --skip-build
```

`--skip-sync --skip-build` after the first run preserves the matched build. Default runs have no optimization overrides in postmaster environment. Noev overrides the two requested switches; `bcdb_dt_gate_lookahead_enabled()` also requires early validation (`src/backend/bcdb/shm_transaction.c`), so noev disables lookahead indirectly. Settle remains enabled.

## Per-run results

| Order | Case / config / trial | TPS_majority_visible | Empty steady 100ms buckets | Max completion gap ms | Gateway finalized | Runner exit |
|---:|---|---:|---:|---:|---|---:|
| 1 | C / default / 1 | UNKNOWN | UNKNOWN | UNKNOWN | No | 143 |
| 2 | C / noev / 1 | UNKNOWN | UNKNOWN | UNKNOWN | No | 143 |
| 3 | C / default / 2 | UNKNOWN | UNKNOWN | UNKNOWN | No | 143 |
| 4 | C / noev / 2 | UNKNOWN | UNKNOWN | UNKNOWN | No | 143 |
| 5 | A / default / 1 | 8823.69 | 0 | 19.58 | Yes | 0 |

No C run produced `run_summary.env`, `tx_latency.csv` or a terminal gateway profile. Consequently TPS, empty buckets and maximum completion gaps cannot be recovered as valid client measurements. The later `completed_tps` progress field includes audit waiting and is not majority-visible TPS. These missing metrics remain UNKNOWN, not zero. A is 0.30% below the published A's 8,850.05 TPS, and 0.006% below report 2's same-day A of 8,824.18 TPS; one reference trial is not a stability study.

| Order | Permanent failures | Divergence | Gateway all-three audit | Client completions observed | Phase 8 |
|---:|---|---|---|---:|---|
| 1 | 0 last observed; terminal UNKNOWN | raw=1; terminal normalized UNKNOWN | Not finalized | 160000/160000 | NOT RUN |
| 2 | 0 last observed; terminal UNKNOWN | raw=1; terminal normalized UNKNOWN | Not finalized | 160000/160000 | NOT RUN |
| 3 | 0 last observed; terminal UNKNOWN | raw=1; terminal normalized UNKNOWN | Not finalized | 160000/160000 | NOT RUN |
| 4 | 0 last observed; terminal UNKNOWN | raw=1; terminal normalized UNKNOWN | Not finalized | 160000/160000 | NOT RUN |
| 5 | 0 terminal | 0 terminal | valid=yes; verified=160000/160000; failure/timeout/missing=0 | 160000/160000 | PASS on all three |

Raw divergence in C is the deliberately injected corruption detected before repair. Without the terminal profile it cannot be reported as a measured final normalized zero.

Run directories beneath local `3_runs/` (the canonical remote originals are beneath gateway `scripts/bench_full_results/`):

| Order | Run ID |
|---:|---|
| 1 | `cluster4_3_C_default_t1_20261008_122537` |
| 2 | `cluster4_3_C_noev_t1_20261008_123340` |
| 3 | `cluster4_3_C_default_t2_20261008_123607` |
| 4 | `cluster4_3_C_noev_t2_20261008_123844` |
| 5 | `cluster4_3_A_default_t1_20261008_124120` |

## Recovery timing and complete events

All C events: damaged node=4, result=PASS, reference=2/user4, live=1, no full copy. The published C used reference=1/admin123 and total_ms=10824 (cut=2741, repair=59, catch-up=7970). The donor difference and large repair-time difference remain relevant unexplained variables; this matrix does not attribute them to a particular mechanism.

| Order | Config | cut_ms | repair_ms | catchup_ms | total_ms | Differing leaves | Rows upserted |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | default | 100 | 55328 | 11461 | 68369 | 87 | 93 |
| 2 | noev | 101 | 54083 | 11420 | 65784 | 91 | 94 |
| 3 | default | 46 | 54867 | 11508 | 66603 | 87 | 92 |
| 4 | noev | 40 | 53763 | 11428 | 65554 | 84 | 89 |
| 5 | A default | 0 | 0 | 0 | 0 | 0 | 0 |

A has no RECOVERY_EVENT; terminal recovery triggered/succeeded/failed counters are 0/0/0. C terminal aggregate counters are unavailable; each has exactly one observed live PASS event. Original gateway lines, without wrapper duplicates:

```
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=170 B=43263 detect_to_quarantine_ms=0 cut_ms=100 recover_call_ms=68250 repair_ms=55328 drain_ms=82 catchup_ms=11461 live=1 mismatched_partitions=77 differing_leaves=87 rows_deleted=0 rows_upserted=93 full_copies=0 replay_from=171 replay_target=626 total_ms=68369 digest=usertable_small:d1b8d6b583449404f6fa2163dd8d77e4a740b1b75386018c4910eddb100ee34d
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=171 B=43519 detect_to_quarantine_ms=0 cut_ms=101 recover_call_ms=65663 repair_ms=54083 drain_ms=128 catchup_ms=11420 live=1 mismatched_partitions=78 differing_leaves=91 rows_deleted=0 rows_upserted=94 full_copies=0 replay_from=172 replay_target=626 total_ms=65784 digest=usertable_small:e7c8cb886dd411ced79dd4be0126c059ca61bf480b1351594bb2c78564aa0faa
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=169 B=43007 detect_to_quarantine_ms=0 cut_ms=46 recover_call_ms=66537 repair_ms=54867 drain_ms=129 catchup_ms=11508 live=1 mismatched_partitions=73 differing_leaves=87 rows_deleted=0 rows_upserted=92 full_copies=0 replay_from=170 replay_target=626 total_ms=66603 digest=usertable_small:32bd616dd68dc64c668e0c724e4ca03dc9f56c13789492836159cddc88a505cf
RECOVERY_EVENT node=4 reason=result_divergence result=PASS ref=2 attempt=1 L=172 B=43775 detect_to_quarantine_ms=0 cut_ms=40 recover_call_ms=65494 repair_ms=53763 drain_ms=269 catchup_ms=11428 live=1 mismatched_partitions=70 differing_leaves=84 rows_deleted=0 rows_upserted=89 full_copies=0 replay_from=173 replay_target=626 total_ms=65554 digest=usertable_small:118e0955478af0f7dc464df5731b7249f5239c1cb988829e43c121344cbb4f09
```

## Postmaster proof for both noev trials

Collected directly from `/proc/<pid>/environ` on every replica, using the PID in canonical `postmaster.pid`. Full observations including startup/restarts are in `3_run2_C_noev_postmaster_environ.jsonl` and `3_run4_C_noev_postmaster_environ.jsonl`.

| Order | Replica | Workload postmaster PID | Observed overrides |
|---:|---|---:|---|
| 2 | admin123 | 594428 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |
| 2 | user4 | 1172979 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |
| 2 | utkarsh | 462048 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |
| 4 | admin123 | 604445 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |
| 4 | user4 | 1179372 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |
| 4 | utkarsh | 468746 | `BCDB_DT_EARLY_VALIDATE=0 BCDB_DT_TAG_DEDUP=0` |

The recovery wrapper calls the committed `run_4node_raft_cluster.sh`, which applies `BCDB_PG_ENV_PREFIX` to postmaster start/restart commands. The environment proof confirms that the passthrough is operative here, not merely configured in the caller.

## Where the failed audits are stuck

Watchdog SQL on **each node in each C run** returns committed txid 159999, matching published watermark 159999. Gate diagnostics show no active wait states. Activity/locks and the full PostgreSQL logs are preserved in each run. This is not a follower still waiting to execute the replay prefix.

After each gateway abort and before the next canonical topic reset, an independent Kafka console consumer read `ariabc_results` from the beginning. Its raw T1 records are retained as `3_kafka_records.txt.gz`; per-request coverage is independently decoded by `3_analyze_campaign.py` and retained in `3_kafka_audit.json` and `3_missing_third_results.csv`. The consumer terminates on its 10-second idle timeout after reading the same message count that the gateway reported; its Java timeout message is an end-of-capture condition, not a benchmark error.

| Order | Unique requests | Unique node/request results | Duplicate results | Node 1 / 2 / 4 unique results | Requests lacking a third result | Hash mismatches above repair boundary |
|---:|---:|---:|---:|---|---:|---:|
| 1 | 160000 | 480000 | 0 | 160000 / 160000 / 160000 | 0 | 0 |
| 2 | 160000 | 480000 | 0 | 160000 / 160000 / 160000 | 0 | 0 |
| 3 | 160000 | 480000 | 0 | 160000 / 160000 / 160000 | 0 | 0 |
| 4 | 160000 | 480000 | 0 | 160000 / 160000 / 160000 | 0 | 0 |

**Missing tx IDs: none. Missing node: none**, for requests 1–160000 / det sequence 0–159999. All four missing-result CSVs contain only the header. There are respectively 8/8/12/8 requests with differing hashes, all at or below the repair boundary, where the gateway's recovery-aware audit replaces the damaged node's old state with snapshot coverage. No replayed request above the cut has divergent hashes. Complete third-result publication does not make the unfinalized gateway audit valid.

The gateway source explains why this can happen despite complete results:

1. `ariabc_pg/src/ariabc_pg_gateway.cxx:2671` converts default `poll_count<=0` to a 30-second wait. The overall wait deadline at line 2679 is independent of recovery's extensions to individual request deadlines. An empty ready queue at that deadline returns false at line 2740.
2. `start_async_audit_thread`, line 6244, breaks out of the consumer loop on that false return, rather than retrying while recovery is in progress.
3. `drain_async_all3_audit`, line 6350, waits until processed audit counters reach completed clients, with no consumer-liveness check. Late replay votes cannot advance those counters once the audit thread has exited.

The approximately 54-second repair interval allows the audit queue to remain empty longer than 30 seconds; the published approximately 10.8-second recovery avoids that condition. This mechanism is an inference from the current source and runtime trace; no runtime thread stack or internal pending-counter snapshot was captured, so the exact set of unprocessed gateway entries is not claimed.

`3_audit_timeout_blame.txt` shows the consumer-break logic and 30-second fallback in `afe98064a` (2026-06-26); the overall deadline was added in `362139b05` (2026-09-17). `3_gateway_merge_diff.txt` is empty: the det merge `19e8acb2` did not change this gateway file. Therefore the identified gateway failure path was not introduced by that merge. This does not by itself explain the slower repair, nor exclude an effect of untested default-on settle on its duration.

## PostgreSQL warning scan and Phase 8 roots

All 15 complete PostgreSQL logs (five runs × three nodes), including the damaged node's repair/replay interval, were searched for each requested marker. **Zero matches** for `BCDB_HANG`, `conflict_commit_stuck`, `serial_gate_stuck`, `BCDB_INVARIANT_POST_PUBLISH_APPLY`, or `ERROR`. Exact log paths, line counts and match arrays are in `3_evidence.json`; full logs are `3_full_postgres_<node>.log` inside each run. The corresponding `3_postgres_warning_context_<node>.txt` files are empty because there are no hits.

Phase 8 was never entered in any C run; repair-event cut digests are not final post-marker roots. Explicit per-node results:

| Order | Node | Phase 8 root | Rows | Native verify |
|---:|---|---|---:|---|
| 1 | admin123 | NOT RUN | UNKNOWN | NOT RUN |
| 1 | user4 | NOT RUN | UNKNOWN | NOT RUN |
| 1 | utkarsh | NOT RUN | UNKNOWN | NOT RUN |
| 2 | admin123 | NOT RUN | UNKNOWN | NOT RUN |
| 2 | user4 | NOT RUN | UNKNOWN | NOT RUN |
| 2 | utkarsh | NOT RUN | UNKNOWN | NOT RUN |
| 3 | admin123 | NOT RUN | UNKNOWN | NOT RUN |
| 3 | user4 | NOT RUN | UNKNOWN | NOT RUN |
| 3 | utkarsh | NOT RUN | UNKNOWN | NOT RUN |
| 4 | admin123 | NOT RUN | UNKNOWN | NOT RUN |
| 4 | user4 | NOT RUN | UNKNOWN | NOT RUN |
| 4 | utkarsh | NOT RUN | UNKNOWN | NOT RUN |
| 5 | admin123 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | 12001 | t |
| 5 | user4 | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | 12001 | t |
| 5 | utkarsh | `80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5` | 12001 | t |

A's three independent post-marker readbacks also share data MD5 `9a15b0794b52e087376bc7190d48cc58`.

## Constraints, abnormalities and final state

**Canonical socket exception, disclosed before startup:** all three unchanged canonical configs resolve `unix_socket_directories` to `/tmp`. Their Unix socket and lock files necessarily used `/tmp`; the literal no-write-to-/tmp constraint therefore cannot simultaneously hold with unchanged canonical socket settings. No socket-config workaround was applied. Managed scratch/build temporary files use `~/ariabc_data/detopt_cluster_20261008/scratch`. Temporary shell startup exports propagated TMPDIR through canonical SSH without editing recovery scripts. Utkarsh uses zsh, so its first-run postmaster did not inherit the initial bash-only export; this was noticed during that run and a temporary `.zshenv` supplied TMPDIR for subsequent startups. Both noev trials prove TMPDIR and both requested overrides on all three postmasters. All shell startup edits were restored afterward.

An initial independent C++ prebuild encountered a workstation-path CMake cache accidentally copied in the first source sync. The subsequent sync excludes the build directory; the stale generated cache/tree was preserved under persistent scratch, the prebuild retry passed, and the first measured run then independently forced the full canonical build. Initial error/retry evidence is retained in `3_prebuild_cpp.log` / `3_prebuild_cpp_retry.log`. These preparatory builds submitted no workload. Compiler deprecation/ignored-return-value warnings are retained in build logs.

All canonical `postgresql.conf` and `postgresql.auto.conf` files were backed up before execution and restored after the campaign; final SHA256 on each host equals its original SHA256. No canonical postmaster PID file remains, and no neel-owned benchmark PostgreSQL/server/gateway process remains. Benchmark ports 5438, 9000 and the configured client ports are stopped; utkarsh's pre-existing unrelated port-8000 service and the pre-existing Kafka brokers were preserved. Evidence: `3_config_backup_<IP>.txt`, `3_final_config_and_stop_<IP>.txt`, `3_final_shell_restore_<IP>.txt`, and each run's `3_cleanup_<node>.json`.

No C/C++ source or `Final_Results/` file was edited. The pre-existing local user modifications to `ARCHITECTURE_EXPLORER.html`, `Final_Results/COMMANDS.md` and untracked `Final_Results/DET_OPTIMIZATION/` remain. Existing report-2 artifacts were not overwritten. The CSV preserves exactly the columns of `cluster_results_2.csv`; failed-C terminal metrics are blank, and `merkle_pass=0` means not reached/passed, not a measured root mismatch.

**Conclusion: both, not a default-fails/noev-passes separation.** Four canonical C failures with verified noev environments, complete three-node result publication, finished deterministic execution, clean PostgreSQL error scans and a passing same-day A point to gateway audit finalization during prolonged recovery. The old 30-second audit-thread exit path is a strong explanation. The experiment does not justify blaming the early-validation/tag-dedup/lookahead switches, nor a blanket claim that every merged det change is exonerated: settle was not disabled and the approximately 54-second repair slowdown remains unexplained.
