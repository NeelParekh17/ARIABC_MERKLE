# Review of the 100M YCSB benchmark and in-memory comparison

Reviewed on 2026-09-14. The original runner was **not reliable enough for the proposed comparison**. The Python runner and the single-node YCSB acceptance path have been corrected. The full remote benchmark has **not** been executed as part of this review; its runtime checks and Merkle verification must pass before using new throughput results.

The existing results, plots, user-modified analysis, and golden database were not changed. Original script copies for this review are under `.bench_tmp/oom_audit_original/`.

## Problems found and corrections

| Area | Original problem | Current behavior |
|---|---|---|
| Command line | `--workers 1 8 16`, `--skews 0.0 0.99`, and `--output-dir` were unsupported. | Space-separated and comma-separated lists work; `--output-dir` and `--out-dir` are aliases. Inputs are checked before remote work. |
| Zipf distribution | The 100M sampler normalized only the first 1M ranks, used an incorrect eta expression, divided by zero at theta=1, and used the approximation outside its valid domain at theta>1. | Full finite-keyspace inverse CDF with bounded memory; handles theta=1 and 1.2. Directly summed prefix plus a numerically checked Euler–Maclaurin tail. |
| Workload comparability | Different RNG seed/value generation and operation sampling; D lacked read-latest behavior; F was just independent 67/33 operations. | Reuses `generate_ycsb_workloads.py` with its original seed rule and SQL semantics. At 12K keys, A at both requested skews reproduces the existing suite files byte-for-byte. |
| Suite-copy option | Renaming `usertable_small` kept the 12K working set and could collide with preloaded rows on inserts. | Removed. Large-database cases generate against `--db-rows`. |
| Reset | In-place mode retained updates, tuple versions, and previously dropped Merkle indexes. | Only a pristine physical copy is accepted. No reflink/hardlink reset. |
| Golden database | A directory or `done.flag` was treated as proof of a complete 100M database. | Requires clean control-file shutdown state; checks exact row count and min/max keys on the restored copy. Rejects incompatible Merkle layout and nonzero apply state. |
| Schema | Initial generation created placeholder Raft ledger tables. | New generation uses the repository ledger schema. Old empty placeholders are replaced only in the disposable copy; nonempty placeholders cause failure. |
| Mode indexes | PG and deterministic-only retained the Merkle lookup B-tree, unlike the small-table restore. | Both Merkle AM and lookup indexes are removed from non-Merkle cases. |
| Buffer settings | Writing `postgresql.conf` could be overridden by copied `postgresql.auto.conf`. | Writes an explicit auto configuration to the disposable copy, then checks effective buffers, workers, mode and durability settings. Saves all effective GUCs. |
| Cache clearing | Hardcoded password; failures ignored by non-strict remote shells; in-place restart kept shared buffers warm. | Strict remote shells, checked sudo, stop → sync/drop caches → start. Any failure aborts. No password in source or artifacts. |
| PostgreSQL I/O | Buffer misses were described as physical NVMe reads; failures became zeros; worker statistics could be unreported. | Keeps buffer and block-device metrics separate; errors abort. Clean shutdown publishes BCDB worker counters before reading persisted statistics. |
| Physical I/O | Hardcoded `nvme0n1`; no deferred-write accounting. | Resolves the data filesystem's block device through `findmnt` and `/sys/dev/block`; saves raw counters and a separate post-run checkpoint interval. |
| TPS acceptance | Missing metrics became successful defaults; first progress counters could hide later failures; failed verification still produced TPS rows. | Requires final elapsed time, correct loaded count, zero final errors and completion evidence appropriate to the direct execution path. Full Merkle verification is mandatory for Merkle cases. |
| Artifacts | Reused workloads, appended incompatible CSV rows, discarded gateway output, overwrote shared remote logs. | Each invocation gets a unique child directory; saves workload hashes, gateway/server/PG logs, effective settings, binary hashes, raw I/O, verification and accepted rows. |
| Processes/WAL | Killed arbitrary port owners; manually deleted WAL files. | Refuses occupied ports, uses owned server PID and PostgreSQL shutdown, preserves PostgreSQL-managed WAL. Directory lock prevents concurrent OOM campaigns. |

Custom `balanced_dml`, `delete_heavy`, `dml_heavy`, and `pure_dml` are now explicitly rejected by this large-keyspace runner. The original versions did not preserve the suite's partitioned/recycled-key semantics. Supported families are A/B/C/D/F and all_update/all_insert/all_delete. The requested Workload A campaign is supported. `--hard-reset` and in-place reset are intentionally unavailable; existing `pgdata_base` is the supported baseline.

## What 32MB does and does not mean

`shared_buffers=32MB` bounds PostgreSQL's shared buffer pool. It does **not** bound Linux's page cache or total PostgreSQL memory, including BCDB/Merkle allocations. PostgreSQL buffer misses can be served from the kernel cache. PostgreSQL's statistics also update asynchronously. [PostgreSQL statistics documentation](https://www.postgresql.org/docs/13/monitoring-stats.html)

Read-only checks on `10.129.148.247` found:

- Physical RAM: 16,121,221,120 bytes, about 15.01 GiB.
- Database directory filesystem: ext4 on `/dev/nvme0n1p2`.
- Available filesystem space at inspection: 334,853,537,792 bytes.
- Swap was in use (971,091,968 bytes). This is an observation, not proof that the benchmark will swap; each campaign records memory status again.
- `pgdata_base` control file reported `shut down`.
- The small-table benchmark's current `postgresql.auto.conf` specified `shared_buffers='32MB'`, `synchronous_commit='on'`, and the same main deterministic gate/pipeline settings.
- Unattended `sudo -n` was unavailable: a sudo password was required.

The existing storage breakdown reports roughly 25.6GB of heap and 32.44GB total database-directory bytes. Those sizes were **not** remeasured by scanning the offline golden database during this review. The corrected runner records actual relation/database sizes for each case.

A 100M database is larger than this machine's RAM, but 20K point queries touch only a fraction of it. At high skew, hot pages can fit in RAM. Describe the result as **cold-start 100M-row YCSB with 32MB shared buffers and measured device I/O**, not “every access goes to SSD,” “32MB total memory,” or sustained disk saturation. This is out-of-core testing, not an OOM-killer test or a sort/hash spill benchmark.

The revised interval is:

1. Restore and configure a fresh copy; prepare schema and check exact keyspace/index state.
2. Stop PostgreSQL; `sync`; drop Linux caches; restart PostgreSQL.
3. Start the server, stage the workload, collect initial counters, run the gateway.
4. Collect device counters immediately after completion. This window includes gateway startup/SSH and measurement overhead; its wall duration is recorded separately from TPS time.
5. Stop the server; measure a separate SQL `CHECKPOINT` and its device writeback.
6. Cleanly stop/restart PostgreSQL to publish long-lived workers' statistics, then read the persisted buffer counters.
7. Verify Merkle state, save logs, and stop PostgreSQL.

The PostgreSQL counter interval includes small monitoring/startup/shutdown overhead. Device counters include all traffic on the filesystem's block device, not just this database. The separate checkpoint metric is not included in TPS. Neither metric includes the subsequent Merkle heap verification scan. Linux can still defer some filesystem metadata writes beyond an individual snapshot; the counters are interval observations, not an exact total of all physical effects.

## Comparison with the existing report

Reference files:

- `scripts/bench_full_results/ycsb_all_72_sweep/YCSB_72_WORKLOADS_DETAILED_ANALYSIS.md`
- `scripts/bench_full_results/ycsb_all_72_sweep/summary.csv`
- `scripts/bench_full_results/ycsb_all_72_sweep/sweep.log`
- `scripts/bench_full_results/oom_100m_sweep/summary.csv`
- `scripts/bench_full_results/oom_100m_sweep/workloads/ycsb_100m_a_skew_0.99_20000.txt`

The reference CSV contains 1,920 unique cases: 480 per mode, each with 20,000 queries and stored flags `merkle_pass=1`, `divergence_count=0`, `permanent_failures=0`. However, **all 1,440 single-node rows have empty `shared_buffers` and `source_fingerprint` fields**. Across all modes, 1,319 rows have no run ID. The 480 cluster rows record 32MB and a source fingerprint. This supports reporting the stored flags, but does not establish identical binaries/settings or complete per-run raw evidence for the single-node historical results.

The old OOM Workload A file at theta=.99 has 10,071 updates / 9,929 reads, 13,244 distinct keys, and 1,317 accesses to key 1. The in-memory A file has 10,008 updates / 9,992 reads, 4,642 distinct keys, and 1,955 accesses to key 1. They were not the same generator scaled up.

Even the corrected same-theta distributions inherently have different contention:

| Finite keyspace, theta=.99 | Normalization H(N, theta) | Probability of hottest key |
|---|---:|---:|
| 12,000 | 10.42444637 | 9.593% |
| 100,000,000 | 20.80293049 | 4.807% |

Therefore the old-vs-new ratio combines **database size, locality, key conflict frequency, cache state and potentially software changes**. It does not isolate storage overhead. Uniform skew is the cleaner first comparison, although the number of repeated keys still changes.

Both runners use the same single-node server/gateway settings: PG pool equals requested workers; deterministic worker/block initialization equals requested workers; 96 client terminals; PG submitLimit=512 and nondetWindow=8; deterministic window=65536, batch=256, pipeline depth=1024, and the same block/pipeline environment. TPS retains the historical `queries / overall time taken` denominator, with `overall wall time including drains` saved separately. Do not substitute `completed_tps` into that comparison.

The gateway's `success_count` and `client_quorum_complete_count` are Kafka/quorum counters and remain zero in these direct single-node paths. The validator instead requires final DET progress with every statement completed and no outstanding requests, or a completed PG worker loop with exactly the expected number of request attempts and no rejects/retries. Socket read-call counts are **not** response counts. All final failure counters must still be zero.

The small-table runner now also checks final single-node results, effective settings and exact Merkle output, uploads the actual local workload bytes, and writes all 14 CSV fields including provenance. Its YCSB setup uses SQL error-stop behavior. This does not retroactively validate old rows. Its TPC-C and distributed orchestration paths were not comprehensively re-audited here.

The existing analysis also contains claims its artifacts do not establish:

- It calls the baseline PostgreSQL 14, while this checkout identifies itself as `13devel`; runtime binary version should be recorded rather than assumed.
- `pg` uses the project's PostgreSQL build with deterministic execution disabled, not a separately established stock PostgreSQL installation.
- Workload F in the suite emits adjacent SELECT/UPDATE statements; it does not wrap them in one explicit SQL transaction. Report throughput as statements/sec or autocommit transactions/sec, not completed multi-statement RMW transactions/sec.
- Merkle self-consistency and zero observed failures are not proof of serializability or Byzantine fault tolerance. These require separate correctness arguments/tests; single-node divergence=0 is not cross-replica evidence.
- The log contains failed earlier cluster attempts; later replaced CSV rows cannot be validated simply by reading that old log's final lines.

## Commands

The requested command now works:

```bash
python3 scripts/distributed/run_oom_100m_benchmark.py \
  --workloads a \
  --skews 0.0 0.99 \
  --workers 1 8 16 \
  --txs 20000 \
  --shared-buffers 32MB \
  --remote-host 10.129.148.247 \
  --remote-dir /tmp/ariabc_oom_100m \
  --output-dir scripts/bench_full_results/oom_100m_sweep
```

This produces **18 cases**. Each invocation creates `oom_100m_sweep/run_<timestamp>_<id>/`, preserving previous results. Full Merkle verification and physical-copy reset are defaults. Existing baseline copies took roughly 16–17 minutes per case in the supplied old CSV, so copying alone may take several hours for 18 cases. The full row-count checks and Merkle scans add time outside TPS.

For variation estimates, add `--trials 3` (54 cases); modes rotate order across trials and the report/plots use medians. For local validation only, append `--dry-run`. For host checks without starting databases or clearing caches, append `--preflight-only`.

An interactive run prompts once for the DB host's sudo password if necessary. For unattended execution, populate `ARIABC_BENCH_SUDO_PASSWORD` without putting the password in a command-line argument or source file, for example:

```bash
read -rsp 'DB sudo password: ' ARIABC_BENCH_SUDO_PASSWORD
export ARIABC_BENCH_SUDO_PASSWORD
```

Stop other benchmarks before running: this runner refuses occupied 5438/8000/9000 ports, and clearing the OS cache affects the whole DB host. Following an unclean controller death, inspect `<remote-dir>/benchmark.lock/owner.txt` and stop any remaining owned processes before removing the stale lock.

For a fresh small-table comparison using the same two suite files:

```bash
python3 scripts/distributed/run_all_modes_gateway_sweep.py \
  --modes pg,bcdb_det,bcdb_merkle \
  --workers 1,8,16 \
  --workloads scripts/ycsb_suite/ycsb_workload_a_skew_0_00_20k.txt,scripts/ycsb_suite/ycsb_workload_a_skew_0_99_20k.txt \
  --db-shared-buffers 32MB \
  --out-dir scripts/bench_full_results/ycsb_a_32mb_fresh_comparison
```

Use a fresh output directory if that name already contains results. This is the suite's warm small-table restore methodology, not a cold-cache counterpart. Compare the effective GUCs and binary versions/hashes saved for the new runs. A storage-only causal comparison needs an additional experiment controlling the active key set and its physical placement, or the same 100M workload under a verified larger memory/cache budget; the historical CSV alone cannot supply it.

## Validation performed

- Local Python compilation and `git diff --check`.
- Unit tests for CLI handling, artifact preservation, direct/quorum completion validation, late failures, missing evidence, Zipf normalization/ranges, exact Workload A suite reproduction, F statement semantics, counter failures, and generated remote-shell syntax.
- Requested 20K-query command with `--dry-run`, generating both 100M Workload A files under `.bench_tmp/oom_review_validation/`.
- Read-only remote checks of memory, filesystem/device, free space, current config files, control-file shutdown state and sudo availability.

No database was started, no cache was cleared, no golden copy was changed, and no new TPS or Merkle PASS result is claimed by this review. Runtime compatibility with installed binaries and the existing golden schema remains to be established by the actual run.
