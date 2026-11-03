# Insert-only 2.4M statements: Merkle lookup comparison and LSM experiments

Results from October 21–22, 2026. Each insert run adds 2,400,000 new rows to the
100M-row baseline, using 16 workers, SERIALIZABLE and 32 MB `shared_buffers`.
Database and server: `10.129.148.247`; gateway and controller: `10.129.27.111`.
The original engine build was `5f2a078e`; bulk-merge experiments used a separate
experimental extension build, as recorded in the report and case evidence.

**Transaction TPS = 2,400,000 / `timing.json.gateway_s`. It excludes the post-run
LSM drain and checkpoint.** These times remain separate columns in the CSV.

| Setup | Trial 1 TPS | Trial 2 TPS |
|---|---:|---:|
| Det, no Merkle | 8,641 | 8,223 |
| Merkle, no lookup index (ablation) | 6,380 | — |
| Merkle + B-tree lookup | 3,405 | 3,027 |
| Merkle + LSM: 3 levels, L0 1 MiB, ratio 16 | 3,963 | 3,524 |
| Merkle + LSM: 3 levels, L0 4 MiB, ratio 64 | 5,209 | — |
| Same 4 MiB / 64 layout, experimental bulk merge | 5,013 | — |
| Experimental bulk merge: 4 levels, L0 1 MiB, ratio 16 | 4,919 | — |

Separate runs with CPU/wait profiling measured 6,208 TPS for Merkle without
lookup and 4,869 TPS for the 4 MiB / 64 LSM with the original merge code. These
are marked `profiled=1` in the CSV; do not pool them with the unprofiled trials.

## Files

- [summary.csv](summary.csv): all 12 insert runs, including the two profiled runs;
  configuration, transaction TPS, drain/checkpoint times, case status and evidence path.
- [FINAL_REPORT.md](FINAL_REPORT.md): original detailed report, including configuration
  addenda, I/O, WAL, latency, correctness and profiling. Preserved verbatim; earlier
  hypotheses in the report are followed by later measurements and corrections.
- [evidence/cases/](evidence/cases/): per-case timings, result validation, Merkle
  verification, relation counters, I/O histograms and WAL summaries.
- [evidence/profile/](evidence/profile/): timings, PASS records, wait samples and
  text perf reports for the two profiled runs.
- [update_summary.csv](update_summary.csv): six supplementary update-only audit runs.
- [evidence/controller/status_final.txt](evidence/controller/status_final.txt): final
  remote execution log, including the configuration experiments and profiled rerun.
- [harness/](harness/), [analysis/](analysis/): snapshots of the audit and analysis
  scripts. They retain their original remote paths and are reference artifacts.
- [experimental_source/merge.c](experimental_source/merge.c): bulk-merge source
  snapshot from the isolated audit checkout; archived for reference, not installed
  into the main engine by this archival operation.
- [COPY_PROVENANCE.json](COPY_PROVENANCE.json): source paths and hashes of copied files.
- [SHA256SUMS](SHA256SUMS): SHA-256 hashes of every other file in this archive.

## Qualifications

- The original comparison has two trials per setup. Configuration follow-ups and
  the no-lookup ablation have one trial each. The original second trials were
  5–13% slower; the follow-ups do not establish a reliable ranking among 5,209,
  5,013 and 4,919 TPS.
- The no-lookup case kept Merkle maintenance enabled and had zero splits. It
  isolates lookup maintenance cost for this workload; it is not a general
  replacement for the lookup index used by splits and repair.
- `insert_lsm_t1_t4r64_bulk` completed its timed workload and recorded Merkle verify
  `t` and equal lookup/heap counts. Its subsequent diagnostic probe timed out:
  50K probe inserts did not fill the 4 MiB L0. The archive retains `FAILED.txt`,
  has no PASS record for that case, and marks it `POST_RUN_DIAGNOSTIC_FAILED`.
- Larger LSM levels deferred base merges beyond the timed workload. This is a
  transaction-throughput measurement at the stated run length, not a sustained
  compaction throughput claim.
- Profiling found most sampled active backends waiting on WAL commit flushing.
  Use the report's final profiling addendum when interpreting the earlier I/O
  hypotheses; the relative contribution of each cause is bounded by these runs.

## Original locations and chat

Local source: `.bench_tmp/lsm_relation_audit_20261021/agents/`. Original files are
preserved. The full raw traces, workload files and binary perf data are not copied
into this archive; remote originals remain at:

```text
10.129.27.111:/home/neel/claude_ctl/lsm_relation_audit_20261021/
10.129.148.247:/home/neel/claude_checks/lsm_relation_audit_20261021/
10.129.148.247:/home/neel/claude_checks/lsm_bulkmerge_20261022/
```

The Claude Code conversation recording the comparisons and follow-ups is session
`97ee2aad-b660-4ed9-8267-565b8d13cf9b`, stored locally at
`/home/neel/.claude/projects/-work-ARIABC-AriaBC/97ee2aad-b660-4ed9-8267-565b8d13cf9b.jsonl`.
The private conversation itself is not included in the archive.
