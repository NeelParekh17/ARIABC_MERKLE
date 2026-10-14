# Merkle fanout, live recovery, and cluster throughput

This campaign measures a single source snapshot on the lab cluster. The existing
published recovery configuration is **F32 / S1024 / M256 / P200**: 1024 is the
leaf split threshold, not fanout. Current `merkle.c` accepts power-of-two fanout
from 2 through 256. Do not try 512 or 1024 with this implementation.

The published 12,000-row geometry contains 200 leaf roots and no internal nodes.
Retain that case for comparison, and add 4,000,000 rows with the same 200
partitions, split threshold 1024, and merge threshold 256. Expected uniform
occupancy is approximately 20,000 rows/partition: F16 should need two split
levels and F32–256 usually one. Actual pre/post geometry is collected on every
replica; the large case is rejected if internal nodes are absent.

## Fixed experiment

| Dimension | Values |
|---|---|
| Fanout | 16, 32, 64, 128, 256 |
| Initial rows | 12,000; 4,000,000 |
| Repetitions | 3, rotating fanout and scenario order; size order alternates |
| A | Recovery off, no fault: a separate baseline for every size/fanout |
| B | Both recovery detectors enabled, no fault |
| M_passive | Follower node 4, 100 UPDATE faults, periodic Merkle detection |
| M_mixed | Follower node 4, 33 UPDATE + 33 DELETE + 34 INSERT faults |
| L_mix | Leader node 1, the same mixed fault types/count |
| Workload | 160,000 YCSB-A statements, uniform keys, 96 client lanes |
| Geometry | 200 partitions, split 1024, merge 256 |
| Durability | fsync, synchronous_commit, full_page_writes on |
| Maintenance | Synchronous direct Merkle maintenance |
| Isolation | SERIALIZABLE, including fault transactions and retries |
| Execution | Controller/gateway on .247; replicas .247/.246/.248 |
| Services | Fresh campaign PGDATA/raft/log paths; ports 5448/9018/8018/8019 |

There are 150 matrix targets plus eight separate endpoint smoke targets
(two sizes × F16/F256 × baseline/passive fault). Smoke measurements are never
included in matrix statistics. The source and all binaries are frozen for the
campaign. Current source differs from the older paper commit, and the gateway
has moved from .111 to .247: compare fresh baselines within this campaign.

For a given size and repetition, the fault keys are fixed across fanouts.
The injector selects them through the primary key rather than sorting the
entire table by random(). Workload operation order and update values are fixed;
the large workload deterministically remaps the small workload's keys uniformly
over 1..4,000,000. Both workload hashes and restore hashes are retained.

## Measurements and acceptance

Keep snapshot-cut, Merkle repair, replay/catch-up, and total recovery times
separate. Report candidate rows, differing leaves/partitions, rows transferred,
upserts/deletes, full-copy fallbacks, tree levels/occupancies/node storage,
majority-visible cluster TPS, all-three audit drain, completion gaps, and
per-node health/profile evidence. `candidate_rows / affected_rows` measures
localization amplification; it is not network byte throughput.

Concurrent workload updates can overwrite injected UPDATE faults before
detection. Interpret repair time with the actual rows repaired and candidate
volume; 100 injected faults do not necessarily leave 100 damaged rows at the
snapshot boundary. Different detection boundaries also change replay backlog.
This is a live-workload sensitivity experiment, not an isolated fixed-damage
repair microbenchmark.

Every accepted run must complete all 160,000 quorum results, pass the all-three
result audit and the post-marker audit, have zero terminal divergence/permanent
failures/missing votes/timeouts, exit cleanly, match the requested geometry and
durability, and have matching counts, roots and heap digests with passing
`merkle_verify()` on all replicas. Final count is initial rows plus the marker.
Fault cases additionally require successful injection and exactly one live
recovery event on the intended target. Passive detection must say
`merkle_compare`; full-table fallback is rejected.

Matrix heap audits stream primary-key-ordered binary COPY into SHA-256, so
checking 4M rows does not allocate one enormous `string_agg` value. Setup and
final heap-readback SSH deadlines are 300 seconds for every fanout; these checks
are outside workload TPS/recovery timing. The all-three result deadline remains
180 seconds. Earlier smoke attempts using the older digest/deadlines are retained.

More than 64 MiB swap I/O on any node during the workload/audit interval is a
predeclared noise flag. Failed and noisy attempts remain preserved and stop
the controller for diagnosis. No attempt is silently discarded or replaced.
Summaries contain all attempts and accepted-only medians, min/max, CV, and TPS
change against the baseline of the same size/fanout. Completion stalls are
measured outcomes and are not themselves an exclusion rule. This tests
controlled replica corruption during live traffic; it is not a crash-recovery
or all-fault correctness proof.

## Running and monitoring

Stage the source into a **fresh** `/home/neel/ariabc_data/fanout_TAG/repo` on
.247, including the small restore, 160k workload fixture, and the historical
`paper_20261010/harness/preparation` templates. Preserve source archive hashes,
Git HEAD, and workspace status/diff in the new campaign's provenance folder.
Preparation creates guarded copies; it never edits the historical campaign.

```bash
python3 repo/scripts/distributed/recovery_fanout/prepare.py --base "$CAMPAIGN" --head "$SOURCE_HEAD"
python3 repo/scripts/distributed/recovery_fanout/build_remote.py --base "$CAMPAIGN"
python3 repo/scripts/distributed/recovery_fanout/deploy.py --base "$CAMPAIGN"
python3 "$CAMPAIGN/controller.py" --base "$CAMPAIGN" --phase smoke
# Proceed only after all smoke targets are accepted.
python3 "$CAMPAIGN/controller.py" --base "$CAMPAIGN" --phase matrix
```

The build uses the existing Ubuntu 22 image and compatible librdkafka, on .247
only. Deployment initializes only the new campaign PGDATA. The pre-existing
isolated Kafka brokers at port 9192 are reused with unique new topics. No broker
formatting, canonical database cleanup, or workstation execution is performed.

Use `results/status.json`, `results/{smoke,matrix}_attempts.jsonl`,
`results/{smoke,matrix}_all_attempts.csv`, and
`results/{smoke,matrix}_summary.csv` to inspect progress. Raw runs are under
`repo/scripts/bench_full_results/cluster4_fanout_TAG_*`. Each includes geometry
before/after, logs, health samples, prepared harness, and acceptance JSON.
After diagnosis, use `--resume` to skip accepted targets and preserve all old
attempts. A detached supervisor must record its exit status and only launch
the matrix after smoke exits zero.

If shared-node memory pressure causes a noise exclusion, the optional
`wait_and_resume.py --base "$CAMPAIGN"` watcher can wait outside the measured
harness and resume the identical matrix. It requires five minutes with
MemAvailable >=8/4/6 GiB on .247/.246/.248 and <=1 MiB swap I/O per host in that
preflight window by default. `--max-preflight-swap-mib 8` permits up to 8 MiB
of background swap traffic in that window when ample RAM and low pressure
are independently verified; record any override in provenance. It preserves
the matrix's 64 MiB measured-interval noise rule. For the current four-million-row
retry, `--min-available-246-gib 11 --max-preflight-swap-mib 64` requires more
RAM headroom and bounds background swap I/O over the full five-minute window;
the measured-interval exclusion remains 64 MiB. It never changes host settings
or terminates unrelated processes.
Correctness/collection failures or three exclusions at one target require
review. Progress is recorded in `results/resource_wait_status.json` and
`results/resource_wait_samples.jsonl`; successful completion runs `finish.py`
for the final report and PDF/PNG plots.
