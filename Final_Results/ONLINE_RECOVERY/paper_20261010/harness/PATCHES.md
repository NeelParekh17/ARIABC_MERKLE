# RECOV-1 campaign harness changes

The PostgreSQL and AriaBC engine sources are unchanged at
`16577072ecf888fbbbb5d80d0e90fc340c8ca342`. No commit was made.

Generated harness patches are stored in the publication `launch_evidence/`
and on `.111:/home/neel/ariabc_data/paper_20261010_recov/`:

- `cluster_runner_paper_20261010_recov.diff`: the existing S1024 generator's
  isolated paths and SERIALIZABLE copies of both the connection helper and
  fault injector. Whole-transaction serialization retries remain intact.
- `generator_adaptation.diff`: permit the campaign's persistent `ariabc_data`
  directory rather than its older Desktop path.
- `guardrail_adaptation.diff`: redirect remaining canonical paths; replace
  port-wide process killing with a recorded PID and executable check; remove
  the user-wide PostgreSQL cleanup function; record gateway exit explicitly;
  give servers a campaign working directory; use isolated Kafka port 9192.
- `injector_jitter.diff`: replace the old injector's linear retry sleep with
  capped exponential full-jitter backoff, retaining ten whole-transaction
  attempts and SERIALIZABLE. This implements the campaign's retry rule.
- `geometry_current.diff`: the archived catalog helper assumed node-table names
  based on index OID. Frozen code uses `merkle_node_usertable_small` for the
  public workload table. Query that actual native table and record durability
  settings. Smoke A attempt 3 finished 160000 completions and passed all-three
  final audits but is retained as rejected because catalog collection failed.
  A new complete smoke attempt is used for launch qualification.

New preparation/controller scripts are in `preparation/`. They build in an
Ubuntu 22.04 container on .247, use `--enable-depend` and optimized builds,
stage the exact tracked libpq `exports.list` into its out-of-tree build,
verify installed binary hashes, initialize new PGDATA, collect health and
all-three audits, rotate scenario order, and resume only accepted runs.

`C_deep` enables existing gate telemetry, PostgreSQL block profiles and
per-transaction phase tracing plus OS profiling. This is a separately labeled
diagnostic variant; it is not a recreation of an undocumented old tracing
configuration. Other scenarios retain the common lightweight health sampler.

An independent Kafka cluster uses existing Kafka 3.7.0 software with new
configuration, cluster identity, log directories, topics, PIDs and ports.
Broker heaps are fixed at 512 MiB on all replicas. This differs from historical
shared-broker conditions and must be reported with the measurements.

External build dependencies: the container image digest, portable CMake
3.28.3 archive/hash, and Ubuntu 22-compatible librdkafka from .246 are recorded.
The first copied .247 librdkafka required GLIBC 2.38 and is preserved under
`rdkafka_u24_attempt3`; it is not used by the accepted build.

Preparation failures are preserved: initial transfer ordering; the local
clone's borrowed Git object path; CMake 3.22 being below NuRaft's 3.26 minimum;
and a retry before the portable CMake finished staging. None is a measured
trial. Successful builds verify identical source fingerprints before/after.

Smoke A attempt 1 stopped during setup, before running any transactions,
because the clone excludes the untracked small restore and 160k workload.
Both fixtures were copied unchanged from the canonical workspace after their
SHA-256 hashes matched `runs_s1024/launch_evidence/experiment_inputs.sha256`
exactly. Their hashes are recorded in the new provenance. The incomplete run
and failed audits remain raw; the next smoke attempt uses a fresh run/topic.

Smoke A attempt 2 executed only its first submission window and completed
zero quorum results. The original runner overwrites colocated Kafka bootstrap
with literal 9092 endpoints twice, overriding the requested port. The generated
copy now redirects both gateway bootstrap assignments as well as server
bootstrap to 9192. That failed attempt, watchdog evidence, and its old harness
copy are retained; they are not paper throughput results.

Smoke C attempt 1 repaired successfully but its gateway audit rejected 14483
late all-three votes under the old 60-second result deadline. The run remains
rejected. Matrix configuration uses the existing gateway flags
`--pollIntervalUs 500 --pollCount 360000` (180-second deadline), held fixed for
every scenario. It does not relax final audit, divergence or terminal success
requirements and does not change engine source. The generated watchdog now
counts committed-watermark advancement on replicas as progress during live
catchup; client-visible completions can already be complete during that stage.
Health readbacks have a bounded timeout. Repair/catchup delays and result-audit
drain times remain reported rather than being hidden by these bounds.

Noise rule is held fixed across scenarios: more than 64 MiB cumulative swap
input plus output on any host within the recorded gateway/all-three-drain
interval excludes a otherwise-valid attempt and schedules an extra attempt.
Failed correctness/provenance/audit trials are rejected and retained. Recovery
completion gaps are reported as measured behavior and are not themselves a
noise exclusion. Load, CPU/disk counters, clocksource, memory and replica lag
remain raw evidence for phase 2 review.

Smoke C attempt 1 also exhausted .248 memory and its 4 GiB swap; health recorded approximately 3.3 GiB swap IO. The host subsequently rebooted externally (the campaign issued no reboot). Its failed audits and missing post-reboot geometry remain rejected. SSH and geometry collection are now bounded, preventing a disconnected host from hanging the controller. Only the campaign Kafka instance is restarted with its existing identity/data/configuration.
