# YCSB measurement interpretation

See MEASUREMENT_QUALIFICATION.md for every matched configuration, spread and sample CV.
PG denotes the PostgreSQL path in the custom AriaBC installation; use per-attempt binary/version evidence.
D reads recently completed inserts via point selects under latest distribution; F executes each read-modify-write as one materialized CTE statement.
These version 5 workloads must not be merged with historical D/F results.
Standalone TPS uses terminal completion wall time. Cluster comparison TPS measures client majority-visible throughput (matching client completion in consensus replication), while all-three follower audit drain metrics are tracked in attempt metadata.
Inspect telemetry, retries, cache evidence, and duration before attributing a ranking to engine overhead.
A successful single-node Merkle check verifies its local index; it does not prove replica agreement or serializability.
A speed ranking is not a correctness invariant. Short runs and high variability need longer independent reruns.

## pg rerun, 2026-09-29 (retry jitter)

The pg rows in `summary.csv` were measured on 2026-09-29. Everything else about the setup
matches the original campaign: same harness flags, byte-identical v5 workloads, same hosts.
pg retries serialization failures with exponential backoff and **full jitter**, capped at
100 ms (`ariabc_pg/src/pg_retry_policy.hxx`). The original pg rows used backoff without
jitter. Retries on a hot row then collided in lockstep, and pg collapsed at θ ≥ 0.99 with
8–16 workers (for example A θ0.99 w16: 3,439 TPS; B θ1.2 w16: 6,757 TPS).

- **pg is 3 trials per point.** The graphs and `summary_median.csv` use the median. det,
  det + Merkle and the cluster keep their original single trials.
- **The other modes still reproduce.** A 32-case subset rerun on 2026-09-29 (A θ0 and θ0.99,
  C θ0.5, F θ1.2 at w4 and w16) reproduced them: cluster within ±3%, Merkle within 7.5%, det
  within 0.7% except C w16 (+5–6%). One det run hit a one-off SSD stall on the DB host and
  was rerun.
- **Gateway clock fix.** After a reboot on 2026-09-28, the gateway host (.111) had switched
  its clocksource from TSC to HPET, and each clock read cost 1.4 µs. That cut pg throughput
  by about 10% at its short, high-TPS points. `tsc=reliable` restored the TSC before the pg
  rerun; clock reads are 67 ns, and gateway signing is back to about 190–350 ns per request.
- **Uncontended pg points** (θ 0 and 0.5) match the original within about ±5%. The one
  exception is B θ0 w8 (−13%); runs of about 0.3 s at more than 40k TPS have about ±10%
  single-trial noise.

Median throughput ratios over the 20 workloads (from `summary_median.csv`):

| Workers | det / pg | det+Merkle / det | cluster / det+Merkle |
|---:|---|---|---|
| 1 | 0.80–0.99 (median 0.90) | 0.89–0.99 | 0.96–1.07 |
| 16 | 0.44–0.79 (median 0.62) | 0.90–1.01 | 0.94–1.03 |

pg keeps scaling at every skew once retries are jittered, so no θ shows det or the cluster
ahead of pg.
