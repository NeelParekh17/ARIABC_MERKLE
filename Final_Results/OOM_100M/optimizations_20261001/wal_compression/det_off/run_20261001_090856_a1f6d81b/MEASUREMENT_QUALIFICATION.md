# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 1 | 1 | 1587.93 | 1587.93 | 1587.93 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 16 | 1 | 5053.06 | 5053.06 | 5053.06 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 2025.11 | 2025.11 | 2025.11 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 7535.80 | 7535.80 | 7535.80 | unknown | insufficient_repeats |
