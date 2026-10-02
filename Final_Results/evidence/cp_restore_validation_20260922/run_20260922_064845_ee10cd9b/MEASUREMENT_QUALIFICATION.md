# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.99 / 8 | 1 | 5780.35 | 5780.35 | 5780.35 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 8 | 1 | 5847.95 | 5847.95 | 5847.95 | unknown | insufficient_repeats |
| pg / a / 0.99 / 8 | 1 | 3448.28 | 3448.28 | 3448.28 | unknown | insufficient_repeats |
