# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.99 / 1 | 1 | 1953.51 | 1953.51 | 1953.51 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 1 | 1 | 1757.47 | 1757.47 | 1757.47 | unknown | insufficient_repeats |
| pg / a / 0.99 / 1 | 1 | 2092.05 | 2092.05 | 2092.05 | unknown | insufficient_repeats |
