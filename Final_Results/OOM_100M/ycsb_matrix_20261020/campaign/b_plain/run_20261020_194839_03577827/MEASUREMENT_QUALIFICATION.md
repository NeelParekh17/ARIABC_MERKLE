# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / b / 0.0 / 16 | 1 | 8670.11 | 8670.11 | 8670.11 | unknown | insufficient_repeats |
| bcdb_det / b / 0.5 / 16 | 1 | 9979.96 | 9979.96 | 9979.96 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 16 | 1 | 14919.16 | 14919.16 | 14919.16 | unknown | insufficient_repeats |
| bcdb_det / b / 1.2 / 16 | 1 | 21907.80 | 21907.80 | 21907.80 | unknown | insufficient_repeats |
| pg / b / 0.0 / 16 | 1 | 9791.96 | 9791.96 | 9791.96 | unknown | insufficient_repeats |
| pg / b / 0.5 / 16 | 1 | 10228.44 | 10228.44 | 10228.44 | unknown | insufficient_repeats |
| pg / b / 0.99 / 16 | 1 | 29361.39 | 29361.39 | 29361.39 | unknown | insufficient_repeats |
| pg / b / 1.2 / 16 | 1 | 38507.20 | 38507.20 | 38507.20 | unknown | insufficient_repeats |
