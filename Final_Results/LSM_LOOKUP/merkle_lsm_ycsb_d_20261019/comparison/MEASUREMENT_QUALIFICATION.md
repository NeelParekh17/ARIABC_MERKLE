# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / d / 0.99 / 16 | 3 | 18436.29 | 18422.43 | 18462.96 | 0.11 | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 3 | 13090.72 | 12355.72 | 13160.71 | 3.47 | insufficient_repeats |
| pg / d / 0.99 / 16 | 3 | 30675.63 | 30670.53 | 30720.39 | 0.09 | insufficient_repeats |
