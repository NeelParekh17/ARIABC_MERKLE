# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 8 | 2 | 2326.21 | 2172.26 | 2480.16 | 9.36 | insufficient_repeats |
| pg / a / 0.0 / 8 | 2 | 3789.51 | 3762.23 | 3816.79 | 1.02 | insufficient_repeats |
