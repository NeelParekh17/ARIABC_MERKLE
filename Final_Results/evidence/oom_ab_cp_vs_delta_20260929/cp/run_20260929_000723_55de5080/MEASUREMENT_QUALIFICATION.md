# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 8 | 2 | 2270.46 | 2259.12 | 2281.80 | 0.71 | insufficient_repeats |
| pg / a / 0.0 / 8 | 2 | 3849.09 | 3812.43 | 3885.76 | 1.35 | insufficient_repeats |
