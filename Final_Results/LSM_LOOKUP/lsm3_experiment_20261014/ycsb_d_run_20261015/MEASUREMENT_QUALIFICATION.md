# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| merkle_btree / d / 0.99 / 16 | 2 | 10543.33 | 10293.36 | 10793.31 | 3.35 | insufficient_repeats |
| merkle_lsm / d / 0.99 / 16 | 2 | 11464.61 | 11454.75 | 11474.47 | 0.12 | insufficient_repeats |
