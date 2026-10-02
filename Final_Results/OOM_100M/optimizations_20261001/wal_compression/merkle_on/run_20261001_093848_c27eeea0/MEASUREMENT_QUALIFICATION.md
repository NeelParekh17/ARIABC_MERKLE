# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 1 | 1 | 965.58 | 965.58 | 965.58 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 16 | 1 | 4026.58 | 4026.58 | 4026.58 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 1 | 1 | 1503.76 | 1503.76 | 1503.76 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 5165.29 | 5165.29 | 5165.29 | unknown | insufficient_repeats |
