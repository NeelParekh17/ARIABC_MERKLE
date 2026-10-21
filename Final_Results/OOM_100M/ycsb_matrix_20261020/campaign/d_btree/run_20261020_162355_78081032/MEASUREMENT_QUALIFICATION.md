# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / d / 0.0 / 16 | 1 | 8464.59 | 8464.59 | 8464.59 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.5 / 16 | 1 | 8728.92 | 8728.92 | 8728.92 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 1 | 11731.29 | 11731.29 | 11731.29 | unknown | insufficient_repeats |
| bcdb_merkle / d / 1.2 / 16 | 1 | 13423.12 | 13423.12 | 13423.12 | unknown | insufficient_repeats |
