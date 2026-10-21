# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / c / 0.0 / 16 | 1 | 16428.90 | 16428.90 | 16428.90 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.5 / 16 | 1 | 17127.44 | 17127.44 | 17127.44 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 16 | 1 | 27210.88 | 27210.88 | 27210.88 | unknown | insufficient_repeats |
| bcdb_merkle / c / 1.2 / 16 | 1 | 35558.19 | 35558.19 | 35558.19 | unknown | insufficient_repeats |
