# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / f / 0.0 / 16 | 1 | 2815.11 | 2815.11 | 2815.11 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.5 / 16 | 1 | 2487.90 | 2487.90 | 2487.90 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 4681.58 | 4681.58 | 4681.58 | unknown | insufficient_repeats |
| bcdb_merkle / f / 1.2 / 16 | 1 | 5428.18 | 5428.18 | 5428.18 | unknown | insufficient_repeats |
