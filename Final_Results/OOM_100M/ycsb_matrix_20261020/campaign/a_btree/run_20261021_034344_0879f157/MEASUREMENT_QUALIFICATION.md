# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 16 | 1 | 2801.59 | 2801.59 | 2801.59 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.5 / 16 | 1 | 2612.81 | 2612.81 | 2612.81 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 16 | 1 | 4765.14 | 4765.14 | 4765.14 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 16 | 1 | 5520.68 | 5520.68 | 5520.68 | unknown | insufficient_repeats |
