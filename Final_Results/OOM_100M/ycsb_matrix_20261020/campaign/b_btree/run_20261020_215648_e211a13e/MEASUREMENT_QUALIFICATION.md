# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / b / 0.0 / 16 | 1 | 8519.04 | 8519.04 | 8519.04 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.5 / 16 | 1 | 7782.05 | 7782.05 | 7782.05 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 16 | 1 | 14789.71 | 14789.71 | 14789.71 | unknown | insufficient_repeats |
| bcdb_merkle / b / 1.2 / 16 | 1 | 20774.36 | 20774.36 | 20774.36 | unknown | insufficient_repeats |
