# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / c / 0.0 / 16 | 1 | 16537.01 | 16537.01 | 16537.01 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.5 / 16 | 1 | 17326.52 | 17326.52 | 17326.52 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 16 | 1 | 27217.36 | 27217.36 | 27217.36 | unknown | insufficient_repeats |
| bcdb_merkle / c / 1.2 / 16 | 1 | 35560.82 | 35560.82 | 35560.82 | unknown | insufficient_repeats |
