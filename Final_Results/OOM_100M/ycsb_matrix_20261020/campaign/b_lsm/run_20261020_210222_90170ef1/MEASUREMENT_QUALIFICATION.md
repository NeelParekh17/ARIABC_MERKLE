# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / b / 0.0 / 16 | 1 | 7519.60 | 7519.60 | 7519.60 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.5 / 16 | 1 | 7786.52 | 7786.52 | 7786.52 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 16 | 1 | 15987.00 | 15987.00 | 15987.00 | unknown | insufficient_repeats |
| bcdb_merkle / b / 1.2 / 16 | 1 | 19431.31 | 19431.31 | 19431.31 | unknown | insufficient_repeats |
