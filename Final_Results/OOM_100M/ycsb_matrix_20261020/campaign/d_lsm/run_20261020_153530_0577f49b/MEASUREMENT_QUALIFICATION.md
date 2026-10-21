# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / d / 0.0 / 16 | 1 | 11432.76 | 11432.76 | 11432.76 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.5 / 16 | 1 | 11897.56 | 11897.56 | 11897.56 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 1 | 17751.48 | 17751.48 | 17751.48 | unknown | insufficient_repeats |
| bcdb_merkle / d / 1.2 / 16 | 1 | 22299.86 | 22299.86 | 22299.86 | unknown | insufficient_repeats |
