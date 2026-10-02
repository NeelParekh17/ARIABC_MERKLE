# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.99 / 1 | 1 | 2157.73 | 2157.73 | 2157.73 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 16 | 1 | 8309.10 | 8309.10 | 8309.10 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 1880.41 | 1880.41 | 1880.41 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 7920.79 | 7920.79 | 7920.79 | unknown | insufficient_repeats |
