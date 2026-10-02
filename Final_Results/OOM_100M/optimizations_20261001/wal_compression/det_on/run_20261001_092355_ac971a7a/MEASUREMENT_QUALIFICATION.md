# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 1 | 1 | 1295.34 | 1295.34 | 1295.34 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 16 | 1 | 6416.43 | 6416.43 | 6416.43 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 1757.62 | 1757.62 | 1757.62 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 7047.22 | 7047.22 | 7047.22 | unknown | insufficient_repeats |
