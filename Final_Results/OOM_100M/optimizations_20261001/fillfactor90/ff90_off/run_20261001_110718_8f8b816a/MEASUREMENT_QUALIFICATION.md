# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 1 | 1 | 1431.95 | 1431.95 | 1431.95 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 16 | 1 | 4944.38 | 4944.38 | 4944.38 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 1878.11 | 1878.11 | 1878.11 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 7363.77 | 7363.77 | 7363.77 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 1 | 1 | 1300.05 | 1300.05 | 1300.05 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 16 | 1 | 4501.46 | 4501.46 | 4501.46 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 1 | 1 | 1614.73 | 1614.73 | 1614.73 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 5629.05 | 5629.05 | 5629.05 | unknown | insufficient_repeats |
