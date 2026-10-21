# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / c / 0.0 / 16 | 1 | 16370.85 | 16370.85 | 16370.85 | unknown | insufficient_repeats |
| bcdb_det / c / 0.5 / 16 | 1 | 17402.78 | 17402.78 | 17402.78 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 16 | 1 | 27457.44 | 27457.44 | 27457.44 | unknown | insufficient_repeats |
| bcdb_det / c / 1.2 / 16 | 1 | 35305.54 | 35305.54 | 35305.54 | unknown | insufficient_repeats |
| pg / c / 0.0 / 16 | 1 | 20674.68 | 20674.68 | 20674.68 | unknown | insufficient_repeats |
| pg / c / 0.5 / 16 | 1 | 22237.46 | 22237.46 | 22237.46 | unknown | insufficient_repeats |
| pg / c / 0.99 / 16 | 1 | 46737.16 | 46737.16 | 46737.16 | unknown | insufficient_repeats |
| pg / c / 1.2 / 16 | 1 | 55050.92 | 55050.92 | 55050.92 | unknown | insufficient_repeats |
