# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 16 | 1 | 2829.12 | 2829.12 | 2829.12 | unknown | insufficient_repeats |
| bcdb_det / a / 0.5 / 16 | 1 | 2772.10 | 2772.10 | 2772.10 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 16 | 1 | 4882.58 | 4882.58 | 4882.58 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 16 | 1 | 5916.62 | 5916.62 | 5916.62 | unknown | insufficient_repeats |
| pg / a / 0.0 / 16 | 1 | 2935.21 | 2935.21 | 2935.21 | unknown | insufficient_repeats |
| pg / a / 0.5 / 16 | 1 | 3437.76 | 3437.76 | 3437.76 | unknown | insufficient_repeats |
| pg / a / 0.99 / 16 | 1 | 7428.39 | 7428.39 | 7428.39 | unknown | insufficient_repeats |
| pg / a / 1.2 / 16 | 1 | 4339.67 | 4339.67 | 4339.67 | unknown | insufficient_repeats |
