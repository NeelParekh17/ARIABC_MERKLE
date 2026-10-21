# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / f / 0.0 / 16 | 1 | 3016.00 | 3016.00 | 3016.00 | unknown | insufficient_repeats |
| bcdb_det / f / 0.5 / 16 | 1 | 3087.21 | 3087.21 | 3087.21 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 4359.27 | 4359.27 | 4359.27 | unknown | insufficient_repeats |
| bcdb_det / f / 1.2 / 16 | 1 | 6143.76 | 6143.76 | 6143.76 | unknown | insufficient_repeats |
| pg / f / 0.0 / 16 | 1 | 3014.07 | 3014.07 | 3014.07 | unknown | insufficient_repeats |
| pg / f / 0.5 / 16 | 1 | 3358.76 | 3358.76 | 3358.76 | unknown | insufficient_repeats |
| pg / f / 0.99 / 16 | 1 | 7086.61 | 7086.61 | 7086.61 | unknown | insufficient_repeats |
| pg / f / 1.2 / 16 | 1 | 4125.21 | 4125.21 | 4125.21 | unknown | insufficient_repeats |
