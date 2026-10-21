# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / d / 0.0 / 16 | 1 | 11720.35 | 11720.35 | 11720.35 | unknown | insufficient_repeats |
| bcdb_det / d / 0.5 / 16 | 1 | 12316.03 | 12316.03 | 12316.03 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 16 | 1 | 18530.10 | 18530.10 | 18530.10 | unknown | insufficient_repeats |
| bcdb_det / d / 1.2 / 16 | 1 | 23640.89 | 23640.89 | 23640.89 | unknown | insufficient_repeats |
| pg / d / 0.0 / 16 | 1 | 13747.20 | 13747.20 | 13747.20 | unknown | insufficient_repeats |
| pg / d / 0.5 / 16 | 1 | 14589.31 | 14589.31 | 14589.31 | unknown | insufficient_repeats |
| pg / d / 0.99 / 16 | 1 | 30914.69 | 30914.69 | 30914.69 | unknown | insufficient_repeats |
| pg / d / 1.2 / 16 | 1 | 46659.93 | 46659.93 | 46659.93 | unknown | insufficient_repeats |
