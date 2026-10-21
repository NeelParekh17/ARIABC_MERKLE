# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 16 | 1 | 2579.50 | 2579.50 | 2579.50 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.5 / 16 | 1 | 2638.95 | 2638.95 | 2638.95 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 16 | 1 | 4434.35 | 4434.35 | 4434.35 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 16 | 1 | 5546.54 | 5546.54 | 5546.54 | unknown | insufficient_repeats |
