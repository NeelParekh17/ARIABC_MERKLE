# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / f / 0.0 / 16 | 1 | 2758.74 | 2758.74 | 2758.74 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.5 / 16 | 1 | 2517.11 | 2517.11 | 2517.11 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 4680.73 | 4680.73 | 4680.73 | unknown | insufficient_repeats |
| bcdb_merkle / f / 1.2 / 16 | 1 | 5224.17 | 5224.17 | 5224.17 | unknown | insufficient_repeats |
