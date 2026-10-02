# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_merkle / a / 0.0 / 1 | 1 | 1170.14 | 1170.14 | 1170.14 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 16 | 1 | 3964.32 | 3964.32 | 3964.32 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 4 | 1 | 2302.82 | 2302.82 | 2302.82 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 8 | 1 | 2989.09 | 2989.09 | 2989.09 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 1 | 1 | 1794.20 | 1794.20 | 1794.20 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 16 | 1 | 5934.72 | 5934.72 | 5934.72 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 4 | 1 | 2929.54 | 2929.54 | 2929.54 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 8 | 1 | 4518.75 | 4518.75 | 4518.75 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 1 | 1 | 2266.55 | 2266.55 | 2266.55 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 16 | 1 | 6161.43 | 6161.43 | 6161.43 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 4 | 1 | 3770.74 | 3770.74 | 3770.74 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 8 | 1 | 5064.57 | 5064.57 | 5064.57 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 1 | 1 | 3478.26 | 3478.26 | 3478.26 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 16 | 1 | 11210.76 | 11210.76 | 11210.76 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 4 | 1 | 7471.05 | 7471.05 | 7471.05 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 8 | 1 | 9675.86 | 9675.86 | 9675.86 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 1 | 1 | 3442.93 | 3442.93 | 3442.93 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 1 | 11254.92 | 11254.92 | 11254.92 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 4 | 1 | 7228.04 | 7228.04 | 7228.04 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 8 | 1 | 9337.07 | 9337.07 | 9337.07 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 1 | 1 | 1549.55 | 1549.55 | 1549.55 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 5915.41 | 5915.41 | 5915.41 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 4 | 1 | 2921.84 | 2921.84 | 2921.84 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 8 | 1 | 3954.13 | 3954.13 | 3954.13 | unknown | insufficient_repeats |
