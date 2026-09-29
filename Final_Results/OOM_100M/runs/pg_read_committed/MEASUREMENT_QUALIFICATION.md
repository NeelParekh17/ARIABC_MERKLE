# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| pg_rc / a / 0.0 / 1 | 1 | 1449.59 | 1449.59 | 1449.59 | unknown | insufficient_repeats |
| pg_rc / a / 0.0 / 16 | 1 | 5982.65 | 5982.65 | 5982.65 | unknown | insufficient_repeats |
| pg_rc / a / 0.0 / 4 | 1 | 2984.18 | 2984.18 | 2984.18 | unknown | insufficient_repeats |
| pg_rc / a / 0.0 / 8 | 1 | 4067.52 | 4067.52 | 4067.52 | unknown | insufficient_repeats |
| pg_rc / a / 0.99 / 1 | 1 | 2086.16 | 2086.16 | 2086.16 | unknown | insufficient_repeats |
| pg_rc / a / 0.99 / 16 | 1 | 11487.65 | 11487.65 | 11487.65 | unknown | insufficient_repeats |
| pg_rc / a / 0.99 / 4 | 1 | 4660.92 | 4660.92 | 4660.92 | unknown | insufficient_repeats |
| pg_rc / a / 0.99 / 8 | 1 | 6646.73 | 6646.73 | 6646.73 | unknown | insufficient_repeats |
| pg_rc / a / 1.2 / 1 | 1 | 2770.08 | 2770.08 | 2770.08 | unknown | insufficient_repeats |
| pg_rc / a / 1.2 / 16 | 1 | 11185.68 | 11185.68 | 11185.68 | unknown | insufficient_repeats |
| pg_rc / a / 1.2 / 4 | 1 | 6397.95 | 6397.95 | 6397.95 | unknown | insufficient_repeats |
| pg_rc / a / 1.2 / 8 | 1 | 9960.16 | 9960.16 | 9960.16 | unknown | insufficient_repeats |
| pg_rc / b / 0.99 / 1 | 1 | 4031.45 | 4031.45 | 4031.45 | unknown | insufficient_repeats |
| pg_rc / b / 0.99 / 16 | 1 | 18639.33 | 18639.33 | 18639.33 | unknown | insufficient_repeats |
| pg_rc / b / 0.99 / 4 | 1 | 12026.46 | 12026.46 | 12026.46 | unknown | insufficient_repeats |
| pg_rc / b / 0.99 / 8 | 1 | 16353.23 | 16353.23 | 16353.23 | unknown | insufficient_repeats |
| pg_rc / c / 0.99 / 1 | 1 | 4733.73 | 4733.73 | 4733.73 | unknown | insufficient_repeats |
| pg_rc / c / 0.99 / 16 | 1 | 34013.61 | 34013.61 | 34013.61 | unknown | insufficient_repeats |
| pg_rc / c / 0.99 / 4 | 1 | 16949.15 | 16949.15 | 16949.15 | unknown | insufficient_repeats |
| pg_rc / c / 0.99 / 8 | 1 | 24271.84 | 24271.84 | 24271.84 | unknown | insufficient_repeats |
| pg_rc / d / 0.99 / 1 | 1 | 4239.98 | 4239.98 | 4239.98 | unknown | insufficient_repeats |
| pg_rc / d / 0.99 / 16 | 1 | 17699.12 | 17699.12 | 17699.12 | unknown | insufficient_repeats |
| pg_rc / d / 0.99 / 4 | 1 | 12062.73 | 12062.73 | 12062.73 | unknown | insufficient_repeats |
| pg_rc / d / 0.99 / 8 | 1 | 16090.10 | 16090.10 | 16090.10 | unknown | insufficient_repeats |
