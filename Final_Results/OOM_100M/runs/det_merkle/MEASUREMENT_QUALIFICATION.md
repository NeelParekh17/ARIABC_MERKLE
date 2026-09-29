# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 1 | 1 | 1427.25 | 1427.25 | 1427.25 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 16 | 1 | 4977.60 | 4977.60 | 4977.60 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 4 | 1 | 3078.82 | 3078.82 | 3078.82 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 8 | 1 | 3743.22 | 3743.22 | 3743.22 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 1 | 1 | 2149.38 | 2149.38 | 2149.38 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 16 | 1 | 7296.61 | 7296.61 | 7296.61 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 4 | 1 | 3907.78 | 3907.78 | 3907.78 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 8 | 1 | 5178.66 | 5178.66 | 5178.66 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 1 | 1 | 2641.66 | 2641.66 | 2641.66 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 16 | 1 | 7880.22 | 7880.22 | 7880.22 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 4 | 1 | 4692.63 | 4692.63 | 4692.63 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 8 | 1 | 6651.15 | 6651.15 | 6651.15 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 1 | 1 | 4030.63 | 4030.63 | 4030.63 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 16 | 1 | 13821.70 | 13821.70 | 13821.70 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 4 | 1 | 8357.71 | 8357.71 | 8357.71 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 8 | 1 | 10822.51 | 10822.51 | 10822.51 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 1 | 1 | 4561.00 | 4561.00 | 4561.00 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 16 | 1 | 18298.26 | 18298.26 | 18298.26 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 4 | 1 | 9828.01 | 9828.01 | 9828.01 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 8 | 1 | 14144.27 | 14144.27 | 14144.27 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 1 | 1 | 3932.36 | 3932.36 | 3932.36 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 16 | 1 | 13422.82 | 13422.82 | 13422.82 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 4 | 1 | 8517.89 | 8517.89 | 8517.89 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 8 | 1 | 11191.94 | 11191.94 | 11191.94 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 2013.29 | 2013.29 | 2013.29 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 7499.06 | 7499.06 | 7499.06 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 4 | 1 | 3872.22 | 3872.22 | 3872.22 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 8 | 1 | 5275.65 | 5275.65 | 5275.65 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 1 | 1 | 907.36 | 907.36 | 907.36 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 16 | 1 | 2536.46 | 2536.46 | 2536.46 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 4 | 1 | 1786.35 | 1786.35 | 1786.35 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 8 | 1 | 2165.21 | 2165.21 | 2165.21 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 1 | 1 | 1318.65 | 1318.65 | 1318.65 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 16 | 1 | 3908.54 | 3908.54 | 3908.54 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 4 | 1 | 2272.47 | 2272.47 | 2272.47 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 8 | 1 | 2916.73 | 2916.73 | 2916.73 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 1 | 1 | 1987.87 | 1987.87 | 1987.87 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 16 | 1 | 4531.04 | 4531.04 | 4531.04 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 4 | 1 | 3000.30 | 3000.30 | 3000.30 | unknown | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 8 | 1 | 3768.61 | 3768.61 | 3768.61 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 1 | 1 | 3402.52 | 3402.52 | 3402.52 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 16 | 1 | 10235.41 | 10235.41 | 10235.41 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 4 | 1 | 6578.95 | 6578.95 | 6578.95 | unknown | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 8 | 1 | 8532.42 | 8532.42 | 8532.42 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 1 | 1 | 4244.48 | 4244.48 | 4244.48 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 16 | 1 | 19029.50 | 19029.50 | 19029.50 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 4 | 1 | 10101.01 | 10101.01 | 10101.01 | unknown | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 8 | 1 | 14224.75 | 14224.75 | 14224.75 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 1 | 1 | 3159.56 | 3159.56 | 3159.56 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 1 | 9478.67 | 9478.67 | 9478.67 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 4 | 1 | 6437.08 | 6437.08 | 6437.08 | unknown | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 8 | 1 | 7880.22 | 7880.22 | 7880.22 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 1 | 1 | 1298.53 | 1298.53 | 1298.53 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 1 | 3269.58 | 3269.58 | 3269.58 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 4 | 1 | 2191.06 | 2191.06 | 2191.06 | unknown | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 8 | 1 | 2683.48 | 2683.48 | 2683.48 | unknown | insufficient_repeats |
