# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / a / 0.0 / 1 | 1 | 1413.53 | 1413.53 | 1413.53 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 16 | 1 | 4971.41 | 4971.41 | 4971.41 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 4 | 1 | 2783.19 | 2783.19 | 2783.19 | unknown | insufficient_repeats |
| bcdb_det / a / 0.0 / 8 | 1 | 3741.81 | 3741.81 | 3741.81 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 1 | 1 | 1980.00 | 1980.00 | 1980.00 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 16 | 1 | 6828.27 | 6828.27 | 6828.27 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 4 | 1 | 3584.87 | 3584.87 | 3584.87 | unknown | insufficient_repeats |
| bcdb_det / a / 0.99 / 8 | 1 | 5234.23 | 5234.23 | 5234.23 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 1 | 1 | 2495.94 | 2495.94 | 2495.94 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 16 | 1 | 7309.94 | 7309.94 | 7309.94 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 4 | 1 | 4428.70 | 4428.70 | 4428.70 | unknown | insufficient_repeats |
| bcdb_det / a / 1.2 / 8 | 1 | 6155.74 | 6155.74 | 6155.74 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 1 | 1 | 3757.28 | 3757.28 | 3757.28 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 16 | 1 | 12247.40 | 12247.40 | 12247.40 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 4 | 1 | 8093.89 | 8093.89 | 8093.89 | unknown | insufficient_repeats |
| bcdb_det / b / 0.99 / 8 | 1 | 10256.41 | 10256.41 | 10256.41 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 1 | 1 | 4137.36 | 4137.36 | 4137.36 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 16 | 1 | 17316.02 | 17316.02 | 17316.02 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 4 | 1 | 9341.43 | 9341.43 | 9341.43 | unknown | insufficient_repeats |
| bcdb_det / c / 0.99 / 8 | 1 | 13568.52 | 13568.52 | 13568.52 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 1 | 1 | 3855.79 | 3855.79 | 3855.79 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 16 | 1 | 12690.36 | 12690.36 | 12690.36 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 4 | 1 | 8058.02 | 8058.02 | 8058.02 | unknown | insufficient_repeats |
| bcdb_det / d / 0.99 / 8 | 1 | 10666.67 | 10666.67 | 10666.67 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 1 | 1 | 1828.49 | 1828.49 | 1828.49 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 16 | 1 | 6772.77 | 6772.77 | 6772.77 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 4 | 1 | 3480.68 | 3480.68 | 3480.68 | unknown | insufficient_repeats |
| bcdb_det / f / 0.99 / 8 | 1 | 4904.36 | 4904.36 | 4904.36 | unknown | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 1 | 3 | 1319.00 | 1312.51 | 1323.63 | 0.42 | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 16 | 3 | 4663.09 | 4607.23 | 4935.83 | 3.71 | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 4 | 3 | 2605.86 | 2601.46 | 2616.77 | 0.30 | insufficient_repeats |
| bcdb_merkle / a / 0.0 / 8 | 3 | 3723.70 | 3660.32 | 3741.81 | 1.15 | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 1 | 3 | 1797.75 | 1781.10 | 1856.84 | 2.20 | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 16 | 3 | 6437.08 | 6159.53 | 6510.42 | 2.91 | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 4 | 3 | 3410.06 | 3324.47 | 3442.93 | 1.80 | insufficient_repeats |
| bcdb_merkle / a / 0.99 / 8 | 3 | 4602.99 | 4573.52 | 4774.41 | 2.33 | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 1 | 3 | 2279.72 | 2254.54 | 2289.64 | 0.80 | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 16 | 3 | 6119.95 | 5950.61 | 6404.10 | 3.72 | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 4 | 3 | 4018.49 | 4015.26 | 4022.53 | 0.09 | insufficient_repeats |
| bcdb_merkle / a / 1.2 / 8 | 3 | 5402.49 | 1908.76 | 5437.74 | 47.71 | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 1 | 3 | 3471.02 | 3470.41 | 3517.41 | 0.77 | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 16 | 3 | 11461.32 | 11331.44 | 11474.47 | 0.69 | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 4 | 3 | 7352.94 | 7342.14 | 7443.25 | 0.75 | insufficient_repeats |
| bcdb_merkle / b / 0.99 / 8 | 3 | 9573.96 | 9560.23 | 9770.40 | 1.22 | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 1 | 3 | 4105.93 | 4043.67 | 4122.86 | 1.02 | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 16 | 3 | 16992.35 | 16792.61 | 17167.38 | 1.10 | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 4 | 3 | 9402.91 | 9225.09 | 9416.20 | 1.14 | insufficient_repeats |
| bcdb_merkle / c / 0.99 / 8 | 3 | 13297.87 | 13003.90 | 13315.58 | 1.33 | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 1 | 3 | 3327.79 | 3320.05 | 3333.33 | 0.20 | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 16 | 3 | 10509.72 | 10493.18 | 10621.35 | 0.66 | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 4 | 3 | 6781.96 | 6777.36 | 6939.63 | 1.35 | insufficient_repeats |
| bcdb_merkle / d / 0.99 / 8 | 3 | 8729.81 | 8714.60 | 8988.76 | 1.75 | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 1 | 3 | 1738.37 | 1722.65 | 1745.05 | 0.66 | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 16 | 3 | 6319.12 | 6045.95 | 6365.37 | 2.76 | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 4 | 3 | 3254.15 | 3155.07 | 3354.02 | 3.06 | insufficient_repeats |
| bcdb_merkle / f / 0.99 / 8 | 3 | 4646.84 | 4518.75 | 4675.08 | 1.81 | insufficient_repeats |
