# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| pg / a / 0.0 / 1 | 1 | 1454.02 | 1454.02 | 1454.02 | unknown | insufficient_repeats |
| pg / a / 0.0 / 16 | 1 | 5961.25 | 5961.25 | 5961.25 | unknown | insufficient_repeats |
| pg / a / 0.0 / 4 | 1 | 2942.47 | 2942.47 | 2942.47 | unknown | insufficient_repeats |
| pg / a / 0.0 / 8 | 1 | 4373.50 | 4373.50 | 4373.50 | unknown | insufficient_repeats |
| pg / a / 0.99 / 1 | 1 | 2208.72 | 2208.72 | 2208.72 | unknown | insufficient_repeats |
| pg / a / 0.99 / 16 | 1 | 9779.95 | 9779.95 | 9779.95 | unknown | insufficient_repeats |
| pg / a / 0.99 / 4 | 1 | 4470.27 | 4470.27 | 4470.27 | unknown | insufficient_repeats |
| pg / a / 0.99 / 8 | 1 | 6961.36 | 6961.36 | 6961.36 | unknown | insufficient_repeats |
| pg / a / 1.2 / 1 | 1 | 2731.87 | 2731.87 | 2731.87 | unknown | insufficient_repeats |
| pg / a / 1.2 / 16 | 1 | 9803.92 | 9803.92 | 9803.92 | unknown | insufficient_repeats |
| pg / a / 1.2 / 4 | 1 | 5849.66 | 5849.66 | 5849.66 | unknown | insufficient_repeats |
| pg / a / 1.2 / 8 | 1 | 7908.26 | 7908.26 | 7908.26 | unknown | insufficient_repeats |
| pg / b / 0.99 / 1 | 1 | 4220.30 | 4220.30 | 4220.30 | unknown | insufficient_repeats |
| pg / b / 0.99 / 16 | 1 | 17683.47 | 17683.47 | 17683.47 | unknown | insufficient_repeats |
| pg / b / 0.99 / 4 | 1 | 11092.62 | 11092.62 | 11092.62 | unknown | insufficient_repeats |
| pg / b / 0.99 / 8 | 1 | 15772.87 | 15772.87 | 15772.87 | unknown | insufficient_repeats |
| pg / c / 0.99 / 1 | 1 | 4511.62 | 4511.62 | 4511.62 | unknown | insufficient_repeats |
| pg / c / 0.99 / 16 | 1 | 32948.93 | 32948.93 | 32948.93 | unknown | insufficient_repeats |
| pg / c / 0.99 / 4 | 1 | 15772.87 | 15772.87 | 15772.87 | unknown | insufficient_repeats |
| pg / c / 0.99 / 8 | 1 | 23866.35 | 23866.35 | 23866.35 | unknown | insufficient_repeats |
| pg / d / 0.99 / 1 | 1 | 4145.08 | 4145.08 | 4145.08 | unknown | insufficient_repeats |
| pg / d / 0.99 / 16 | 1 | 18034.27 | 18034.27 | 18034.27 | unknown | insufficient_repeats |
| pg / d / 0.99 / 4 | 1 | 12217.47 | 12217.47 | 12217.47 | unknown | insufficient_repeats |
| pg / d / 0.99 / 8 | 1 | 15822.78 | 15822.78 | 15822.78 | unknown | insufficient_repeats |
| pg / f / 0.99 / 1 | 1 | 1937.05 | 1937.05 | 1937.05 | unknown | insufficient_repeats |
| pg / f / 0.99 / 16 | 1 | 8768.08 | 8768.08 | 8768.08 | unknown | insufficient_repeats |
| pg / f / 0.99 / 4 | 1 | 4501.46 | 4501.46 | 4501.46 | unknown | insufficient_repeats |
| pg / f / 0.99 / 8 | 1 | 6240.25 | 6240.25 | 6240.25 | unknown | insufficient_repeats |
