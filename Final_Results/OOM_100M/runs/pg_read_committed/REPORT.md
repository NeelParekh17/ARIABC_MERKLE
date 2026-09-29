# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | pg_rc | 1 | 1 | 1449.59 | 1449.59 | 1449.59 | 327.39 |
| a | 0.0 | pg_rc | 4 | 1 | 2984.18 | 2984.18 | 2984.18 | 327.28 |
| a | 0.0 | pg_rc | 8 | 1 | 4067.52 | 4067.52 | 4067.52 | 327.26 |
| a | 0.0 | pg_rc | 16 | 1 | 5982.65 | 5982.65 | 5982.65 | 327.25 |
| a | 0.99 | pg_rc | 1 | 1 | 2086.16 | 2086.16 | 2086.16 | 147.05 |
| a | 0.99 | pg_rc | 4 | 1 | 4660.92 | 4660.92 | 4660.92 | 146.84 |
| a | 0.99 | pg_rc | 8 | 1 | 6646.73 | 6646.73 | 6646.73 | 146.84 |
| a | 0.99 | pg_rc | 16 | 1 | 11487.65 | 11487.65 | 11487.65 | 146.88 |
| a | 1.2 | pg_rc | 1 | 1 | 2770.08 | 2770.08 | 2770.08 | 45.90 |
| a | 1.2 | pg_rc | 4 | 1 | 6397.95 | 6397.95 | 6397.95 | 45.86 |
| a | 1.2 | pg_rc | 8 | 1 | 9960.16 | 9960.16 | 9960.16 | 45.82 |
| a | 1.2 | pg_rc | 16 | 1 | 11185.68 | 11185.68 | 11185.68 | 45.80 |
| b | 0.99 | pg_rc | 1 | 1 | 4031.45 | 4031.45 | 4031.45 | 147.49 |
| b | 0.99 | pg_rc | 4 | 1 | 12026.46 | 12026.46 | 12026.46 | 147.60 |
| b | 0.99 | pg_rc | 8 | 1 | 16353.23 | 16353.23 | 16353.23 | 147.44 |
| b | 0.99 | pg_rc | 16 | 1 | 18639.33 | 18639.33 | 18639.33 | 147.46 |
| c | 0.99 | pg_rc | 1 | 1 | 4733.73 | 4733.73 | 4733.73 | 147.70 |
| c | 0.99 | pg_rc | 4 | 1 | 16949.15 | 16949.15 | 16949.15 | 147.53 |
| c | 0.99 | pg_rc | 8 | 1 | 24271.84 | 24271.84 | 24271.84 | 147.43 |
| c | 0.99 | pg_rc | 16 | 1 | 34013.61 | 34013.61 | 34013.61 | 147.61 |
| d | 0.99 | pg_rc | 1 | 1 | 4239.98 | 4239.98 | 4239.98 | 141.19 |
| d | 0.99 | pg_rc | 4 | 1 | 12062.73 | 12062.73 | 12062.73 | 140.55 |
| d | 0.99 | pg_rc | 8 | 1 | 16090.10 | 16090.10 | 16090.10 | 140.69 |
| d | 0.99 | pg_rc | 16 | 1 | 17699.12 | 17699.12 | 17699.12 | 140.67 |
