# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| c | 0.0 | bcdb_det | 16 | 1 | 16370.85 | 16370.85 | 16370.85 | 21866.04 |
| c | 0.0 | pg | 16 | 1 | 20674.68 | 20674.68 | 20674.68 | 21532.29 |
| c | 0.5 | bcdb_det | 16 | 1 | 17402.78 | 17402.78 | 17402.78 | 19801.52 |
| c | 0.5 | pg | 16 | 1 | 22237.46 | 22237.46 | 22237.46 | 19973.26 |
| c | 0.99 | bcdb_det | 16 | 1 | 27457.44 | 27457.44 | 27457.44 | 5812.20 |
| c | 0.99 | pg | 16 | 1 | 46737.16 | 46737.16 | 46737.16 | 5812.84 |
| c | 1.2 | bcdb_det | 16 | 1 | 35305.54 | 35305.54 | 35305.54 | 1292.02 |
| c | 1.2 | pg | 16 | 1 | 55050.92 | 55050.92 | 55050.92 | 1292.53 |
