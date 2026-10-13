# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.99 | bcdb_det | 1 | 1 | 1953.51 | 1953.51 | 1953.51 | 214.80 |
| a | 0.99 | bcdb_merkle | 1 | 1 | 1757.47 | 1757.47 | 1757.47 | 176.55 |
| a | 0.99 | pg | 1 | 1 | 2092.05 | 2092.05 | 2092.05 | 146.40 |
