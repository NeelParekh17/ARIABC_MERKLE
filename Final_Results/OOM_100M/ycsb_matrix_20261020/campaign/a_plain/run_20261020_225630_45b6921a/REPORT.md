# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_det | 16 | 1 | 2829.12 | 2829.12 | 2829.12 | 21907.27 |
| a | 0.0 | pg | 16 | 1 | 2935.21 | 2935.21 | 2935.21 | 22030.44 |
| a | 0.5 | bcdb_det | 16 | 1 | 2772.10 | 2772.10 | 2772.10 | 20504.39 |
| a | 0.5 | pg | 16 | 1 | 3437.76 | 3437.76 | 3437.76 | 20332.66 |
| a | 0.99 | bcdb_det | 16 | 1 | 4882.58 | 4882.58 | 4882.58 | 5823.96 |
| a | 0.99 | pg | 16 | 1 | 7428.39 | 7428.39 | 7428.39 | 5842.72 |
| a | 1.2 | bcdb_det | 16 | 1 | 5916.62 | 5916.62 | 5916.62 | 1359.59 |
| a | 1.2 | pg | 16 | 1 | 4339.67 | 4339.67 | 4339.67 | 1538.09 |
