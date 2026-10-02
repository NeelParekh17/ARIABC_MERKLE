# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_det | 1 | 1 | 1431.95 | 1431.95 | 1431.95 | 327.31 |
| a | 0.0 | bcdb_det | 16 | 1 | 4944.38 | 4944.38 | 4944.38 | 327.57 |
| a | 0.0 | bcdb_merkle | 1 | 1 | 1300.05 | 1300.05 | 1300.05 | 394.16 |
| a | 0.0 | bcdb_merkle | 16 | 1 | 4501.46 | 4501.46 | 4501.46 | 394.05 |
| f | 0.99 | bcdb_det | 1 | 1 | 1878.11 | 1878.11 | 1878.11 | 150.65 |
| f | 0.99 | bcdb_det | 16 | 1 | 7363.77 | 7363.77 | 7363.77 | 150.77 |
| f | 0.99 | bcdb_merkle | 1 | 1 | 1614.73 | 1614.73 | 1614.73 | 216.84 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 5629.05 | 5629.05 | 5629.05 | 217.02 |
