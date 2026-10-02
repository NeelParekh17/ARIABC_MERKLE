# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_det | 1 | 1 | 1295.34 | 1295.34 | 1295.34 | 322.32 |
| a | 0.0 | bcdb_det | 16 | 1 | 6416.43 | 6416.43 | 6416.43 | 322.32 |
| f | 0.99 | bcdb_det | 1 | 1 | 1757.62 | 1757.62 | 1757.62 | 142.08 |
| f | 0.99 | bcdb_det | 16 | 1 | 7047.22 | 7047.22 | 7047.22 | 141.87 |
