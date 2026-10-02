# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.99 | bcdb_det | 1 | 1 | 2157.73 | 2157.73 | 2157.73 | 142.16 |
| a | 0.99 | bcdb_det | 16 | 1 | 8309.10 | 8309.10 | 8309.10 | 141.95 |
| f | 0.99 | bcdb_det | 1 | 1 | 1880.41 | 1880.41 | 1880.41 | 142.01 |
| f | 0.99 | bcdb_det | 16 | 1 | 7920.79 | 7920.79 | 7920.79 | 142.36 |
