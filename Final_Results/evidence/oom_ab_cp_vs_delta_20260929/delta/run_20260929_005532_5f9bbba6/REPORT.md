# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 8 | 2 | 2326.21 | 2172.26 | 2480.16 | 673.82 |
| a | 0.0 | pg | 8 | 2 | 3789.51 | 3762.23 | 3816.79 | 327.32 |
