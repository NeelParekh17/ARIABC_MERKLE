# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| d | 0.99 | merkle_btree | 16 | 2 | 10543.33 | 10293.36 | 10793.31 | 170.12 |
| d | 0.99 | merkle_lsm | 16 | 2 | 11464.61 | 11454.75 | 11474.47 | 155.71 |
