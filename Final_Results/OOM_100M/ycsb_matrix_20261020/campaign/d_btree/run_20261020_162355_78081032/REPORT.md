# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| d | 0.0 | bcdb_merkle | 16 | 1 | 8464.59 | 8464.59 | 8464.59 | 22346.16 |
| d | 0.5 | bcdb_merkle | 16 | 1 | 8728.92 | 8728.92 | 8728.92 | 20496.52 |
| d | 0.99 | bcdb_merkle | 16 | 1 | 11731.29 | 11731.29 | 11731.29 | 6565.69 |
| d | 1.2 | bcdb_merkle | 16 | 1 | 13423.12 | 13423.12 | 13423.12 | 2124.60 |
