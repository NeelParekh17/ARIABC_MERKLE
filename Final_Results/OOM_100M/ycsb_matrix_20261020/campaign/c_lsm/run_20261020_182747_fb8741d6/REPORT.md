# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| c | 0.0 | bcdb_merkle | 16 | 1 | 16537.01 | 16537.01 | 16537.01 | 21478.95 |
| c | 0.5 | bcdb_merkle | 16 | 1 | 17326.52 | 17326.52 | 17326.52 | 19774.73 |
| c | 0.99 | bcdb_merkle | 16 | 1 | 27217.36 | 27217.36 | 27217.36 | 5826.57 |
| c | 1.2 | bcdb_merkle | 16 | 1 | 35560.82 | 35560.82 | 35560.82 | 1292.32 |
