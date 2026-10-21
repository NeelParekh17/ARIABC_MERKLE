# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| b | 0.0 | bcdb_merkle | 16 | 1 | 7519.60 | 7519.60 | 7519.60 | 22288.84 |
| b | 0.5 | bcdb_merkle | 16 | 1 | 7786.52 | 7786.52 | 7786.52 | 20952.35 |
| b | 0.99 | bcdb_merkle | 16 | 1 | 15987.00 | 15987.00 | 15987.00 | 5846.66 |
| b | 1.2 | bcdb_merkle | 16 | 1 | 19431.31 | 19431.31 | 19431.31 | 1332.27 |
