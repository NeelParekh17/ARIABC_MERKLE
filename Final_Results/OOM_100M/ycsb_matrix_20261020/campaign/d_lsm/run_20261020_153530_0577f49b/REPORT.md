# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| d | 0.0 | bcdb_merkle | 16 | 1 | 11432.76 | 11432.76 | 11432.76 | 20803.47 |
| d | 0.5 | bcdb_merkle | 16 | 1 | 11897.56 | 11897.56 | 11897.56 | 19221.32 |
| d | 0.99 | bcdb_merkle | 16 | 1 | 17751.48 | 17751.48 | 17751.48 | 5639.89 |
| d | 1.2 | bcdb_merkle | 16 | 1 | 22299.86 | 22299.86 | 22299.86 | 1261.82 |
