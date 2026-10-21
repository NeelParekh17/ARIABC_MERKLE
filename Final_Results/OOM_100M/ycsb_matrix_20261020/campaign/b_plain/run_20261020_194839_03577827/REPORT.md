# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| b | 0.0 | bcdb_det | 16 | 1 | 8670.11 | 8670.11 | 8670.11 | 22051.41 |
| b | 0.0 | pg | 16 | 1 | 9791.96 | 9791.96 | 9791.96 | 21560.27 |
| b | 0.5 | bcdb_det | 16 | 1 | 9979.96 | 9979.96 | 9979.96 | 19814.21 |
| b | 0.5 | pg | 16 | 1 | 10228.44 | 10228.44 | 10228.44 | 19840.48 |
| b | 0.99 | bcdb_det | 16 | 1 | 14919.16 | 14919.16 | 14919.16 | 5828.13 |
| b | 0.99 | pg | 16 | 1 | 29361.39 | 29361.39 | 29361.39 | 5805.27 |
| b | 1.2 | bcdb_det | 16 | 1 | 21907.80 | 21907.80 | 21907.80 | 1291.56 |
| b | 1.2 | pg | 16 | 1 | 38507.20 | 38507.20 | 38507.20 | 1289.11 |
