# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| f | 0.0 | bcdb_det | 16 | 1 | 3016.00 | 3016.00 | 3016.00 | 22213.45 |
| f | 0.0 | pg | 16 | 1 | 3014.07 | 3014.07 | 3014.07 | 21979.14 |
| f | 0.5 | bcdb_det | 16 | 1 | 3087.21 | 3087.21 | 3087.21 | 20799.65 |
| f | 0.5 | pg | 16 | 1 | 3358.76 | 3358.76 | 3358.76 | 20392.50 |
| f | 0.99 | bcdb_det | 16 | 1 | 4359.27 | 4359.27 | 4359.27 | 5836.80 |
| f | 0.99 | pg | 16 | 1 | 7086.61 | 7086.61 | 7086.61 | 5847.32 |
| f | 1.2 | bcdb_det | 16 | 1 | 6143.76 | 6143.76 | 6143.76 | 1314.68 |
| f | 1.2 | pg | 16 | 1 | 4125.21 | 4125.21 | 4125.21 | 1538.46 |
