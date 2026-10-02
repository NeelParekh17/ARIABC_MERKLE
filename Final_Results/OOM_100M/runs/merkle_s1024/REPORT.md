# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 1 | 1 | 1170.14 | 1170.14 | 1170.14 | 444.36 |
| a | 0.0 | bcdb_merkle | 4 | 1 | 2302.82 | 2302.82 | 2302.82 | 444.29 |
| a | 0.0 | bcdb_merkle | 8 | 1 | 2989.09 | 2989.09 | 2989.09 | 444.53 |
| a | 0.0 | bcdb_merkle | 16 | 1 | 3964.32 | 3964.32 | 3964.32 | 444.01 |
| a | 0.99 | bcdb_merkle | 1 | 1 | 1794.20 | 1794.20 | 1794.20 | 225.79 |
| a | 0.99 | bcdb_merkle | 4 | 1 | 2929.54 | 2929.54 | 2929.54 | 226.05 |
| a | 0.99 | bcdb_merkle | 8 | 1 | 4518.75 | 4518.75 | 4518.75 | 225.78 |
| a | 0.99 | bcdb_merkle | 16 | 1 | 5934.72 | 5934.72 | 5934.72 | 225.63 |
| a | 1.2 | bcdb_merkle | 1 | 1 | 2266.55 | 2266.55 | 2266.55 | 85.33 |
| a | 1.2 | bcdb_merkle | 4 | 1 | 3770.74 | 3770.74 | 3770.74 | 85.39 |
| a | 1.2 | bcdb_merkle | 8 | 1 | 5064.57 | 5064.57 | 5064.57 | 85.69 |
| a | 1.2 | bcdb_merkle | 16 | 1 | 6161.43 | 6161.43 | 6161.43 | 85.89 |
| b | 0.99 | bcdb_merkle | 1 | 1 | 3478.26 | 3478.26 | 3478.26 | 165.24 |
| b | 0.99 | bcdb_merkle | 4 | 1 | 7471.05 | 7471.05 | 7471.05 | 165.32 |
| b | 0.99 | bcdb_merkle | 8 | 1 | 9675.86 | 9675.86 | 9675.86 | 165.36 |
| b | 0.99 | bcdb_merkle | 16 | 1 | 11210.76 | 11210.76 | 11210.76 | 165.41 |
| d | 0.99 | bcdb_merkle | 1 | 1 | 3442.93 | 3442.93 | 3442.93 | 167.06 |
| d | 0.99 | bcdb_merkle | 4 | 1 | 7228.04 | 7228.04 | 7228.04 | 167.42 |
| d | 0.99 | bcdb_merkle | 8 | 1 | 9337.07 | 9337.07 | 9337.07 | 167.14 |
| d | 0.99 | bcdb_merkle | 16 | 1 | 11254.92 | 11254.92 | 11254.92 | 167.12 |
| f | 0.99 | bcdb_merkle | 1 | 1 | 1549.55 | 1549.55 | 1549.55 | 225.66 |
| f | 0.99 | bcdb_merkle | 4 | 1 | 2921.84 | 2921.84 | 2921.84 | 225.64 |
| f | 0.99 | bcdb_merkle | 8 | 1 | 3954.13 | 3954.13 | 3954.13 | 225.90 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 5915.41 | 5915.41 | 5915.41 | 226.09 |
