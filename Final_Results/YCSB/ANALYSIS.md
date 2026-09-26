# YCSB measurement interpretation

See MEASUREMENT_QUALIFICATION.md for every matched configuration, spread and sample CV.
PG denotes the PostgreSQL path in the custom AriaBC installation; use per-attempt binary/version evidence.
D reads recently completed inserts via point selects under latest distribution; F executes each read-modify-write as one materialized CTE statement.
These version 5 workloads must not be merged with historical D/F results.
Standalone TPS uses terminal completion wall time; cluster TPS includes the all-three audit drain.
Majority-visible throughput is retained separately in cluster attempt metadata.
Inspect telemetry, retries, cache evidence, and duration before attributing a ranking to engine overhead.
A successful single-node Merkle check verifies its local index; it does not prove replica agreement or serializability.
A speed ranking is not a correctness invariant. Short runs and high variability need longer independent reruns.
