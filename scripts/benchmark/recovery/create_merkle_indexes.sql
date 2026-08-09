SET enable_merkle_index = on;

DROP INDEX IF EXISTS healthy.usertable_merkle_idx;
DROP INDEX IF EXISTS damaged.usertable_merkle_idx;

CREATE INDEX usertable_merkle_idx
ON healthy.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold, partitions = :partitions);

CREATE INDEX usertable_merkle_idx
ON damaged.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold, partitions = :partitions);

CREATE INDEX usertable_merkle_partition_lookup_idx
ON healthy.usertable
(
  merkle_partition_for_hash(merkle_key_hash(ycsb_key), :partitions),
  merkle_key_hash(ycsb_key),
  ycsb_key
);

CREATE INDEX usertable_merkle_partition_lookup_idx
ON damaged.usertable
(
  merkle_partition_for_hash(merkle_key_hash(ycsb_key), :partitions),
  merkle_key_hash(ycsb_key),
  ycsb_key
);

ANALYZE healthy.usertable;
ANALYZE damaged.usertable;
