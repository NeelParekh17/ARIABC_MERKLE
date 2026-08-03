SET enable_merkle_index = on;

DROP INDEX IF EXISTS healthy.usertable_merkle_covering_idx;
DROP INDEX IF EXISTS damaged.usertable_merkle_covering_idx;
DROP INDEX IF EXISTS healthy.usertable_merkle_idx;
DROP INDEX IF EXISTS damaged.usertable_merkle_idx;

CREATE INDEX usertable_merkle_idx
ON healthy.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold);

CREATE INDEX usertable_merkle_idx
ON damaged.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold);

CREATE INDEX usertable_merkle_covering_idx
ON healthy.usertable
( merkle_key_hash(ycsb_key), merkle_tuple_hash(healthy.usertable.*), ycsb_key );

CREATE INDEX usertable_merkle_covering_idx
ON damaged.usertable
( merkle_key_hash(ycsb_key), merkle_tuple_hash(damaged.usertable.*), ycsb_key );

ANALYZE healthy.usertable;
ANALYZE damaged.usertable;
