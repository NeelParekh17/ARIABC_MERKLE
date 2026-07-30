SET enable_merkle_index = on;

DROP INDEX IF EXISTS healthy.usertable_leaf_lookup_idx;
DROP INDEX IF EXISTS damaged.usertable_leaf_lookup_idx;
DROP INDEX IF EXISTS healthy.usertable_merkle_idx;
DROP INDEX IF EXISTS damaged.usertable_merkle_idx;

CREATE INDEX usertable_merkle_idx
ON healthy.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold);

CREATE INDEX usertable_merkle_idx
ON damaged.usertable USING merkle (ycsb_key)
WITH (fanout = :fanout, split_threshold = :split_threshold, merge_threshold = :merge_threshold);

CREATE INDEX usertable_leaf_lookup_idx
ON healthy.usertable ((merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, ycsb_key)));

CREATE INDEX usertable_leaf_lookup_idx
ON damaged.usertable ((merkle_bucket_for_key('damaged.usertable_merkle_idx'::regclass, ycsb_key)));

ANALYZE healthy.usertable;
ANALYZE damaged.usertable;
