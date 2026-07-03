-- Primary timed recovery path: compare requested nodes only.
SELECT * FROM merkle_get_partition_root_hashes(:index_or_table::regclass);
SELECT * FROM merkle_get_child_hashes(:index_or_table::regclass, :partition, :node_in_partition);

-- Leaf candidate lookup path. The expression matches the functional B-tree index.
SELECT ycsb_key,
       field0, field1, field2, field3, field4,
       field5, field6, field7, field8, field9
FROM healthy.usertable
WHERE merkle_bucket_for_key('healthy.usertable_merkle_idx'::regclass, ycsb_key) = :leaf_id;

SELECT ycsb_key,
       field0, field1, field2, field3, field4,
       field5, field6, field7, field8, field9
FROM damaged.usertable
WHERE merkle_bucket_for_key('damaged.usertable_merkle_idx'::regclass, ycsb_key) = :leaf_id;
