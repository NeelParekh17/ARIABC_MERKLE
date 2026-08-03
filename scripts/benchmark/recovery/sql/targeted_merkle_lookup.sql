-- Dynamic Merkle Leaf Lookup query.
SELECT ycsb_key,
       field0, field1, field2, field3, field4,
       field5, field6, field7, field8, field9
FROM healthy.usertable
WHERE merkle_key_hash(ycsb_key) BETWEEN :lower_bound AND :upper_bound;

SELECT ycsb_key,
       field0, field1, field2, field3, field4,
       field5, field6, field7, field8, field9
FROM damaged.usertable
WHERE merkle_key_hash(ycsb_key) BETWEEN :lower_bound AND :upper_bound;
