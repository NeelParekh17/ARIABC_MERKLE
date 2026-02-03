-- Transaction to ensure atomicity
BEGIN;

-- Create temporary table with safe data
CREATE TABLE usertable_restore AS SELECT * FROM usertable WHERE ycsb_key <= 12000;

-- Drop the old corrupted table
DROP TABLE usertable;

-- Rename restore table to original name
ALTER TABLE usertable_restore RENAME TO usertable;

-- Recreate Primary Key
ALTER TABLE usertable ADD PRIMARY KEY (ycsb_key);

-- Create the requested Merkle Index
CREATE INDEX usertable_merkle_multikey_variable ON usertable USING merkle(ycsb_key, field1) WITH (subtrees = 150, leaves_per_tree = 8);

COMMIT;
