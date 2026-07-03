create index usertable_small_merkle_default on usertable_small using merkle(ycsb_key);

drop index usertable_small_merkle_default;

create index usertable_small_merkle_variable on usertable_small using merkle(ycsb_key) with (partitions = 150, leaves_per_partition = 8);

drop index usertable_small_merkle_variable;

create index usertable_small_merkle_fanout10 on usertable_small using merkle(ycsb_key) with (partitions = 150, leaves_per_partition = 100, fanout = 10);

drop index usertable_small_merkle_fanout10;

create index usertable_small_merkle_multikey on usertable_small using merkle(ycsb_key, field1);

drop index usertable_small_merkle_multikey;

create index usertable_small_merkle_multikey_variable on usertable_small using merkle(ycsb_key, field1) with (partitions = 150, leaves_per_partition = 8);

drop index usertable_small_merkle_multikey_variable;


select merkle_verify('usertable_small');

select merkle_root_hash('usertable_small');

select merkle_tree_stats('usertable_small');

select * from merkle_node_hash('usertable_small');

select * from merkle_leaf_tuples('usertable_small');

select * from merkle_leaf_id('usertable_small', 1199);

select * from merkle_get_partition_root_hashes('usertable_small');


-- for testing on usertable_small_merkle_multikey index:
insert into usertable_small values (120000, 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9', 'field10');
select * from merkle_leaf_id('usertable_small', 120000, 'field1');
