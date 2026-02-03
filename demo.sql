create index usertable_merkle_default on usertable using merkle(ycsb_key);

drop index usertable_merkle_default;

create index usertable_merkle_variable on usertable using merkle(ycsb_key) with (subtrees = 150, leaves_per_tree = 8);

drop index usertable_merkle_variable;

create index usertable_merkle_multikey on usertable using merkle(ycsb_key, field1);

drop index usertable_merkle_multikey;

create index usertable_merkle_multikey_variable on usertable using merkle(ycsb_key, field1) with (subtrees = 150, leaves_per_tree = 8);

drop index usertable_merkle_multikey_variable;


select merkle_verify('usertable');

select merkle_root_hash('usertable');

select merkle_tree_stats('usertable');

select * from merkle_node_hash('usertable');

select * from merkle_leaf_tuples('usertable');

select merkle_leaf_id('usertable', 1199);


-- for testing on usertable_merkle_multikey index:
insert into usertable values (120000, 'field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9', 'field10');
select merkle_leaf_id('usertable', 120000, 'field1');


