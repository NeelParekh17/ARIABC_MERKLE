import psycopg
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
for s,am in [('db','btree'),('dl','lsm3')]:
 c.execute(f'CREATE SCHEMA {s}');c.execute(f'CREATE TABLE {s}.usertable(ycsb_key bigint PRIMARY KEY,v text) WITH (fillfactor=100)')
 c.execute(f'CREATE INDEX usertable_merkle_lookup_idx ON {s}.usertable USING {am}(merkle_partition_for_hash(merkle_key_hash(ycsb_key),200),merkle_key_hash(ycsb_key),ycsb_key)')
 c.execute(f'CREATE INDEX usertable_merkle_idx ON {s}.usertable USING merkle(ycsb_key) WITH (partitions=200,fanout=32,split_threshold=1024,merge_threshold=256)')
