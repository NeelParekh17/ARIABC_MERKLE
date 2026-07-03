SET enable_merkle_index = on;

CREATE SCHEMA IF NOT EXISTS healthy;
CREATE SCHEMA IF NOT EXISTS damaged;

DROP TABLE IF EXISTS healthy.usertable CASCADE;
DROP TABLE IF EXISTS damaged.usertable CASCADE;

CREATE TABLE healthy.usertable (
    ycsb_key bigint NOT NULL,
    field0 text NOT NULL,
    field1 text NOT NULL,
    field2 text NOT NULL,
    field3 text NOT NULL,
    field4 text NOT NULL,
    field5 text NOT NULL,
    field6 text NOT NULL,
    field7 text NOT NULL,
    field8 text NOT NULL,
    field9 text NOT NULL,
    PRIMARY KEY (ycsb_key)
);

CREATE TABLE damaged.usertable (LIKE healthy.usertable INCLUDING ALL);
