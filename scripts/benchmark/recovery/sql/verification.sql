SELECT count(*) AS healthy_minus_damaged
FROM (
    SELECT * FROM healthy.usertable
    EXCEPT ALL
    SELECT * FROM damaged.usertable
) diff;

SELECT count(*) AS damaged_minus_healthy
FROM (
    SELECT * FROM damaged.usertable
    EXCEPT ALL
    SELECT * FROM healthy.usertable
) diff;

SELECT merkle_root_hash('healthy.usertable'::regclass) = merkle_root_hash('damaged.usertable'::regclass) AS roots_match;
SELECT merkle_verify('healthy.usertable'::regclass) AS healthy_merkle_verify;
SELECT merkle_verify('damaged.usertable'::regclass) AS damaged_merkle_verify;
