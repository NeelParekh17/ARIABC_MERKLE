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

SELECT merkle_root_hash_index('healthy.usertable_merkle_idx'::regclass) = merkle_root_hash_index('damaged.usertable_merkle_idx'::regclass) AS roots_match;
SELECT merkle_verify_index('healthy.usertable_merkle_idx'::regclass) AS healthy_merkle_verify;
SELECT merkle_verify_index('damaged.usertable_merkle_idx'::regclass) AS damaged_merkle_verify;
