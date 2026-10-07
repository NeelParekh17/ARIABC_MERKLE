-- Post-publication re-execution reproducer: schema and procedures.
DROP TABLE IF EXISTS acct, outt, uq CASCADE;
CREATE TABLE acct (k int PRIMARY KEY, v bigint NOT NULL);
CREATE TABLE outt (k int PRIMARY KEY, v bigint NOT NULL);
-- Two unique indexes: the key index (id) and a second one (email).
CREATE TABLE uq (id int PRIMARY KEY, email int NOT NULL UNIQUE, v bigint NOT NULL);
INSERT INTO acct SELECT g, g FROM generate_series(0, 63) g;
INSERT INTO outt SELECT g, 0 FROM generate_series(0, 63) g;
INSERT INTO uq SELECT g, g, 0 FROM generate_series(0, 31) g;

-- Reads acct[a]; the value read feeds a write to a different table.
CREATE OR REPLACE FUNCTION public.ab_proc(a int, b int) RETURNS void LANGUAGE plpgsql AS $$
DECLARE x bigint;
BEGIN
    SELECT v INTO x FROM acct WHERE k = a;
    UPDATE outt SET v = (v * 31 + x) % 1000000007 WHERE k = b;
END $$;

-- Successor-style writer of acct.
CREATE OR REPLACE FUNCTION public.bump_proc(a int, d int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE acct SET v = v + d WHERE k = a;
END $$;

-- Moves row id to a new email: delete + insert on a two-unique-index table.
-- Raises 23505 at its serial position when another row already owns email e.
CREATE OR REPLACE FUNCTION public.uq_proc(i int, e int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM uq WHERE id = i;
    INSERT INTO uq VALUES (i, e, e * 7);
END $$;
