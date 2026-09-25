SET enable_merkle_index = on;

-- Existing databases do not receive new built-in pg_proc rows during a
-- binary upgrade.  Register the partition routing helper idempotently so
-- recovery can bootstrap a current database without requiring initdb.  The
-- implementation is in the current postgres binary; an old binary therefore
-- fails here before dataset construction instead of failing halfway through
-- partition-aware recovery.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'merkle_partition_for_hash'
    ) THEN
        EXECUTE 'CREATE FUNCTION pg_catalog.merkle_partition_for_hash(' ||
                'key_hash bytea, partitions integer) ' ||
                'RETURNS smallint AS ''merkle_partition_for_hash'' ' ||
                'LANGUAGE internal IMMUTABLE STRICT PARALLEL SAFE';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc
        WHERE proname = 'merkle_find_spurious_key'
    ) THEN
        EXECUTE 'CREATE FUNCTION pg_catalog.merkle_find_spurious_key(' ||
                'lower_bound bytea, upper_bound bytea, partition_id integer, ' ||
                'partitions integer, base_offset bigint, max_attempts integer) ' ||
                'RETURNS bigint AS ''merkle_find_spurious_key_sql'' ' ||
                'LANGUAGE internal IMMUTABLE STRICT PARALLEL SAFE';
    END IF;
END $$;

CREATE OR REPLACE FUNCTION public.recovery_corrupted_value(k bigint, seed integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT 'corrupt-' || seed::text || '-' || k::text
$$;
