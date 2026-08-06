SET enable_merkle_index = on;

-- Existing databases do not receive new built-in pg_proc rows during a
-- binary upgrade.  Register the partition routing helper idempotently so
-- recovery can bootstrap a current database without requiring initdb.  The
-- implementation is in the current postgres binary; an old binary therefore
-- fails here before dataset construction instead of failing halfway through
-- partition-aware recovery.
CREATE OR REPLACE FUNCTION pg_catalog.merkle_partition_for_hash(
    key_hash bytea, partitions integer
)
RETURNS integer
AS 'merkle_partition_for_hash'
LANGUAGE internal IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION public.recovery_corrupted_value(k bigint, seed integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT 'corrupt-' || seed::text || '-' || k::text
$$;
