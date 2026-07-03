SET enable_merkle_index = on;

CREATE OR REPLACE FUNCTION public.recovery_corrupted_value(k bigint, seed integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT 'corrupt-' || seed::text || '-' || k::text
$$;
