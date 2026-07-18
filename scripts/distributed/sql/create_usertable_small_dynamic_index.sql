-- Dynamic Merkle setup layered on the canonical 12k-row base restore.
-- The caller must execute scripts/restore_usertable_small.sql first with
-- ariabc_skip_legacy_merkle_index defined.

DO $$
BEGIN
    IF current_setting('ariabc.allow_destructive_benchmark_reset', true)
           IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION
            'destructive benchmark reset refused; set ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1 in the runner';
    END IF;
    IF EXISTS (SELECT 1 FROM ariabc_internal.merkle_dynamic_state) THEN
        RAISE EXCEPTION
            'dynamic Merkle state remained after dropping benchmark index';
    END IF;
END
$$;

TRUNCATE ariabc_internal.merkle_dynamic_build_stage,
         ariabc_internal.merkle_dynamic_seen,
         ariabc_internal.merkle_dynamic_leaf_item,
         ariabc_internal.merkle_dynamic_node,
         ariabc_internal.merkle_dynamic_state;

SET enable_merkle_index = on;

DROP INDEX IF EXISTS public.usertable_merkle_multikey_variable;
DROP INDEX IF EXISTS public.usertable_small_dynamic_merkle_idx;

CREATE INDEX usertable_small_dynamic_merkle_idx
    ON public.usertable_small
    USING merkle (ycsb_key)
    WITH (
        dynamic              = true,
        partitions           = 150,
        leaves_per_partition = 1024,
        fanout               = 32,
        leaf_capacity        = 32,
        merge_threshold      = 8,
        leaf_byte_capacity   = 65536,
        max_key_bytes        = 1024,
        update_mode          = 'synchronous_cow'
    );

SELECT merkle_dynamic_verify(
    'public.usertable_small_dynamic_merkle_idx'::regclass
);
