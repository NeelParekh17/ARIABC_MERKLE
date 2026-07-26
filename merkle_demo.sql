-- =============================================================================
-- AriaBC Native Dynamic Merkle Index (v8) Demonstration & Inspection Script
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Index Creation (v8 Native Dynamic Layout)
-- -----------------------------------------------------------------------------
DROP INDEX IF EXISTS public.usertable_small_dynamic_merkle_idx;

CREATE INDEX usertable_small_dynamic_merkle_idx
    ON public.usertable_small USING merkle(ycsb_key)
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

-- -----------------------------------------------------------------------------
-- 2. Structural Verification & High-Level Tree Stats
-- -----------------------------------------------------------------------------
-- Verify structural integrity of the Merkle index (returns true if consistent)
SELECT merkle_dynamic_verify('usertable_small_dynamic_merkle_idx'::regclass);

-- Overall combined root hash (64-character hex string)
SELECT merkle_root_hash('usertable_small');

-- Comprehensive JSON metrics (page count, node count, leaf count, split/merge counts)
SELECT jsonb_pretty(merkle_dynamic_tree_stats('usertable_small_dynamic_merkle_idx'::regclass)::jsonb);

-- -----------------------------------------------------------------------------
-- 3. Full Merkle Tree Inspection (All Nodes across All Partitions)
-- -----------------------------------------------------------------------------
-- View EVERY node in the dynamic tree across all partitions (roots, internal split nodes, leaves)
SELECT partition_id,
       prefix_len,
       encode(prefix_bytes, 'hex') AS prefix_hex,
       is_leaf,
       tuple_count,
       subtree_bytes,
       encode(data_xor, 'hex') AS data_xor_hash,
       encode(structure_hash, 'hex') AS structure_hash
  FROM ariabc_internal.merkle_dynamic_node
 WHERE index_oid = 'usertable_small_dynamic_merkle_idx'::regclass
 ORDER BY partition_id, prefix_len, prefix_bytes;

-- Per-partition tree topology summary (total nodes, root, internal split nodes, leaf nodes, max depth, total tuples)
SELECT partition_id,
       count(*) AS total_nodes,
       count(*) FILTER (WHERE NOT is_leaf AND prefix_len = 0) AS root_nodes,
       count(*) FILTER (WHERE NOT is_leaf AND prefix_len > 0) AS internal_nodes,
       count(*) FILTER (WHERE is_leaf) AS leaf_nodes,
       max(prefix_len) AS max_prefix_depth,
       sum(tuple_count) FILTER (WHERE is_leaf) AS total_tuples
  FROM ariabc_internal.merkle_dynamic_node
 WHERE index_oid = 'usertable_small_dynamic_merkle_idx'::regclass
 GROUP BY partition_id
 ORDER BY partition_id;

-- Hierarchical node tree view for Partition 0 (roots -> internal nodes -> leaves)
SELECT partition_id,
       repeat('  ', prefix_len) || (CASE WHEN prefix_len = 0 THEN '[ROOT]' WHEN is_leaf THEN '[LEAF]' ELSE '[INTERNAL]' END) || ' len=' || prefix_len || ' prefix=' || substring(encode(prefix_bytes, 'hex') from 1 for 8) || '...' AS node_hierarchy,
       is_leaf,
       tuple_count,
       encode(data_xor, 'hex') AS data_xor_hash,
       encode(structure_hash, 'hex') AS structure_hash
  FROM ariabc_internal.merkle_dynamic_node
 WHERE index_oid = 'usertable_small_dynamic_merkle_idx'::regclass
   AND partition_id = 0
 ORDER BY prefix_len, prefix_bytes;

-- -----------------------------------------------------------------------------
-- 4. Partition Root Node Inspection
-- -----------------------------------------------------------------------------
-- View top-level partition roots across the 150 partitions
SELECT partition_id,
       prefix_len,
       tuple_count,
       is_leaf,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_partition_roots('usertable_small_dynamic_merkle_idx'::regclass)
 ORDER BY partition_id
 LIMIT 10;

-- View partition roots that are non-leaf internal nodes (WHERE NOT is_leaf)
SELECT partition_id,
       tuple_count,
       is_leaf,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_partition_roots('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE NOT is_leaf
 ORDER BY partition_id
 LIMIT 10;

-- -----------------------------------------------------------------------------
-- 5. Leaf Frontier Inspection
-- -----------------------------------------------------------------------------
-- View active leaf buckets across all partitions (WHERE is_leaf = true)
SELECT partition_id,
       prefix_len,
       tuple_count,
       encode(prefix, 'hex') AS prefix_hex,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE is_leaf = true
 ORDER BY partition_id, prefix_len, prefix
 LIMIT 10;

-- -----------------------------------------------------------------------------
-- 6. Subtree Node & Item Level Inspection
-- -----------------------------------------------------------------------------
-- Query specific subtree node summary using JSON request payload
SELECT partition_id,
       prefix_len,
       tuple_count,
       is_leaf,
       encode(prefix, 'hex') AS prefix_hex,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_ranges(
         'usertable_small_dynamic_merkle_idx'::regclass,
         '[{"partition_id": 0, "prefix_length": 2, "prefix_value": "0000000000000000000000000000000000000000000000000000000000000000"}]'::jsonb
       );

-- Inspect individual row keys, route digests, and 256-bit tuple hashes inside a leaf bucket
SELECT partition_id,
       prefix_len,
       key_text,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         'usertable_small_dynamic_merkle_idx'::regclass,
         '[{"partition_id": 0, "prefix_length": 2, "prefix_value": "0000000000000000000000000000000000000000000000000000000000000000"}]'::jsonb
       )
 LIMIT 10;

-- Direct leaf items stored in ariabc_internal.merkle_dynamic_leaf_item
SELECT partition_id,
       prefix_len,
       encode(prefix_bytes, 'hex') AS leaf_prefix_hex,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM ariabc_internal.merkle_dynamic_leaf_item
 WHERE index_oid = 'usertable_small_dynamic_merkle_idx'::regclass
 ORDER BY partition_id, prefix_len, prefix_bytes
 LIMIT 10;

-- -----------------------------------------------------------------------------
-- 7. DML Mutation Test (Insert / Delete Root Hash Updates)
-- -----------------------------------------------------------------------------
-- Insert a test tuple and observe root hash update
INSERT INTO usertable_small (ycsb_key, field1)
VALUES (999999, 'merkle_test_val')
ON CONFLICT (ycsb_key) DO UPDATE SET field1 = EXCLUDED.field1;

SELECT merkle_root_hash('usertable_small') AS root_after_insert;

-- Clean up test tuple and verify root hash reverts
DELETE FROM usertable_small WHERE ycsb_key = 999999;

SELECT merkle_root_hash('usertable_small') AS root_after_delete;

-- -----------------------------------------------------------------------------
-- 8. Specific Key Partition & Leaf Bucket Lookup (Single or Multiple Keys)
-- -----------------------------------------------------------------------------
-- Find exactly which partition_id, leaf prefix, route digest, and tuple_hash an inserted key belongs to (single key):
SELECT partition_id,
       prefix_len,
       encode(prefix, 'hex') AS prefix_hex,
       key_text,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         'usertable_small_dynamic_merkle_idx'::regclass,
         (SELECT jsonb_agg(jsonb_build_object(
                   'partition_id', partition_id,
                   'prefix_length', prefix_len,
                   'prefix_value', encode(prefix, 'hex')
                 ))
            FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass))
       )
 WHERE key_text = '100';

-- Find leaf bucket and hashes for MULTIPLE keys at once (using IN operator):
SELECT partition_id,
       prefix_len,
       encode(prefix, 'hex') AS prefix_hex,
       key_text,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         'usertable_small_dynamic_merkle_idx'::regclass,
         (SELECT jsonb_agg(jsonb_build_object(
                   'partition_id', partition_id,
                   'prefix_length', prefix_len,
                   'prefix_value', encode(prefix, 'hex')
                 ))
            FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass))
       )
 WHERE key_text IN ('292', '414')
 ORDER BY key_text;

-- -----------------------------------------------------------------------------
-- Helper Function: Retrieve all keys for ANY prefix pattern (e.g. 'f8%', '90%', '00%')
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION merkle_get_keys_by_prefix(
    p_index regclass,
    p_partition int,
    p_prefix_pattern text
)
RETURNS TABLE (
    key_text text,
    level int,
    prefix_len int,
    prefix_hex text,
    route_digest_hex text,
    tuple_hash_hex text
) AS $$
SELECT key_text,
       (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       encode(prefix, 'hex') AS prefix_hex,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         p_index,
         COALESCE(
           (SELECT jsonb_agg(jsonb_build_object(
                     'partition_id', partition_id,
                     'prefix_length', prefix_len,
                     'prefix_value', encode(prefix, 'hex')
                   ))
              FROM merkle_dynamic_get_leaf_frontier(p_index)
             WHERE partition_id = p_partition
               AND encode(prefix, 'hex') LIKE p_prefix_pattern),
           (SELECT jsonb_agg(jsonb_build_object(
                     'partition_id', partition_id,
                     'prefix_length', prefix_len,
                     'prefix_value', encode(prefix, 'hex')
                   ))
              FROM merkle_dynamic_get_leaf_frontier(p_index)
             WHERE partition_id = p_partition),
           '[]'::jsonb
         )
       )
 WHERE encode(prefix, 'hex') LIKE p_prefix_pattern
    OR encode(route_digest, 'hex') LIKE p_prefix_pattern
 ORDER BY key_text;
$$ LANGUAGE sql;

-- Simple Query: Pass ANY prefix pattern to get all keys & info (e.g. '00%', 'f8%', '90%'):
SELECT * FROM merkle_get_keys_by_prefix('usertable_small_dynamic_merkle_idx'::regclass, 1, '00%');

-- -----------------------------------------------------------------------------
-- 9. Dedicated Inspection Commands for Partition 1
-- -----------------------------------------------------------------------------

-- Command 1: ALL INTERNAL NODES (is_leaf = false)
-- Shows top-level root (level 0) AND all promoted internal branch nodes for Partition 1
SELECT (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       encode(prefix, 'hex') AS prefix_hex,
       tuple_count,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_all_internal_nodes('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE partition_id = 1
 ORDER BY level, prefix_len, prefix;

-- Command 2: LEAF NODES ONLY (is_leaf = true)
-- Shows active leaf buckets for Partition 1 along with their tree level
SELECT (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       tuple_count,
       encode(prefix, 'hex') AS prefix_hex,
       encode(data_xor, 'hex') AS data_xor_hash
  FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE partition_id = 1
 ORDER BY level, prefix_len, prefix;

-- Command 3: COMBINED VIEW (All Internal Nodes + Leaf Buckets in 1 View)
SELECT partition_id,
       (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       tuple_count,
       is_leaf,
       encode(prefix, 'hex') AS prefix_hex,
       encode(data_xor, 'hex') AS data_xor_hash,
       (CASE WHEN prefix_len = 0 THEN 'ROOT NODE' ELSE 'INTERNAL BRANCH' END) AS node_type
  FROM merkle_dynamic_get_all_internal_nodes('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE partition_id = 1
UNION ALL
SELECT partition_id,
       (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       tuple_count,
       is_leaf,
       encode(prefix, 'hex') AS prefix_hex,
       encode(data_xor, 'hex') AS data_xor_hash,
       'LEAF BUCKET' AS node_type
  FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass)
 WHERE partition_id = 1
 ORDER BY level ASC, is_leaf ASC, prefix_len ASC, prefix_hex ASC;

-- Command 4: ALL KEYS & HASHES INSIDE LEAF BUCKETS OF PARTITION 1
SELECT partition_id,
       (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       key_text,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         'usertable_small_dynamic_merkle_idx'::regclass,
         (SELECT jsonb_agg(jsonb_build_object(
                   'partition_id', partition_id,
                   'prefix_length', prefix_len,
                   'prefix_value', encode(prefix, 'hex')
                 ))
            FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass)
           WHERE partition_id = 1)
       );

-- Command 5: ALL KEYS & HASHES INSIDE LEAF BUCKETS ACROSS ALL PARTITIONS
SELECT partition_id,
       (CASE WHEN prefix_len = 0 THEN 0 ELSE (prefix_len + 4) / 5 END) AS level,
       prefix_len,
       key_text,
       encode(route_digest, 'hex') AS route_digest_hex,
       encode(tuple_hash, 'hex') AS tuple_hash_hex
  FROM merkle_dynamic_get_range_items(
         'usertable_small_dynamic_merkle_idx'::regclass,
         (SELECT jsonb_agg(jsonb_build_object(
                   'partition_id', partition_id,
                   'prefix_length', prefix_len,
                   'prefix_value', encode(prefix, 'hex')
                 ))
            FROM merkle_dynamic_get_leaf_frontier('usertable_small_dynamic_merkle_idx'::regclass))
       )
 ORDER BY partition_id, level, prefix_len, key_text;

-- -----------------------------------------------------------------------------
-- 10. Static Merkle Index Full Node Inspection (for dynamic = false indexes)
-- -----------------------------------------------------------------------------
-- If using static page-based Merkle index, view all tree nodes across partitions:
-- SELECT tablename, nodeid, partition, node_in_partition, is_leaf, leaf_id, hash
--   FROM merkle_node_hash('usertable_small'::regclass)
--  ORDER BY partition, node_in_partition;
