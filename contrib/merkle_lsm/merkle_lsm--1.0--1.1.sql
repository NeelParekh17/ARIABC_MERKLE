/* contrib/merkle_lsm/merkle_lsm--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION merkle_lsm UPDATE TO '1.1'" to load this file. \quit

-- 1.1 adds a lookup-index mode:
--   'tuplehash' (default): (merkle_partition_for_hash(H, P), H, merkle_tuple_hash(<table>.*))
--       Splits and replica repair read (H, tuple hash) from the index, and repair
--       ships only tuple hashes plus the mismatching rows.  The whole-row column
--       disables HOT, so every INSERT and UPDATE writes a lookup entry.
--   'keyhash': (merkle_partition_for_hash(H, P), H, <key columns>), the 1.0 shape.
--       Non-key UPDATEs stay HOT; splits re-hash heap rows and replica repair
--       ships the full contents of each differing leaf.
-- Core Merkle remains independent of this contrib module. A setup that requests
-- the default AM must install the extension explicitly; no silent AM fallback.
DROP FUNCTION merkle_create_lookup_index(regclass, text, integer, integer, integer);
CREATE FUNCTION merkle_create_lookup_index(target regclass,
 am text DEFAULT 'merkle_lsm', levels integer DEFAULT 3,
 top_index_size integer DEFAULT 1024, size_ratio integer DEFAULT 16,
 mode text DEFAULT 'tuplehash')
RETURNS regclass LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
 m record;
 key_cols text;
 lead_cols text;
 hash_expr text;
 lookup_name text;
 table_name text;
 schema_name text;
 existing regclass;
 existing_am text;
 result regclass;
 options text := '';
BEGIN
 IF am NOT IN ('btree', 'merkle_lsm') THEN
   RAISE EXCEPTION 'lookup AM must be btree or merkle_lsm';
 END IF;
 IF mode IS NULL OR mode NOT IN ('keyhash', 'tuplehash') THEN
   RAISE EXCEPTION 'lookup mode must be keyhash or tuplehash';
 END IF;
 SELECT i.indexrelid, i.indnkeyatts,
   coalesce((SELECT option_value::int FROM pg_options_to_table(c.reloptions) WHERE option_name='partitions'),200) AS partitions,
   coalesce((SELECT option_value::int FROM pg_options_to_table(c.reloptions) WHERE option_name='partition_key_columns'),0) AS lead_n,
   coalesce((SELECT option_value::int FROM pg_options_to_table(c.reloptions) WHERE option_name='subpartitions'),1) AS subparts
 INTO STRICT m FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
 JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid=target AND a.amname='merkle' AND i.indisvalid;
 SELECT string_agg(pg_get_indexdef(m.indexrelid,k,true), ', ' ORDER BY k),
   string_agg(pg_get_indexdef(m.indexrelid,k,true), ', ' ORDER BY k) FILTER (WHERE k<=m.lead_n)
 INTO key_cols, lead_cols FROM generate_series(1,m.indnkeyatts) k;
 IF m.lead_n>0 THEN
   hash_expr := format('merkle_key_hash_routed(ROW(%s),ROW(%s),%s,%s)',key_cols,lead_cols,m.partitions,m.subparts);
 ELSIF m.indnkeyatts>1 THEN
   hash_expr := format('merkle_key_hash(ROW(%s))',key_cols);
 ELSE
   hash_expr := format('merkle_key_hash(%s)',key_cols);
 END IF;
 SELECT n.nspname,c.relname INTO schema_name,table_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=target;
 lookup_name := table_name || '_merkle_lookup_idx';
 -- Long table names retain a deterministic suffix and a table OID disambiguator.
 IF octet_length(lookup_name)>63 THEN
   lookup_name := 'merkle_lookup_' || target::oid || '_idx';
 END IF;
 existing := to_regclass(format('%I.%I',schema_name,lookup_name));
 IF existing IS NOT NULL THEN
   SELECT a.amname INTO existing_am FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=existing;
   RAISE EXCEPTION 'lookup index % already exists using %; drop it explicitly before conversion to %',existing,existing_am,am;
 END IF;
 IF am='merkle_lsm' THEN
   options := format(' WITH (levels=%s,top_index_size=%s,size_ratio=%s)',levels,top_index_size,size_ratio);
 END IF;
 IF mode='tuplehash' THEN
   EXECUTE format('CREATE INDEX %I ON %s USING %I (merkle_partition_for_hash(%s,%s),%s,merkle_tuple_hash(%I.*))%s',
     lookup_name,target,am,hash_expr,m.partitions,hash_expr,table_name,options);
 ELSE
   EXECUTE format('CREATE INDEX %I ON %s USING %I (merkle_partition_for_hash(%s,%s),%s,%s)%s',
     lookup_name,target,am,hash_expr,m.partitions,hash_expr,key_cols,options);
 END IF;
 result := to_regclass(format('%I.%I',schema_name,lookup_name));
 RETURN result;
END $$;
