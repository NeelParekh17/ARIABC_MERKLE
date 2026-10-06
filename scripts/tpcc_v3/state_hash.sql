-- All columns, including every timestamp. Commutative diagnostic checksum (not a cryptographic proof).
\set ON_ERROR_STOP on
\pset format unaligned
\pset tuples_only on
SET datestyle = 'ISO, YMD';
SET timezone = 'UTC';
SET extra_float_digits = 3;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT string_agg(t || '=' || n || ':' || h, ' ' ORDER BY t) FROM (
 SELECT 'warehouse' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.warehouse x
 UNION ALL
 SELECT 'district' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.district x
 UNION ALL
 SELECT 'customer' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.customer x
 UNION ALL
 SELECT 'history' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.history x
 UNION ALL
 SELECT 'item' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.item x
 UNION ALL
 SELECT 'stock' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.stock x
 UNION ALL
 SELECT 'oorder' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.oorder x
 UNION ALL
 SELECT 'new_order' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.new_order x
 UNION ALL
 SELECT 'order_line' t,count(*) n,coalesce(sum(hashtextextended(to_jsonb(x)::text,0)::numeric),0) h FROM public.order_line x
) all_tables;
COMMIT;
