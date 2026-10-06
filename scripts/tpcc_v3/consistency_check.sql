\set ON_ERROR_STOP on
\pset format unaligned
\pset tuples_only on
BEGIN ISOLATION LEVEL REPEATABLE READ;
CREATE TEMP TABLE tpcc_v3_checks(condition text PRIMARY KEY, ok boolean NOT NULL) ON COMMIT DROP;
INSERT INTO tpcc_v3_checks VALUES
('condition_1_warehouse_district_ytd', NOT EXISTS (
 SELECT 1 FROM public.warehouse w LEFT JOIN (SELECT d_w_id,sum(d_ytd) y FROM public.district GROUP BY d_w_id) d
 ON d.d_w_id=w.w_id WHERE d.y IS DISTINCT FROM w.w_ytd)),
('condition_2_next_order_ids', NOT EXISTS (
 SELECT 1 FROM public.district d LEFT JOIN (SELECT o_w_id,o_d_id,max(o_id) m FROM public.oorder GROUP BY 1,2) o
 ON (o.o_w_id,o.o_d_id)=(d.d_w_id,d.d_id)
 LEFT JOIN (SELECT no_w_id,no_d_id,max(no_o_id) m FROM public.new_order GROUP BY 1,2) n
 ON (n.no_w_id,n.no_d_id)=(d.d_w_id,d.d_id)
 WHERE o.m IS DISTINCT FROM d.d_next_o_id-1 OR (n.m IS NOT NULL AND n.m<>d.d_next_o_id-1))),
('condition_3_new_order_contiguous', NOT EXISTS (
 SELECT 1 FROM public.new_order GROUP BY no_w_id,no_d_id HAVING max(no_o_id)-min(no_o_id)+1<>count(*))),
('condition_4_district_order_lines', NOT EXISTS (
 SELECT 1 FROM (SELECT o_w_id w,o_d_id d,sum(o_ol_cnt) n FROM public.oorder GROUP BY 1,2) o FULL JOIN
 (SELECT ol_w_id w,ol_d_id d,count(*) n FROM public.order_line GROUP BY 1,2) l USING(w,d)
 WHERE o.n IS DISTINCT FROM l.n)),
('extra_order_line_counts', NOT EXISTS (
 SELECT 1 FROM public.oorder o FULL JOIN
 (SELECT ol_w_id w,ol_d_id d,ol_o_id id,count(*) n,min(ol_number) lo,max(ol_number) hi FROM public.order_line GROUP BY 1,2,3) l
 ON (o.o_w_id,o.o_d_id,o.o_id)=(l.w,l.d,l.id)
 WHERE l.n IS DISTINCT FROM o.o_ol_cnt OR l.lo<>1 OR l.hi<>o.o_ol_cnt)),
('extra_carrier_new_order', NOT EXISTS (
 SELECT 1 FROM public.oorder o FULL JOIN public.new_order n
 ON (o.o_w_id,o.o_d_id,o.o_id)=(n.no_w_id,n.no_d_id,n.no_o_id)
 WHERE o.o_id IS NULL OR ((o.o_carrier_id IS NULL) IS DISTINCT FROM (n.no_o_id IS NOT NULL)))),
('extra_delivery_timestamp', NOT EXISTS (
 SELECT 1 FROM public.order_line l JOIN public.oorder o
 ON (o.o_w_id,o.o_d_id,o.o_id)=(l.ol_w_id,l.ol_d_id,l.ol_o_id)
 WHERE (l.ol_delivery_d IS NULL) IS DISTINCT FROM (o.o_carrier_id IS NULL))),
('extra_warehouse_history_ytd', NOT EXISTS (
 SELECT 1 FROM public.warehouse w LEFT JOIN (SELECT h_w_id,sum(h_amount) y FROM public.history GROUP BY 1) h
 ON w.w_id=h.h_w_id WHERE w.w_ytd IS DISTINCT FROM h.y)),
('extra_district_history_ytd', NOT EXISTS (
 SELECT 1 FROM public.district d LEFT JOIN (SELECT h_w_id,h_d_id,sum(h_amount) y FROM public.history GROUP BY 1,2) h
 ON (d.d_w_id,d.d_id)=(h.h_w_id,h.h_d_id) WHERE d.d_ytd IS DISTINCT FROM h.y)),
('extra_customer_payment_history', NOT EXISTS (
 SELECT 1 FROM public.customer c LEFT JOIN
 (SELECT h_c_w_id w,h_c_d_id d,h_c_id id,sum(h_amount) y,count(*) n FROM public.history GROUP BY 1,2,3) h
 ON (c.c_w_id,c.c_d_id,c.c_id)=(h.w,h.d,h.id)
 WHERE c.c_ytd_payment IS DISTINCT FROM h.y OR c.c_payment_cnt IS DISTINCT FROM h.n)),
('extra_customer_balance', NOT EXISTS (
 SELECT 1 FROM public.customer c LEFT JOIN
 (SELECT o.o_w_id w,o.o_d_id d,o.o_c_id id,sum(l.ol_amount) amount FROM public.oorder o JOIN public.order_line l
 ON (o.o_w_id,o.o_d_id,o.o_id)=(l.ol_w_id,l.ol_d_id,l.ol_o_id)
 WHERE o.o_carrier_id IS NOT NULL GROUP BY 1,2,3) delivered
 ON (c.c_w_id,c.c_d_id,c.c_id)=(delivered.w,delivered.d,delivered.id)
 WHERE c.c_balance IS DISTINCT FROM coalesce(delivered.amount,0)-c.c_ytd_payment)),
('extra_customer_delivery_count', NOT EXISTS (
 SELECT 1 FROM public.customer c LEFT JOIN
 (SELECT o_w_id w,o_d_id d,o_c_id id,count(*) n FROM public.oorder WHERE o_id>=2101 AND o_carrier_id IS NOT NULL GROUP BY 1,2,3) delivered
 ON (c.c_w_id,c.c_d_id,c.c_id)=(delivered.w,delivered.d,delivered.id)
 WHERE c.c_delivery_cnt IS DISTINCT FROM coalesce(delivered.n,0)));
SELECT condition||'='||ok::text FROM tpcc_v3_checks ORDER BY condition;
SELECT 'consistency_ok='||CASE WHEN bool_and(ok) THEN 't' ELSE 'f' END FROM tpcc_v3_checks;
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM tpcc_v3_checks WHERE NOT ok) THEN
   RAISE EXCEPTION 'TPC-C v3 consistency failed';
 END IF;
END $$;
COMMIT;
