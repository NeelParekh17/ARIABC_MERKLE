-- TPCC stored procedures and helper setup
-- Run this AFTER restoring tpcc-pgdump-full.sql

-- Fast parallel in-memory index creation
SET maintenance_work_mem = '2GB';
SET max_parallel_maintenance_workers = 4;

-- Standard TPCC primary key and query indexes (not in dump; required for query performance).
CREATE UNIQUE INDEX pk_warehouse    ON public.warehouse  (w_id);
CREATE UNIQUE INDEX pk_district     ON public.district   (d_w_id, d_id);
CREATE UNIQUE INDEX pk_customer     ON public.customer   (c_w_id, c_d_id, c_id);
CREATE        INDEX idx_customer_name_mid ON public.customer (c_w_id, c_d_id, c_last, c_first, c_id);
CREATE UNIQUE INDEX pk_item         ON public.item       (i_id);
CREATE UNIQUE INDEX pk_stock        ON public.stock      (s_w_id, s_i_id);
CREATE UNIQUE INDEX pk_oorder       ON public.oorder     (o_w_id, o_d_id, o_id);
CREATE        INDEX idx_oorder_customer_latest
    ON public.oorder (o_w_id, o_d_id, o_c_id, o_id DESC)
    INCLUDE (o_entry_d, o_carrier_id);
CREATE UNIQUE INDEX pk_new_order    ON public.new_order  (no_w_id, no_d_id, no_o_id);
CREATE UNIQUE INDEX pk_order_line   ON public.order_line (ol_w_id, ol_d_id, ol_o_id, ol_number);


-- V3 procedures follow.
-- TP001 is reserved for this one expected, deterministic business abort.
-- RAISE occurs at the last (invalid) item, AFTER earlier writes were attempted.
CREATE FUNCTION public.new_order_proc_exec(
    warehouse_id int, district_id int, customer_id int,
    itemids int[], supplierwarehouseids int[], quantities int[], tx_timestamp timestamp
) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    next_id int; n int := array_length(itemids,1); k int;
    wtax numeric; dtax numeric; discount numeric; lastname text; credit text;
    price numeric; dist text; amount numeric; subtotal numeric := 0;
BEGIN
    IF n NOT BETWEEN 5 AND 15 OR array_length(supplierwarehouseids,1) IS DISTINCT FROM n
       OR array_length(quantities,1) IS DISTINCT FROM n OR tx_timestamp IS NULL THEN
        RAISE EXCEPTION 'invalid NewOrder arguments';
    END IF;
    SELECT w_tax INTO STRICT wtax FROM public.warehouse WHERE w_id=warehouse_id;
    SELECT c_discount,c_last,c_credit INTO STRICT discount,lastname,credit
      FROM public.customer WHERE c_w_id=warehouse_id AND c_d_id=district_id AND c_id=customer_id;
    UPDATE public.district SET d_next_o_id=d_next_o_id+1
      WHERE d_w_id=warehouse_id AND d_id=district_id
      RETURNING d_next_o_id-1,d_tax INTO STRICT next_id,dtax;
    INSERT INTO public.oorder(o_w_id,o_d_id,o_id,o_c_id,o_carrier_id,o_ol_cnt,o_all_local,o_entry_d)
      VALUES(warehouse_id,district_id,next_id,customer_id,NULL,n,
             CASE WHEN ARRAY[warehouse_id] @> supplierwarehouseids THEN 1 ELSE 0 END,tx_timestamp);
    INSERT INTO public.new_order VALUES(warehouse_id,district_id,next_id);
    FOR k IN 1..n LOOP
        SELECT i_price INTO price FROM public.item WHERE i_id=itemids[k];
        IF NOT FOUND THEN
            IF k=n AND itemids[k]=100001 THEN
                RAISE EXCEPTION USING ERRCODE='TP001', MESSAGE='TPC-C expected NewOrder rollback: invalid item';
            END IF;
            RAISE EXCEPTION 'unexpected missing item %', itemids[k];
        END IF;
        IF quantities[k] NOT BETWEEN 1 AND 10 THEN RAISE EXCEPTION 'invalid quantity'; END IF;
        UPDATE public.stock SET
          s_quantity=CASE WHEN s_quantity>=quantities[k]+10 THEN s_quantity-quantities[k]
                          ELSE s_quantity+91-quantities[k] END,
          s_ytd=s_ytd+quantities[k],s_order_cnt=s_order_cnt+1,
          s_remote_cnt=s_remote_cnt+CASE WHEN supplierwarehouseids[k]<>warehouse_id THEN 1 ELSE 0 END
          WHERE s_w_id=supplierwarehouseids[k] AND s_i_id=itemids[k]
          RETURNING CASE district_id
            WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02 WHEN 3 THEN s_dist_03 WHEN 4 THEN s_dist_04
            WHEN 5 THEN s_dist_05 WHEN 6 THEN s_dist_06 WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08
            WHEN 9 THEN s_dist_09 WHEN 10 THEN s_dist_10 END INTO STRICT dist;
        amount := price*quantities[k]; subtotal := subtotal+amount;
        INSERT INTO public.order_line VALUES(warehouse_id,district_id,next_id,k,itemids[k],NULL,
                                             amount,supplierwarehouseids[k],quantities[k],dist);
    END LOOP;
    RETURN subtotal*(1-discount)*(1+wtax+dtax);
END;
$$;

CREATE FUNCTION public.customer_by_name_id(warehouse_id int,district_id int,last_name text)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE result int; n int;
BEGIN
    SELECT count(*) INTO n FROM public.customer
      WHERE c_w_id=warehouse_id AND c_d_id=district_id AND c_last=last_name;
    IF n=0 THEN RAISE EXCEPTION 'customer last name missing'; END IF;
    SELECT c_id INTO STRICT result FROM public.customer
      WHERE c_w_id=warehouse_id AND c_d_id=district_id AND c_last=last_name
      ORDER BY c_first,c_id LIMIT 1 OFFSET (n-1)/2;
    RETURN result;
END;
$$;

CREATE FUNCTION public.payment_proc_exec(customer_id int,customer_district_id int,customer_warehouse_id int,
    warehouse_id int,district_id int,payment_amount numeric,tx_timestamp timestamp)
RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE wname text; dname text; result numeric; customer_info public.customer%ROWTYPE;
BEGIN
    IF payment_amount NOT BETWEEN 1.00 AND 5000.00 OR tx_timestamp IS NULL THEN
        RAISE EXCEPTION 'invalid Payment arguments';
    END IF;
    UPDATE public.warehouse SET w_ytd=w_ytd+payment_amount WHERE w_id=warehouse_id
      RETURNING w_name INTO STRICT wname;
    UPDATE public.district SET d_ytd=d_ytd+payment_amount WHERE d_w_id=warehouse_id AND d_id=district_id
      RETURNING d_name INTO STRICT dname;
    UPDATE public.customer SET c_balance=c_balance-payment_amount,c_ytd_payment=c_ytd_payment+payment_amount,
      c_payment_cnt=c_payment_cnt+1,
      c_data=CASE WHEN c_credit='BC' THEN left(customer_id::text||' '||customer_district_id||' '||
        customer_warehouse_id||' '||district_id||' '||warehouse_id||' '||payment_amount||' | '||c_data,500)
        ELSE c_data END
      WHERE c_w_id=customer_warehouse_id AND c_d_id=customer_district_id AND c_id=customer_id
      RETURNING * INTO STRICT customer_info;
    result := customer_info.c_balance;
    INSERT INTO public.history VALUES(customer_id,customer_district_id,customer_warehouse_id,district_id,
                                       warehouse_id,tx_timestamp,payment_amount,wname||'    '||dname);
    RETURN result;
END;
$$;

CREATE FUNCTION public.payment_by_name_proc_exec(last_name text,customer_district_id int,customer_warehouse_id int,
    warehouse_id int,district_id int,payment_amount numeric,tx_timestamp timestamp)
RETURNS numeric LANGUAGE plpgsql AS $$
BEGIN
    RETURN public.payment_proc_exec(public.customer_by_name_id(customer_warehouse_id,customer_district_id,last_name),
              customer_district_id,customer_warehouse_id,warehouse_id,district_id,payment_amount,tx_timestamp);
END;
$$;

-- Read the customer AND every field required from every line of the latest order.
-- Return a digest of the complete line read to keep BCDB's 1024-byte result slot bounded.
CREATE FUNCTION public.order_status_proc_exec(warehouse_id int,district_id int,customer_id int)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE cust record; ord record; lines jsonb;
BEGIN
    SELECT c_balance,c_first,c_middle,c_last INTO STRICT cust FROM public.customer
      WHERE c_w_id=warehouse_id AND c_d_id=district_id AND c_id=customer_id;
    SELECT o_id,o_entry_d,o_carrier_id INTO STRICT ord FROM public.oorder
      WHERE o_w_id=warehouse_id AND o_d_id=district_id AND o_c_id=customer_id ORDER BY o_id DESC LIMIT 1;
    SELECT jsonb_agg(jsonb_build_array(ol_number,ol_i_id,ol_supply_w_id,ol_quantity,ol_amount,ol_delivery_d)
                     ORDER BY ol_number) INTO lines FROM public.order_line
      WHERE ol_w_id=warehouse_id AND ol_d_id=district_id AND ol_o_id=ord.o_id;
    RETURN jsonb_build_object('customer',to_jsonb(cust),'order',to_jsonb(ord),
                             'line_count',jsonb_array_length(lines),'lines_md5',md5(lines::text));
END;
$$;

CREATE FUNCTION public.order_status_by_name_exec(warehouse_id int,district_id int,last_name text)
RETURNS jsonb LANGUAGE plpgsql AS $$
BEGIN
    RETURN public.order_status_proc_exec(warehouse_id,district_id,
                          public.customer_by_name_id(warehouse_id,district_id,last_name));
END;
$$;

CREATE FUNCTION public.delivery_proc_exec(warehouse_id int,carrier_id int,tx_timestamp timestamp)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE d int; oid int; cid int; total numeric; delivered int := 0;
BEGIN
    IF carrier_id NOT BETWEEN 1 AND 10 OR tx_timestamp IS NULL THEN RAISE EXCEPTION 'invalid Delivery arguments'; END IF;
    FOR d IN 1..10 LOOP
        SELECT no_o_id INTO oid FROM public.new_order WHERE no_w_id=warehouse_id AND no_d_id=d
          ORDER BY no_o_id LIMIT 1;
        CONTINUE WHEN NOT FOUND;
        DELETE FROM public.new_order WHERE no_w_id=warehouse_id AND no_d_id=d AND no_o_id=oid;
        UPDATE public.oorder SET o_carrier_id=carrier_id
          WHERE o_w_id=warehouse_id AND o_d_id=d AND o_id=oid RETURNING o_c_id INTO STRICT cid;
        UPDATE public.order_line SET ol_delivery_d=tx_timestamp
          WHERE ol_w_id=warehouse_id AND ol_d_id=d AND ol_o_id=oid;
        SELECT sum(ol_amount) INTO total FROM public.order_line
          WHERE ol_w_id=warehouse_id AND ol_d_id=d AND ol_o_id=oid;
        UPDATE public.customer SET c_balance=c_balance+total,c_delivery_cnt=c_delivery_cnt+1
          WHERE c_w_id=warehouse_id AND c_d_id=d AND c_id=cid;
        delivered := delivered+1;
    END LOOP;
    RETURN delivered;
END;
$$;

CREATE FUNCTION public.stock_level_exec(warehouse_id int,district_id int,threshold int)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE next_id int; result int;
BEGIN
    SELECT d_next_o_id INTO STRICT next_id FROM public.district WHERE d_w_id=warehouse_id AND d_id=district_id;
    SELECT count(DISTINCT s.s_i_id) INTO result FROM public.order_line ol JOIN public.stock s
      ON s.s_w_id=warehouse_id AND s.s_i_id=ol.ol_i_id
      WHERE ol.ol_w_id=warehouse_id AND ol.ol_d_id=district_id
        AND ol.ol_o_id>=next_id-20 AND ol.ol_o_id<next_id AND s.s_quantity<threshold;
    RETURN result;
END;
$$;

-- Merkle indexes for determinism verification across ALL TPC-C tables
\if :{?bench_enable_merkle}
\else
\set bench_enable_merkle 1
\endif

\if :{?bench_merkle_fanout}
\else
\set bench_merkle_fanout 32
\endif

\if :{?bench_merkle_partitions}
\else
\set bench_merkle_partitions 200
\endif

\if :{?bench_merkle_split_threshold}
\else
SELECT CASE WHEN :bench_merkle_fanout = 32 THEN 1024 ELSE 32 END
       AS bench_merkle_split_threshold \gset
\endif

\if :{?bench_merkle_merge_threshold}
\else
SELECT GREATEST(1, :bench_merkle_split_threshold / 4)
       AS bench_merkle_merge_threshold \gset
\endif

-- Leading-key partition routing.  0 = partition by hash(full key) % partitions.
-- N > 0 = rows sharing their first N key columns (the warehouse id for N = 1)
-- use one group of bench_merkle_subpartitions partitions; partitions must be
-- a multiple of subpartitions.
\if :{?bench_merkle_partition_key_columns}
\else
\set bench_merkle_partition_key_columns 0
\endif

\if :{?bench_merkle_subpartitions}
\else
\set bench_merkle_subpartitions 1
\endif

DROP INDEX IF EXISTS public.warehouse_merkle_idx;
DROP INDEX IF EXISTS public.district_merkle_idx;
DROP INDEX IF EXISTS public.customer_merkle_idx;
DROP INDEX IF EXISTS public.history_merkle_idx;
DROP INDEX IF EXISTS public.item_merkle_idx;
DROP INDEX IF EXISTS public.stock_merkle_idx;
DROP INDEX IF EXISTS public.oorder_merkle_idx;
DROP INDEX IF EXISTS public.new_order_merkle_idx;
DROP INDEX IF EXISTS public.order_line_merkle_idx;

DROP INDEX IF EXISTS public.warehouse_merkle_lookup_idx;
DROP INDEX IF EXISTS public.district_merkle_lookup_idx;
DROP INDEX IF EXISTS public.customer_merkle_lookup_idx;
DROP INDEX IF EXISTS public.history_merkle_lookup_idx;
DROP INDEX IF EXISTS public.item_merkle_lookup_idx;
DROP INDEX IF EXISTS public.stock_merkle_lookup_idx;
DROP INDEX IF EXISTS public.oorder_merkle_lookup_idx;
DROP INDEX IF EXISTS public.new_order_merkle_lookup_idx;
DROP INDEX IF EXISTS public.order_line_merkle_lookup_idx;

\if :bench_enable_merkle
ALTER TABLE public.warehouse   SET LOGGED;
ALTER TABLE public.district    SET LOGGED;
ALTER TABLE public.customer    SET LOGGED;
ALTER TABLE public.history     SET LOGGED;
ALTER TABLE public.item        SET LOGGED;
ALTER TABLE public.stock       SET LOGGED;
ALTER TABLE public.oorder      SET LOGGED;
ALTER TABLE public.new_order   SET LOGGED;
ALTER TABLE public.order_line  SET LOGGED;

-- Clusters initialised before merkle_key_hash_routed existed lack its catalog
-- row; the running binary provides the builtin.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_key_hash_routed'
                   AND pronamespace = 'pg_catalog'::regnamespace) THEN
    CREATE FUNCTION pg_catalog.merkle_key_hash_routed(full_key "any", leading_key "any",
                                                      partitions integer, subpartitions integer)
      RETURNS bytea LANGUAGE internal IMMUTABLE STRICT PARALLEL SAFE AS 'merkle_key_hash_routed_sql';
  END IF;
END
$$;

\set bench_merkle_with 'partitions=' :bench_merkle_partitions ', fanout=' :bench_merkle_fanout ', split_threshold=' :bench_merkle_split_threshold ', merge_threshold=' :bench_merkle_merge_threshold ', partition_key_columns=' :bench_merkle_partition_key_columns ', subpartitions=' :bench_merkle_subpartitions

CREATE INDEX warehouse_merkle_idx  ON public.warehouse  USING merkle (w_id)                                  WITH (:bench_merkle_with);
CREATE INDEX district_merkle_idx   ON public.district   USING merkle (d_w_id, d_id)                         WITH (:bench_merkle_with);
CREATE INDEX customer_merkle_idx   ON public.customer   USING merkle (c_w_id, c_d_id, c_id)                 WITH (:bench_merkle_with);
CREATE INDEX history_merkle_idx    ON public.history    USING merkle (h_c_w_id, h_c_d_id, h_c_id)           WITH (:bench_merkle_with);
CREATE INDEX item_merkle_idx       ON public.item       USING merkle (i_id)                                 WITH (:bench_merkle_with);
CREATE INDEX stock_merkle_idx      ON public.stock      USING merkle (s_w_id, s_i_id)                       WITH (:bench_merkle_with);
CREATE INDEX oorder_merkle_idx     ON public.oorder     USING merkle (o_w_id, o_d_id, o_id)                 WITH (:bench_merkle_with);
CREATE INDEX new_order_merkle_idx  ON public.new_order  USING merkle (no_w_id, no_d_id, no_o_id)            WITH (:bench_merkle_with);
CREATE INDEX order_line_merkle_idx ON public.order_line USING merkle (ol_w_id, ol_d_id, ol_o_id, ol_number) WITH (:bench_merkle_with);

-- Lookup indexes serve the Merkle split query.  Their route-hash expression
-- must be the one get_index_key_expr_str() (merkleapply.c) generates for the
-- index, so it is derived from the catalog and the index's routing options.
DO $$
DECLARE
  r record;
  key_cols text;
  lead_cols text;
  key_hash text;
BEGIN
  FOR r IN
    SELECT t.relname AS tbl, i.indexrelid, i.indnatts,
           coalesce((SELECT (regexp_match(array_to_string(c.reloptions, ','), '(?:^|,)partitions=(\d+)'))[1]::int), 200) AS partitions,
           coalesce((SELECT (regexp_match(array_to_string(c.reloptions, ','), '(?:^|,)partition_key_columns=(\d+)'))[1]::int), 0) AS lead_n,
           coalesce((SELECT (regexp_match(array_to_string(c.reloptions, ','), '(?:^|,)subpartitions=(\d+)'))[1]::int), 1) AS subparts
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indexrelid
      JOIN pg_class t ON t.oid = i.indrelid
      JOIN pg_am am ON am.oid = c.relam
     WHERE am.amname = 'merkle' AND t.relnamespace = 'public'::regnamespace
       AND t.relname IN ('warehouse', 'district', 'customer', 'history', 'item',
                         'stock', 'oorder', 'new_order', 'order_line')
  LOOP
    SELECT string_agg(pg_get_indexdef(r.indexrelid, k, true), ', ' ORDER BY k),
           string_agg(pg_get_indexdef(r.indexrelid, k, true), ', ' ORDER BY k)
             FILTER (WHERE k <= r.lead_n)
      INTO key_cols, lead_cols
      FROM generate_series(1, r.indnatts) AS k;

    IF r.lead_n > 0 THEN
      key_hash := format('merkle_key_hash_routed(ROW(%s), ROW(%s), %s, %s)',
                         key_cols, lead_cols, r.partitions, r.subparts);
    ELSIF r.indnatts > 1 THEN
      key_hash := format('merkle_key_hash(ROW(%s))', key_cols);
    ELSE
      key_hash := format('merkle_key_hash(%s)', key_cols);
    END IF;

    EXECUTE format('CREATE INDEX %I ON public.%I (merkle_partition_for_hash(%s, %s), %s, %s)',
                   r.tbl || '_merkle_lookup_idx', r.tbl, key_hash, r.partitions, key_hash, key_cols);
  END LOOP;
END
$$;
\endif
