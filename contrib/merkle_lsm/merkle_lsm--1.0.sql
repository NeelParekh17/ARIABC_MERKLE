-- Operator definitions derived from Postgres Professional lsm3 (PostgreSQL License).
-- Lsm3 operators

CREATE OR REPLACE FUNCTION merkle_lsm_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD merkle_lsm TYPE INDEX HANDLER merkle_lsm_handler;

CREATE OPERATOR FAMILY integer_ops USING merkle_lsm;

CREATE OPERATOR CLASS int2_ops DEFAULT
	FOR TYPE int2 USING merkle_lsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2,int2);

CREATE OPERATOR CLASS int4_ops DEFAULT
	FOR TYPE int4 USING merkle_lsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4,int4);

CREATE OPERATOR CLASS int8_ops DEFAULT
	FOR TYPE int8 USING merkle_lsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8,int8);

ALTER OPERATOR FAMILY integer_ops USING merkle_lsm ADD
	OPERATOR 1  < (int2,int4),
	OPERATOR 1  < (int2,int8),
	OPERATOR 1  < (int4,int2),
	OPERATOR 1  < (int4,int8),
	OPERATOR 1  < (int8,int2),
	OPERATOR 1  < (int8,int4),

	OPERATOR 2  <= (int2,int4),
	OPERATOR 2  <= (int2,int8),
	OPERATOR 2  <= (int4,int2),
	OPERATOR 2  <= (int4,int8),
	OPERATOR 2  <= (int8,int2),
	OPERATOR 2  <= (int8,int4),

	OPERATOR 3  = (int2,int4),
	OPERATOR 3  = (int2,int8),
	OPERATOR 3  = (int4,int2),
	OPERATOR 3  = (int4,int8),
	OPERATOR 3  = (int8,int2),
	OPERATOR 3  = (int8,int4),

	OPERATOR 4  >= (int2,int4),
	OPERATOR 4  >= (int2,int8),
	OPERATOR 4  >= (int4,int2),
	OPERATOR 4  >= (int4,int8),
	OPERATOR 4  >= (int8,int2),
	OPERATOR 4  >= (int8,int4),

	OPERATOR 5  > (int2,int4),
	OPERATOR 5  > (int2,int8),
	OPERATOR 5  > (int4,int2),
	OPERATOR 5  > (int4,int8),
	OPERATOR 5  > (int8,int2),
	OPERATOR 5  > (int8,int4),

	FUNCTION 1(int2,int4)  btint24cmp(int2,int4),
	FUNCTION 1(int2,int8)  btint28cmp(int2,int8),
	FUNCTION 1(int4,int2)  btint42cmp(int4,int2),
	FUNCTION 1(int4,int8)  btint48cmp(int4,int8),
	FUNCTION 1(int8,int4)  btint84cmp(int8,int4),
	FUNCTION 1(int8,int2)  btint82cmp(int8,int2),

	FUNCTION 2(int2,int2)  btint2sortsupport(internal),
	FUNCTION 2(int4,int4)  btint4sortsupport(internal),
	FUNCTION 2(int8,int8)  btint8sortsupport(internal),

    FUNCTION 3(int2,int8)  in_range(int2,int2,int8,bool,bool),
    FUNCTION 3(int2,int4)  in_range(int2,int2,int4,bool,bool),
    FUNCTION 3(int2,int2)  in_range(int2,int2,int2,bool,bool),
    FUNCTION 3(int4,int8)  in_range(int4,int4,int8,bool,bool),
    FUNCTION 3(int4,int4)  in_range(int4,int4,int4,bool,bool),
    FUNCTION 3(int4,int2)  in_range(int4,int4,int2,bool,bool),
    FUNCTION 3(int8,int8)  in_range(int8,int8,int8,bool,bool);

CREATE OPERATOR FAMILY float_ops USING merkle_lsm;

CREATE OPERATOR CLASS float4_ops DEFAULT
	FOR TYPE float4 USING merkle_lsm FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat4cmp(float4,float4);

CREATE OPERATOR CLASS float8_ops DEFAULT
	FOR TYPE float8 USING merkle_lsm FAMILY float_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btfloat8cmp(float8,float8);


ALTER OPERATOR FAMILY float_ops USING merkle_lsm ADD
	OPERATOR 1  < (float4,float8),
	OPERATOR 1  < (float8,float4),

	OPERATOR 2  <= (float4,float8),
	OPERATOR 2  <= (float8,float4),

	OPERATOR 3  = (float4,float8),
	OPERATOR 3  = (float8,float4),

	OPERATOR 4  >= (float4,float8),
	OPERATOR 4  >= (float8,float4),

	OPERATOR 5  > (float4,float8),
	OPERATOR 5  > (float8,float4),

	FUNCTION 1(float4,float8)  btfloat48cmp(float4,float8),
	FUNCTION 1(float8,float4)  btfloat84cmp(float8,float4),

	FUNCTION 2(float4,float4)  btfloat4sortsupport(internal),
	FUNCTION 2(float8,float8)  btfloat8sortsupport(internal),

    FUNCTION 3(float4,float8)  in_range(float4,float4,float8,bool,bool),
    FUNCTION 3(float8,float8)  in_range(float8,float8,float8,bool,bool);

CREATE OPERATOR CLASS bool_ops DEFAULT
	FOR TYPE bool USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btboolcmp(bool,bool);

CREATE OPERATOR CLASS bpchar_ops DEFAULT
	FOR TYPE bpchar USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  bpcharcmp(bpchar,bpchar),
	FUNCTION 2  bpchar_sortsupport(internal);

CREATE OPERATOR CLASS bytea_ops DEFAULT
	FOR TYPE bytea USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  byteacmp(bytea,bytea),
	FUNCTION 2  bytea_sortsupport(internal);

CREATE OPERATOR CLASS char_ops DEFAULT
	FOR TYPE "char" USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btcharcmp("char","char");

CREATE OPERATOR FAMILY datetime_ops USING merkle_lsm;

CREATE OPERATOR CLASS date_ops DEFAULT
	FOR TYPE date USING merkle_lsm FAMILY datetime_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  date_cmp(date,date),
	FUNCTION 2  date_sortsupport(internal);

CREATE OPERATOR CLASS timestamp_ops DEFAULT
	FOR TYPE timestamp USING merkle_lsm FAMILY datetime_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  timestamp_cmp(timestamp,timestamp),
	FUNCTION 2  timestamp_sortsupport(internal);

CREATE OPERATOR CLASS timestamptz_ops DEFAULT
	FOR TYPE timestamptz USING merkle_lsm FAMILY datetime_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  timestamptz_cmp(timestamptz,timestamptz),
	FUNCTION 2  timestamp_sortsupport(internal);

ALTER OPERATOR FAMILY datetime_ops USING merkle_lsm ADD
	OPERATOR 1  <  (date,timestamp),
	OPERATOR 2  <= (date,timestamp),
	OPERATOR 3  =  (date,timestamp),
	OPERATOR 4  >= (date,timestamp),
	OPERATOR 5  >  (date,timestamp),
	FUNCTION 1(date,timestamp)   date_cmp_timestamp(date,timestamp),

    OPERATOR 1  <  (date,timestamptz),
	OPERATOR 2  <= (date,timestamptz),
	OPERATOR 3  =  (date,timestamptz),
	OPERATOR 4  >= (date,timestamptz),
	OPERATOR 5  >  (date,timestamptz),
	FUNCTION 1(date,timestamptz) date_cmp_timestamptz(date,timestamptz),

	OPERATOR 1  <  (timestamp,date),
	OPERATOR 2  <= (timestamp,date),
	OPERATOR 3  =  (timestamp,date),
	OPERATOR 4  >= (timestamp,date),
	OPERATOR 5  >  (timestamp,date),
	FUNCTION 1(timestamp,date)   timestamp_cmp_date(timestamp,date),

	OPERATOR 1  <  (timestamptz,date),
	OPERATOR 2  <= (timestamptz,date),
	OPERATOR 3  =  (timestamptz,date),
	OPERATOR 4  >= (timestamptz,date),
	OPERATOR 5  >  (timestamptz,date),
	FUNCTION 1(timestamptz,date)   timestamptz_cmp_date(timestamptz,date),

	OPERATOR 1  <  (timestamp,timestamptz),
	OPERATOR 2  <= (timestamp,timestamptz),
	OPERATOR 3  =  (timestamp,timestamptz),
	OPERATOR 4  >= (timestamp,timestamptz),
	OPERATOR 5  >  (timestamp,timestamptz),
	FUNCTION 1(timestamp,timestamptz)   timestamp_cmp_timestamptz(timestamp,timestamptz),

	OPERATOR 1  <  (timestamptz,timestamp),
	OPERATOR 2  <= (timestamptz,timestamp),
	OPERATOR 3  =  (timestamptz,timestamp),
	OPERATOR 4  >= (timestamptz,timestamp),
	OPERATOR 5  >  (timestamptz,timestamp),
	FUNCTION 1(timestamptz,timestamp)   timestamptz_cmp_timestamp(timestamptz,timestamp),

    FUNCTION 3(date,interval) in_range(date,date,interval,bool,bool),
    FUNCTION 3(timestamp,interval) in_range(timestamp,timestamp,interval,bool,bool),
    FUNCTION 3(timestamptz,interval) in_range(timestamptz,timestamptz,interval,bool,bool);

CREATE OPERATOR CLASS interval_ops DEFAULT
	FOR TYPE interval USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  interval_cmp(interval,interval),
    FUNCTION 3  in_range(interval,interval,interval,bool,bool);

CREATE OPERATOR CLASS macaddr_ops DEFAULT
	FOR TYPE macaddr USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  macaddr_cmp(macaddr,macaddr),
	FUNCTION 2  macaddr_sortsupport(internal);

CREATE OPERATOR CLASS numeric_ops DEFAULT
	FOR TYPE numeric USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  numeric_cmp(numeric,numeric),
	FUNCTION 2  numeric_sortsupport(internal),
    FUNCTION 3  in_range(numeric,numeric,numeric,bool,bool);

CREATE OPERATOR CLASS oid_ops DEFAULT
	FOR TYPE oid USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btoidcmp(oid,oid),
	FUNCTION 2  btoidsortsupport(internal);

CREATE OPERATOR FAMILY text_ops USING merkle_lsm;

CREATE OPERATOR CLASS text_ops DEFAULT
	FOR TYPE text USING merkle_lsm FAMILY text_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  bttextcmp(text,text),
	FUNCTION 2  bttextsortsupport(internal);

CREATE OPERATOR CLASS name_ops DEFAULT
	FOR TYPE name USING merkle_lsm FAMILY text_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btnamecmp(name,name),
	FUNCTION 2  btnamesortsupport(internal);

ALTER OPERATOR FAMILY text_ops USING merkle_lsm ADD
	OPERATOR 1  <  (text,name),
	OPERATOR 2  <= (text,name),
	OPERATOR 3  =  (text,name),
	OPERATOR 4  >= (text,name),
	OPERATOR 5  >  (text,name),
	FUNCTION 1(text,name)   bttextnamecmp(text,name),

	OPERATOR 1  <  (name,text),
	OPERATOR 2  <= (name,text),
	OPERATOR 3  =  (name,text),
	OPERATOR 4  >= (name,text),
	OPERATOR 5  >  (name,text),
	FUNCTION 1(name,text)   btnametextcmp(name,text);

CREATE OPERATOR CLASS time_ops DEFAULT
	FOR TYPE time USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  time_cmp(time,time),
    FUNCTION 3  in_range(time,time,interval,bool,bool);

CREATE OPERATOR CLASS timetz_ops DEFAULT
	FOR TYPE timetz USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  timetz_cmp(timetz,timetz),
    FUNCTION 3  in_range(timetz,timetz,interval,bool,bool);

CREATE OPERATOR CLASS money_ops DEFAULT
	FOR TYPE money USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  cash_cmp(money,money);

CREATE OPERATOR CLASS uuid_ops DEFAULT
	FOR TYPE uuid USING merkle_lsm AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  uuid_cmp(uuid,uuid),
	FUNCTION 2  uuid_sortsupport(internal);


CREATE FUNCTION merkle_lsm_meta_handler(internal) RETURNS index_am_handler
AS 'MODULE_PATHNAME' LANGUAGE C;
CREATE ACCESS METHOD merkle_lsm_meta TYPE INDEX HANDLER merkle_lsm_meta_handler;

CREATE FUNCTION merkle_lsm_config(regclass) RETURNS json
AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;
CREATE FUNCTION merkle_lsm_flush(regclass) RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION merkle_lsm_components(regclass,
 OUT level int, OUT slot int, OUT relation regclass, OUT role text,
 OUT bytes bigint, OUT entries bigint, OUT merge_epoch bigint,
 OUT merge_count bigint, OUT last_merge_us bigint)
RETURNS SETOF record AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
CREATE VIEW merkle_lsm_status AS
 SELECT c.oid::regclass AS index, x.*, worker.pid AS worker_pid,
        worker.state AS worker_state, worker.wait_event
 FROM pg_class c JOIN pg_am a ON a.oid=c.relam
 CROSS JOIN LATERAL merkle_lsm_components(c.oid) x
 LEFT JOIN LATERAL
   (SELECT pid,state,wait_event FROM pg_stat_activity a
    WHERE a.datid=(SELECT oid FROM pg_database WHERE datname=current_database())
      AND a.backend_type='merkle_lsm merger' LIMIT 1) worker ON true
 WHERE a.amname='merkle_lsm';

-- Core Merkle remains independent of this contrib module. A setup that requests
-- the default AM must install the extension explicitly; no silent AM fallback.
CREATE FUNCTION merkle_create_lookup_index(target regclass,
 am text DEFAULT 'merkle_lsm', levels integer DEFAULT 3,
 top_index_size integer DEFAULT 1024, size_ratio integer DEFAULT 16)
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
 EXECUTE format('CREATE INDEX %I ON %s USING %I (merkle_partition_for_hash(%s,%s),%s,%s)%s',
   lookup_name,target,am,hash_expr,m.partitions,hash_expr,key_cols,options);
 result := to_regclass(format('%I.%I',schema_name,lookup_name));
 RETURN result;
END $$;
