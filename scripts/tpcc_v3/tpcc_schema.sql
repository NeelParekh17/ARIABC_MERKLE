-- Fresh deterministic v3 population; caller controls LOGGED conversion.
CREATE UNLOGGED TABLE public.customer (
    c_w_id integer NOT NULL,
    c_d_id integer NOT NULL,
    c_id integer NOT NULL,
    c_discount numeric(4,4) NOT NULL,
    c_credit character(2) NOT NULL,
    c_last character varying(16) NOT NULL,
    c_first character varying(16) NOT NULL,
    c_credit_lim numeric(12,2) NOT NULL,
    c_balance numeric(12,2) NOT NULL,
    c_ytd_payment numeric(12,2) NOT NULL,
    c_payment_cnt integer NOT NULL,
    c_delivery_cnt integer NOT NULL,
    c_street_1 character varying(20) NOT NULL,
    c_street_2 character varying(20) NOT NULL,
    c_city character varying(20) NOT NULL,
    c_state character(2) NOT NULL,
    c_zip character(9) NOT NULL,
    c_phone character(16) NOT NULL,
    c_since timestamp without time zone NOT NULL,
    c_middle character(2) NOT NULL,
    c_data character varying(500) NOT NULL
);

CREATE UNLOGGED TABLE public.district (
    d_w_id integer NOT NULL,
    d_id integer NOT NULL,
    d_ytd numeric(12,2) NOT NULL,
    d_tax numeric(4,4) NOT NULL,
    d_next_o_id integer NOT NULL,
    d_name character varying(10) NOT NULL,
    d_street_1 character varying(20) NOT NULL,
    d_street_2 character varying(20) NOT NULL,
    d_city character varying(20) NOT NULL,
    d_state character(2) NOT NULL,
    d_zip character(9) NOT NULL
);

CREATE UNLOGGED TABLE public.history (
    h_c_id integer NOT NULL,
    h_c_d_id integer NOT NULL,
    h_c_w_id integer NOT NULL,
    h_d_id integer NOT NULL,
    h_w_id integer NOT NULL,
    h_date timestamp without time zone NOT NULL,
    h_amount numeric(6,2) NOT NULL,
    h_data character varying(24) NOT NULL
);

CREATE UNLOGGED TABLE public.item (
    i_id integer NOT NULL,
    i_name character varying(24) NOT NULL,
    i_price numeric(5,2) NOT NULL,
    i_data character varying(50) NOT NULL,
    i_im_id integer NOT NULL
);

CREATE UNLOGGED TABLE public.new_order (
    no_w_id integer NOT NULL,
    no_d_id integer NOT NULL,
    no_o_id integer NOT NULL
);

CREATE UNLOGGED TABLE public.oorder (
    o_w_id integer NOT NULL,
    o_d_id integer NOT NULL,
    o_id integer NOT NULL,
    o_c_id integer NOT NULL,
    o_carrier_id integer,
    o_ol_cnt integer NOT NULL,
    o_all_local integer NOT NULL,
    o_entry_d timestamp without time zone NOT NULL
);

CREATE UNLOGGED TABLE public.order_line (
    ol_w_id integer NOT NULL,
    ol_d_id integer NOT NULL,
    ol_o_id integer NOT NULL,
    ol_number integer NOT NULL,
    ol_i_id integer NOT NULL,
    ol_delivery_d timestamp without time zone,
    ol_amount numeric(12,2) NOT NULL,
    ol_supply_w_id integer NOT NULL,
    ol_quantity numeric(6,2) NOT NULL,
    ol_dist_info character(24) NOT NULL
);

CREATE UNLOGGED TABLE public.stock (
    s_w_id integer NOT NULL,
    s_i_id integer NOT NULL,
    s_quantity integer NOT NULL,
    s_ytd numeric(8,2) NOT NULL,
    s_order_cnt integer NOT NULL,
    s_remote_cnt integer NOT NULL,
    s_data character varying(50) NOT NULL,
    s_dist_01 character(24) NOT NULL,
    s_dist_02 character(24) NOT NULL,
    s_dist_03 character(24) NOT NULL,
    s_dist_04 character(24) NOT NULL,
    s_dist_05 character(24) NOT NULL,
    s_dist_06 character(24) NOT NULL,
    s_dist_07 character(24) NOT NULL,
    s_dist_08 character(24) NOT NULL,
    s_dist_09 character(24) NOT NULL,
    s_dist_10 character(24) NOT NULL
);

CREATE UNLOGGED TABLE public.warehouse (
    w_id integer NOT NULL,
    w_ytd numeric(12,2) NOT NULL,
    w_tax numeric(4,4) NOT NULL,
    w_name character varying(10) NOT NULL,
    w_street_1 character varying(20) NOT NULL,
    w_street_2 character varying(20) NOT NULL,
    w_city character varying(20) NOT NULL,
    w_state character(2) NOT NULL,
    w_zip character(9) NOT NULL
);
