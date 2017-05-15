--[2017-04-08 16:48:49] Summary: 14 of 14 statements executed in 2s 533ms (766 symbols in file)
SET UNDO_LOG 0;
SET LOG 0;
SET CACHE_SIZE 10000000;

CREATE ALIAS HALF_ROUND FOR "com.yahoo.sdvornik.db.Func.half_round";

CREATE ALIAS FINAL_UNCONS_MOD FOR "com.yahoo.sdvornik.db.Func.final_uncons_mod";

CREATE UNIQUE HASH INDEX loc_base_location_idx ON #LOC_BASE_FCST_PRODUCT#(location, week_indx);

CREATE UNIQUE HASH INDEX DEPARTMENT_product_idx ON DEPARTMENT(product);

CREATE UNIQUE HASH INDEX V_RCPT_INT_product_idx ON V_RCPT_INT(product);

CREATE HASH INDEX DC_ADJ_product_idx ON DC_ADJ(product);

CREATE HASH INDEX FRONTLINE_product_idx ON FRONTLINE(product);

CREATE HASH INDEX STORE_LOOKUP_department_idx ON STORE_LOOKUP(department);

CREATE HASH INDEX INV_MODEL_department_idx ON INV_MODEL(product);

CREATE HASH INDEX INV_MODEL_sizes_too_idx ON INV_MODEL(sizes, too);

CREATE INDEX STORE_LOOKUP_time_idx ON STORE_LOOKUP(id_indx);

CREATE INDEX INV_MODEL_aps_lower_idx ON INV_MODEL(aps_lower);

CREATE INDEX INV_MODEL_aps_idx ON INV_MODEL(aps);

CREATE HASH INDEX CL_STR_location_str_climate_idx ON CL_STR(location, str_climate);

--[2017-04-01 18:59:39] completed in 0ms
create table DEPARTMENT_SET AS (
  select distinct
    department
  from DEPARTMENT
  JOIN ARGS ON DEPARTMENT.product = ARGS.product
);

--[2017-04-01 16:01:52] completed in 1ms
create table FRONT_SOURCE_0 AS (
  select distinct
    location
    ,dbtwk_indx
    ,erlstmkdnwk_indx
    ,exitdate_indx
  from FRONTLINE
  JOIN ARGS ON FRONTLINE.product = ARGS.product
);

CREATE HASH INDEX DEPARTMENT_SET_department_idx ON DEPARTMENT_SET(department);

--[2017-04-01 16:06:41] completed in 0ms
create table TEMP_STORE_NUMSIZE AS (
  SELECT
    num_sizes
  FROM FRONT_SIZES JOIN ARGS ON FRONT_SIZES.product = ARGS.product
);

--[2017-04-01 16:01:19] completed in 15ms
create table FRONT_EXIT AS (
  select distinct
    ARGS.*
    ,greatest(initrcptwk_indx, ARGS.v_plancurrent) AS bottom
    ,least(exitdate_indx, ARGS.v_planend+1) AS up
  from FRONTLINE
  JOIN ARGS ON FRONTLINE.product = ARGS.product
);

--[2017-04-08 18:22:39] completed in 4ms
create table V_RCPT_INT_SET AS (
  select
    v_rcpt_int
  from V_RCPT_INT
  JOIN ARGS ON V_RCPT_INT.product = ARGS.product
);

--[2017-04-01 16:02:37] completed in 0ms
create table V_IRW_TABLE AS (
  select distinct
    initrcptwk_indx AS v_irw
  from FRONTLINE
  JOIN ARGS ON FRONTLINE.product = ARGS.product
);

--[2017-04-01 16:03:06] completed in 0ms
create table TIME_LIMIT_TABLE AS (
  select distinct
    coalesce(lastdcrcpt_indx,least(ARGS.v_planend, exitdate_indx)) AS time_limit
  from FRONTLINE
  JOIN ARGS ON FRONTLINE.product = ARGS.product
);

create table ADD_ARGS AS (
  SELECT
    v_irw
    ,v_rcpt_int
    ,time_limit
  FROM V_IRW_TABLE, V_RCPT_INT_SET, TIME_LIMIT_TABLE
);

--[2017-04-01 15:59:33] completed in 0ms
create table DC_ADJ_SET AS (
  select
    time_indx
    ,user_vrp
    ,locked_qty
    ,user_adj_qty
    ,on_order_qty
    ,oo_revision_qty
    ,adj_cost
  from DC_ADJ
  JOIN ARGS ON DC_ADJ.product = ARGS.product
);

create table EOH_BY_PRODUCT AS (
  select
    location,
    eoh AS value
  from EOH
  JOIN ARGS ON EOH.product = ARGS.product
);

CREATE HASH INDEX EOH_location_idx ON EOH_BY_PRODUCT(location);