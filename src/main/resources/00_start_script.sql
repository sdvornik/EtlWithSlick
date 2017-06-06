--[2017-04-08 16:48:49] Summary: 14 of 14 statements executed in 2s 533ms (766 symbols in file)
SET UNDO_LOG 0;
SET LOG 0;
SET CACHE_SIZE 10000000;

CREATE ALIAS create_toh_input_table_scala FOR "com.yahoo.sdvornik.db_scala.Func.create_toh_input_table";

CREATE ALIAS HALF_ROUND FOR "com.yahoo.sdvornik.db.Func.half_round";

CREATE ALIAS FINAL_UNCONS_MOD FOR "com.yahoo.sdvornik.db.Func.final_uncons_mod";

CREATE UNIQUE HASH INDEX loc_base_location_idx ON #LOC_BASE_FCST_PRODUCT#("location", "indx");

CREATE UNIQUE HASH INDEX DEPARTMENT_product_idx ON DEPARTMENT("product");

CREATE UNIQUE HASH INDEX V_RCPT_INT_product_idx ON V_RCPT_INT("product");

CREATE HASH INDEX DC_ADJ_product_idx ON DC_ADJ("product");

CREATE HASH INDEX FRONTLINE_product_idx ON FRONTLINE("product");

CREATE HASH INDEX STORE_LOOKUP_department_idx ON STORE_LOOKUP("department");

CREATE HASH INDEX INV_MODEL_department_idx ON INV_MODEL("department");

CREATE HASH INDEX INV_MODEL_sizes_too_idx ON INV_MODEL("num_sizes", "too");

CREATE INDEX INV_MODEL_aps_lower_idx ON INV_MODEL("aps_lower");

CREATE INDEX INV_MODEL_aps_idx ON INV_MODEL("aps");

CREATE INDEX STORE_LOOKUP_time_idx ON STORE_LOOKUP("indx");

CREATE HASH INDEX CL_STR_location_strclimate_idx ON CL_STR("location", "strclimate");

--[2017-04-01 18:59:39] completed in 0ms
create table DEPARTMENT_SET AS (
  select distinct
    "department"
  from DEPARTMENT
  JOIN ARGS ON DEPARTMENT."product" = ARGS.product
);
CREATE HASH INDEX DEPARTMENT_SET_department_idx ON DEPARTMENT_SET("department");

--[2017-04-01 16:01:52] completed in 1ms
create table FRONT_SOURCE_0 AS (
  select distinct
    "location"
    ,"dbtwk_indx"
    ,"erlstmkdnwk_indx"
    ,"exitdate_indx"
  from FRONTLINE
  JOIN ARGS ON FRONTLINE."product" = ARGS.product
);

--[2017-04-01 16:06:41] completed in 0ms
create table TEMP_STORE_NUMSIZE AS (
  SELECT
    "num_sizes"
  FROM FRONT_SIZES JOIN ARGS ON FRONT_SIZES."product" = ARGS.product
);



--[2017-04-01 16:01:19] completed in 15ms
create table FRONT_EXIT AS (
  select distinct
    greatest("initrcptwk_indx", ARGS.v_plancurrent) AS bottom
    ,least("exitdate_indx", ARGS.v_planend+1) AS up
  from FRONTLINE
  JOIN ARGS ON FRONTLINE."product" = ARGS.product
);

--[2017-04-08 18:22:39] completed in 4ms
create table V_RCPT_INT_SET AS (
  select
    v_rcpt_int
  from V_RCPT_INT
  JOIN ARGS ON V_RCPT_INT."product" = ARGS.product
);

--[2017-04-01 16:02:37] completed in 0ms
create table V_IRW_TABLE AS (
  select distinct
    "initrcptwk_indx" AS v_irw
  from FRONTLINE
  JOIN ARGS ON FRONTLINE."product" = ARGS.product
);

--[2017-04-01 16:03:06] completed in 0ms
create table TIME_LIMIT_TABLE AS (
  select distinct
    coalesce(lastdcrcpt_indx,least(ARGS.v_planend, "exitdate_indx")) AS time_limit
  from FRONTLINE
  JOIN ARGS ON FRONTLINE."product" = ARGS.product
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
  JOIN ARGS ON DC_ADJ."product" = ARGS.product
);

create table EOH_BY_PRODUCT AS (
  select
    "location" AS location,
    "eoh" AS value
  from EOH
  JOIN ARGS ON EOH."product" = ARGS.product
);

CREATE HASH INDEX EOH_location_idx ON EOH_BY_PRODUCT(location);

--[2017-04-08 18:27:55] completed in 11ms
create table VRP_SET_0 AS (
  SELECT
    TIME_INDX."indx" AS time_indx
    ,ROWNUM() AS rev_indx
    ,0 AS sys_vrp
    ,0 AS final_vrp
  FROM TIME_INDX, ARGS, ADD_ARGS
  WHERE
    TIME_INDX."indx" >= least(ARGS.v_plancurrent, ADD_ARGS.v_irw) AND
    TIME_INDX."indx" <= ARGS.v_planend
  ORDER BY "indx"
);


--[2017-04-08 18:28:17] completed in 3ms
create table GET_INDEX AS (
  SELECT DISTINCT
    mod(VRP_SET_0.rev_indx, ADD_ARGS.v_rcpt_int) AS value
  FROM 	VRP_SET_0
  JOIN ADD_ARGS ON ADD_ARGS.v_irw = VRP_SET_0.time_indx
);

--[2017-04-02 14:16:15] completed in 6ms
create table VRP_SET_1 AS (
  SELECT
    time_indx
    ,
    CASE
      WHEN
        mod(rev_indx, ADD_ARGS.v_rcpt_int) = GET_INDEX.value AND
        VRP_SET_0.time_indx >= ADD_ARGS.v_irw and
        VRP_SET_0.time_indx <= ADD_ARGS.time_limit THEN 1
      ELSE sys_vrp
    END AS sys_vrp
    ,final_vrp
  FROM VRP_SET_0, ADD_ARGS, ARGS, GET_INDEX
  WHERE
    VRP_SET_0.time_indx >= ARGS.v_plancurrent
);



--[2017-04-08 18:30:21] completed in 3ms
create table VRP_SET_2 AS (
  SELECT
    VRP_SET_1.time_indx
    ,sys_vrp
    ,final_vrp
    ,user_vrp
    ,user_adj_qty
    ,locked_qty
    ,on_order_qty
    ,oo_revision_qty
    ,DC_ADJ_SET.adj_cost
  FROM VRP_SET_1
  LEFT JOIN DC_ADJ_SET ON
     VRP_SET_1.time_indx = DC_ADJ_SET.time_indx
);

--[2017-04-04 22:53:16] completed in 9ms
create table VRP_SET_3 AS (
  SELECT
    time_indx
    ,ROWNUM() AS row_number
    ,
    CASE
      WHEN
        (sys_vrp = 1 and user_vrp != 0) or
        (sys_vrp = 1 and user_vrp is null) or
        user_vrp = 1 or
        user_adj_qty is not null or
        locked_qty is not null or
        on_order_qty is not null or
        oo_revision_qty is not null
        THEN 1
      ELSE final_vrp
    END AS final_vrp
    ,
    CASE
      WHEN
        user_adj_qty is not null or
        locked_qty is not null or
        on_order_qty is not null or
        oo_revision_qty is not null
      THEN coalesce(oo_revision_qty, on_order_qty, user_adj_qty, locked_qty)
      ELSE null
    END AS final_qty
    ,
    CASE
      WHEN user_adj_qty is not null or
           locked_qty is not null or
           on_order_qty is not null or
           oo_revision_qty is not null
        THEN 1
      ELSE null
    END AS cons
  FROM VRP_SET_2
  ORDER BY time_indx
);

--[2017-04-04 23:16:36] completed in 10ms
create table FINAL_VRP_CALC AS (
  WITH RECURSIVE REC(row_number, time_indx, sbkt) AS (
    SELECT
      row_number
      ,time_indx
      ,final_vrp AS sbkt
    FROM VRP_SET_3
    WHERE row_number = 1

    UNION ALL

    SELECT
      REC.row_number+1 AS row_number
      ,VRP_SET_3.time_indx
      ,REC.sbkt + final_vrp AS sbkt
    FROM REC
    JOIN VRP_SET_3 ON REC.row_number+1 = VRP_SET_3.row_number
  )
  SELECT * FROM REC
);

--[2017-04-04 23:18:24] completed in 7ms
create table VRP_SET_4 AS (
  SELECT
    FINAL_VRP_CALC.time_indx
    ,final_vrp
    ,final_qty
    ,cons
    ,sbkt
  FROM VRP_SET_3
  LEFT JOIN FINAL_VRP_CALC ON VRP_SET_3.time_indx = FINAL_VRP_CALC.time_indx
);

--[2017-04-08 18:34:29] completed in 9ms
insert into VRP_TEST ("indx", "cons", "sbkt", "final_vrp", "final_qty")
  SELECT
    time_indx AS "indx"
    ,
    CASE
      WHEN flag IS NOT NULL THEN 1
      ELSE cons
    END AS "cons"
    ,VRP_SET_4.sbkt AS "sbkt"
    ,final_vrp AS "final_vrp"
    ,coalesce(CAST(final_qty AS int),0) AS "final_qty"
  FROM VRP_SET_4
  LEFT JOIN (
    select distinct
      sbkt
      ,1 AS flag
    from VRP_SET_4
    where cons=1
  ) AS CONS_CALC ON VRP_SET_4.sbkt = CONS_CALC.sbkt
;


drop table VRP_SET_0;
drop table GET_INDEX;
drop table VRP_SET_1;
drop table VRP_SET_2;
drop table VRP_SET_3;
drop table FINAL_VRP_CALC;
drop table VRP_SET_4;

--drop table VRP_TEST;
