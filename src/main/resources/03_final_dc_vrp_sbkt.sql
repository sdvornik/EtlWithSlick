--[2017-04-08 18:52:12] Summary: 15 of 15 statements executed in 62ms (3325 symbols in file)

--[2017-04-08 18:27:55] completed in 11ms
create table VRP_SET_0 AS (
  SELECT
    TIME_INDX.indx AS time_indx
    ,ROWNUM() AS rev_indx
    ,0 AS sys_vrp
    ,0 AS final_vrp
  FROM TIME_INDX, ARGS, ADD_ARGS
  WHERE
    TIME_INDX.indx >= least(ARGS.v_plancurrent, ADD_ARGS.v_irw) AND
    TIME_INDX.indx <= ARGS.v_planend
  ORDER BY indx
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
      THEN coalesce(oo_revision_qty, coalesce(on_order_qty, coalesce(user_adj_qty, locked_qty)))
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
create table VRP_TEST AS (
  SELECT
    time_indx AS indx
    ,
    CASE
      WHEN flag IS NOT NULL THEN 1
      ELSE cons
    END AS cons
    ,VRP_SET_4.sbkt
    ,final_vrp
    ,coalesce(CAST(final_qty AS int),0) AS final_qty
  FROM VRP_SET_4
  LEFT JOIN (
    select distinct
      sbkt
      ,1 AS flag
    from VRP_SET_4
    where cons=1
  ) AS CONS_CALC ON VRP_SET_4.sbkt = CONS_CALC.sbkt
);


drop table VRP_SET_0;
drop table GET_INDEX;
drop table VRP_SET_1;
drop table VRP_SET_2;
drop table VRP_SET_3;
drop table FINAL_VRP_CALC;
drop table VRP_SET_4;








