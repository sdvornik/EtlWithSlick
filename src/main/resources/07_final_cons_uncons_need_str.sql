--[2017-04-08 23:32:24] Summary: 13 of 13 statements executed in 4s 271ms (9094 symbols in file)
--[2017-04-08 21:46:53] completed in 375ms
create table ITERATOR AS (
  SELECT
    v_sbkt_id
    ,v_sbkt_start
    ,v_sbkt_stop
    ,location
    ,indx
  FROM
    (
      SELECT
        VRP_TEST.sbkt AS v_sbkt_id
        ,indx
        ,v_sbkt_start
        ,v_sbkt_stop
      from VRP_TEST
      join (
        SELECT
          sbkt
          ,min(indx) AS v_sbkt_start
          ,max(indx) AS v_sbkt_stop
        from VRP_TEST
        group by sbkt
      ) AS MIN_MAX_INDX_BY_SBKT ON VRP_TEST.sbkt = MIN_MAX_INDX_BY_SBKT.sbkt
    ) AS VRP_ITERATOR,
    (
      SELECT DISTINCT
        location
      FROM RCPT
    ) AS RCPT_ITERATOR
);


--[2017-04-08 22:08:23] completed in 245ms
CREATE HASH INDEX ITERATOR_location_indx_idx ON ITERATOR(location, indx);
--[2017-04-08 22:12:11] completed in 170ms
CREATE HASH INDEX ITERATOR_location_idx ON ITERATOR(location);

--[2017-04-08 22:14:31] completed in 1s 125ms
create table RCPT_CUR AS (
  SELECT
    v_sbkt_id
    ,v_sbkt_start
    ,v_sbkt_stop
    ,ITERATOR.location
    ,ITERATOR.indx
    ,lead_time
    ,unc_fcst_lt
    ,toh_lt
    ,cons_eoh_lt
    ,FIRST_SBKT_INDX.indx AS v_frstSbkt
    ,MAX_CONS.value AS cons
    ,EOH.eoh
  FROM ITERATOR
  LEFT JOIN RCPT ON
    ITERATOR.location = RCPT.location AND
    ITERATOR.indx = RCPT.indx
  LEFT JOIN MAX_CONS ON
    ITERATOR.location = MAX_CONS.location
  LEFT JOIN EOH ON
    ITERATOR.location = EOH.location
  JOIN (
    SELECT
      MIN(indx) AS indx
    FROM 	VRP_TEST
    JOIN (
      SELECT
        min(sbkt) AS sbkt
      FROM 	VRP_TEST
      WHERE final_vrp > 0
    ) AS MIN_SBKT ON VRP_TEST.sbkt = MIN_SBKT.sbkt
  ) AS FIRST_SBKT_INDX ON 1=1
);

--[2017-04-08 21:53:05] completed in 119ms
CREATE INDEX RCPT_CUR_indx_idx ON RCPT_CUR(indx);
--[2017-04-08 21:53:24] completed in 189ms
CREATE HASH INDEX RCPT_CUR_location_idx ON RCPT_CUR(location);
--[2017-04-08 21:53:42] completed in 258ms
CREATE HASH INDEX RCPT_CUR_sbkt_location_indx_idx ON RCPT_CUR(v_sbkt_id, location, indx);

--[2017-04-08 23:22:00] completed in 1s 433ms
create table SET_CREATED AS (
    WITH RECURSIVE REC(
        v_sbkt_id
        ,location
        ,indx
        ,temp_boh
        ,temp_cons
        ,temp_need
        ,temp_rcpt
        ,temp_eoh
        ,temp_slsu) AS (
      SELECT
        v_sbkt_id
        ,RCPT_CUR.location
        ,indx
        ,
        CASE
          WHEN indx = v_plancurrent AND RCPT_CUR.lead_time = 0 THEN coalesce(RCPT_CUR.eoh, 0)
          ELSE coalesce(RCPT_CUR.Cons_EOH_lt,0)
        END AS temp_boh
        ,
        CASE
          WHEN indx = v_frstSbkt THEN RCPT_CUR.cons
          ELSE 0
        END AS temp_cons
        ,
        CASE
          WHEN indx = v_plancurrent and RCPT_CUR.lead_time = 0 THEN
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0 THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT, 0) - coalesce(RCPT_CUR.eoh, 0)) --
                  ELSE 0 --
                END
              ELSE 0 --
            END
          ELSE
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0
                    THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT,0) - coalesce(RCPT_CUR.Cons_EOH_lt,0)) --
                  ELSE 0 --
                END
              ELSE 0 --
            END
        END AS temp_need
        ,
        CASE
          WHEN indx = v_plancurrent and RCPT_CUR.lead_time = 0 THEN
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0 THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT, 0) - coalesce(RCPT_CUR.eoh, 0)) --
                  ELSE 0 --
                END
              ELSE 0 --
            END
          ELSE
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0
                    THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT,0) - coalesce(RCPT_CUR.Cons_EOH_lt,0)) --
                  ELSE 0 --
                END
              ELSE 0 --
            END
        END AS temp_rcpt
        ,
        CASE
          WHEN indx = v_plancurrent and RCPT_CUR.lead_time = 0 THEN
            CASE
            WHEN indx = v_frstSbkt THEN
              CASE
                WHEN RCPT_CUR.cons > 0
                      THEN greatest(0, greatest(coalesce(RCPT_CUR.eoh, 0), coalesce(RCPT_CUR.TOH_LT, 0)) - coalesce(RCPT_CUR.UNC_FCST_LT, 0)) --
                ELSE greatest(0, coalesce(RCPT_CUR.eoh, 0) - coalesce(RCPT_CUR.UNC_FCST_LT, 0)) --
              END
            ELSE greatest(0, coalesce(RCPT_CUR.eoh, 0) - coalesce(RCPT_CUR.UNC_FCST_LT, 0)) --
            END
          ELSE
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0
                    THEN greatest(0, greatest(coalesce(RCPT_CUR.Cons_EOH_lt,0), coalesce(RCPT_CUR.TOH_LT,0)) - coalesce(RCPT_CUR.UNC_FCST_LT,0)) --
                  ELSE greatest(0, coalesce(RCPT_CUR.Cons_EOH_lt,0) - coalesce(RCPT_CUR.UNC_FCST_LT,0)) --
                END
              ELSE greatest(0, coalesce(RCPT_CUR.Cons_EOH_lt,0) - coalesce(RCPT_CUR.UNC_FCST_LT,0)) --
            END
        END AS temp_eoh
        ,
        CASE
          WHEN indx = v_plancurrent and RCPT_CUR.lead_time = 0 THEN
            CASE
            WHEN indx = v_frstSbkt THEN
              CASE
                WHEN RCPT_CUR.cons > 0 THEN least(coalesce(RCPT_CUR.UNC_FCST_LT, 0) , greatest(coalesce(RCPT_CUR.eoh, 0), coalesce(RCPT_CUR.TOH_LT, 0))) --
                ELSE least (coalesce(RCPT_CUR.UNC_FCST_LT, 0), coalesce(RCPT_CUR.eoh, 0)) --
              END
            ELSE least (coalesce(RCPT_CUR.UNC_FCST_LT, 0), coalesce(RCPT_CUR.eoh, 0)) --
            END
          ELSE
            CASE
              WHEN indx = v_frstSbkt THEN
                CASE
                  WHEN RCPT_CUR.cons > 0
                    THEN least(coalesce(RCPT_CUR.UNC_FCST_LT,0) , greatest(coalesce(RCPT_CUR.Cons_EOH_lt,0), coalesce(RCPT_CUR.TOH_LT,0))) --
                  ELSE least (coalesce(RCPT_CUR.UNC_FCST_LT,0) , coalesce(RCPT_CUR.Cons_EOH_lt,0)) --
                END
              ELSE least (coalesce(RCPT_CUR.UNC_FCST_LT,0) , coalesce(RCPT_CUR.Cons_EOH_lt,0)) --
            END
        END AS temp_slsu

      FROM RCPT_CUR
      JOIN ARGS ON 1 = 1
      WHERE RCPT_CUR.indx = v_sbkt_start

      UNION ALL

      SELECT
        REC.v_sbkt_id
        ,REC.location
        ,REC.indx + 1
        ,coalesce(REC.Temp_EOH,0) AS temp_boh
        ,greatest(0, coalesce(REC.Temp_Cons,0) - coalesce(REC.Temp_Rcpt,0))  AS temp_cons
        ,
        CASE
          WHEN coalesce(REC.Temp_Cons,0) > coalesce(REC.Temp_Rcpt,0)
            THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT,0) - coalesce(REC.Temp_EOH,0)) --
          ELSE 0 --
        END AS temp_need
        ,
        CASE
          WHEN coalesce(REC.Temp_Cons,0) > coalesce(REC.Temp_Rcpt,0)
            THEN greatest(0 , coalesce(RCPT_CUR.TOH_LT,0) - coalesce(REC.Temp_EOH,0)) --
          ELSE 0 --
        END AS temp_rcpt
        ,
        CASE
          WHEN coalesce(REC.Temp_Cons,0) > coalesce(REC.Temp_Rcpt,0)
            THEN greatest(0, greatest(coalesce(REC.Temp_EOH,0), coalesce(RCPT_CUR.TOH_LT,0)) - coalesce(RCPT_CUR.UNC_FCST_LT,0)) --
          ELSE greatest(0, coalesce(REC.Temp_EOH,0) - coalesce(RCPT_CUR.UNC_FCST_LT,0)) --
        END AS temp_eoh
        ,
        CASE
          WHEN coalesce(REC.Temp_Cons,0) > coalesce(REC.Temp_Rcpt,0)
            THEN least(coalesce(RCPT_CUR.UNC_FCST_LT,0) , greatest(coalesce(REC.Temp_EOH,0), coalesce(RCPT_CUR.TOH_LT,0))) --
          ELSE least(coalesce(RCPT_CUR.UNC_FCST_LT,0), coalesce(REC.Temp_EOH,0)) --
        END AS temp_slsu
      FROM REC
      --JOIN ARGS ON 1 = 1
      LEFT JOIN RCPT_CUR ON
        REC.v_sbkt_id = RCPT_CUR.v_sbkt_id AND
        REC.location = RCPT_CUR.location AND
        REC.indx + 1  = RCPT_CUR.indx
      WHERE REC.indx < v_sbkt_stop
    )
    SELECT * FROM REC
);
--[2017-04-08 23:29:20] completed in 237ms
CREATE HASH INDEX SET_CREATED_location_idx ON SET_CREATED(location, indx);

--[2017-04-08 23:29:38] completed in 1s 11ms
create table RCPT_2 AS (
  SELECT
    RCPT.location
    ,RCPT.indx
    ,lead_time
    ,toh
    ,unc_boh
    ,unc_need
    ,unc_fcst
    ,exist_inv
    ,exist_slsu
    ,unc_fcst_lt
    ,unc_need_lt
    ,toh_lt
    ,exist_inv_lt
    ,exist_slsu_lt
    ,cons_slsu
    ,cons_eoh
    ,cons_slsu_lt
    ,cons_eoh_lt
    ,SET_CREATED.temp_cons
    ,SET_CREATED.temp_rcpt
    ,SET_CREATED.temp_eoh
    ,SET_CREATED.temp_boh
    ,SET_CREATED.temp_need
    ,SET_CREATED.temp_slsu
    ,SET_CREATED.v_sbkt_id AS dc_sbkt

  FROM RCPT
  LEFT JOIN SET_CREATED ON
    RCPT.location = SET_CREATED.location AND
    RCPT.indx = SET_CREATED.indx
  WHERE SET_CREATED.indx is null

  UNION ALL

  SELECT
    RCPT.location
    ,RCPT.indx
    ,lead_time
    ,toh
    ,unc_boh
    ,unc_need
    ,unc_fcst
    ,exist_inv
    ,exist_slsu
    ,unc_fcst_lt
    ,unc_need_lt
    ,toh_lt
    ,exist_inv_lt
    ,exist_slsu_lt
    ,cons_slsu
    ,cons_eoh
    ,cons_slsu_lt
    ,cons_eoh_lt
    ,SET_CREATED.temp_cons
    ,SET_CREATED.temp_rcpt
    ,SET_CREATED.temp_eoh
    ,SET_CREATED.temp_boh
    ,SET_CREATED.temp_need
    ,SET_CREATED.temp_slsu
    ,SET_CREATED.v_sbkt_id AS dc_sbkt

  FROM RCPT
  LEFT JOIN SET_CREATED ON
    RCPT.location = SET_CREATED.location AND
    RCPT.indx = SET_CREATED.indx
  WHERE SET_CREATED.indx is not null
);

drop table ITERATOR;
drop table RCPT_CUR;
drop table SET_CREATED;
--drop table RCPT_2;