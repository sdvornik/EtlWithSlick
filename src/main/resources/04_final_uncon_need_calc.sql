--[2017-04-08 21:43:29] Summary: 15 of 15 statements executed in 4s 398ms (6907 symbols in file)

--[2017-04-08 16:40:17] completed in 87ms
create table V_LT AS (
  SELECT
    BOD.location
    ,bod
  from BOD
  join DEPARTMENT_SET ON
    BOD.department = DEPARTMENT_SET.department
  join (
    SELECT distinct
      location
    FROM TOH_INPUT_1
  ) AS STR ON
    BOD.location = STR.location
);

--[2017-04-08 16:41:29] completed in 400ms
create table TOH_INPUT_2 AS (
  SELECT
    TOH_INPUT_1.location
    ,indx
    ,coalesce(unc_fcst,0) AS unc_fcst
    ,coalesce(CAST(toh AS int),0) AS toh, coalesce(EOH.eoh, 0) AS eoh_by_location
  FROM TOH_INPUT_1
  LEFT JOIN EOH ON
     TOH_INPUT_1.location = EOH.location
  JOIN ARGS ON
    TOH_INPUT_1.indx >= ARGS.v_plancurrent AND
    TOH_INPUT_1.indx <= ARGS.v_planend
  ORDER BY location, indx
);
CREATE HASH INDEX TOH_INPUT_2_location_indx_idx ON TOH_INPUT_2(location, indx);

--[2017-04-08 16:47:05] completed in 1s 119ms
create table RCPT_CREATED AS (
    WITH RECURSIVE REC(
        location
        ,indx
        ,lead_time
        ,unc_fcst
        ,toh
        ,exist_inv
        ,exist_slsu
        ,unc_boh
        ,unc_need
        ,cons_slsu
        ,cons_eoh) AS (
      SELECT
        TOH_INPUT_2.location
        ,indx
        ,coalesce(V_LT.bod ,0) AS lead_time
        ,coalesce(TOH_INPUT_2.unc_fcst,0) as unc_fcst
        ,coalesce(TOH_INPUT_2.toh,0) as toh
        ,greatest(0, coalesce(eoh_by_location,0)) as exist_inv
        ,least(
          greatest(0, coalesce(eoh_by_location,0)),
          coalesce(TOH_INPUT_2.unc_fcst,0)
        ) AS exist_slsu
        ,greatest(0, coalesce(eoh_by_location, 0)) AS unc_boh
        ,CASE
          WHEN indx < ARGS.v_plancurrent + coalesce(V_LT.bod ,0) THEN 0
          ELSE greatest( 0, coalesce(TOH_INPUT_2.toh,0) - greatest(0, coalesce(eoh_by_location,0)))
        END AS unc_need
        ,CASE
          WHEN indx < ARGS.v_plancurrent + coalesce(V_LT.bod ,0)
            THEN least(coalesce(eoh_by_location,0), coalesce(TOH_INPUT_2.Unc_Fcst,0))
          ELSE 0
        END AS cons_slsu
        ,CASE
          WHEN indx < ARGS.v_plancurrent + coalesce(V_LT.bod ,0)
            THEN coalesce(eoh_by_location,0) - least(coalesce(eoh_by_location,0), coalesce(TOH_INPUT_2.Unc_Fcst,0))
          ELSE 0
        END  AS cons_eoh

      FROM TOH_INPUT_2
      JOIN ARGS ON 1 = 1
      LEFT JOIN V_LT ON TOH_INPUT_2.location = V_LT.location
      WHERE indx = ARGS.v_plancurrent

      UNION ALL

      SELECT
        REC.location
        ,REC.indx + 1 AS indx
        ,REC.lead_time
        ,coalesce(TOH_INPUT_2.unc_fcst,0) as unc_fcst
        ,coalesce(TOH_INPUT_2.toh,0) as toh
        ,greatest (0, coalesce(REC.unc_boh,0) - coalesce(REC.unc_fcst,0)) AS exist_inv

        ,least (
          greatest (0, coalesce(REC.unc_boh,0) - coalesce(REC.unc_fcst,0)),
          coalesce(TOH_INPUT_2.Unc_Fcst,0)
         ) AS exist_slsu

        ,greatest(
          0,
          coalesce(REC.Unc_BOH,0) + coalesce(REC.Unc_Need,0) - coalesce(REC.Unc_Fcst,0)
         ) AS unc_boh
        ,CASE
          WHEN REC.indx + 1 < ARGS.v_plancurrent + REC.lead_time THEN 0
          ELSE
            greatest(
              0,
              coalesce(TOH_INPUT_2.toh,0) -
                greatest(
                  0,
                  coalesce(REC.unc_boh,0) + coalesce(REC.unc_need,0) - coalesce(REC.unc_fcst,0)
                )
            )
        END AS unc_need
        ,CASE
          WHEN REC.indx + 1 < ARGS.v_plancurrent + REC.lead_time THEN
            least(
              greatest(
                0,
                coalesce(REC.exist_inv,0) - coalesce(REC.exist_slsu,0)
              ),
              coalesce(TOH_INPUT_2.unc_fcst,0)
            )
          ELSE 0
        END AS cons_slsu
        ,CASE
          WHEN REC.indx + 1 < ARGS.v_plancurrent + REC.lead_time THEN
            greatest(0, coalesce(REC.exist_inv,0) - coalesce(REC.exist_slsu,0)) -
            least(
              greatest(
                0,
                coalesce(REC.exist_inv,0) - coalesce(REC.exist_slsu,0)
              ),
              coalesce(TOH_INPUT_2.unc_fcst,0)
            )
          ELSE 0
        END  AS cons_eoh
      FROM REC
      JOIN ARGS ON 1=1
      LEFT JOIN TOH_INPUT_2 ON
        REC.location = TOH_INPUT_2.location AND
        REC.indx + 1 = TOH_INPUT_2.indx
      WHERE REC.indx<ARGS.v_planend
    )

    SELECT * FROM REC
);
--[2017-04-08 16:47:44] completed in 461ms
create table RCPT_EXTENDED AS (
  SELECT
    location,
    indx,
    lead_time,
    unc_fcst,
    toh,
    exist_inv,
    exist_slsu,
    unc_boh,
    unc_need,
    cons_slsu,
    cons_eoh,
    CAST(NULL AS INT) AS unc_fcst_lt,
    CAST(NULL AS INT) AS toh_lt,
    CAST(NULL AS INT) AS exist_inv_lt,
    CAST(NULL AS INT) AS exist_slsu_lt,
    CAST(NULL AS INT) AS unc_need_lt,
    CAST(NULL AS INT) AS cons_slsu_lt,
    CAST(NULL AS INT) AS cons_eoh_lt
  FROM RCPT_CREATED
);
CREATE HASH INDEX RCPT_EXTENDED_location_indx_idx ON RCPT_EXTENDED(location, indx);

--[2017-04-02 14:02:48] completed in 1s 136ms
create table RCPT_EXTENDED_ADD AS (
  SELECT
    location
    ,indx - lead_time AS indx
    ,lead_time
    ,CAST(null AS int) AS unc_fcst
    ,CAST(null AS int) AS toh
    ,CAST(null AS int) AS exist_inv
    ,CAST(null AS int) AS exist_slsu
    ,CAST(null AS int) AS unc_boh
    ,CAST(null AS int) AS unc_need
    ,CAST(null AS int) AS cons_slsu
    ,CAST(null AS int) AS cons_eoh
    ,unc_fcst AS unc_fcst_lt
    ,toh AS toh_lt
    ,exist_inv AS exist_inv_lt
    ,exist_slsu AS exist_slsu_lt
    ,unc_need AS unc_need_lt
    ,cons_slsu AS cons_slsu_lt
    ,cons_eoh AS cons_eoh_lt
  FROM RCPT_CREATED
);
CREATE HASH INDEX RCPT_EXTENDED_ADD_location_indx_idx ON RCPT_EXTENDED_ADD(location, indx);

  --TODO
--[2017-04-02 14:08:23] completed in 1s 201ms
create table RCPT AS (
  SELECT
    RCPT_EXTENDED.*
  FROM RCPT_EXTENDED
  LEFT JOIN RCPT_EXTENDED_ADD ON
     RCPT_EXTENDED.location = RCPT_EXTENDED_ADD.location AND
     RCPT_EXTENDED.indx = RCPT_EXTENDED_ADD.indx
  WHERE RCPT_EXTENDED_ADD.indx is null

  UNION ALL

  SELECT
    RCPT_EXTENDED_ADD.*
  FROM RCPT_EXTENDED_ADD
  LEFT JOIN RCPT_EXTENDED ON
     RCPT_EXTENDED.location = RCPT_EXTENDED_ADD.location AND
     RCPT_EXTENDED.indx = RCPT_EXTENDED_ADD.indx
  WHERE RCPT_EXTENDED.indx is null

  UNION ALL

  SELECT
    RCPT_EXTENDED.location,
    RCPT_EXTENDED.indx,
    RCPT_EXTENDED.lead_time,
    RCPT_EXTENDED.unc_fcst,
    RCPT_EXTENDED.toh,
    RCPT_EXTENDED.exist_inv,
    RCPT_EXTENDED.exist_slsu,
    RCPT_EXTENDED.unc_boh,
    RCPT_EXTENDED.unc_need,
    RCPT_EXTENDED.cons_slsu,
    RCPT_EXTENDED.cons_eoh,
    RCPT_EXTENDED_ADD.unc_fcst_lt,
    RCPT_EXTENDED_ADD.toh_lt,
    RCPT_EXTENDED_ADD.exist_inv_lt,
    RCPT_EXTENDED_ADD.exist_slsu_lt,
    RCPT_EXTENDED_ADD.unc_need_lt,
    RCPT_EXTENDED_ADD.cons_slsu_lt,
    RCPT_EXTENDED_ADD.cons_eoh_lt
  FROM RCPT_EXTENDED
  LEFT JOIN RCPT_EXTENDED_ADD ON
     RCPT_EXTENDED.location = RCPT_EXTENDED_ADD.location AND
     RCPT_EXTENDED.indx = RCPT_EXTENDED_ADD.indx
  WHERE RCPT_EXTENDED_ADD.indx is not null
);
CREATE HASH INDEX RCPT_location_indx_idx ON RCPT(location, indx);

drop table V_LT;
drop table TOH_INPUT_2;
drop table RCPT_CREATED;
drop table RCPT_EXTENDED;
drop table RCPT_EXTENDED_ADD;

--drop table RCPT;