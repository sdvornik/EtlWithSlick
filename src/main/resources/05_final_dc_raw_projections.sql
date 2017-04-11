--[2017-04-08 21:45:04] Summary: 7 of 7 statements executed in 99ms (6116 symbols in file)

--[2017-04-08 15:24:11] 0 row(s) affected in 4ms
create table EXTENDED_ARGS AS (
  SELECT
    ARGS.*
    ,FIRST_SBKT_INDX.indx AS first_sbkt_indx
  FROM 
    ARGS
    ,(
      SELECT
        MIN(indx) AS indx
      FROM 	VRP_TEST
      JOIN (
        SELECT
          min(sbkt) AS sbkt
        FROM 	VRP_TEST
        WHERE final_vrp > 0      
      ) AS MIN_SBKT ON VRP_TEST.sbkt = MIN_SBKT.sbkt   
    ) AS FIRST_SBKT_INDX
);

--[2017-04-08 15:24:11] 0 row(s) affected in 36ms
create table RCPT_AGG AS (
  SELECT
    indx
    ,CAST(sum(unc_need_lt) AS int) AS unc_need_dc
    ,CAST(sum(toh_lt) AS int) AS unc_toh_dc
  FROM RCPT
  GROUP BY indx
);

--[2017-04-08 15:24:11] 0 row(s) affected in 18ms
create table DC_CREATED AS (
    WITH RECURSIVE REC(
      location
      ,indx
      ,dc_poh
      ,dc_raw
      ,outbound
      ,deficit
      ,calc_var) AS (
      SELECT
        'DC'
        ,RCPT_AGG.indx, coalesce(EOH.eoh, 0) AS dc_poh
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN 0
          WHEN VRP_TEST.cons = 1 THEN coalesce(VRP_TEST.final_qty, 0)
          ELSE greatest(0, coalesce(RCPT_AGG.unc_need_dc, 0) - coalesce(EOH.eoh, 0))
        END AS dc_raw
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN
            least(coalesce(RCPT_AGG.unc_need_dc, 0), coalesce(EOH.eoh, 0))
          WHEN VRP_TEST.cons = 1 THEN
            least(coalesce(RCPT_AGG.unc_need_dc, 0), coalesce(EOH.eoh, 0) + coalesce(VRP_TEST.final_qty, 0))
          ELSE
            coalesce(RCPT_AGG.unc_need_dc, 0)
        END AS outbound
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN
            greatest(0, coalesce(RCPT_AGG.unc_need_dc, 0) - coalesce(EOH.eoh, 0))
          WHEN VRP_TEST.cons = 1 THEN
            greatest (
              0,
              coalesce(RCPT_AGG.unc_need_dc, 0) - coalesce(EOH.eoh, 0) - coalesce(VRP_TEST.final_qty, 0)
            )
          ELSE
            0
        END AS deficit
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN
            greatest(0, coalesce(EOH.eoh, 0) - coalesce(RCPT_AGG.unc_need_dc, 0))
          WHEN VRP_TEST.cons = 1 THEN
            greatest(0, coalesce(EOH.eoh, 0) + coalesce(VRP_TEST.final_qty, 0) - coalesce(RCPT_AGG.unc_need_dc, 0))
          ELSE
            greatest(0, coalesce(EOH.eoh, 0) - coalesce(RCPT_AGG.unc_need_dc, 0))
        END AS calc_var

      FROM RCPT_AGG
      JOIN EXTENDED_ARGS ON 1=1
      LEFT JOIN VRP_TEST ON VRP_TEST.indx = RCPT_AGG.indx
      LEFT JOIN EOH AS EOH ON EOH.location = 'DC'
      WHERE RCPT_AGG.indx = EXTENDED_ARGS.v_plancurrent

      UNION ALL

      SELECT
        'DC'
        ,REC.indx + 1 AS indx
        ,coalesce(REC.calc_var, 0) AS dc_poh
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN 0
          WHEN VRP_TEST.cons = 1 THEN
            coalesce(VRP_TEST.final_qty, 0)
          ELSE
            greatest(0, least(coalesce(RCPT_AGG.unc_toh_dc, 0), coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)) - REC.calc_var)
        END AS dc_raw
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN
            least(
                coalesce(RCPT_AGG.unc_toh_dc, 0),
                coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0),
                REC.calc_var
            )
          WHEN VRP_TEST.cons = 1 THEN
            least(
              coalesce(RCPT_AGG.unc_toh_dc, 0),
              coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0),
              REC.calc_var + coalesce(VRP_TEST.final_qty, 0)
            )
          ELSE
            least(
              coalesce(RCPT_AGG.unc_toh_dc, 0),
              coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
            )
        END AS outbound
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx THEN
            greatest(
              0,
              least(
                  coalesce(RCPT_AGG.unc_toh_dc, 0),
                  coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
              ) - REC.calc_var
            )
          WHEN VRP_TEST.cons = 1 THEN
            greatest(
              0,
              least(
                  coalesce(RCPT_AGG.unc_toh_dc, 0),
                  coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
              ) - REC.calc_var - coalesce(VRP_TEST.final_qty, 0)
            )
          ELSE
            0
        END AS deficit
        ,
        CASE
          WHEN RCPT_AGG.indx < EXTENDED_ARGS.first_sbkt_indx
            THEN coalesce(REC.calc_var,0) -
              greatest(
                0,
                REC.calc_var -
                  least(
                    coalesce(RCPT_AGG.unc_toh_dc, 0),
                    coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
                  )
              )
          WHEN VRP_TEST.cons = 1
            THEN greatest(
              0,
              REC.calc_var + coalesce(VRP_TEST.final_qty, 0) -
                least(
                  coalesce(RCPT_AGG.unc_toh_dc, 0),
                  coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
                )
            )
          ELSE
            greatest(
              0,
              REC.calc_var -
                least(
                  coalesce(RCPT_AGG.unc_toh_dc, 0),
                  coalesce(RCPT_AGG.unc_need_dc, 0)  + coalesce(REC.deficit,0)
                )
            )
        END AS calc_var
      FROM REC
      JOIN EXTENDED_ARGS ON 1=1
      LEFT JOIN RCPT_AGG ON
        REC.indx + 1 = RCPT_AGG.indx
        LEFT JOIN VRP_TEST ON
          VRP_TEST.indx = RCPT_AGG.indx
        LEFT JOIN EOH ON
          EOH.location = 'DC'

      WHERE REC.indx < EXTENDED_ARGS.v_planend
    )
    SELECT * FROM REC
);

--[2017-04-08 15:24:11] 0 row(s) affected in 7ms
create table DC AS (
  SELECT
    location
    ,DC_CREATED.indx
    ,dc_sbkt
    ,dc_raw
    ,outbound
    ,dc_poh
    ,deficit
  FROM DC_CREATED
  LEFT JOIN (
    select
      indx
      ,min(sbkt) AS dc_sbkt
    from VRP_TEST
    GROUP BY indx      
  ) AS DC_SBKT ON DC_CREATED.indx = DC_SBKT.indx
);
drop table EXTENDED_ARGS;
drop table RCPT_AGG;
drop table DC_CREATED;

--drop table DC;