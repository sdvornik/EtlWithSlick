--[2017-04-08 18:09:26] Summary: 22 of 22 statements executed in 10s 380ms (3464 symbols in file)
--[2017-04-08 17:51:48] completed in 389ms
create table UPDATE_TOH AS (
    SELECT
      location
      ,week_indx AS indx
      ,debut
      ,too
      ,md_start
      ,exit
      ,num_sizes
      ,toh_calc
      ,fcst AS unc_fcst
      ,0 AS toh
    FROM TOH_INPUT
);
--[2017-04-08 21:36:25] completed in 158ms
CREATE HASH INDEX UPDATE_TOH_location_idx ON UPDATE_TOH(location);
--[2017-04-08 21:36:47] completed in 168ms
CREATE INDEX UPDATE_TOH_indx_idx ON UPDATE_TOH(indx);
--[2017-04-08 21:37:07] completed in 176ms
CREATE HASH INDEX UPDATE_TOH_location_indx_idx ON UPDATE_TOH(location, indx);

--[2017-04-01 23:58:46] completed in 285ms
create table REC_LOCATION AS (
     SELECT
      location
      ,indx
      ,debut
      ,too
      ,md_start
      ,exit
      ,num_sizes
      ,toh_calc
      ,unc_fcst
      ,toh
    FROM UPDATE_TOH
    WHERE toh_calc = 1
);
--[2017-04-08 21:37:51] completed in 129ms
CREATE HASH INDEX REC_LOCATION_sizes_too_idx ON REC_LOCATION(num_sizes, too);
--[2017-04-08 21:38:11] completed in 93ms
CREATE INDEX REC_LOCATION_unc_fcst_idx ON REC_LOCATION(unc_fcst);
--[2017-04-08 21:38:27] completed in 121ms
CREATE HASH INDEX REC_LOCATION_location_indx_idx ON REC_LOCATION(location, indx);

--[2017-04-08 17:54:48] completed in 1s 750ms
create table LKP_REC AS (
    select
      REC_LOCATION.location
      ,REC_LOCATION.indx
      ,min(woc) AS woc
    from INV_MODEL
    LEFT JOIN DEPARTMENT_SET ON
       DEPARTMENT_SET.department = INV_MODEL.product
    LEFT JOIN REC_LOCATION ON
      INV_MODEL.sizes = REC_LOCATION.num_sizes and
      INV_MODEL.too = REC_LOCATION.too
    where
      INV_MODEL.aps_lower <  REC_LOCATION.Unc_Fcst and
      INV_MODEL.aps >= REC_LOCATION.Unc_Fcst and
      REC_LOCATION.unc_fcst > 0
    group by REC_LOCATION.location, REC_LOCATION.indx

  UNION ALL

    select
      REC_LOCATION.location
      ,REC_LOCATION.indx
      ,min(woc) AS woc
    from INV_MODEL
    LEFT JOIN DEPARTMENT_SET ON
    DEPARTMENT_SET.department = INV_MODEL.product
    LEFT JOIN REC_LOCATION ON
      INV_MODEL.sizes = REC_LOCATION.num_sizes and
      INV_MODEL.too = REC_LOCATION.too
    where
      INV_MODEL.aps_lower <  REC_LOCATION.Unc_Fcst and
      INV_MODEL.aps >= REC_LOCATION.Unc_Fcst and
      REC_LOCATION.unc_fcst <=0
    group by REC_LOCATION.location, REC_LOCATION.indx
);

--[2017-04-08 21:39:21] completed in 185ms
CREATE HASH INDEX LKP_REC_location_indx_idx ON LKP_REC(location, indx);

--[2017-04-02 10:43:25] completed in 548ms
create table REC_LOCATION_EXT AS (
  SELECT distinct
    REC_LOCATION.location
    ,REC_LOCATION.indx
    ,REC_LOCATION.indx + LKP_REC.woc AS max_index
    ,debut
    ,too
    ,md_start
    ,exit
    ,num_sizes
    ,toh_calc
    ,unc_fcst
    ,toh
  FROM REC_LOCATION
  LEFT join LKP_REC ON
    REC_LOCATION.location = LKP_REC.location and
    REC_LOCATION.indx = LKP_REC.indx
);
--[2017-04-08 21:39:51] completed in 169ms
CREATE HASH INDEX REC_LOCATION_EXT_location_idx ON REC_LOCATION_EXT(location);

--[2017-04-02 10:49:43] completed in 6s 426ms
create table V_TOH_MOD AS (
  SELECT
    REC_LOCATION_EXT.location
    ,REC_LOCATION_EXT.indx
    ,sum(UPDATE_TOH.Unc_Fcst) AS toh
  FROM UPDATE_TOH
  JOIN REC_LOCATION_EXT ON
    UPDATE_TOH.location = REC_LOCATION_EXT.location AND
    UPDATE_TOH.indx >= REC_LOCATION_EXT.indx AND
    UPDATE_TOH.indx < REC_LOCATION_EXT.max_index
  GROUP BY REC_LOCATION_EXT.location, REC_LOCATION_EXT.indx
);

--[2017-04-08 18:08:21] completed in 150ms
CREATE HASH INDEX V_TOH_MOD_location_idx ON V_TOH_MOD(location, indx);

--[2017-04-02 11:26:14] completed in 648ms
create table TOH_INPUT_1 AS (
  SELECT
    UPDATE_TOH.location,
    UPDATE_TOH.indx,
    debut,
    too,
    md_start,
    exit,
    num_sizes,
    toh_calc,
    unc_fcst,
    V_TOH_MOD.toh
  FROM UPDATE_TOH
  LEFT JOIN V_TOH_MOD ON
    UPDATE_TOH.location = V_TOH_MOD.location AND
    UPDATE_TOH.indx = V_TOH_MOD.indx
);
--[2017-04-08 21:41:14] completed in 137ms
CREATE HASH INDEX TOH_INPUT_1_indx_idx ON TOH_INPUT_1(indx);
--[2017-04-08 21:41:33] completed in 134ms
CREATE HASH INDEX TOH_INPUT_1_location_idx ON TOH_INPUT_1(location);

drop table UPDATE_TOH;
drop table REC_LOCATION;
drop table LKP_REC;
drop table REC_LOCATION_EXT;
drop table V_TOH_MOD;
--drop table TOH_INPUT_1;