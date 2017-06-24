create table FRONT_CLIMATE AS (
    select distinct
      "flrset"
      ,"grade"
      ,"strclimate"
    from  FRONTLINE
    JOIN ARGS ON FRONTLINE."product" = ARGS.product
);

create table AM_WK AS (
  select
    ATTR_TIME."indx"
    ,"grade"
    ,"strclimate"
  from ATTR_TIME
  JOIN DEPARTMENT_SET ON ATTR_TIME."department" = DEPARTMENT_SET."department"
  JOIN FRONT_CLIMATE ON FRONT_CLIMATE."flrset" = ATTR_TIME."flrset"
);

CREATE HASH INDEX AM_WK_week_grade_idx ON AM_WK("indx", "grade");

create table GR_STR_WK AS (
  select
    STORE_LOOKUP."indx"
    ,STORE_LOOKUP."grade" AS grade
    ,STORE_LOOKUP."location" AS location
  from STORE_LOOKUP
  JOIN DEPARTMENT_SET ON STORE_LOOKUP."department" = DEPARTMENT_SET."department"
  JOIN FRONT_EXIT ON
    STORE_LOOKUP."indx" >= FRONT_EXIT.bottom  and
    STORE_LOOKUP."indx" < FRONT_EXIT.up
);

CREATE HASH INDEX GR_STR_WK_week_grade_idx ON GR_STR_WK("indx", grade);

create table STORE_LIST AS (
  SELECT distinct
    GR_STR_WK.location
    ,AM_WK."indx"
  FROM AM_WK
  JOIN GR_STR_WK ON
     AM_WK."indx" = GR_STR_WK."indx" AND
     AM_WK."grade" = GR_STR_WK.grade
  JOIN CL_STR ON
    CL_STR."location" = GR_STR_WK.location AND
    CL_STR."strclimate" = AM_WK."strclimate"
);

CREATE INDEX STORE_LIST_week_indx_idx ON STORE_LIST("indx");
CREATE HASH INDEX STORE_LIST_location_idx ON STORE_LIST(location);

create table STORE_LYFECYLE AS (
  SELECT DISTINCT
    location,
    greatest("dbtwk_indx", MIN_INDEX_BY_LOCATION.flrset_min)   AS debut,
    greatest("erlstmkdnwk_indx", MIN_INDEX_BY_LOCATION.flrset_min) AS md_start,
    (greatest("erlstmkdnwk_indx", MIN_INDEX_BY_LOCATION.flrset_min) -
     greatest("dbtwk_indx", MIN_INDEX_BY_LOCATION.flrset_min)) AS too,
    greatest("exitdate_indx", MIN_INDEX_BY_LOCATION.flrset_min)    AS exit
  FROM (
    select
      location
      ,min("indx") AS flrset_min
    from STORE_LIST
    group by location
  ) AS MIN_INDEX_BY_LOCATION,
  (
    select distinct
      "dbtwk_indx"
      ,"erlstmkdnwk_indx"
      ,"exitdate_indx"
    from FRONT_SOURCE_0
  ) AS FRONTLINE_WITH_TIME
);

CREATE HASH INDEX STORE_LYFECYLE_location_idx ON STORE_LYFECYLE(location);

create table STORE_LIST_WITH_LYFECYLE AS (
  SELECT
    STORE_LIST.location
    ,STORE_LIST."indx" AS indx
    ,STORE_LYFECYLE.debut
    ,STORE_LYFECYLE.too
    ,STORE_LYFECYLE.md_start
    ,STORE_LYFECYLE.exit
    ,
    CASE
      WHEN STORE_LIST."indx" < STORE_LYFECYLE.md_start THEN 1
      ELSE 0
    END AS toh_calc

  FROM STORE_LIST
  JOIN STORE_LYFECYLE ON
    STORE_LIST.location = STORE_LYFECYLE.location
  WHERE
    STORE_LIST."indx" >= STORE_LYFECYLE.debut AND
    STORE_LIST."indx" < STORE_LYFECYLE.exit
);

CREATE HASH INDEX STORE_LIST_WITH_LYFECYLE_location_idx ON STORE_LIST_WITH_LYFECYLE(location, indx);

create table PRE_TEMP_TOH_INPUT AS (
  SELECT
    STORE_LIST_WITH_LYFECYLE.location
    ,STORE_LIST_WITH_LYFECYLE.indx
    ,STORE_LIST_WITH_LYFECYLE.debut
    ,STORE_LIST_WITH_LYFECYLE.too
    ,STORE_LIST_WITH_LYFECYLE.md_start
    ,STORE_LIST_WITH_LYFECYLE.exit
    ,STORE_LIST_WITH_LYFECYLE.toh_calc
    ,TEMP_STORE_NUMSIZE."num_sizes" AS num_sizes
    ,#LOC_BASE_FCST_PRODUCT#."fcst" AS fcst
  FROM #LOC_BASE_FCST_PRODUCT#
  JOIN STORE_LIST_WITH_LYFECYLE ON
    #LOC_BASE_FCST_PRODUCT#."indx" = STORE_LIST_WITH_LYFECYLE.indx AND
    #LOC_BASE_FCST_PRODUCT#."location" = STORE_LIST_WITH_LYFECYLE.location
  JOIN TEMP_STORE_NUMSIZE ON 1=1
);

create table LOCATION_INDX_KEYS AS (
  select distinct
    location
    ,indx
  from PRE_TEMP_TOH_INPUT
);

CREATE HASH INDEX LOCATION_INDX_KEYS_location_indx_idx ON LOCATION_INDX_KEYS(location, indx);

create table LOC_BASE_FCST_WITH_FRONTLINE AS (
  select
    #LOC_BASE_FCST_PRODUCT#."location"  AS location
    ,#LOC_BASE_FCST_PRODUCT#."indx" AS indx
    ,#LOC_BASE_FCST_PRODUCT#."fcst" AS fcst
  FROM #LOC_BASE_FCST_PRODUCT#
  JOIN ARGS ON 1=1
  JOIN FRONT_SOURCE_0 ON
    #LOC_BASE_FCST_PRODUCT#."indx" >= greatest(FRONT_SOURCE_0."dbtwk_indx", ARGS.v_plancurrent)  and
    #LOC_BASE_FCST_PRODUCT#."indx" < least(FRONT_SOURCE_0."exitdate_indx", ARGS.v_planend+1)
);

CREATE HASH INDEX LOC_BASE_FCST_WITH_FRONTLINE_location_week_idx ON LOC_BASE_FCST_WITH_FRONTLINE(location, indx);

create table TOH_INPUT AS (
  SELECT
    location,
    indx,

    too,
    num_sizes,

    toh_calc,
    fcst AS unc_fcst
  FROM PRE_TEMP_TOH_INPUT

  UNION ALL

  SELECT
    LOC_BASE_FCST_WITH_FRONTLINE.location,
    LOC_BASE_FCST_WITH_FRONTLINE.indx,

    CAST(NULL AS INT) AS too,
    CAST(NULL AS INT) AS num_sizes,

    0 AS toh_calc,
    fcst  AS unc_fcst
  FROM LOC_BASE_FCST_WITH_FRONTLINE
  LEFT JOIN LOCATION_INDX_KEYS ON
    LOC_BASE_FCST_WITH_FRONTLINE.location = LOCATION_INDX_KEYS.location AND
    LOC_BASE_FCST_WITH_FRONTLINE.indx = LOCATION_INDX_KEYS.indx
  WHERE LOCATION_INDX_KEYS.indx IS NULL
);

CREATE HASH INDEX TOH_INPUT_location_idx ON TOH_INPUT(location);

CREATE INDEX TOH_INPUT_indx_idx ON TOH_INPUT(indx);

CREATE HASH INDEX TOH_INPUT_location_indx_idx ON TOH_INPUT(location, indx);

create table REC_LOCATION AS (
     SELECT
      location
      ,indx

      ,too
      ,num_sizes

      ,unc_fcst

    FROM TOH_INPUT
    WHERE toh_calc = 1
);

CREATE HASH INDEX REC_LOCATION_sizes_too_idx ON REC_LOCATION(num_sizes, too);

CREATE INDEX REC_LOCATION_unc_fcst_idx ON REC_LOCATION(unc_fcst);

CREATE HASH INDEX REC_LOCATION_location_indx_idx ON REC_LOCATION(location, indx);

create table LKP_REC AS (
    select
      REC_LOCATION.location
      ,REC_LOCATION.indx
      ,min(INV_MODEL."woc") AS woc
    from INV_MODEL
    --TODO Change Left to inner
    JOIN DEPARTMENT_SET ON
       DEPARTMENT_SET."department" = INV_MODEL."department"
    JOIN REC_LOCATION ON
      INV_MODEL."num_sizes" = REC_LOCATION.num_sizes and
      INV_MODEL."too" = REC_LOCATION.too
    where
      INV_MODEL."aps_lower" <  REC_LOCATION.Unc_Fcst and
      INV_MODEL."aps" >= REC_LOCATION.Unc_Fcst and
      REC_LOCATION.unc_fcst > 0
    group by REC_LOCATION.location, REC_LOCATION.indx

  UNION ALL

    select
      REC_LOCATION.location
      ,REC_LOCATION.indx
      ,min("woc") AS woc
    from INV_MODEL
    JOIN DEPARTMENT_SET ON
      DEPARTMENT_SET."department" = INV_MODEL."department"
    JOIN REC_LOCATION ON
      INV_MODEL."num_sizes" = REC_LOCATION.num_sizes and
      INV_MODEL."too" = REC_LOCATION.too
    where
      INV_MODEL."aps_lower" <  REC_LOCATION.Unc_Fcst and
      INV_MODEL."aps" >= REC_LOCATION.Unc_Fcst and
      REC_LOCATION.unc_fcst <=0
    group by REC_LOCATION.location, REC_LOCATION.indx
);

CREATE HASH INDEX LKP_REC_location_indx_idx ON LKP_REC(location, indx);

create table REC_LOCATION_EXT AS (
  SELECT distinct
    REC_LOCATION.location
    ,REC_LOCATION.indx
    ,REC_LOCATION.indx + LKP_REC.woc AS max_index
  FROM REC_LOCATION
  LEFT join LKP_REC ON
    REC_LOCATION.location = LKP_REC.location and
    REC_LOCATION.indx = LKP_REC.indx
  ORDER BY location, indx
);

CREATE HASH INDEX REC_LOCATION_EXT_location_idx ON REC_LOCATION_EXT(location);

create table V_TOH_MOD AS (
    SELECT
      REC_LOCATION_EXT.location
      ,REC_LOCATION_EXT.indx
      ,sum(TOH_INPUT.Unc_Fcst) AS toh
    FROM TOH_INPUT
    JOIN REC_LOCATION_EXT ON REC_LOCATION_EXT.location = TOH_INPUT.location
    WHERE TOH_INPUT.indx >= REC_LOCATION_EXT.indx AND
          TOH_INPUT.indx < REC_LOCATION_EXT.max_index
    GROUP BY REC_LOCATION_EXT.location, REC_LOCATION_EXT.indx
);

CREATE HASH INDEX V_TOH_MOD_location_idx ON V_TOH_MOD(location, indx);

create table TOH_INPUT_FINAL AS (
  SELECT
    TOH_INPUT.location,
    TOH_INPUT.indx,
    TOH_INPUT.unc_fcst,
    V_TOH_MOD.toh
  FROM TOH_INPUT
  LEFT JOIN V_TOH_MOD ON
    TOH_INPUT.location = V_TOH_MOD.location AND
    TOH_INPUT.indx = V_TOH_MOD.indx
  JOIN ARGS ON
    TOH_INPUT.indx >= ARGS.v_plancurrent AND
    TOH_INPUT.indx <= ARGS.v_planend
);

CREATE HASH INDEX TOH_INPUT_FINAL_location_idx ON TOH_INPUT_FINAL(location);

create table V_LT AS (
  SELECT
    BOD."location" AS location
    ,"bod" AS value
  from BOD
  join DEPARTMENT_SET ON
    BOD."department" = DEPARTMENT_SET."department"
  join (
    SELECT distinct
      location
    FROM TOH_INPUT_FINAL
  ) AS STR ON
    BOD."location" = STR.location
);

drop table AM_WK;
drop table GR_STR_WK;
drop table STORE_LIST;
drop table STORE_LYFECYLE;
drop table LOCATION_INDX_KEYS;
drop table STORE_LIST_WITH_LYFECYLE;
drop table PRE_TEMP_TOH_INPUT;
drop table LOC_BASE_FCST_WITH_FRONTLINE;
drop table TOH_INPUT;
drop table TOH_INPUT_1;
drop table TOH_INPUT;
drop table REC_LOCATION;
drop table LKP_REC;
drop table REC_LOCATION_EXT;
drop table V_TOH_MOD;