--[2017-04-08 17:46:53] Summary: 30 of 30 statements executed in 4s 124ms (5804 symbols in file)
--[2017-04-08 20:15:10] completed in 23ms
create table AM_WK AS (
  select
    ATTR_TIME."indx"
    ,"grade"
    ,"strclimate"
  from ATTR_TIME
  JOIN DEPARTMENT_SET ON ATTR_TIME."department" = DEPARTMENT_SET."department"
  JOIN (
    select distinct
      "flrset"
      ,"grade"
      ,"strclimate"
    from  FRONTLINE
    JOIN ARGS ON FRONTLINE."product" = ARGS.product
  ) AS FRONT_CLIMATE ON FRONT_CLIMATE."flrset" = ATTR_TIME."flrset"
);

--[2017-04-08 20:07:56] completed in 18ms
CREATE HASH INDEX AM_WK_week_grade_idx ON AM_WK("indx", "grade");

--[2017-04-08 20:29:50] completed in 384ms
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

--[2017-04-08 20:31:10] completed in 139ms
CREATE HASH INDEX GR_STR_WK_week_grade_idx ON GR_STR_WK("indx", grade);

--[2017-04-08 20:53:46] completed in 835ms
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
--[2017-04-08 20:57:04] completed in 143ms
CREATE INDEX STORE_LIST_week_indx_idx ON STORE_LIST("indx");
--[2017-04-08 20:57:24] completed in 171ms
CREATE HASH INDEX STORE_LIST_location_idx ON STORE_LIST(location);

--[2017-04-08 17:32:57] completed in 47ms
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
--[2017-04-08 20:59:38] completed in 6ms
CREATE HASH INDEX STORE_LYFECYLE_location_idx ON STORE_LYFECYLE(location);

--[2017-04-08 21:00:03] completed in 320ms
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
  --ORDER BY location, week_indx
);
--[2017-04-08 21:00:28] completed in 152ms
CREATE HASH INDEX STORE_LIST_WITH_LYFECYLE_location_idx ON STORE_LIST_WITH_LYFECYLE(location, indx);

--[2017-04-08 21:01:09] completed in 478ms
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

--[2017-04-01 23:34:46] completed in 301ms
create table LOCATION_INDX_KEYS AS (
  select distinct
    location
    ,indx
  from PRE_TEMP_TOH_INPUT
);
--[2017-04-08 17:40:37] completed in 144ms
CREATE HASH INDEX LOCATION_INDX_KEYS_location_indx_idx ON LOCATION_INDX_KEYS(location, indx);

--[2017-04-08 21:02:00] completed in 435ms
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
--[2017-04-08 21:02:20] completed in 207ms
CREATE HASH INDEX LOC_BASE_FCST_WITH_FRONTLINE_location_week_idx ON LOC_BASE_FCST_WITH_FRONTLINE(location, indx);

--[2017-04-01 23:56:08] completed in 658ms
create table TOH_INPUT AS (
  SELECT
    location,
    indx,
    toh_calc,
    debut,
    fcst,
    too,
    md_start,
    exit,
    num_sizes
  FROM PRE_TEMP_TOH_INPUT

  UNION ALL

  SELECT
    LOC_BASE_FCST_WITH_FRONTLINE.location,
    LOC_BASE_FCST_WITH_FRONTLINE.indx,
    0 AS toh_calc,
    CAST(NULL AS INT) AS debut,
    fcst,
    CAST(NULL AS INT) AS too,
    CAST(NULL AS INT) AS md_start,
    CAST(NULL AS INT) AS exit,
    CAST(NULL AS INT) AS num_sizes
  FROM LOC_BASE_FCST_WITH_FRONTLINE
  LEFT JOIN LOCATION_INDX_KEYS ON
    LOC_BASE_FCST_WITH_FRONTLINE.location = LOCATION_INDX_KEYS.location AND
    LOC_BASE_FCST_WITH_FRONTLINE.indx = LOCATION_INDX_KEYS.indx
  WHERE LOCATION_INDX_KEYS.indx IS NULL
);

--drop table AM_WK;
--drop table GR_STR_WK;

--drop table STORE_LIST;
--drop table STORE_LYFECYLE;
--drop table LOCATION_INDX_KEYS;
--drop table STORE_LIST_WITH_LYFECYLE;
--drop table PRE_TEMP_TOH_INPUT;
--drop table LOC_BASE_FCST_WITH_FRONTLINE;

