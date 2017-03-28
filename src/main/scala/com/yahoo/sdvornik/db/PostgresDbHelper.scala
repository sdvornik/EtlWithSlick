package com.yahoo.sdvornik.db


object PostgresDbHelper {
  import slick.jdbc.PostgresProfile.api._

  val postgresDb: Database = Database.forConfig("postgresInstance")

  val attrTimeQuery = sql"""
    select
       ATTR_TABLE.product
      ,ATTR_TABLE.time
      ,TIME_T.indx
    from public.dptflrsetattributes AS ATTR_TABLE
    JOIN public.time AS TIME_T ON
       TIME_T.levelid = 'Week' AND
       TIME_T.id >= ATTR_TABLE.rcptstart and
       TIME_T.id <= ATTR_TABLE.slsend;""".as[(String, String, Int)]


  val bodQuery = sql"""
    SELECT
      product AS department
      ,tolocation AS location
      ,leadtime AS value
    FROM public.bod;""".as[(String, String, Int)]


  val dcAdjQuery = sql"""
    SELECT
      product
      ,TIME_T.indx AS time_indx
      ,user_vrp
      ,locked_qty
      ,user_adj_qty
      ,on_order_qty
      ,oo_revision_qty
      ,CAST(adj_cost AS DOUBLE PRECISION)
    FROM public.temp_dc_adj AS DC_ADJ
    JOIN public.time AS TIME_T ON
      DC_ADJ.time=TIME_T.id;""".as[(String, Int, Int, Int, Int, Int, Int, Double)]

  val eohQuery = sql"""
    SELECT
      product
      ,location
      ,eoh AS value
    FROM public.eohdata;""".as[(String, String, Int)]

  val frontlineQuery = sql"""
    SELECT
      product
      ,location
      ,time AS flrset
      ,unnest(replace(grade,'''','')::text[]) as grade
      ,unnest(replace(strClimate,'''','')::text[]) as strClimate
      ,TIME_1.indx AS initrcptwk_indx
      ,TIME_2.indx AS exitdate_indx
      ,TIME_3.indx AS dbtwk_indx
      ,TIME_4.indx AS erlstmkdnwk_indx
      ,validsizes
      ,TIME_5.indx AS lastdcrcpt_indx
    FROM  public.frontline
    JOIN public.time AS TIME_1 ON initrcptwk = TIME_1.id
    JOIN public.time AS TIME_2 ON exitdate = TIME_2.id
    JOIN public.time AS TIME_3 ON dbtwk = TIME_3.id
    JOIN public.time AS TIME_4 ON erlstmkdnwk = TIME_4.id
    JOIN public.time AS TIME_5 ON lastdcrcpt = TIME_5.id;"""
    .as[(String, String, String, String, String, Int, Int, Int, Int, String, Int)]

  val invModelQuery = sql"""
    SELECT
      product
      ,sizes
      ,too
      ,aps_lower
      ,aps
      ,CAST(woc AS int)
    FROM public.invmodellkp_mod;""".as[(String, Int, Int, Long, Long, Int)]

  val departmentQuery = sql"""
    SELECT
      id AS product
      ,ancestor3 as department
    FROM public.prodstd AS PROD_STD;""".as[(String, String)]

  val clStrQuery = sql"""
    SELECT
      location
      ,strClimate
    FROM public.storeattributes;""".as[(String, String)]

  val storeLookupQuery = sql"""
    SELECT
      product AS department
      ,time
      ,unnest(replace(stores,'''','')::text[]) as location
      ,grade
    FROM public.storelookup;""".as[(String, String, String, String)]

  val vRcptIntQuery = sql"""
    SELECT
      product
      ,CAST(ccrcptint AS int) AS v_rcpt_int
    FROM public.stylecolorattributes;""".as[(Int, String)]

  val timeIndxQuery = sql"""
    SELECT
      indx
      ,id
    FROM public.time
    WHERE levelid = 'Week';""".as[(Int, String)]

  val timeStdQuery = sql"""
    SELECT
      TIME_T.indx AS id_indx
      ,ancestor3 AS time
    FROM public.timestd AS TIME_STD_T
    JOIN public.time AS TIME_T USING(id);""".as[(Int, String)]

  def productQuery(product: String) = sql"""
    SELECT
      location
      ,TIME_T.indx AS week_indx
      ,fcst
    FROM public.loc_base_fcst AS LOC_BASE_FCST
    JOIN public.time AS TIME_T ON LOC_BASE_FCST.week = TIME_T.id
    WHERE LOC_BASE_FCST.product = $product;""".as[(String, Int, Int)]
}
