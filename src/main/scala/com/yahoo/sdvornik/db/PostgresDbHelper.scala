package com.yahoo.sdvornik.db


object PostgresDbHelper {
  import slick.jdbc.PostgresProfile.api._

  val postgresDb: Database = Database.forConfig("postgresInstance")
//TODO ORDER BY
  val attrTimeQuery = sql"""
    select
       ATTR_TABLE.product
      ,ATTR_TABLE.time
      ,TIME_T.indx
    from public.dptflrsetattributes AS ATTR_TABLE
    JOIN public.time AS TIME_T ON
       TIME_T.levelid = 'Week' AND
       TIME_T.id >= ATTR_TABLE.rcptstart and
       TIME_T.id <= ATTR_TABLE.slsend
       ORDER BY product, time, indx;""".as[(String, String, Int)]


  val bodQuery = sql"""
    SELECT
      product
      ,tolocation
      ,leadtime
    FROM public.bod
    ORDER BY product, tolocation;""".as[(String, String, Int)]


  val dcAdjQuery = sql"""
    SELECT
      product
      ,indx
      ,user_vrp
      ,locked_qty
      ,user_adj_qty
      ,on_order_qty
      ,oo_revision_qty
      ,CAST(adj_cost AS DOUBLE PRECISION)
    FROM public.temp_dc_adj AS DC_ADJ
    JOIN public.time AS TIME_T ON DC_ADJ.time=TIME_T.id
    ORDER BY product,indx DESC;""".as[(String, Int, Int, Int, Int, Int, Int, Double)]

  val eohQuery = sql"""
    SELECT
      product
      ,location
      ,eoh
    FROM public.eohdata
    ORDER BY product,location DESC;""".as[(String, String, Int)]

  val frontlineQuery = sql"""
    SELECT
      product
      ,location
      ,time
      ,unnest(replace(grade,'''','')::text[])
      ,unnest(replace(strClimate,'''','')::text[])
      ,validsizes
      ,TIME_1.indx
      ,TIME_2.indx
      ,TIME_3.indx
      ,TIME_4.indx
      ,TIME_5.indx
    FROM  public.frontline
    JOIN public.time AS TIME_1 ON initrcptwk = TIME_1.id
    JOIN public.time AS TIME_2 ON exitdate = TIME_2.id
    JOIN public.time AS TIME_3 ON dbtwk = TIME_3.id
    JOIN public.time AS TIME_4 ON erlstmkdnwk = TIME_4.id
    JOIN public.time AS TIME_5 ON lastdcrcpt = TIME_5.id
    ORDER BY product,location, time DESC;"""
    .as[(String, String, String, String, String, String, Int, Int, Int, Int, Int)]

  val invModelQuery = sql"""
    SELECT
      product
      ,sizes
      ,too
      ,aps_lower
      ,aps
      ,CAST(woc AS int)
    FROM public.invmodellkp_mod
    ORDER BY product DESC;""".as[(String, Int, Int, Long, Long, Int)]

  val departmentQuery = sql"""
    SELECT
      id
      ,ancestor3
    FROM public.prodstd AS PROD_STD
    WHERE id is not null AND ancestor3 is not null
    ORDER BY id,ancestor3 DESC;""".as[(String, String)]

  val clStrQuery = sql"""
    SELECT
      location
      ,strClimate
    FROM public.storeattributes
    ORDER BY location, strClimate DESC;""".as[(String, String)]

  val storeLookupQuery = sql"""
		select
      product
      ,id_indx
      ,unnest(replace(stores,'''','')::text[])
      ,grade
    from public.storelookup
    join (
        SELECT
          indx AS id_indx
          ,ancestor3 AS time
          FROM public.timestd
          JOIN public.time USING(id)
    ) AS TIME_STD USING(time)
    ORDER BY product, id_indx DESC;""".as[(String, Int, String, String)]

  val vRcptIntQuery = sql"""
    SELECT
      CAST(ccrcptint AS int)
      ,product
    FROM public.stylecolorattributes
    ORDER BY product;""".as[(Int, String)]

  val timeIndxQuery = sql"""
    SELECT
      indx
      ,id
    FROM public.time
    WHERE levelid = 'Week'
    ORDER BY indx DESC;""".as[(Int, String)]


  val frontSizesQuery = sql"""
    SELECT
      greatest(1, count(sizes))
      ,product
    FROM (select distinct
          product
          ,unnest(replace(validsizes,'''','')::text[]) as sizes
        from _frontline_table) t
    GROUP BY product
    ORDER BY product;""".as[(Int,String)]

  def productQuery(product: String) = sql"""
      SELECT
      CAST(location AS VARCHAR(8))
      ,indx
      ,fcst
    FROM public.loc_base_fcst AS LOC_BASE_FCST
    JOIN public.time AS TIME_T ON LOC_BASE_FCST.week = TIME_T.id
    WHERE LOC_BASE_FCST.product = $product
    ORDER BY location, indx;""".as[(String, Int, Int)]
}
