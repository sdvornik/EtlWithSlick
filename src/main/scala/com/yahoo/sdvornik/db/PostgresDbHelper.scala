package com.yahoo.sdvornik.db

import scala.concurrent.Future

object PostgresDbHelper {
  import slick.jdbc.PostgresProfile.api._

  val postgresDb: Database = Database.forConfig("postgresInstance")
  import scala.concurrent.ExecutionContext.Implicits.global

  def getAttrTime = {


    val query = sql"""
      select
         ATTR_TABLE.product as department
        ,ATTR_TABLE.time   as flrset
        ,TIME_T.indx   as week_indx
      from public.dptflrsetattributes AS ATTR_TABLE
      JOIN public.time AS TIME_T ON
         TIME_T.levelid = 'Week' AND
         TIME_T.id >= ATTR_TABLE.rcptstart and
         TIME_T.id <= ATTR_TABLE.slsend;""".as[(String, String, Int)]

    val res: Future[Vector[(String, String, Int)]] = postgresDb.run(query)
    res.onComplete(a => a.get.toList.foreach(x=>println(x._1)))
  }
}
