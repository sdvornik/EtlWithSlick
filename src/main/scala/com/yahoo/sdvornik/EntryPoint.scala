package com.yahoo.sdvornik

object EntryPoint extends App{
  //import com.yahoo.sdvornik.db.H2DbHelper
  //H2DbHelper.createH2Schema()
  import com.yahoo.sdvornik.db.PostgresDbHelper
  val res = PostgresDbHelper.getAttrTime
  println(res.toString)
  Thread.sleep(3000)
}
