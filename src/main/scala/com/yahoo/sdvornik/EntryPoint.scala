package com.yahoo.sdvornik

import scala.concurrent.Future


object EntryPoint extends App{

  import com.yahoo.sdvornik.db_scala.H2DbHelper

  val product = "10127-001"  //"10127-001" //"10127-410"
  val helper = new H2DbHelper(product)

  val server = helper.getTCPServer

  val future1: Future[Unit] = helper.createMemTables()
  val future2 = helper.createProductAndArgsTables(product, 936, 1039, future1)
  val future3 = helper.executeSqlCalculation(product, future2)
  helper.executeScalaCalculation(product, 936, 1039, future3)
  Thread.sleep(10000000)
  helper.stopTCPServer(server)
}
