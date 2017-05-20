package com.yahoo.sdvornik

object EntryPoint extends App{
  import com.yahoo.sdvornik.db.H2DbHelper

  val server = H2DbHelper.getTCPServer

  val product = "10127-001"//"10127-410"//

  val lastFuture = H2DbHelper.createMemTables()
  val nextFuture = H2DbHelper.createProductAndArgsTables(product, 936, 1039, lastFuture)
  H2DbHelper.executeCalculation(product,nextFuture)

  Thread.sleep(10000000)
  H2DbHelper.stopTCPServer(server)
}
