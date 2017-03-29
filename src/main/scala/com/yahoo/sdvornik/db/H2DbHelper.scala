package com.yahoo.sdvornik.db

import com.typesafe.scalalogging.Logger
import org.h2.tools.Server
import slick.jdbc.H2Profile

object H2DbHelper {

  import slick.jdbc.H2Profile.api._

  private val log = Logger(H2DbHelper.getClass)

  import com.typesafe.config.ConfigFactory

  private val EXTERNAL_CONN_STR: String = ConfigFactory.load().getString("h2extInstance.url")

  private val USER: String = ConfigFactory.load().getString("h2extInstance.user")

  private val PWD: String = ConfigFactory.load().getString("h2extInstance.password")

  private val h2Db: Database = Database.forConfig("h2memInstance")

  def getTCPServer: Option[Server] = {
    try {
      Some(Server.createTcpServer()) match {
        case server@Some(x) =>
          x.start()
          server
        case _ => None
      }
    }
    catch {
      case e: Exception =>
        log.error("Can't create TCP Server", e)
        None
    }
  }

  def stopTCPServer(optServer: Option[Server]) {
    try {
      optServer.foreach(
        server => {
          server.shutdown()
          server.stop()
        }
      )
    }
    catch {
      case e: Exception =>
        log.error("Can't stop TCP server", e)
    }
  }

  def createH2Schema() {

    import com.yahoo.sdvornik.mem_tables._
    val attrTime = TableQuery[AttrTime]
    val bod = TableQuery[Bod]
    val clStr = TableQuery[ClStr]
    val dcAdj = TableQuery[DcAdj]
    val department = TableQuery[Department]
    val eoh = TableQuery[Eoh]
    val frontline = TableQuery[Frontline]
    val invModel = TableQuery[InvModel]
    val storeLookup = TableQuery[StoreLookup]
    val timeIndx = TableQuery[TimeIndx]
    val timeStd = TableQuery[TimeStd]
    val vRcptInt = TableQuery[VrcptInt]


    val ddlStatement: H2Profile.DDL =
      attrTime.schema ++
      bod.schema ++
      clStr.schema ++
      dcAdj.schema ++
      department.schema ++
      eoh.schema ++
      frontline.schema ++
      invModel.schema ++
      storeLookup.schema ++
      timeIndx.schema ++
      timeStd.schema ++
      vRcptInt.schema

    val ddlStatementAction: H2Profile.ProfileAction[Unit, NoStream, Effect.Schema] = ddlStatement.create

    val setup: DBIOAction[Unit, NoStream, Effect.Schema] = DBIO.seq(ddlStatementAction)

    import com.yahoo.sdvornik.db.PostgresDbHelper._
    import scala.concurrent.ExecutionContext.Implicits.global

    val attrTimeInsert = h2Db.run(setup)
      .flatMap(_ => postgresDb.run(attrTimeQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(attrTime ++= vector)))

    attrTimeInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to AttrTime MemTable")
      else log.error("Can't write to AttrTime MemTable",x.failed.get)
    )

    val bodInsert = h2Db.run(DBIO.from(attrTimeInsert))
      .flatMap(_ => postgresDb.run(bodQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(bod ++= vector)))

    bodInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to Bod MemTable")
      else log.error("Can't write to Bod MemTable",x.failed.get)
    )

    val clStrInsert = h2Db.run(DBIO.from(bodInsert))
      .flatMap(_ => postgresDb.run(clStrQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(clStr ++= vector)))

    clStrInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to ClStr MemTable")
      else log.error("Can't write to ClStr MemTable",x.failed.get)
    )



  }


    //h2Db.close

}



