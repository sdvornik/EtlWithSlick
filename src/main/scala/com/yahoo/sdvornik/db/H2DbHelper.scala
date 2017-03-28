package com.yahoo.sdvornik.db



import com.typesafe.scalalogging.Logger
import org.h2.tools.Server
import slick.jdbc.H2Profile


/*
object DbHelper {

  private val POSTGRES_CONN_STR: String = "jdbc:postgresql://localhost:5432/calg_big"

  private val EMBEDDED_H2_CONN_STR: String = "jdbc:h2:mem:db_calc"

  private val user: String = "postgres"

  private val pwd: String = "ubuj2420"
}
*/
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

    try {
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

      //val insertInAttrTime: H2Profile.ProfileAction[Unit, NoStream, Effect.Schema] = DBIO.seq()


      import com.yahoo.sdvornik.db.PostgresDbHelper._

        // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
        /*
      coffees ++= Seq(
        ("Colombian",         101, 7.99, 0, 0),
        ("French_Roast",       49, 8.99, 0, 0),
        ("Espresso",          150, 9.99, 0, 0),
        ("Colombian_Decaf",   101, 8.99, 0, 0),
        ("French_Roast_Decaf", 49, 9.99, 0, 0)
      )
*/

      import scala.concurrent.ExecutionContext.Implicits.global
      val setupFuture = h2Db.run(setup)
      setupFuture.onComplete(_ => println("Future exec"))

    }
    catch {
      case e: Exception => System.out.println(e.getMessage)
    }

    //finally h2Db.close
  }
}



