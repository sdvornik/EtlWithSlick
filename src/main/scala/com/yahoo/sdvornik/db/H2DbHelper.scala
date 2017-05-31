package com.yahoo.sdvornik.db

import java.sql.Connection

import com.typesafe.scalalogging.Logger
import org.h2.tools.Server
import slick.jdbc.H2Profile
import slick.sql.SqlAction

import scala.concurrent.Future
import scala.io.Source

object H2DbHelper {

  import slick.jdbc.H2Profile.api._

  private val log = Logger(H2DbHelper.getClass)

  private val sqlFiles: List[String] =
    "00_start_script.sql" ::
    "01_create_toh_input_final.sql" ::
    //"02_create_vrp_test.sql" ::
    "02_final_step.sql" :: Nil


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

  def createMemTables(): Future[Unit] = {

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
    val frontSizes = TableQuery[FrontSizes]


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
      vRcptInt.schema ++
      frontSizes.schema

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

    val dcAdjInsert = h2Db.run(DBIO.from(clStrInsert))
      .flatMap(_ => postgresDb.run(dcAdjQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(dcAdj ++= vector)))

    dcAdjInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to DcAdj MemTable")
      else log.error("Can't write to DcAdj MemTable",x.failed.get)
    )

    val departmentInsert = h2Db.run(DBIO.from(dcAdjInsert))
      .flatMap(_ => postgresDb.run(departmentQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(department ++= vector)))

    departmentInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to Department MemTable")
      else log.error("Can't write to Department MemTable",x.failed.get)
    )

    val eohInsert = h2Db.run(DBIO.from(dcAdjInsert))
      .flatMap(_ => postgresDb.run(eohQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(eoh ++= vector)))

    eohInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to Eoh MemTable")
      else log.error("Can't write to Eoh MemTable",x.failed.get)
    )

    val frontlineInsert = h2Db.run(DBIO.from(eohInsert))
      .flatMap(_ => postgresDb.run(frontlineQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(frontline ++= vector)))

    frontlineInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to Frontline MemTable")
      else log.error("Can't write to Frontline MemTable",x.failed.get)
    )

    val invModelInsert = h2Db.run(DBIO.from(frontlineInsert))
      .flatMap(_ => postgresDb.run(invModelQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(invModel ++= vector)))

    invModelInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to InvModel MemTable")
      else log.error("Can't write to InvModel MemTable",x.failed.get)
    )

    val storeLookupInsert = h2Db.run(DBIO.from(invModelInsert))
      .flatMap(_ => postgresDb.run(storeLookupQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(storeLookup ++= vector)))

    storeLookupInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to StoreLookup MemTable")
      else log.error("Can't write to StoreLookup MemTable",x.failed.get)
    )

    val timeIndxInsert = h2Db.run(DBIO.from(storeLookupInsert))
      .flatMap(_ => postgresDb.run(timeIndxQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(timeIndx ++= vector)))

    timeIndxInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to TimeIndx MemTable")
      else log.error("Can't write to TimeIndx MemTable",x.failed.get)
    )

    val vRcptIntInsert = h2Db.run(DBIO.from(timeIndxInsert))
      .flatMap(_ => postgresDb.run(vRcptIntQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(vRcptInt ++= vector)))

    vRcptIntInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to vRcptInt MemTable")
      else log.error("Can't write to vRcptInt MemTable",x.failed.get)
    )

    val frontSizesInsert: Future[Unit] = h2Db.run(DBIO.from(vRcptIntInsert))
      .flatMap(_ => postgresDb.run(frontSizesQuery))
      .flatMap(vector  => h2Db.run(DBIO.seq(frontSizes ++= vector)))

    frontSizesInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to FrontSizes MemTable")
      else log.error("Can't write to FrontSizes MemTable",x.failed.get)
    )

    frontSizesInsert
  }

  def createProductAndArgsTables(product: String, v_plancurrent: Int, v_planend: Int, lastStep: Future[Unit]): Future[Unit] = {

    class Args(tag: Tag) extends Table[(String, Int, Int)](tag, "ARGS") {
      def product: Rep[String] = column[String]("PRODUCT")

      def v_plancurrent: Rep[Int] = column[Int]("V_PLANCURRENT")

      def v_planend: Rep[Int] = column[Int]("V_PLANEND")

      def * = (product, v_plancurrent, v_planend)
    }

    val locBaseName = s"LOC_BASE_FCST_$product" map { case '-' => '_';  case x => x}

    class LocBaseFcst(tag: Tag) extends Table[(String, Int, Int)](tag, locBaseName) {

      def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)") )

      def indx: Rep[Int] = column[Int](FieldName.INDX)

      def fcst: Rep[Int] = column[Int](FieldName.FCST)

      def * = (location, indx, fcst)
    }

    val locBaseFcst = TableQuery[LocBaseFcst]
    val args = TableQuery[Args]
    val setup: DBIOAction[Unit, NoStream, Effect.Schema] = DBIO.seq(
      (locBaseFcst.schema ++ args.schema).create
    )

    import com.yahoo.sdvornik.db.PostgresDbHelper._
    import scala.concurrent.ExecutionContext.Implicits.global

    val locBaseFcstInsert = h2Db.run(DBIO.from(lastStep))
      .flatMap(_ => h2Db.run(setup) )
      .flatMap(_ => postgresDb.run(productQuery(product)))
      .flatMap(vector  => h2Db.run(DBIO.seq(locBaseFcst ++= vector)))
    locBaseFcstInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to locBaseFcst MemTable")
      else log.error("Can't write to locBaseFcst MemTable",x.failed.get)
    )

    val argsInsert: Future[Unit] = h2Db.run(DBIO.from(locBaseFcstInsert))
      .flatMap(vector  => h2Db.run(DBIO.seq(args += (product, v_plancurrent, v_planend))))
    argsInsert.onComplete(x =>
      if (x.isSuccess) log.info("Successfully write to Args MemTable")
      else log.error("Can't write to Args MemTable",x.failed.get)
    )
    argsInsert
  }

  def executeCalculation(product: String, startPoint: Future[Unit]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val locBaseName = s"LOC_BASE_FCST_$product" map { case '-' => '_';  case x => x}

    val actionList: List[SqlAction[Int, NoStream, Effect]] = sqlFiles.flatMap(
        Source.fromResource(_)
          .mkString
          .replaceAll("#LOC_BASE_FCST_PRODUCT#", locBaseName)
          .replaceAll("\\s*--.*","")
          .split(";")
          .toList
          .map(sql => sqlu"""#$sql""")
    )

    startPoint.onComplete(_ => {
      val startTime = System.currentTimeMillis()
      var curAction: Option[Future[Int]] = None
      for(action <- actionList) {
        val newAction: Future[Int] = curAction match {
          case Some(x) =>
            val start = h2Db.run(DBIO.from(x))
            var optTime: Option[Long] = None
            start.onComplete(_ => {
              optTime = Some(System.currentTimeMillis())
            })
            val end = start.flatMap(_ => h2Db.run(action))
            end.onComplete(_ => {
              for(strQuery <- action.statements) {
                log.info("COMPLETE QUERY:\n " + strQuery.trim +"\n"+
                "Execution time: "+(System.currentTimeMillis() - optTime.get)+ " ms\n\n")
              }

            })
            end

          case None => h2Db.run(action)
        }
        curAction = Some(newAction)
      }
    })

  }
}




