package com.yahoo.sdvornik.db



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

  val h2Db: Database = Database.forConfig("h2memInstance")

  def createH2Schema() {

    import scala.concurrent.ExecutionContext.Implicits.global

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


      val setup = DBIO.seq(
        (
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
          ).create

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
      )

      val setupFuture = h2Db.run(setup)
    }
    finally h2Db.close
  }
}



