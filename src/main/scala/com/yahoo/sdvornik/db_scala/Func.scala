package com.yahoo.sdvornik.db_scala

import java.sql.{Connection, ResultSet, SQLException, Statement, Types}
import java.util.concurrent.TimeUnit

import scala.collection.{mutable => M}
import com.typesafe.scalalogging.Logger
import com.yahoo.sdvornik.db_scala.Tuples._
import com.yahoo.sdvornik.mem_tables.FieldName
import com.yahoo.sdvornik.mem_tables.TableName
import com.yahoo.sdvornik.util.IndexSearcher
import org.h2.tools.SimpleResultSet
import com.yahoo.sdvornik.db_scala.H2DbHelper._
import com.yahoo.sdvornik.mem_tables._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


object Func {

  private val logger = Logger("Func logger")

  val locBaseFcstMap: M.Map[String, TempProductTables] = M.HashMap()

  private val LOC_BASE_FCST_PREFIX: String = "LOC_BASE_FCST_"

  private val DURATION: Duration = Duration(5000, TimeUnit.MILLISECONDS)

  private val PRODUCT: String = "\"" + FieldName.PRODUCT + "\""
  private val DEPARTMENT: String = "\"" + FieldName.DEPARTMENT + "\""
  private val INDX: String = "\"" + FieldName.INDX + "\""
  private val LOCATION: String = "\"" + FieldName.LOCATION + "\""
  private val FLRSET: String = "\"" + FieldName.FLRSET + "\""
  private val GRADE: String = "\"" + FieldName.GRADE + "\""
  private val STRCLIMATE: String = "\"" + FieldName.STRCLIMATE + "\""
  private val DBTWK_INDX: String = "\"" + FieldName.DBTWK_INDX + "\""
  private val ERLSTMKDNWK_INDX: String = "\"" + FieldName.ERLSTMKDNWK_INDX + "\""
  private val INITRCPTWK_INDX: String = "\"" + FieldName.INITRCPTWK_INDX + "\""
  private val EXITDATE_INDX: String = "\"" + FieldName.EXITDATE_INDX + "\""
  private val NUM_SIZES: String = "\"" + FieldName.NUM_SIZES + "\""
  private val FCST: String = "\"" + FieldName.FCST + "\""
  private val TOO: String = "\"" + FieldName.TOO + "\""
  private val APS: String = "\"" + FieldName.APS + "\""
  private val APS_LOWER: String = "\"" + FieldName.APS_LOWER + "\""
  private val WOC: String = "\"" + FieldName.WOC + "\""
  private val EOH: String = "\"" + FieldName.EOH + "\""
  private val BOD: String = "\"" + FieldName.BOD + "\""
  private val CONS: String = "\"" + FieldName.CONS + "\""
  private val FINAL_QTY: String = "\"" + FieldName.FINAL_QTY + "\""
  private val FINAL_VRP: String = "\"" + FieldName.FINAL_VRP + "\""
  private val SBKT: String = "\"" + FieldName.SBKT + "\""


  private def readFromDepartment(conn: Connection, product: String): Set[String] = {
    import slick.jdbc.H2Profile.api._
    val future: Future[Seq[String]] = h2Db.run(department.filter(_.product===product).map(x => x.department).result)
    Await.result(future, DURATION)
    val list: Seq[String] = future.value match {
      case Some(x) => x match {
        case Success(y) => logger.error("Read department table - size: "+y.size); y
        case Failure(e) => logger.error("Can't read department table",e); Seq.empty
      }
      case None => Seq.empty
    }
    list.toSet[String]
  }

  private def readFrontline(conn: Connection, product: String, v_plancurrent: Int, v_planend: Int):
  (M.Map[String, M.Set[FrontlineClimate]], Set[FrontlineExit], Set[FrontlineDbtwkExit], Set[FrontlineWithTime]) = {

    val FRONT_CLIMATE_MAP: M.Map[String, M.Set[FrontlineClimate]] = new M.HashMap()
    import scala.concurrent.ExecutionContext.Implicits.global
    import slick.jdbc.H2Profile.api._

    val frontlineClimateFuture: Future[Unit] = h2Db.run(
      frontline.filter(_.product===product).map(x =>
        (x.flrset, x.grade, x.strclimate)
      ).result
    ).map(list => list.foreach(x => {
      val elmSet = FRONT_CLIMATE_MAP.getOrElseUpdate(x._1, M.HashSet[FrontlineClimate]())
      elmSet+=FrontlineClimate(x._2, x._3)
    }))

    val frontlineExitFuture: Future[Seq[FrontlineExit]] = h2Db.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.exitdate_indx, x.initrcptwk_indx)
      ).result
    ).map(l => l.map(x => FrontlineExit(Math.min(x._1, v_planend + 1), Math.max(x._2, v_plancurrent))))

    val frontlineDbtwkExitFuture: Future[Seq[FrontlineDbtwkExit]] = h2Db.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.exitdate_indx, x.dbtwk_indx)
      ).result
    ).map(l => l.map(x => FrontlineDbtwkExit(Math.min(x._1, v_planend + 1), Math.max(x._2, v_plancurrent))))

    val frontlineWithTimefuture: Future[Seq[FrontlineWithTime]] = h2Db.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.dbtwk_indx, x.erlstmkdnwk_indx, x.exitdate_indx)
      ).result
    ).map(l => l.map(x => FrontlineWithTime(x._1, x._2, x._3)))

    val empty = (FRONT_CLIMATE_MAP, Set.empty[FrontlineExit], Set.empty[FrontlineDbtwkExit], Set.empty[FrontlineWithTime])

    val combinedFuture =
      for {
        f1 <- frontlineClimateFuture
        f2 <- frontlineExitFuture
        f3 <- frontlineDbtwkExitFuture
        f4 <- frontlineWithTimefuture
      } yield (f1, f2, f3, f4)

    Await.result(combinedFuture, DURATION)
    combinedFuture.value match {
      case Some(x) => x match {
        case Success(y) => (FRONT_CLIMATE_MAP, y._2.toSet, y._3.toSet, y._4.toSet)
        case Failure(e) => logger.error("Can't read frontline table", e); empty
      }
      case None => empty
    }

  }

  private def readFromAttrTime(
    conn: Connection,
    product: String,
    DEPARTMENT_SET: Set[String],
    FRONT_CLIMATE_MAP: M.Map[String, M.Set[FrontlineClimate]]
  ): M.Map[AmWkKey, M.ArrayBuffer[String]] = {

    val AM_WK_MAP: M.Map[AmWkKey, M.ArrayBuffer[String]] = new M.HashMap[AmWkKey, M.ArrayBuffer[String]]

    import slick.jdbc.H2Profile.api._
    val attrTimeFuture: Future[Seq[(String, String, Int)]] = h2Db.run(
      attrTime.map(x => (x.flrset, x.department, x.indx)).result
    )
    Await.result(attrTimeFuture, DURATION)

    val list = attrTimeFuture.value match {
      case Some(x) => x match {
        case Success(y) => y
        case Failure(e) => logger.error("Can't read attrTime table", e); Seq.empty
      }
      case None => Seq.empty
    }
    list.foreach(x => {
      if (DEPARTMENT_SET.contains(x._2)) {
        val flrset: String = x._1
        FRONT_CLIMATE_MAP.getOrElse(x._1, M.Set[FrontlineClimate]()).foreach(elm => {
          val strClimateSet = AM_WK_MAP.getOrElseUpdate(AmWkKey(x._3, elm.grade), M.ArrayBuffer[String]())
          strClimateSet += elm.strClimate
        })
      }
    })
    AM_WK_MAP
  }

  private def readFromClStr(conn: Connection): Set[ClStrKey] = {

    import slick.jdbc.H2Profile.api._
    val clStrFuture: Future[Seq[(String, String)]] = h2Db.run(
      clStr.map(x => (x.location, x.strclimate)).result
    )
    Await.result(clStrFuture, DURATION)

    clStrFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.map(z => ClStrKey(z._1, z._2)).toSet
        case Failure(e) => logger.error("Can't read ClStr table", e); Set.empty
      }
      case None => Set.empty
    }
  }

  private def readFromStoreLookup(
    conn: Connection, product: String,
    CL_STR_SET: Set[ClStrKey],
    DEPARTMENT_SET: Set[String],
    AM_WK_MAP: M.Map[AmWkKey, M.ArrayBuffer[String]],
    FRONTLINE_EXIT_SET: Set[FrontlineExit]
  ): M.Map[String, M.Set[Int]]  = {

    val STORE_LIST_MAP: M.Map[String, M.Set[Int]] = M.HashMap[String, M.Set[Int]]()
    val bottom: Int = FRONTLINE_EXIT_SET.head.bottom
    val up: Int = FRONTLINE_EXIT_SET.head.up

    import slick.jdbc.H2Profile.api._

    val storeLookupFuture: Future[Seq[(Int, String, String, String)]] = h2Db.run(
      storeLookup.map(x => (x.indx, x.location, x.grade, x.department)).result
    )
    Await.result(storeLookupFuture, DURATION)

    storeLookupFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach(z => {
          if (DEPARTMENT_SET.contains(z._4)) {
            val idIndx: Int = z._1
            if (idIndx >= bottom && idIndx < up) {
              AM_WK_MAP.getOrElse(AmWkKey(idIndx, z._3), M.ArrayBuffer.empty).foreach(strclimate => {
                if (CL_STR_SET.contains(ClStrKey(z._2, strclimate))) {
                  STORE_LIST_MAP.getOrElseUpdate(z._2, M.SortedSet[Int]()) += idIndx
                }
              })
            }
          }
        })
        case Failure(e) => logger.error("Can't read attrTime table", e); ()
      }
      case None => ()
    }
    STORE_LIST_MAP
  }

  private def createStoreLyfecyle(
    MIN_INDEX_BY_LOCATION: M.Map[String, Int],
    FRONTLINE_WITH_TIME: Set[FrontlineWithTime],
    numSizes: Int
  ): M.Set[LocationPreTohInputKey] = {

    val STORE_LYFECYLE_SET: M.Set[LocationPreTohInputKey] = M.HashSet[LocationPreTohInputKey]()
    MIN_INDEX_BY_LOCATION.foreach(entry => {
      val flrset_min: Integer = entry._2

      FRONTLINE_WITH_TIME.foreach(frontLineWithTime => {
        val debut: Int = Math.max(frontLineWithTime.dbtwkIndx, flrset_min)
        val md_start: Int = Math.max(frontLineWithTime.erlstmkdnwkIndx, flrset_min)
        val too: Int = Math.max(frontLineWithTime.erlstmkdnwkIndx, flrset_min) - Math.max(frontLineWithTime.dbtwkIndx, flrset_min)
        val exit: Int = Math.max(frontLineWithTime.exitDateIndx, flrset_min)

        STORE_LYFECYLE_SET+=LocationPreTohInputKey(entry._1, PreTohInput(debut, md_start, exit, TooNumSizesKey(too, numSizes)))
      })
    })
    STORE_LYFECYLE_SET
  }

  private def joinStoreAndLifecyle(
    STORE_LYFECYLE_SET: M.Set[LocationPreTohInputKey],
    STORE_LIST_MAP: M.Map[String, M.Set[Int]]
  ): M.Map[LocationIndxKey, TohInput] = {

    val STORE_AND_LIFECYLE_MAP: M.Map[LocationIndxKey, TohInput] = M.HashMap[LocationIndxKey, TohInput]()

    STORE_LYFECYLE_SET.foreach( storeLyfecyleElm => {
      val location: String = storeLyfecyleElm.location
      val preTohInput: PreTohInput = storeLyfecyleElm.preTohInput

      val indxArr: Array[Int] = STORE_LIST_MAP.getOrElse(location, M.Set.empty).toArray[Int]

      val searcher: IndexSearcher[Int] = new IndexSearcher[Int](indxArr)
      val start: Int = searcher.binarySearch(preTohInput.debut)
      val end: Int = searcher.binarySearch(preTohInput.exit)

      for (i <- start to end) {
        val tohCalc: Integer = if (indxArr(i) < preTohInput.mdStart) 1 else 0
        STORE_AND_LIFECYLE_MAP.put(LocationIndxKey(LocationKey(location), indxArr(i)), TohInput(preTohInput.tooNumSizesKey, tohCalc))
      }
    })
    STORE_AND_LIFECYLE_MAP
  }

  private def readFromFrontSizes(conn: Connection, product: String): Int = {
    import slick.jdbc.H2Profile.api._

    val numSizesFuture: Future[Seq[Int]] = h2Db.run(
      frontSizes.filter(_.product===product).map(_.num_sizes).result
    )
    Await.result(numSizesFuture, DURATION)

    numSizesFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.head
        case Failure(e) => logger.error("Can't read numSizes table", e); 0
      }
      case None => 0
    }

  }

  private def readLocBaseFcst(conn: Connection, product: String):
  (M.ArrayBuffer[LocBaseFcstKey], M.Map[Int, M.Map[LocationIndxKey, Int]]) = {
    val LOC_BASE_FCST: M.ArrayBuffer[LocBaseFcstKey] = M.ArrayBuffer[LocBaseFcstKey]()
    val LOC_BASE_FCST_LIST_BY_INDX: M.Map[Int, M.Map[LocationIndxKey, Int]] = M.SortedMap[Int, M.Map[LocationIndxKey, Int]]()

    import slick.jdbc.H2Profile.api._
    val locBaseFcst = locBaseFcstMap.get(product).get.locBaseFcst

    val locBaseFcstFuture: Future[Seq[(Int, String, Int)]] = h2Db.run(
      locBaseFcst.map(x => (x.indx, x.location, x.fcst)).result
    )
    Await.result(locBaseFcstFuture, DURATION)

    locBaseFcstFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach( elm => {
          val key = LocationIndxKey(LocationKey(elm._2), elm._1)

          LOC_BASE_FCST+=LocBaseFcstKey(key, elm._3)
          val locBaseFcst: M.Map[LocationIndxKey, Int] = LOC_BASE_FCST_LIST_BY_INDX.getOrElseUpdate(elm._1, M.HashMap[LocationIndxKey, Int]())
          locBaseFcst.put(key, elm._3)

        })
        case Failure(e) => logger.error("Can't read locBaseFcst table", e);
      }
      case None => Unit
    }
    (LOC_BASE_FCST, LOC_BASE_FCST_LIST_BY_INDX)
  }

  private def createTohInput(
    STORE_AND_LIFECYLE_MAP: M.Map[LocationIndxKey, TohInput],
    LOC_BASE_FCST: M.ArrayBuffer[LocBaseFcstKey],
    FRONTLINE_DBTWK_EXIT_SET: Set[FrontlineDbtwkExit],
    TOH_INPUT: M.Map[LocationIndxKey, TohInput],
    UPDATE_TOH_INPUT: M.Map[LocationKey, M.Map[IndxKey, Int]],
    REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]]
  ): Unit = {

    LOC_BASE_FCST.foreach( locBaseFcst => {
      val key: LocationIndxKey = locBaseFcst.key
      val indxKey: IndxKey = IndxKey(key.indx)
      val locKey: LocationKey = key.location
      val fcst: Int = locBaseFcst.fcst
      STORE_AND_LIFECYLE_MAP.get(key) match {
        case Some(tohInput) =>
          tohInput.uncFcst = fcst
          TOH_INPUT.put(key, tohInput)
          val curMap: M.Map[IndxKey, Int] =
            UPDATE_TOH_INPUT.getOrElseUpdate(locKey, M.SortedMap[IndxKey, Int]())
          curMap.put(indxKey, fcst)
          if (tohInput.tohCalc == 1) {
            val map: M.Map[LocationIndxKey, TohInput] =
              REC_LOCATION.getOrElseUpdate(tohInput.tooNumSizesKey, M.HashMap[LocationIndxKey, TohInput]())
            map.put(key, tohInput)
          }

        case None =>
          val indx: Int = key.indx
          val dbtwkExit: FrontlineDbtwkExit = FRONTLINE_DBTWK_EXIT_SET.iterator.next
          if (indx >= dbtwkExit.dbtwkBottom && indx < dbtwkExit.up) {
            val tohInputDefault: TohInput = TohInput()
            tohInputDefault.uncFcst = fcst
            TOH_INPUT.put(key, tohInputDefault)
            val curMap: M.Map[IndxKey, Int] = UPDATE_TOH_INPUT.getOrElseUpdate(locKey, M.SortedMap[IndxKey, Int]())
            curMap.put(indxKey, fcst)
          }
      }
    })
  }

  private def readInvModel(
    conn: Connection,
    DEPARTMENT_SET: Set[String],
    REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]]
  ): M.Map[LocationIndxKey, Int] = {

    val positiveMap: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()
    val negativeMap: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()

    import slick.jdbc.H2Profile.api._
    val invModelFuture: Future[Seq[(Int, Int, String, Long, Long, Int)]] = h2Db.run(
      invModel.map(x => (x.too, x.num_sizes, x.department, x.aps, x.aps_lower, x.woc)).result
    )
    Await.result(invModelFuture, DURATION)

    invModelFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach( elm => {
          if (DEPARTMENT_SET.contains(elm._3)) {
            val key: TooNumSizesKey = TooNumSizesKey(elm._1, elm._2)

            REC_LOCATION.get(key).foreach(recLocation => {
              val aps: Long = elm._4
              val apsLower: Long = elm._5

              recLocation.foreach(entry => {
                val fcst: Int = entry._2.uncFcst
                if (fcst > apsLower && fcst <= aps) {
                  val locIndxKey: LocationIndxKey = entry._1
                  val woc: Int = elm._6
                  if (fcst > 0) {
                    val positiveMin: Int = positiveMap.getOrElse(locIndxKey, Integer.MAX_VALUE)
                    if (woc < positiveMin) positiveMap.put(locIndxKey, woc)
                  }
                  else {
                    val negativeMin: Int = negativeMap.getOrElse(locIndxKey, Integer.MAX_VALUE)
                    if (woc < negativeMin) negativeMap.put(locIndxKey, woc)
                  }
                }
              })
            })
          }

        })
        case Failure(e) => logger.error("Can't read invModel table", e);
      }
      case None => Unit
    }
    for(entry <-negativeMap) {
      positiveMap+=entry
    }
    positiveMap
  }

  private def createRecLocationMap(
    REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]],
    LKP_REC: M.Map[LocationIndxKey, Int]
  ): M.Map[LocationKey, M.Map[IndxKey, Int]] = {

    val REC_LOCATION_EXT: M.Map[LocationKey, M.Map[IndxKey, Int]] = M.HashMap[LocationKey, M.Map[IndxKey, Int]]()

    REC_LOCATION.values.foreach(mapValue => {
      mapValue.keySet.foreach(key => {
        val res: Int = LKP_REC.get(key) match {
          case Some(woc)=> key.indx + woc
          case None => 0
        }

        REC_LOCATION_EXT.getOrElseUpdate(key.location, M.SortedMap[IndxKey, Int]()).put(IndxKey(key.indx), res)
      })
    })
    REC_LOCATION_EXT
  }


  def createTohInputFinal(
    TOH_INPUT: M.Map[LocationIndxKey, TohInput],
    UPDATE_TOH_MAP: M.Map[LocationKey, M.Map[IndxKey, Int]],
    REC_LOCATION_EXT: M.Map[LocationKey, M.Map[IndxKey, Int]],
    LOCATION_SET: M.Set[LocationKey]
  ): Unit = {
    val V_TOH_MOD: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()

    UPDATE_TOH_MAP.foreach(updateTohEntry => {
      val loc: LocationKey = updateTohEntry._1
      val updateTohValue: M.Map[IndxKey, Int] = updateTohEntry._2

      REC_LOCATION_EXT.get(updateTohEntry._1) match {
        case Some(recLocationExtValue) =>
          val recLocationExtIndxArr: Array[IndxKey] = recLocationExtValue.keySet.toArray[IndxKey]
          val searcher: IndexSearcher[IndxKey] = new IndexSearcher[IndxKey](recLocationExtIndxArr)
          logger.info("Create array size: "+recLocationExtIndxArr.length)
          for (indxFcstEntry <- updateTohValue) {
            val updateTohIndex: IndxKey = indxFcstEntry._1
            val fcstValue: Int = indxFcstEntry._2
            val upperBoundIndex: Int = searcher.binarySearch(updateTohIndex)
            if (upperBoundIndex >= 0) {
              for (i <- 0 to upperBoundIndex) {
                val recLocationIndxKey: IndxKey = recLocationExtIndxArr(i)
                val recLocationMaxValue: Int = recLocationExtValue.get(recLocationIndxKey).get
                if (recLocationMaxValue > updateTohIndex.indx) {
                  val aggKey: LocationIndxKey = LocationIndxKey(loc, recLocationIndxKey.indx)
                  var curSum: Int = V_TOH_MOD.getOrElse(aggKey, 0)
                  curSum += fcstValue
                  V_TOH_MOD.put(aggKey, curSum)
                }
              }
            }
          }

        case None =>
      }
    })
    for (entry <- TOH_INPUT) {
      val key: LocationIndxKey = entry._1
      val tohInput: TohInput = entry._2
      V_TOH_MOD.get(key) match {
        case Some(tohValue) => tohInput.toh = tohValue
        case None =>
      }
      LOCATION_SET.add(key.location)
    }
  }

  private def readFromBod(
    conn: Connection,
    DEPARTMENT_SET: Set[String],
    LOCATION_SET: M.Set[LocationKey]
  ): M.Map[LocationKey, Int] = {
    val V_LT_MAP: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()

    import slick.jdbc.H2Profile.api._
    val bodFuture: Future[Seq[(String, String, Int)]] = h2Db.run(
      bod.map(x => (x.location, x.department, x.bod)).result
    )
    Await.result(bodFuture, DURATION)

    bodFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach(x => {
          if (DEPARTMENT_SET.contains(x._2)) {
            val locKey: LocationKey = LocationKey(x._1)
            if (LOCATION_SET.contains(locKey)) {
              V_LT_MAP.put(locKey, x._3)
            }
          }
        })
        case Failure(e) => logger.error("Can't read bod table", e);
      }
    }

    V_LT_MAP
  }

  private def readFromVrpTest(conn: Connection):
  (Int, Int, M.Map[IndxKey, VrpTestSource], M.Map[SbktKey, M.Set[IndxKey]], M.Map[IndxKey, SbktKey]) = {
    val VRP_TEST_SOURCE_MAP: M.Map[IndxKey, VrpTestSource] = M.HashMap[IndxKey, VrpTestSource]()
    val SBKT_MAP: M.Map[SbktKey, M.Set[IndxKey]] = M.SortedMap[SbktKey, M.Set[IndxKey]]()
    val MIN_SBKT_BY_INDEX: M.Map[IndxKey, SbktKey] = M.SortedMap[IndxKey, SbktKey]()

    val FRST_SBKT_FINAL_VRP_SET: M.Set[IndxKey] = M.SortedSet[IndxKey]()

    import slick.jdbc.H2Profile.api._
    val vrpTestFuture: Future[Seq[(Int,Option[Int],Int, Int, Int)]] = h2Db.run(
      vrpTest.map(x => (x.indx, x.cons, x.final_qty, x.final_vrp, x.sbkt)).result
    )
    Await.result(vrpTestFuture, DURATION)

    vrpTestFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach(x => {
          val indxKey: IndxKey = IndxKey(x._1)

          val sbktKey: SbktKey = SbktKey(x._5)

          val cons: Int = x._2.getOrElse(0)

          val vrpTestSource: VrpTestSource = VrpTestSource(cons, x._3, x._4)

          VRP_TEST_SOURCE_MAP.put(indxKey, vrpTestSource)
          SBKT_MAP.getOrElseUpdate(sbktKey, M.SortedSet[IndxKey]())+=indxKey

          val dcSbkt: SbktKey = MIN_SBKT_BY_INDEX.getOrElseUpdate(indxKey, SbktKey(Integer.MAX_VALUE))
          if (dcSbkt.sbkt > sbktKey.sbkt) MIN_SBKT_BY_INDEX.put(indxKey, sbktKey)

          if (vrpTestSource.finalVrp > 0) FRST_SBKT_FINAL_VRP_SET+=indxKey

        })
        case Failure(e) => logger.error("Can't read bod table", e);
      }
    }

    val frstSbktFinalVrp: Int = FRST_SBKT_FINAL_VRP_SET.head.indx
    FRST_SBKT_FINAL_VRP_SET.clear()
    val indxKeySet: M.Set[IndxKey] = SBKT_MAP.getOrElse(SBKT_MAP.keySet.head, M.Set.empty)
    val vFrstSbkt: Int = indxKeySet.head.indx
    (frstSbktFinalVrp, vFrstSbkt, VRP_TEST_SOURCE_MAP, SBKT_MAP, MIN_SBKT_BY_INDEX)
  }

  private def readFromEoh(conn: Connection, product: String): M.Map[LocationKey, Int] = {
    val EOH_BY_PRODUCT: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()
    import slick.jdbc.H2Profile.api._
    val eohFuture: Future[Seq[(String, Int)]] = h2Db.run(
      eoh.filter(_.product===product).map(x => (x.location, x.eoh)).result
    )
    Await.result(eohFuture, DURATION)

    eohFuture.value match {
      case Some(x) => x match {
        case Success(y) => y.foreach(x => {
          EOH_BY_PRODUCT.put(LocationKey(x._1), x._2)
        })
        case Failure(e) => logger.error("Can't read eoh table", e);
      }
    }
    EOH_BY_PRODUCT
  }

  private def fillRcptMap(
    v_plancurrent: Int,
    v_planend: Int,
    LOCATION_SET: M.Set[LocationKey],
    V_LT: M.Map[LocationKey, Int],
    EOH_BY_PRODUCT: M.Map[LocationKey, Int],
    TOH_INPUT: M.Map[LocationIndxKey, TohInput]): (M.Map[LocationIndxKey, Rcpt], M.Map[IndxKey, M.ArrayBuffer[Rcpt]]) = {

    val RCPT_MAP: M.Map[LocationIndxKey, Rcpt] = M.HashMap[LocationIndxKey, Rcpt]()
    val RCPT_MAP_BY_INDX: M.Map[IndxKey, M.ArrayBuffer[Rcpt]] = M.HashMap[IndxKey, M.ArrayBuffer[Rcpt]]()

    for (str <- LOCATION_SET) {
      val vLtValue: Int = V_LT.getOrElse(str, 0)
      val eohValue: Int = EOH_BY_PRODUCT.getOrElse(str, 0)

      for(t<- v_plancurrent to v_planend) { //Change this to Max (Plan_Current, Debut_Week) to Min (Exit_Week, Plan_End)
        val key: LocationIndxKey = LocationIndxKey(str, t)
        val prevKey: LocationIndxKey = LocationIndxKey(str, t - 1)
        val TOH_INPUT_CUR: TohInput = TOH_INPUT.getOrElse(key, TohInput.Default)
        val RCPT_PREV: Rcpt = RCPT_MAP.getOrElse(prevKey, Rcpt.Default)
        val vUncFcst: Int = TOH_INPUT_CUR.uncFcst
        val vToh: Int = TOH_INPUT_CUR.toh
        val vUncBoh: Int =
          if (t == v_plancurrent) eohValue
          else Math.max(0, RCPT_PREV.uncBoh + RCPT_PREV.uncNeed - RCPT_PREV.uncFcst)

        val vExistInv: Int =
          if(t == v_plancurrent) eohValue
          else Math.max(0, RCPT_PREV.uncBoh - RCPT_PREV.uncFcst)

        val vUncNeed: Int =
          if(t < v_plancurrent + vLtValue) 0
          else Math.max(0, vToh - vUncBoh)

        val vExistInvTemp: Int =
          if(t == v_plancurrent) eohValue
          else Math.max(0, RCPT_PREV.existInv - RCPT_PREV.existSlsu)

        val vConsSlsu: Int =
          if(t < v_plancurrent + vLtValue) 0
          else Math.min(vExistInvTemp, vUncFcst)

        val vConsEoh: Int =
          if(t < v_plancurrent + vLtValue) 0
          else vExistInvTemp - vConsSlsu

        val vExistSlsu: Int = Math.min(vExistInv, vUncFcst)
        val rcpt: Rcpt = Rcpt(vLtValue, vToh, vUncBoh, vUncNeed, vUncFcst, vExistInv, vExistSlsu, vConsSlsu, vConsEoh)
        RCPT_MAP.put(key, rcpt)
        val list: M.ArrayBuffer[Rcpt] = RCPT_MAP_BY_INDX.getOrElseUpdate(IndxKey(t), M.ArrayBuffer[Rcpt]())
        list+=rcpt
        val laggedKey: LocationIndxKey = LocationIndxKey(str, t - vLtValue)

        RCPT_MAP.get(laggedKey) match {
          case Some(sourceLagged) =>
            sourceLagged.consEohLt = vConsEoh
            sourceLagged.tohLt = vToh
            sourceLagged.uncFcstLt = vUncFcst
            sourceLagged.uncNeedLt = vUncNeed

          case None =>
            val rcptLagged: Rcpt = Rcpt(vLtValue, vUncFcst, vToh, vUncNeed, vConsEoh)
            RCPT_MAP.put(laggedKey, rcptLagged)
            val list2: M.ArrayBuffer[Rcpt] = RCPT_MAP_BY_INDX.getOrElseUpdate(IndxKey(t), M.ArrayBuffer[Rcpt]())
            list2+=rcptLagged
        }
      }
    }
    (RCPT_MAP, RCPT_MAP_BY_INDX)
  }

  private def fillDcMap(
    v_plancurrent: Int,
    v_planend: Int,
    EOH_BY_PRODUCT: M.Map[LocationKey, Int],
    VRP_TEST_SOURCE_MAP: M.Map[IndxKey, VrpTestSource],
    MIN_SBKT_BY_INDEX: M.Map[IndxKey, SbktKey],
    RCPT_MAP_BY_INDX: M.Map[IndxKey, M.ArrayBuffer[Rcpt]],
    frstSbktFinalVrp: Int
  ): (Int, M.Map[IndxKey, Dc], M.Map[SbktKey, M.ArrayBuffer[Dc]]) = {
    val DC_MAP: M.Map[IndxKey, Dc] = M.HashMap[IndxKey, Dc]()
    val DC_MAP_BY_SBKT: M.Map[SbktKey, M.ArrayBuffer[Dc]] = M.SortedMap[SbktKey, M.ArrayBuffer[Dc]]()

    val EOH_BY_PRODUCT_DC: Int = EOH_BY_PRODUCT.getOrElse(LocationKey("DC"), 0)
    var Ttl_DC_Rcpt: Int = 0

    for(t<-v_plancurrent to v_planend) {

      val indxKey: IndxKey = IndxKey(t)
      val prevIndxKey: IndxKey = IndxKey(t - 1)
      val VRP_TEST: VrpTestSource = VRP_TEST_SOURCE_MAP.getOrElse(indxKey, VrpTestSource.Default)
      val sbktKey: SbktKey = MIN_SBKT_BY_INDEX.get(indxKey).get
      val rcptList: M.ArrayBuffer[Rcpt] = RCPT_MAP_BY_INDX.getOrElse(indxKey, M.ArrayBuffer[Rcpt]())

      var RCPT_CUR_Agg_Unc_Need_DC: Int = 0
      var RCPT_CUR_Agg_Unc_TOH_DC: Int = 0

      for (rcpt <- rcptList) {
        RCPT_CUR_Agg_Unc_Need_DC += rcpt.uncNeedLt
        RCPT_CUR_Agg_Unc_TOH_DC += rcpt.tohLt
      }
      rcptList.clear()
      val DC_PREV: Dc = DC_MAP.getOrElse(prevIndxKey, Dc.Default)
      var DC_POH: Int =
        if (t == v_plancurrent) EOH_BY_PRODUCT_DC
        else DC_PREV.dcPoh + DC_PREV.dcRaw - DC_PREV.outbound

      val Used_Need: Int =
        if(t == v_plancurrent) RCPT_CUR_Agg_Unc_Need_DC
        else Math.min(RCPT_CUR_Agg_Unc_TOH_DC, RCPT_CUR_Agg_Unc_Need_DC + DC_PREV.deficit)

      val DC_Raw: Int =
        if(t < frstSbktFinalVrp) 0
        else {
          if(VRP_TEST.cons == 1) VRP_TEST.finalQty
          else Math.max(0, Used_Need - DC_POH)
        }
      Ttl_DC_Rcpt += DC_Raw
      val Outbound: Int = Math.min(Used_Need, DC_POH + DC_Raw)
      val Deficit: Int = Math.max(0, Used_Need - Outbound)
      val dcSource: Dc = new Dc(DC_POH, DC_Raw, Outbound, sbktKey.sbkt, 0, Deficit)
      DC_MAP.put(indxKey, dcSource)
      val list: M.ArrayBuffer[Dc] = DC_MAP_BY_SBKT.getOrElseUpdate(sbktKey, M.ArrayBuffer[Dc]())
      list+=dcSource

    }
    (Ttl_DC_Rcpt, DC_MAP, DC_MAP_BY_SBKT)
  }

  private def calculateInitMaxCons(
    Ttl_DC_Rcpt: Int,
    RCPT_MAP: M.Map[LocationIndxKey, Rcpt],
    EOH_BY_PRODUCT: M.Map[LocationKey, Int]
  ): M.Map[LocationKey, Int]  = {
    val INIT_MAX_CONS: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()
    var Ttl_Str_Unc_Need: Int = 0
    val Str_Unc_Need: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()

    for (rcptEntry <- RCPT_MAP) {
      val key: LocationIndxKey = rcptEntry._1
      val locationKey: LocationKey = key.location
      val rcpt: Rcpt = rcptEntry._2
      var localStr_Unc_Need: Int = Str_Unc_Need.getOrElse(locationKey, 0)
      localStr_Unc_Need += rcpt.uncNeedLt
      Str_Unc_Need.put(locationKey, localStr_Unc_Need)
      Ttl_Str_Unc_Need += rcpt.uncNeedLt
    }

    val EOH_BY_PRODUCT_DC: Int = EOH_BY_PRODUCT.getOrElse(LocationKey("DC"), 0)

    for(strUncNeedEntry <- Str_Unc_Need) {
      val locationKey: LocationKey = strUncNeedEntry._1
      val StrUncNeedValue: Int = strUncNeedEntry._2
      var value: Int =
        if (Ttl_Str_Unc_Need == 0) 0
        else half_round((Ttl_DC_Rcpt + EOH_BY_PRODUCT_DC) * StrUncNeedValue / Ttl_Str_Unc_Need.toDouble)

      INIT_MAX_CONS.put(locationKey, value)
    }
    logger.info("INIT_MAX_CONS size: " + INIT_MAX_CONS.size)
    INIT_MAX_CONS
  }

  private def finalCalculation(
    v_plancurrent: Int,
    vFrstSbkt: Int,
    SBKT_MAP: M.Map[SbktKey, M.Set[IndxKey]],
    LOCATION_SET: M.Set[LocationKey],
    INIT_MAX_CONS: M.Map[LocationKey, Int],
    EOH_BY_PRODUCT: M.Map[LocationKey, Int],
    DC_MAP: M.Map[IndxKey, Dc],
    DC_MAP_BY_SBKT: M.Map[SbktKey, M.ArrayBuffer[Dc]],
    RCPT_MAP: M.Map[LocationIndxKey, Rcpt]
  ): Unit = {

    for (entry <- SBKT_MAP) {
      val v_sbkt_id: SbktKey = entry._1
      val INDX_SET: M.Set[IndxKey] = entry._2
      var v_sbkt_start: Int = -1
      var temp_need_sbkt_dc: Int = 0
      val tempNeedSbktByLocation: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()

      for (indxKey <- INDX_SET) {
        val t: Int = indxKey.indx
        if (v_sbkt_start < 0) {
          v_sbkt_start = t
        }

        for (str <- LOCATION_SET) {
          val key: LocationIndxKey = LocationIndxKey(str, t)
          val prevKey: LocationIndxKey = LocationIndxKey(str, t - 1)
          val RCPT_CUR: Rcpt = RCPT_MAP.getOrElse(key, Rcpt.Default)
          val RCPT_PREV: Rcpt = RCPT_MAP.getOrElse(prevKey, Rcpt.Default)
          var Temp_BOH: Int = 0
          var Temp_Cons: Int = 0
          if (t == v_sbkt_start) {
            Temp_BOH =
              if (t == v_plancurrent && RCPT_CUR.leadTime == 0) EOH_BY_PRODUCT.getOrElse(str, 0)
              else RCPT_PREV.consEohLt

            Temp_Cons =
              if (t == vFrstSbkt) INIT_MAX_CONS.getOrElse(str, 0)
              else Math.max(0, RCPT_PREV.maxConsLt - RCPT_PREV.consRcptLt)
          }
          else {
            Temp_BOH = RCPT_PREV.tempEoh
            Temp_Cons = Math.max(0, RCPT_PREV.tempCons - RCPT_PREV.tempRcpt)
          }
          val Temp_Need: Int =
            if (Temp_Cons > 0) Math.max(0, RCPT_CUR.tohLt - Temp_BOH)
            else 0

          val Temp_Rcpt: Int = Temp_Need
          val Temp_SlsU: Int = Math.min(RCPT_CUR.uncFcstLt, Temp_BOH + Temp_Rcpt)
          val Temp_EOH: Int = Temp_BOH + Temp_Rcpt - Temp_SlsU
          RCPT_CUR.tempBoh=Temp_BOH
          RCPT_CUR.tempNeed=Temp_Need
          RCPT_CUR.tempRcpt=Temp_Rcpt
          RCPT_CUR.tempEoh=Temp_EOH
          RCPT_CUR.tempSlsu=Temp_SlsU
          RCPT_CUR.tempCons=Temp_Cons
          RCPT_CUR.dcSbkt=v_sbkt_id.sbkt
          temp_need_sbkt_dc += Temp_Need
          val temp_need_sbkt: Int = tempNeedSbktByLocation.getOrElse(str, 0) + Temp_Need
          tempNeedSbktByLocation.put(str, temp_need_sbkt)
        }
      }
      val ratioMap: M.Map[LocationKey, Double] = M.HashMap[LocationKey, Double]()

      for (temp_need_sbkt_entry <- tempNeedSbktByLocation) {
        val ratio: Double =
          if(temp_need_sbkt_dc == 0) 0.0
          else temp_need_sbkt_entry._2 / temp_need_sbkt_dc.asInstanceOf[Double]

        ratioMap.put(temp_need_sbkt_entry._1, ratio)
      }
      tempNeedSbktByLocation.clear()
      val listBySbkt: M.ArrayBuffer[Dc] = DC_MAP_BY_SBKT.getOrElse(v_sbkt_id, M.ArrayBuffer[Dc]())
      var sum: Int = 0

      for (dc <- listBySbkt) {
        sum += dc.dcRaw
      }
      DC_MAP_BY_SBKT.remove(v_sbkt_id)
      val dcSource: Dc = DC_MAP.get(IndxKey(v_sbkt_start)).get
      dcSource.dcRcpt=sum

      for (indxKey <- INDX_SET) {
        val t: Int = indxKey.indx
        val prevIndxKey: IndxKey = IndxKey(t - 1)
        val DC_CUR: Dc = DC_MAP.getOrElse(indxKey, Dc.Default)
        val DC_PREV: Dc = DC_MAP.getOrElse(prevIndxKey, Dc.Default)
        val DC_OH_Rsv: Int =
          if(t == v_plancurrent) DC_CUR.dcPoh
          else DC_PREV.dcOhRsv + DC_PREV.dcRcpt - DC_PREV.aOut

        val DC_ATA: Int =
          if(t == v_sbkt_start) DC_CUR.dcRcpt + DC_OH_Rsv
          else DC_OH_Rsv

        var A_OUT: Int = 0

        for (str <- LOCATION_SET) {
          val key: LocationIndxKey = LocationIndxKey(str, t)
          val prevKey: LocationIndxKey = LocationIndxKey(str, t - 1)
          val RCPT_CUR: Rcpt = RCPT_MAP.getOrElse(key, Rcpt.Default)
          val RCPT_PREV: Rcpt = RCPT_MAP.getOrElse(prevKey, Rcpt.Default)
          val Cons_BOH_LT: Int =
            if(t == v_plancurrent) {
              if(RCPT_CUR.leadTime == 0) EOH_BY_PRODUCT.getOrElse(str, 0)
              else RCPT_PREV.consEohLt
            }
            else RCPT_PREV.consBohLt + RCPT_PREV.consRcptLt - RCPT_PREV.consSlsuLt
          RCPT_CUR.consBohLt=Cons_BOH_LT

          val PO_Alloc_LT: Int =
            if(t == v_sbkt_start) half_round(DC_ATA * ratioMap.getOrElse(str, 0.0))
            else 0
          RCPT_CUR.poAllocLt=PO_Alloc_LT
          val Cons_Need_LT: Int = Math.max(0, RCPT_CUR.tohLt - Cons_BOH_LT)
          RCPT_CUR.consNeedLt=Cons_Need_LT
          val Max_Cons_LT: Int =
            if(t == vFrstSbkt) INIT_MAX_CONS.getOrElse(str, 0)
            else Math.max(0, RCPT_PREV.maxConsLt - RCPT_PREV.consRcptLt)

          RCPT_CUR.maxConsLt=Max_Cons_LT
          val Cons_Avl_LT: Int =
            if(t == v_sbkt_start) Math.min(Max_Cons_LT, PO_Alloc_LT)
            else RCPT_PREV.unAllocLt

          RCPT_CUR.consAvlLt=Cons_Avl_LT
          val Cons_Rcpt_LT: Int = Math.min(Cons_Need_LT, Cons_Avl_LT)
          RCPT_CUR.consRcptLt=Cons_Rcpt_LT
          val UnAlloc_LT: Int = Math.max(0, Cons_Avl_LT - Cons_Rcpt_LT)
          RCPT_CUR.unAllocLt=UnAlloc_LT
          val Cons_SlsU_LT: Int = Math.min(RCPT_CUR.uncFcstLt, Cons_BOH_LT + Cons_Rcpt_LT)
          RCPT_CUR.consSlsuLt=Cons_SlsU_LT
          val Cons_EOH_LT: Int = Math.max(0, Cons_BOH_LT + Cons_Rcpt_LT - Cons_SlsU_LT)
          RCPT_CUR.consEohLt=Cons_EOH_LT
          A_OUT += Cons_Rcpt_LT
        }
        DC_CUR.aOut=A_OUT
        DC_CUR.dcAta=DC_ATA
        DC_CUR.dcOhRsv=DC_OH_Rsv
      }
    }
  }

  @throws[SQLException]
  def create_toh_input_table(
    conn: Connection,
    product: String,
    v_plancurrent: Int,
    v_planend: Int
  ): ResultSet = {
    val rs: SimpleResultSet = createOutputResultSet
    if (conn.getMetaData.getURL == "jdbc:columnlist:connection") rs else {
      try {
        var start: Long = System.currentTimeMillis
        val globalStart: Long = start
        val t = readFrontline(conn, product, v_plancurrent, v_planend)
        var end: Long = System.currentTimeMillis
        logger.info("readFrontline() " + (end - start))

        val FRONT_CLIMATE_MAP = t._1
        val FRONTLINE_EXIT_SET = t._2
        val FRONTLINE_DBTWK_EXIT_SET = t._3
        val FRONTLINE_WITH_TIME = t._4
        logger.info("FRONT_CLIMATE_MAP size: " + FRONT_CLIMATE_MAP.size)
        logger.info("FRONTLINE_EXIT_SET size: " + FRONTLINE_EXIT_SET.size)
        logger.info("FRONTLINE_WITH_TIME size: " + FRONTLINE_WITH_TIME.size)
        logger.info("FRONTLINE_DBTWK_EXIT_SET size: " + FRONTLINE_DBTWK_EXIT_SET.size)

        start = end
        val numSizes: Int = readFromFrontSizes(conn, product)
        end = System.currentTimeMillis
        logger.info("readFromFrontSizes() " + (end - start))

        start = end
        val DEPARTMENT_SET: Set[String] = readFromDepartment(conn, product)
        end = System.currentTimeMillis
        logger.info("readFromDepartment() " + (end - start))
        logger.info("DEPARTMENT_SET size: " + DEPARTMENT_SET.size)

        start = end
        val CL_STR_SET: Set[ClStrKey] = readFromClStr(conn)
        end = System.currentTimeMillis
        logger.info("readFromClStr() " + (end - start))
        logger.info("CL_STR_SET size: " + CL_STR_SET.size)

        start = end
        val AM_WK_MAP: M.Map[AmWkKey, M.ArrayBuffer[String]] = readFromAttrTime(conn, product, DEPARTMENT_SET, FRONT_CLIMATE_MAP)
        end = System.currentTimeMillis
        logger.info("readFromAttrTime() " + (end - start))
        logger.info("AM_WK_MAP size: " + AM_WK_MAP.size)

        start = end
        val STORE_LIST_MAP: M.Map[String, M.Set[Int]] = readFromStoreLookup(conn, product, CL_STR_SET, DEPARTMENT_SET, AM_WK_MAP, FRONTLINE_EXIT_SET)
        end = System.currentTimeMillis
        logger.info("readFromStoreLookup() " + (end - start))
        logger.info("STORE_LIST_MAP size: " + STORE_LIST_MAP.size)

        start = end
        val MIN_INDEX_BY_LOCATION: M.Map[String, Int] = M.HashMap[String, Int]()
        STORE_LIST_MAP.foreach(entry => MIN_INDEX_BY_LOCATION.put(entry._1, entry._2.head))
        end = System.currentTimeMillis
        logger.info("create MIN_INDEX_BY_LOCATION " + (end - start))
        logger.info("MIN_INDEX_BY_LOCATION size: " + MIN_INDEX_BY_LOCATION.size)

        start = end
        val STORE_LYFECYLE_SET: M.Set[LocationPreTohInputKey] = createStoreLyfecyle(MIN_INDEX_BY_LOCATION, FRONTLINE_WITH_TIME, numSizes)
        end = System.currentTimeMillis
        logger.info("createStoreLyfecyle() " + (end - start))
        logger.info("STORE_LYFECYLE_SET size: " + STORE_LYFECYLE_SET.size)

        start = end
        val STORE_AND_LIFECYLE_MAP: M.Map[LocationIndxKey, TohInput] = joinStoreAndLifecyle(STORE_LYFECYLE_SET, STORE_LIST_MAP)
        end = System.currentTimeMillis
        logger.info("joinStoreAndLifecyle() " + (end - start))
        logger.info("STORE_AND_LIFECYLE_MAP size: " + STORE_AND_LIFECYLE_MAP.size)

        val t1 = readLocBaseFcst(conn, product)
        val LOC_BASE_FCST = t1._1
        val LOC_BASE_FCST_LIST_BY_INDX = t1._2
        end = System.currentTimeMillis
        logger.info("readLocBaseFcst() " + (end - start))
        logger.info("LOC_BASE_FCST size: " + LOC_BASE_FCST.size)
        logger.info("LOC_BASE_FCST_LIST_BY_INDX size: " + LOC_BASE_FCST_LIST_BY_INDX.size)

        start = end
        val TOH_INPUT: M.Map[LocationIndxKey, TohInput] = M.HashMap[LocationIndxKey, TohInput]()
        val UPDATE_TOH_INPUT: M.Map[LocationKey, M.Map[IndxKey, Int]] = M.HashMap[LocationKey, M.Map[IndxKey, Int]]()
        val REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]] = M.HashMap[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]]()
        createTohInput(STORE_AND_LIFECYLE_MAP, LOC_BASE_FCST, FRONTLINE_DBTWK_EXIT_SET, TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION)
        end = System.currentTimeMillis
        logger.info("createTohInput() " + (end - start))

        start = end
        val LKP_REC: M.Map[LocationIndxKey, Int] = readInvModel(conn, DEPARTMENT_SET, REC_LOCATION)
        end = System.currentTimeMillis
        logger.info("readInvModel() " + (end - start))
        logger.info("LKP_REC size: " + LKP_REC.size)

        start = end
        val REC_LOCATION_EXT: M.Map[LocationKey, M.Map[IndxKey, Int]] = createRecLocationMap(REC_LOCATION, LKP_REC)
        end = System.currentTimeMillis
        logger.info("createRecLocationMap() " + (end - start))
        logger.info("REC_LOCATION_EXT size: " + REC_LOCATION_EXT.size)

        start = end
        val LOCATION_SET: M.Set[LocationKey] = M.HashSet[LocationKey]()
        createTohInputFinal(TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION_EXT, LOCATION_SET)
        end = System.currentTimeMillis
        logger.info("createTohInputFinal() " + (end - start))

        start = end
        val V_LT: M.Map[LocationKey, Int] = readFromBod(conn, DEPARTMENT_SET, LOCATION_SET)
        end = System.currentTimeMillis
        logger.info("readFromBod() " + (end - start))
        start = end

        val vrpTestRes = readFromVrpTest(conn)
        val frstSbktFinalVrp: Int = vrpTestRes._1
        val vFrstSbkt: Int = vrpTestRes._2
        val VRP_TEST_SOURCE_MAP = vrpTestRes._3
        val SBKT_MAP = vrpTestRes._4
        val MIN_SBKT_BY_INDEX = vrpTestRes._5
        end = System.currentTimeMillis
        logger.info("readFromVrpTest() " + (end - start))
        logger.info("vFrstSbkt: " + vFrstSbkt)
        logger.info("frstSbktFinalVrp: " + frstSbktFinalVrp)
        logger.info("VRP_TEST size: " + VRP_TEST_SOURCE_MAP.size)

        start = end
        val EOH_BY_PRODUCT: M.Map[LocationKey, Int] = readFromEoh(conn, product)
        end = System.currentTimeMillis
        logger.info("readFromEoh() " + (end - start))
        logger.info("EOH_BY_PRODUCT size: " + EOH_BY_PRODUCT.size)

        start = end
        val t2 = fillRcptMap(v_plancurrent, v_planend, LOCATION_SET, V_LT, EOH_BY_PRODUCT, TOH_INPUT)
        end = System.currentTimeMillis
        logger.info("fillRcptMap() " + (end - start))
        val RCPT_MAP = t2._1
        val RCPT_MAP_BY_INDX = t2._2
        logger.info("RCPT_MAP size: " + RCPT_MAP.size)
        logger.info("RCPT_MAP_BY_INDX size: " + RCPT_MAP_BY_INDX.size)

        start = end
        val t3 = fillDcMap(v_plancurrent, v_planend, EOH_BY_PRODUCT, VRP_TEST_SOURCE_MAP, MIN_SBKT_BY_INDEX, RCPT_MAP_BY_INDX, frstSbktFinalVrp)
        end = System.currentTimeMillis
        logger.info("fillDcMap() " + (end - start))
        val Ttl_DC_Rcpt = t3._1
        val DC_MAP = t3._2
        val DC_MAP_BY_SBKT = t3._3
        logger.info("Ttl_DC_Rcpt: " + Ttl_DC_Rcpt)
        logger.info("DC_MAP size: " + DC_MAP.size)
        logger.info("DC_MAP_BY_SBKT size: " + DC_MAP_BY_SBKT.size)

        start = end
        val INIT_MAX_CONS: M.Map[LocationKey, Int] = calculateInitMaxCons(Ttl_DC_Rcpt, RCPT_MAP, EOH_BY_PRODUCT)
        end = System.currentTimeMillis
        logger.info("calculateInitMaxCons() " + (end - start))


        start = end
        finalCalculation(v_plancurrent, vFrstSbkt, SBKT_MAP, LOCATION_SET, INIT_MAX_CONS, EOH_BY_PRODUCT, DC_MAP, DC_MAP_BY_SBKT, RCPT_MAP)
        end = System.currentTimeMillis
        logger.info("finalCalculation() " + (end - start))
        logger.error("Total execution time: " + (end - globalStart))

        start = end
        //implicit def toJavaInteger(in:Int): Integer = in.asInstanceOf[java.lang.Integer]
        for (rcptEntry <- RCPT_MAP) {
          val key: LocationIndxKey = rcptEntry._1
          val target: Rcpt = rcptEntry._2
          val dcTarget: Dc = DC_MAP.getOrElse(IndxKey(key.indx), Dc.Default)
          rs.addRow(
            key.indx: java.lang.Integer,
            key.location.value,
            target.dcSbkt: java.lang.Integer,
            target.uncNeedLt: java.lang.Integer,
            target.tempBoh: java.lang.Integer,
            target.tempNeed: java.lang.Integer,
            target.tempRcpt: java.lang.Integer,
            target.tempEoh: java.lang.Integer,
            target.tempSlsu: java.lang.Integer,
            target.tempCons: java.lang.Integer,
            target.consBohLt: java.lang.Integer,
            target.consNeedLt: java.lang.Integer,
            target.consRcptLt: java.lang.Integer,
            target.consAvlLt: java.lang.Integer,
            target.maxConsLt: java.lang.Integer,
            target.poAllocLt: java.lang.Integer,
            target.unAllocLt: java.lang.Integer,
            target.consEohLt: java.lang.Integer,
            target.consSlsuLt: java.lang.Integer,
            dcTarget.dcRcpt: java.lang.Integer,
            dcTarget.dcOhRsv: java.lang.Integer,
            dcTarget.dcAta: java.lang.Integer,
            dcTarget.aOut: java.lang.Integer,
            dcTarget.dcRaw: java.lang.Integer,
            dcTarget.outbound: java.lang.Integer,
            dcTarget.dcPoh: java.lang.Integer,
            dcTarget.deficit: java.lang.Integer
          )
        }
        end = System.currentTimeMillis
        logger.info("Output() " + (end - start))
        logger.info("Successfully execute function.")
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      rs
    }
  }

  private def createOutputResultSet: SimpleResultSet
  = {
    val rs: SimpleResultSet = new SimpleResultSet
    rs.addColumn("idx", Types.INTEGER, 10, 0)
    rs.addColumn("location", Types.VARCHAR, 8, 0)
    rs.addColumn("sbkt", Types.INTEGER, 10, 0)
    rs.addColumn("unc_need_lt", Types.INTEGER, 10, 0)
    rs.addColumn("temp_boh", Types.INTEGER, 10, 0)
    rs.addColumn("temp_need", Types.INTEGER, 10, 0)
    rs.addColumn("temp_rcpt", Types.INTEGER, 10, 0)
    rs.addColumn("temp_eoh", Types.INTEGER, 10, 0)
    rs.addColumn("temp_slsu", Types.INTEGER, 10, 0)
    rs.addColumn("temp_cons", Types.INTEGER, 10, 0)
    rs.addColumn("cons_boh_lt", Types.INTEGER, 10, 0)
    rs.addColumn("cons_need_lt", Types.INTEGER, 10, 0)
    rs.addColumn("cons_rcpt_lt", Types.INTEGER, 10, 0)
    rs.addColumn("cons_avl_lt", Types.INTEGER, 10, 0)
    rs.addColumn("max_cons_lt", Types.INTEGER, 10, 0)
    rs.addColumn("po_alloc_lt", Types.INTEGER, 10, 0)
    rs.addColumn("unalloc_lt", Types.INTEGER, 10, 0)
    rs.addColumn("cons_eoh_lt", Types.INTEGER, 10, 0)
    rs.addColumn("cons_slsu_lt", Types.INTEGER, 10, 0)
    rs.addColumn("dc_rcpt", Types.INTEGER, 10, 0)
    rs.addColumn("dc_oh_rsv", Types.INTEGER, 10, 0)
    rs.addColumn("dc_ata", Types.INTEGER, 10, 0)
    rs.addColumn("a_out", Types.INTEGER, 10, 0)
    rs.addColumn("dc_raw", Types.INTEGER, 10, 0)
    rs.addColumn("outbound", Types.INTEGER, 10, 0)
    rs.addColumn("dc_poh", Types.INTEGER, 10, 0)
    rs.addColumn("deficit", Types.INTEGER, 10, 0)
    rs
  }

  def half_round(v: Double): Int = {
    var rounded_number: Long = v.round
    if (rounded_number == v + 0.5) {
      rounded_number -= 1
    }
    rounded_number.toInt
  }
}
