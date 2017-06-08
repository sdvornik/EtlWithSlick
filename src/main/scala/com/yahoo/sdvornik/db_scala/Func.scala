package com.yahoo.sdvornik.db_scala

import java.sql.{Connection, ResultSet, SQLException, Types}
import java.util.concurrent.TimeUnit

import scala.collection.{mutable => M}
import com.typesafe.scalalogging.Logger
import com.yahoo.sdvornik.db_scala.Tuples._
import com.yahoo.sdvornik.mem_tables.FieldName
import com.yahoo.sdvornik.util.IndexSearcher
import org.h2.tools.SimpleResultSet
import com.yahoo.sdvornik.db_scala.H2DbHelper._
import com.yahoo.sdvornik.mem_tables._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}


object Func {

  private val logger = Logger("Func logger")

  val locBaseFcstMap: M.Map[String, TempProductTables] = M.HashMap()

  private val LOC_BASE_FCST_PREFIX: String = "LOC_BASE_FCST_"

  private val DURATION: Duration = Duration(5000, TimeUnit.MILLISECONDS)

  private def readFrontline(product: String, v_plancurrent: Int, v_planend: Int):
  Future[(M.Map[String, M.Set[FrontlineClimate]], Set[FrontlineExit], Set[FrontlineDbtwkExit], Set[FrontlineWithTime])] = {

    import scala.concurrent.ExecutionContext.Implicits.global
    import slick.jdbc.H2Profile.api._

    val frontlineClimateFuture: Future[M.Map[String, M.Set[FrontlineClimate]]] = h2DbExternal.run(
      frontline.filter(_.product===product).map(x =>
        (x.flrset, x.grade, x.strclimate)
      ).result
    )
    .map(l => l.foldLeft(new M.HashMap[String, M.Set[FrontlineClimate]]())((b,a) => {
      b.getOrElseUpdate(a._1, M.HashSet[FrontlineClimate]())+=FrontlineClimate(a._2, a._3)
      b
    }))

    val frontlineExitFuture: Future[Set[FrontlineExit]] = h2DbExternal.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.exitdate_indx, x.initrcptwk_indx)
      ).result
    ).map(l => l.map(x => FrontlineExit(Math.min(x._1, v_planend + 1), Math.max(x._2, v_plancurrent))).toSet)

    val frontlineDbtwkExitFuture: Future[Set[FrontlineDbtwkExit]] = h2DbExternal.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.exitdate_indx, x.dbtwk_indx)
      ).result
    ).map(l => l.map(x => FrontlineDbtwkExit(Math.min(x._1, v_planend + 1), Math.max(x._2, v_plancurrent))).toSet)

    val frontlineWithTimefuture: Future[Set[FrontlineWithTime]] = h2DbExternal.run(
      frontline.filter(_.product===product).distinct.map(x =>
        (x.dbtwk_indx, x.erlstmkdnwk_indx, x.exitdate_indx)
      ).result
    ).map(l => l.map(x => FrontlineWithTime(x._1, x._2, x._3)).toSet)


    for {
      f1 <- frontlineClimateFuture
      f2 <- frontlineExitFuture
      f3 <- frontlineDbtwkExitFuture
      f4 <- frontlineWithTimefuture
    } yield (f1, f2, f3, f4)

  }

  private def readFromDepartment(product: String): Future[Set[String]] = {
    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      department.filter(_.product===product).map(x => x.department).result
    )
      .map((x: Seq[String]) => x.toSet)
  }

  private def readFromFrontSizes(product: String): Future[Int] = {
    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      frontSizes.filter(_.product===product).map(_.num_sizes).result
    )
      .map(y => y.head)
  }

  private def readFromClStr(): Future[Set[ClStrKey]] = {
    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      clStr.map(x => (x.location, x.strclimate)).result
    )
      .map(z => z.map(y => ClStrKey(y._1, y._2)).toSet)
  }

  private def readFromAttrTime(
    product: String,
    DEPARTMENT_SET: Set[String],
    FRONT_CLIMATE_MAP: M.Map[String, M.Set[FrontlineClimate]]
  ): Future[M.Map[AmWkKey, M.ArrayBuffer[String]]] = {

    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      attrTime.map(x => (x.flrset, x.department, x.indx)).result
    )
    .map(y => y.foldLeft(M.HashMap[AmWkKey, M.ArrayBuffer[String]]())(
      ( b: M.HashMap[AmWkKey, M.ArrayBuffer[String]], a: (String, String, Int)) => {
        if (DEPARTMENT_SET.contains(a._2)) {
          FRONT_CLIMATE_MAP.getOrElse(a._1, M.Set[FrontlineClimate]()).foreach(elm => {
            b.getOrElseUpdate(AmWkKey(a._3, elm.grade), M.ArrayBuffer[String]()) += elm.strClimate
          })
        }
        b
    }))
  }

  private def readFromStoreLookup(
    product: String,
    CL_STR_SET: Set[ClStrKey],
    DEPARTMENT_SET: Set[String],
    AM_WK_MAP: M.Map[AmWkKey, M.ArrayBuffer[String]],
    FRONTLINE_EXIT_SET: Set[FrontlineExit]
  ): Future[M.Map[String, M.Set[Int]]]  = {

    val bottom: Int = FRONTLINE_EXIT_SET.head.bottom
    val up: Int = FRONTLINE_EXIT_SET.head.up

    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global

    h2DbExternal.run(
      storeLookup.map(x => (x.indx, x.location, x.grade, x.department)).result
    )
    .map(y => y.foldLeft(M.HashMap[String, M.Set[Int]]())(
      ( b: M.HashMap[String, M.Set[Int]], a: (Int, String, String, String)) => {
        if (DEPARTMENT_SET.contains(a._4)) {
          val idIndx: Int = a._1
          if (idIndx >= bottom && idIndx < up) {
            AM_WK_MAP.getOrElse(AmWkKey(idIndx, a._3), M.ArrayBuffer.empty).foreach(strclimate => {
              if (CL_STR_SET.contains(ClStrKey(a._2, strclimate))) {
                b.getOrElseUpdate(a._2, M.SortedSet[Int]()) += idIndx
              }
            })
          }
        }
        b
      })
    )
  }

  private def readLocBaseFcst(product: String):
    Future[(M.ArrayBuffer[LocBaseFcstKey], M.Map[Int, M.Map[LocationIndxKey, Int]])] = {

    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    val locBaseFcst = locBaseFcstMap(product).locBaseFcst

    h2DbExternal.run(
      locBaseFcst.map(x => (x.indx, x.location, x.fcst)).result
    )
    .map(y => y.foldLeft(M.ArrayBuffer[LocBaseFcstKey](), M.SortedMap[Int, M.Map[LocationIndxKey, Int]]())(
      ( b: (M.ArrayBuffer[LocBaseFcstKey], M.SortedMap[Int, M.Map[LocationIndxKey, Int]]), a: (Int, String, Int)) => {
        val key = LocationIndxKey(LocationKey(a._2), a._1)
        b._1+=LocBaseFcstKey(key, a._3)
        b._2.getOrElseUpdate(a._1, M.HashMap[LocationIndxKey, Int]()).put(key, a._3)
        b
      })
    )
  }

  private def readFromEoh(product: String): Future[M.Map[LocationKey, Int]] = {
    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      eoh.filter(_.product===product).map(x => (x.location, x.eoh)).result
    )
      .map(y => y.foldLeft(M.HashMap[LocationKey, Int]())(
        ( b: M.HashMap[LocationKey, Int], a: (String, Int)) => {
          b.put(LocationKey(a._1), a._2)
          b
        })
      )
  }

  private def readFromVrpTest(): Future[
    (Int, Int, M.Map[IndxKey, VrpTestSource], M.Map[SbktKey, M.Set[IndxKey]], M.Map[IndxKey, SbktKey])
    ] = {
    import slick.jdbc.H2Profile.api._
    import scala.concurrent.ExecutionContext.Implicits.global
    h2DbExternal.run(
      vrpTest.map(x => (x.indx, x.cons, x.final_qty, x.final_vrp, x.sbkt)).result
    )
      .map(y => y
        .foldLeft(
          (M.HashMap[IndxKey, VrpTestSource](),
            M.SortedMap[SbktKey, M.Set[IndxKey]](),
            M.SortedMap[IndxKey, SbktKey](),
            M.SortedSet[IndxKey]()
          )
        )((b, a) => {
          val indxKey: IndxKey = IndxKey(a._1)
          val sbktKey: SbktKey = SbktKey(a._5)
          val vrpTestSource: VrpTestSource = VrpTestSource(a._2.getOrElse(0), a._3, a._4)

          b._1.put(indxKey, vrpTestSource)
          b._2.getOrElseUpdate(sbktKey, M.SortedSet[IndxKey]())+=indxKey

          val dcSbkt: SbktKey = b._3.getOrElseUpdate(indxKey, SbktKey(Integer.MAX_VALUE))
          if (dcSbkt.sbkt > sbktKey.sbkt) b._3.put(indxKey, sbktKey)

          if (vrpTestSource.finalVrp > 0) b._4+=indxKey
          b
        })
    )
    .map(b => (b._4.head.indx, b._2(b._2.keySet.head).head.indx, b._1, b._2, b._3))
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
    DEPARTMENT_SET: Set[String],
    REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]]
  ): M.Map[LocationIndxKey, Int] = {

    val positiveMap: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()
    val negativeMap: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()

    import slick.jdbc.H2Profile.api._
    val invModelFuture: Future[Seq[(Int, Int, String, Long, Long, Int)]] = h2DbExternal.run(
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
    REC_LOCATION_EXT: M.Map[LocationKey, M.Map[IndxKey, Int]]
  ): M.Set[LocationKey] = {
    val LOCATION_SET: M.Set[LocationKey] = M.HashSet[LocationKey]()
    val V_TOH_MOD: M.Map[LocationIndxKey, Int] = M.HashMap[LocationIndxKey, Int]()

    UPDATE_TOH_MAP.foreach(updateTohEntry => {
      val loc: LocationKey = updateTohEntry._1
      val updateTohValue: M.Map[IndxKey, Int] = updateTohEntry._2

      REC_LOCATION_EXT.get(updateTohEntry._1) match {
        case Some(recLocationExtValue) =>
          val recLocationExtIndxArr: Array[IndxKey] = recLocationExtValue.keySet.toArray[IndxKey]
          val searcher: IndexSearcher[IndxKey] = new IndexSearcher[IndxKey](recLocationExtIndxArr)
          for (indxFcstEntry <- updateTohValue) {
            val updateTohIndex: IndxKey = indxFcstEntry._1
            val fcstValue: Int = indxFcstEntry._2
            val upperBoundIndex: Int = searcher.binarySearch(updateTohIndex)
            if (upperBoundIndex >= 0) {
              for (i <- 0 to upperBoundIndex) {
                val recLocationIndxKey: IndxKey = recLocationExtIndxArr(i)
                val recLocationMaxValue: Int = recLocationExtValue(recLocationIndxKey)
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
    TOH_INPUT.foreach(entry => {
      V_TOH_MOD.get(entry._1) match {
        case Some(tohValue) => entry._2.toh = tohValue
        case None =>
      }
      LOCATION_SET.add(entry._1.location)
    })
    LOCATION_SET
  }

  private def readFromBod(
    DEPARTMENT_SET: Set[String],
    LOCATION_SET: M.Set[LocationKey]
  ): M.Map[LocationKey, Int] = {
    val V_LT_MAP: M.Map[LocationKey, Int] = M.HashMap[LocationKey, Int]()

    import slick.jdbc.H2Profile.api._
    val bodFuture: Future[Seq[(String, String, Int)]] = h2DbExternal.run(
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
      val sbktKey: SbktKey = MIN_SBKT_BY_INDEX(indxKey)
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
      val dcSource: Dc = DC_MAP(IndxKey(v_sbkt_start))
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
        import scala.concurrent.ExecutionContext.Implicits.global

        val globalStart: Long = System.currentTimeMillis
        var start: Long = globalStart

        val csF = readFromClStr()
        val vrpF = readFromVrpTest()
        val dsF = readFromDepartment(product)
        val nsF = readFromFrontSizes(product)
        val lbF = readLocBaseFcst(product)
        val eohF = readFromEoh(product)
        val tF = readFrontline(product, v_plancurrent, v_planend)

        val combineFuture = for(
          cs <- csF;
          vrp <- vrpF;
          ds <- dsF;
          ns <- nsF;
          lb <- lbF;
          eoh <- eohF;
          t <- tF;
          amWk <- readFromAttrTime(product, ds, t._1);
          stLookup <- readFromStoreLookup(product, cs, ds, amWk, t._2)
        ) yield (t, ds, ns, cs, amWk, stLookup, lb, eoh, vrp)

        Await.result(combineFuture, DURATION)

        val res: (
          (M.Map[String, M.Set[FrontlineClimate]], Set[FrontlineExit], Set[FrontlineDbtwkExit], Set[FrontlineWithTime]),
          Set[String],
          Int,
          Set[ClStrKey],
          M.Map[AmWkKey, M.ArrayBuffer[String]],
          M.Map[String, M.Set[Int]],
          (M.ArrayBuffer[LocBaseFcstKey], M.Map[Int, M.Map[LocationIndxKey, Int]]),
          M.Map[LocationKey, Int],
          (Int, Int, M.Map[IndxKey, VrpTestSource], M.Map[SbktKey, M.Set[IndxKey]], M.Map[IndxKey, SbktKey])
          ) = combineFuture.value match {
          case Some(x) => x match {
            case Success(y) => y
            case Failure(e) => logger.error("Can't read source tables", e); throw e
          }
          case None => throw new IllegalStateException()
        }
        val FRONT_CLIMATE_MAP = res._1._1
        val FRONTLINE_EXIT_SET = res._1._2
        val FRONTLINE_DBTWK_EXIT_SET = res._1._3
        val FRONTLINE_WITH_TIME = res._1._4

        val DEPARTMENT_SET = res._2
        logger.info(s"DEPARTMENT_SET size: ${DEPARTMENT_SET.size}")

        val numSizes = res._3
        logger.info(s"numSizes value: $numSizes")

        val CL_STR_SET: Set[ClStrKey] = res._4
        logger.info("CL_STR_SET size: " + CL_STR_SET.size)

        val AM_WK_MAP: M.Map[AmWkKey, M.ArrayBuffer[String]] = res._5
        logger.info("AM_WK_MAP size: " + AM_WK_MAP.size)

        val STORE_LIST_MAP: M.Map[String, M.Set[Int]] = res._6
        logger.info("STORE_LIST_MAP size: " + STORE_LIST_MAP.size)

        val LOC_BASE_FCST = res._7._1
        logger.info("LOC_BASE_FCST size: " + LOC_BASE_FCST.size)
        val LOC_BASE_FCST_LIST_BY_INDX = res._7._2
        logger.info("LOC_BASE_FCST_LIST_BY_INDX size: " + LOC_BASE_FCST_LIST_BY_INDX.size)

        val EOH_BY_PRODUCT: M.Map[LocationKey, Int] = res._8
        logger.info("EOH_BY_PRODUCT size: " + EOH_BY_PRODUCT.size)


        val frstSbktFinalVrp: Int = res._9._1
        val vFrstSbkt: Int = res._9._2
        val VRP_TEST_SOURCE_MAP = res._9._3
        val SBKT_MAP = res._9._4
        val MIN_SBKT_BY_INDEX = res._9._5

        logger.info("vFrstSbkt: " + vFrstSbkt)
        logger.info("frstSbktFinalVrp: " + frstSbktFinalVrp)
        logger.info("VRP_TEST size: " + VRP_TEST_SOURCE_MAP.size)

        var end = System.currentTimeMillis
        logger.info("Total time of multithreaded calculation: " + (end - start))

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

        start = end
        val TOH_INPUT: M.Map[LocationIndxKey, TohInput] = M.HashMap[LocationIndxKey, TohInput]()
        val UPDATE_TOH_INPUT: M.Map[LocationKey, M.Map[IndxKey, Int]] = M.HashMap[LocationKey, M.Map[IndxKey, Int]]()
        val REC_LOCATION: M.Map[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]] = M.HashMap[TooNumSizesKey, M.Map[LocationIndxKey, TohInput]]()
        createTohInput(STORE_AND_LIFECYLE_MAP, LOC_BASE_FCST, FRONTLINE_DBTWK_EXIT_SET, TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION)
        end = System.currentTimeMillis
        logger.info("createTohInput() " + (end - start))

        start = end
        val LKP_REC: M.Map[LocationIndxKey, Int] = readInvModel(DEPARTMENT_SET, REC_LOCATION)
        end = System.currentTimeMillis
        logger.info("readInvModel() " + (end - start))
        logger.info("LKP_REC size: " + LKP_REC.size)

        start = end
        val REC_LOCATION_EXT: M.Map[LocationKey, M.Map[IndxKey, Int]] = createRecLocationMap(REC_LOCATION, LKP_REC)
        end = System.currentTimeMillis
        logger.info("createRecLocationMap() " + (end - start))
        logger.info("REC_LOCATION_EXT size: " + REC_LOCATION_EXT.size)

        start = end
        val LOCATION_SET: M.Set[LocationKey] =createTohInputFinal(TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION_EXT)
        end = System.currentTimeMillis
        logger.info("createTohInputFinal() " + (end - start))

        start = end
        val V_LT: M.Map[LocationKey, Int] = readFromBod(DEPARTMENT_SET, LOCATION_SET)
        end = System.currentTimeMillis
        logger.info("readFromBod() " + (end - start))

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

  private def createOutputResultSet: SimpleResultSet = {
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
