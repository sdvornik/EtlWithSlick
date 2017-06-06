package com.yahoo.sdvornik.db_scala



object Tuples {

  case class IndxKey(indx: Int) extends Ordered[IndxKey] {
    override def compare(that: IndxKey): Int = indx.compare(that.indx)
  }
  case class LocationKey(value: String)
  case class LocationIndxKey(location: LocationKey, indx: Int)
  case class AmWkKey(indx: Int, grade: String)
  case class ClStrKey(location: String, climate: String)
  case class FrontlineClimate(grade: String, strClimate: String)
  case class FrontlineDbtwkExit(up: Int, dbtwkBottom: Int)
  case class FrontlineExit(up: Int, bottom: Int)
  case class FrontlineWithTime(dbtwkIndx: Int, erlstmkdnwkIndx: Int, exitDateIndx: Int)
  case class LocationPreTohInputKey(location: String, preTohInput: PreTohInput)
  case class LocBaseFcstKey(key: LocationIndxKey, fcst: Int)
  case class TooNumSizesKey(too: Int, numSizes: Int)
  case class PreTohInput(debut: Int, mdStart: Int, exit: Int, tooNumSizesKey: TooNumSizesKey)
  case class SbktKey(sbkt: Int) extends Ordered[SbktKey] {
    override def compare(that: SbktKey): Int = sbkt.compare(that.sbkt)
  }
  case class VrpTestSource(cons: Int, finalQty: Int, finalVrp: Int)

  object VrpTestSource {
    val Default: VrpTestSource = VrpTestSource(0,0,0)
  }

  object PreTohInput {
    val Default = PreTohInput(0, 0, 0, null)
  }

  object TohInput {
    val Default = new TohInput(null,0)
    def apply(tooNumSizesKey: TooNumSizesKey, tohCalc: Int) = new TohInput (tooNumSizesKey, tohCalc)
    def apply() = new TohInput()
  }

  class TohInput (val tooNumSizesKey: TooNumSizesKey, val tohCalc: Int) {
    def this() {
      this(null,0)
    }
    var uncFcst = 0
    var toh = 0
  }

  class Rcpt(
      val leadTime: Int,

      val toh: Int,
      val uncBoh: Int,
      val uncNeed: Int,
      val uncFcst: Int,
      val existInv: Int,
      val existSlsu: Int,
      val consSlsu: Int,
      val consEoh: Int,

      var uncFcstLt: Int,
      var tohLt: Int,
      var uncNeedLt: Int,
      var consEohLt: Int
  ) {
      var tempBoh: Int = 0
      var tempNeed: Int = 0
      var tempRcpt: Int = 0
      var tempEoh: Int = 0
      var tempSlsu: Int = 0
      var tempCons: Int = 0
      var dcSbkt: Int = 0

      var consBohLt: Int =0
      var consNeedLt: Int = 0
      var consRcptLt: Int = 0
      var consAvlLt: Int = 0
      var maxConsLt: Int = 0
      var poAllocLt: Int = 0
      var unAllocLt: Int = 0
      var consSlsuLt: Int = 0

    def this(lead_time: Int, uncFcstLt: Int, tohLt: Int, uncNeedLt: Int, consEohLt: Int) {
      this(lead_time, 0, 0, 0, 0, 0, 0, 0, 0, uncFcstLt, tohLt, uncNeedLt, consEohLt)
    }

    def this(leadTime: Int, toh: Int, uncBoh: Int, uncNeed: Int, uncFcst: Int, existInv: Int, existSlsu: Int, consSlsu: Int, consEoh: Int){
      this(leadTime, toh, uncBoh, uncNeed, uncFcst, existInv, existSlsu, consSlsu, consEoh, 0, 0, 0, 0)
    }

    private def this() {
      this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  object Rcpt {
    val Default: Rcpt = new Rcpt()

    def apply(lead_time: Int, uncFcstLt: Int, tohLt: Int, uncNeedLt: Int, consEohLt: Int) =
      new Rcpt(lead_time, uncFcstLt, tohLt, uncNeedLt, consEohLt)

    def apply(leadTime: Int, toh: Int, uncBoh: Int, uncNeed: Int, uncFcst: Int, existInv: Int, existSlsu: Int, consSlsu: Int, consEoh: Int) =
      new Rcpt(leadTime, toh, uncBoh, uncNeed, uncFcst, existInv, existSlsu, consSlsu, consEoh)
  }

  class Dc(
            val dcPoh: Int,
            val dcRaw: Int,
            val outbound: Int,
            val dcSbkt: Int,
            var dcRcpt: Int,
            val deficit: Int
  ) {
    var dcOhRsv: Int = 0
    var dcAta: Int = 0
    var aOut: Int = 0

    private def this() {
      this(0, 0, 0, 0, 0, 0)
    }
  }

  object Dc {
    val Default: Dc = new Dc()
    def apply(dcPoh: Int, dcRaw: Int, outbound: Int, dcSbkt: Int, dcRcpt: Int, deficit: Int) =
      new Dc(dcPoh, dcRaw, outbound, dcSbkt, dcRcpt, deficit)
  }
}
