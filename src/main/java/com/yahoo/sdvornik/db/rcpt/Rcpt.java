package com.yahoo.sdvornik.db.rcpt;


public class Rcpt {

  public final static Rcpt Default = new Rcpt();

  private final int leadTime;

  private final int toh;
  private final int uncBoh;
  private final int uncNeed;
  private final int uncFcst;
  private final int existInv;
  private final int existSlsu;
  private final int consSlsu;
  private final int consEoh;

  private int uncFcstLt;
  private int tohLt;
  private int uncNeedLt;
  private int consEohLt;

  private int tempBoh;
  private int tempNeed;
  private int tempRcpt;
  private int tempEoh;
  private int tempSlsu;
  private int tempCons;
  private int dcSbkt;


  private int consBohLt;
  private int consNeedLt;
  private int consRcptLt;
  private int consAvlLt;
  private int maxConsLt;
  private int poAllocLt;
  private int unAllocLt;
  private int consSlsuLt;



  public Rcpt(
    int lead_time,
    int TOH,
    int Unc_BOH,
    int Unc_Need,
    int Unc_Fcst,
    int Exist_Inv,
    int Exist_SlsU,
    int Cons_SlsU,
    int Cons_EOH
  ) {
    this.leadTime = lead_time;
    this.toh = TOH;
    this.uncBoh = Unc_BOH;
    this.uncNeed = Unc_Need;
    this.uncFcst = Unc_Fcst;
    this.existInv = Exist_Inv;
    this.existSlsu = Exist_SlsU;
    this.consSlsu = Cons_SlsU;
    this.consEoh = Cons_EOH;
  }

  public Rcpt(
    int lead_time,
    int Unc_Fcst_LT,
    int TOH_LT,
    int Unc_Need_LT,
    int Cons_EOH_LT

  ) {
    this.leadTime = lead_time;

    this.uncFcstLt = Unc_Fcst_LT;
    this.tohLt = TOH_LT;
    this.uncNeedLt = Unc_Need_LT;
    this.consEohLt = Cons_EOH_LT;

    this.toh = 0;
    this.uncBoh = 0;
    this.uncNeed = 0;
    this.uncFcst = 0;
    this.existInv = 0;
    this.existSlsu = 0;
    this.consSlsu = 0;
    this.consEoh = 0;
  }

  private Rcpt() {
    this.leadTime = 0;

    this.toh = 0;
    this.uncBoh = 0;
    this.uncNeed = 0;
    this.uncFcst = 0;
    this.existInv = 0;
    this.existSlsu = 0;
    this.consSlsu = 0;
    this.consEoh = 0;
  }

  public int getTempBoh() {
    return tempBoh;
  }

  public int getTempNeed() {
    return tempNeed;
  }

  public int getTempRcpt() {
    return tempRcpt;
  }

  public int getTempEoh() {
    return tempEoh;
  }

  public int getTempSlsu() {
    return tempSlsu;
  }

  public int getTempCons() {
    return tempCons;
  }

  public int getDcSbkt() {
    return dcSbkt;
  }

  public int getConsEohLt() {
    return consEohLt;
  }

  public int getConsBohLt() {
    return consBohLt;
  }

  public int getConsNeedLt() {
    return consNeedLt;
  }

  public int getConsRcptLt() {
    return consRcptLt;
  }

  public int getConsAvlLt() {
    return consAvlLt;
  }

  public int getMaxConsLt() {
    return maxConsLt;
  }

  public int getPoAllocLt() {
    return poAllocLt;
  }

  public int getUnAllocLt() {
    return unAllocLt;
  }

  public int getConsSlsuLt() {
    return consSlsuLt;
  }

  public int getUncNeedLt() {
    return uncNeedLt;
  }

  public int getLeadTime() {
    return leadTime;
  }

  public int getToh() {
    return toh;
  }

  public int getUncBoh() {
    return uncBoh;
  }

  public int getUncNeed() {
    return uncNeed;
  }

  public int getUncFcst() {
    return uncFcst;
  }

  public int getExistInv() {
    return existInv;
  }

  public int getExistSlsu() {
    return existSlsu;
  }

  public int getConsSlsu() {
    return consSlsu;
  }

  public int getConsEoh() {
    return consEoh;
  }

  public int getUncFcstLt() {
    return uncFcstLt;
  }

  public int getTohLt() {
    return tohLt;
  }

  public void setTempBoh(int tempBoh) {
    this.tempBoh = tempBoh;
  }

  public void setTempNeed(int tempNeed) {
    this.tempNeed = tempNeed;
  }

  public void setTempRcpt(int tempRcpt) {
    this.tempRcpt = tempRcpt;
  }

  public void setTempEoh(int tempEoh) {
    this.tempEoh = tempEoh;
  }

  public void setTempSlsu(int tempSlsu) {
    this.tempSlsu = tempSlsu;
  }

  public void setTempCons(int tempCons) {
    this.tempCons = tempCons;
  }

  public void setDcSbkt(int dcSbkt) {
    this.dcSbkt = dcSbkt;
  }

  public void setConsEohLt(int consEohLt) {
    this.consEohLt = consEohLt;
  }

  public void setConsBohLt(int consBohLt) {
    this.consBohLt = consBohLt;
  }

  public void setConsNeedLt(int consNeedLt) {
    this.consNeedLt = consNeedLt;
  }

  public void setConsRcptLt(int consRcptLt) {
    this.consRcptLt = consRcptLt;
  }

  public void setConsAvlLt(int consAvlLt) {
    this.consAvlLt = consAvlLt;
  }

  public void setMaxConsLt(int maxConsLt) {
    this.maxConsLt = maxConsLt;
  }

  public void setPoAllocLt(int poAllocLt) {
    this.poAllocLt = poAllocLt;
  }

  public void setUnAllocLt(int unAllocLt) {
    this.unAllocLt = unAllocLt;
  }

  public void setConsSlsuLt(int consSlsuLt) {
    this.consSlsuLt = consSlsuLt;
  }

  public void setUncFcstLt(int uncFcstLt) {
    this.uncFcstLt = uncFcstLt;
  }

  public void setTohLt(int tohLt) {
    this.tohLt = tohLt;
  }

  public void setUncNeedLt(int uncNeedLt) {
    this.uncNeedLt = uncNeedLt;
  }
}
