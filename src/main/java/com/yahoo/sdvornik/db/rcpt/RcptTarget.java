package com.yahoo.sdvornik.db.rcpt;


public class RcptTarget {

  public final static RcptTarget Default = new RcptTarget();


  private final int tempBoh;
  private final int tempNeed;
  private final int tempRcpt;
  private final int tempEoh;
  private final int tempSlsu;
  private final int tempCons;
  private final int dcSbkt;

  private final int uncNeedLt;

  private int consEohLt;


  private int consBohLt;
  private int consNeedLt;
  private int consRcptLt;
  private int consAvlLt;
  private int maxConsLt;
  private int poAllocLt;
  private int unAllocLt;
  private int consSlsuLt;



  public RcptTarget(
    int tempBoh,
    int tempNeed,
    int tempRcpt,
    int tempEoh,
    int tempSlsu,
    int tempCons,
    int dcSbkt,

    int uncNeedLt,

    int consEohLt


  ){
    this.tempBoh = tempBoh;
    this.tempNeed = tempNeed;
    this.tempRcpt = tempRcpt;
    this.tempEoh = tempEoh;
    this.tempSlsu = tempSlsu;
    this.tempCons = tempCons;
    this.dcSbkt = dcSbkt;

    this.uncNeedLt = uncNeedLt;

    this.consEohLt = consEohLt;
    this.consBohLt = 0;
    this.consNeedLt = 0;
    this.consRcptLt = 0;
    this.consAvlLt = 0;
    this.maxConsLt = 0;
    this.poAllocLt = 0;
    this.unAllocLt = 0;
    this.consSlsuLt = 0;

  }

  private RcptTarget(){
    this.tempBoh = 0;
    this.tempNeed = 0;
    this.tempRcpt = 0;
    this.tempEoh = 0;
    this.tempSlsu = 0;
    this.tempCons = 0;
    this.dcSbkt = 0;

    this.uncNeedLt = 0;
    this.consEohLt = 0;

    this.consBohLt = 0;
    this.consNeedLt = 0;
    this.consRcptLt = 0;
    this.consAvlLt = 0;
    this.maxConsLt = 0;
    this.poAllocLt = 0;
    this.unAllocLt = 0;
    this.consSlsuLt = 0;
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
}
