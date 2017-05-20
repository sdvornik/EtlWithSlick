package com.yahoo.sdvornik.db.rcpt;

public final class RcptSource {

  public final static RcptSource Default = new RcptSource();


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



  public RcptSource(
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


    this.uncFcstLt = 0;
    this.tohLt = 0;
    this.uncNeedLt = 0;
    this.consEohLt = 0;
  }

  public RcptSource(
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

  private RcptSource() {
    this.leadTime = 0;

    this.toh = 0;
    this.uncBoh = 0;
    this.uncNeed = 0;
    this.uncFcst = 0;
    this.existInv = 0;
    this.existSlsu = 0;
    this.consSlsu = 0;
    this.consEoh = 0;


    this.uncFcstLt = 0;
    this.tohLt = 0;
    this.uncNeedLt = 0;
    this.consEohLt = 0;
  }

  public int getLeadTime() {
    return leadTime;
  }

  public int getUncFcstLt() {
    return uncFcstLt;
  }

  public int getTohLt() {
    return tohLt;
  }

  public int getUncNeedLt() {
    return uncNeedLt;
  }

  public int getConsEohLt() {
    return consEohLt;
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

  public void setUncFcstLt(int uncFcstLt) {
    this.uncFcstLt = uncFcstLt;
  }

  public void setTohLt(int tohLt) {
    this.tohLt = tohLt;
  }

  public void setUncNeedLt(int uncNeedLt) {
    this.uncNeedLt = uncNeedLt;
  }

  public void setConsEohLt(int consEohLt) {
    this.consEohLt = consEohLt;
  }
}
