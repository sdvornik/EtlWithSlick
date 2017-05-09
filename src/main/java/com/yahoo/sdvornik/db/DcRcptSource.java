package com.yahoo.sdvornik.db;

public final class DcRcptSource {
  private final int sbkt;
  private final int lead_time;
  private final int unc_fcst_lt;
  private final int toh_lt;
  private final int dc_poh;
  private final int dc_rcpt;
  private final int eoh;
  private final int init_max_cons;
  private final double ratio;
  private final int v_sbkt_start;

  private static DcRcptSource INSTANCE = new DcRcptSource();

  public static DcRcptSource getEmpty() {
    return INSTANCE;
  }

  private DcRcptSource() {
    this.sbkt = 0;
    this.lead_time = 0;
    this.unc_fcst_lt = 0;
    this.toh_lt = 0;
    this.dc_poh = 0;
    this.dc_rcpt = 0;
    this.eoh = 0;
    this.init_max_cons = 0;
    this.ratio = 0;
    this.v_sbkt_start=0;
  }


  public DcRcptSource(
    int sbkt,
    int lead_time,
    int unc_fcst_lt,
    int toh_lt,
    int dc_poh,
    int dc_rcpt,
    int eoh,
    int init_max_cons,
    double ratio,
    int v_sbkt_start
  ) {
      this.sbkt = sbkt;
      this.lead_time = lead_time;
      this.unc_fcst_lt = unc_fcst_lt;
      this.toh_lt = toh_lt;
      this.dc_poh = dc_poh;
      this.dc_rcpt = dc_rcpt;
      this.eoh = eoh;
      this.init_max_cons = init_max_cons;
      this.ratio = ratio;
      this.v_sbkt_start = v_sbkt_start;
  }

  public int getSbkt() {
    return sbkt;
  }

  public int getLead_time() {
    return lead_time;
  }

  public int getUnc_fcst_lt() {
    return unc_fcst_lt;
  }

  public int getToh_lt() {
    return toh_lt;
  }

  public int getDc_poh() {
    return dc_poh;
  }

  public int getDc_rcpt() {
    return dc_rcpt;
  }

  public int getEoh() {
    return eoh;
  }

  public int getInit_max_cons() {
    return init_max_cons;
  }

  public double getRatio() {
    return ratio;
  }

  public int getV_sbkt_start() {
    return v_sbkt_start;
  }

}
