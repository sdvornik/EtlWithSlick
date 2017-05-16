package com.yahoo.sdvornik.db.rcpt;

import com.yahoo.sdvornik.db.LocationIndxKey;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class RcptSource {

  public final static RcptSource Default = new RcptSource();

  private final static String RCPT_NAME = "RCPT";

  private final static String LEAD_TIME_NAME = "lead_time";
  private final static String UNC_FCST_LT_NAME = "unc_fcst_lt";
  private final static String TOH_LT_NAME = "toh_lt";
  private final static String UNC_NEED_LT_NAME = "unc_need_lt";
  private final static String CONS_EOH_LT_NAME = "cons_eoh_lt";

  public final static String GET_RCPT_SOURCE = "SELECT "+
    LocationIndxKey.LOCATION_NAME +", "+
    LocationIndxKey.INDEX_NAME + ",	"+
    LEAD_TIME_NAME + ",	"+
    UNC_FCST_LT_NAME + ",	"+
    TOH_LT_NAME + ",	"+
    UNC_NEED_LT_NAME + ",	"+
    CONS_EOH_LT_NAME +

    " FROM "+RCPT_NAME;

  private final int leadTime;
  private final int uncFcstLt;
  private final int tohLt;
  private final int uncNeedLt;
  private final int consEohLt;


  public RcptSource(ResultSet rs) throws SQLException {
    this.leadTime = rs.getInt(LEAD_TIME_NAME);
    this.uncFcstLt = rs.getInt(UNC_FCST_LT_NAME);
    this.tohLt = rs.getInt(TOH_LT_NAME);
    this.uncNeedLt = rs.getInt(UNC_NEED_LT_NAME);
    this.consEohLt = rs.getInt(CONS_EOH_LT_NAME);
  }

  private RcptSource() {
    this.leadTime = 0;
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
}
