package com.yahoo.sdvornik.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DcSource {

  public final static DcSource Default = new DcSource();
  //SELECT 	coalesce(DC_Rcpt,0) AS dc_rcpt, coalesce(dc_poh,0) AS dc_poh WHERE  indx = '||t||'

  //UPDATE TEMP_DC as A SET dc_rcpt = b.dc_rcpt FROM (
  //SELECT dc_sbkt, MIN(indx) AS indx, SUM(dc_raw) AS dc_rcpt FROM 	TEMP_DC
  //WHERE  dc_sbkt = '||$2||' GROUP BY dc_sbkt) as B  WHERE a.product=b.product AND a.indx=b.indx'
  private final static String DC_NAME = "DC";
  private final static String DC_POH_NAME = "dc_poh";
  private final static String DC_RAW_NAME = "dc_raw";
  private final static String DC_SBKT_NAME = "dc_sbkt";

  public final static String GET_DC_SOURCE = "SELECT "+
    IndxKey.INDEX_NAME + ",	"+
    DC_POH_NAME + ",	"+
    DC_RAW_NAME + ",	"+
    DC_SBKT_NAME +

    " FROM "+DC_NAME;
  private final int dcPoh;
  private final int dcRaw;
  private final int dcSbkt;
  private int dcRcpt;

  DcSource(ResultSet rs) throws SQLException {
    this.dcPoh = rs.getInt(DC_POH_NAME);
    this.dcRaw = rs.getInt(DC_RAW_NAME);
    this.dcSbkt = rs.getInt(DC_SBKT_NAME);
    this.dcRcpt = 0;
  }

  DcSource() {
    this.dcPoh = 0;
    this.dcRaw = 0;
    this.dcSbkt = 0;
    this.dcRcpt = 0;
  }

  public int getDcPoh() {
    return dcPoh;
  }

  public int getDcRaw() {
    return dcRaw;
  }

  public int getDcSbkt() {
    return dcSbkt;
  }

  public int getDcRcpt() {
    return dcRcpt;
  }

  public void setDcRcpt(int dcRcpt) {
    this.dcRcpt = dcRcpt;
  }
}
