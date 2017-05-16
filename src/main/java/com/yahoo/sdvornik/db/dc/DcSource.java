package com.yahoo.sdvornik.db.dc;

import com.yahoo.sdvornik.db.IndxKey;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DcSource {

  public final static DcSource Default = new DcSource();

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

  public DcSource(ResultSet rs) throws SQLException {
    this.dcPoh = rs.getInt(DC_POH_NAME);
    this.dcRaw = rs.getInt(DC_RAW_NAME);
    this.dcSbkt = rs.getInt(DC_SBKT_NAME);
    this.dcRcpt = 0;
  }

  private DcSource() {
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
