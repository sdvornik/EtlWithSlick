package com.yahoo.sdvornik.db.toh_input;

import com.yahoo.sdvornik.db.LocationIndxKey;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TohInputSource {
  public final static TohInputSource Default = new TohInputSource();

  private final static String TOH_INPUT_NAME = "TOH_INPUT_2";
  private final static String UNC_FCST_NAME = "unc_fcst";
  private final static String TOH_NAME = "toh";

  public final static String GET_TOH_INPUT_SOURCE = "SELECT "+
    LocationIndxKey.LOCATION_NAME +", "+
    LocationIndxKey.INDEX_NAME + ",	"+
    UNC_FCST_NAME + ",	"+
    TOH_NAME +
    " FROM "+TOH_INPUT_NAME;

  private final int uncFcst;
  private final int toh;

  public TohInputSource(ResultSet rs) throws SQLException{
    this.uncFcst = rs.getInt(UNC_FCST_NAME);
    this.toh = rs.getInt(TOH_NAME);
  }

  private TohInputSource() {
    this.uncFcst = 0;
    this.toh = 0;
  }

  public int getUncFcst() {
    return uncFcst;
  }

  public int getToh() {
    return toh;
  }
}
