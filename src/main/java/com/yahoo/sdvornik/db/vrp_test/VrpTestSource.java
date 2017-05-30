package com.yahoo.sdvornik.db.vrp_test;

import com.yahoo.sdvornik.db.Func;
import com.yahoo.sdvornik.db.IndxKey;
import com.yahoo.sdvornik.db.LocationIndxKey;
import com.yahoo.sdvornik.db.SbktKey;

import java.sql.ResultSet;
import java.sql.SQLException;

public class VrpTestSource {

  public final static VrpTestSource Default = new VrpTestSource();

  private final static String VRP_TEST_NAME = "VRP_TEST";

  private final static String CONS_NAME = "cons";
  private final static String FINAL_QTY_NAME = "final_qty";
  private final static String FINAL_VRP_NAME = "final_vrp";


  public final static String GET_VRP_TEST_SOURCE = "SELECT "+
    IndxKey.INDEX_NAME + ",	"+
    CONS_NAME + ",	"+
    FINAL_QTY_NAME + ",	"+
    FINAL_VRP_NAME + ",	"+
    SbktKey.SBKT_NAME +
    " FROM "+VRP_TEST_NAME;

  private final int cons;
  private final int finalQty;
  private final int finalVrp;

  public VrpTestSource(ResultSet rs) throws SQLException {
    this.cons = rs.getInt(CONS_NAME);
    this.finalQty = rs.getInt(FINAL_QTY_NAME);
    this.finalVrp = rs.getInt(FINAL_VRP_NAME);
  }

  private VrpTestSource() {
    this.cons = 0;
    this.finalQty = 0;
    this.finalVrp = 0;
  }

  public int getCons() {
    return cons;
  }

  public int getFinalQty() {
    return finalQty;
  }

  public int getFinalVrp() {
    return finalVrp;
  }
}
