package com.yahoo.sdvornik.db.dc;

import com.yahoo.sdvornik.db.IndxKey;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DcSource {

  public final static DcSource Default = new DcSource();


  private final int dcPoh;
  private final int dcRaw;
  private final int outbound;
  private final int dcSbkt;
  private int dcRcpt;
  private final int deficit;

  public DcSource(
    int dcPoh,
    int dcRaw,
    int outbound,
    int dcSbkt,
    int dcRcpt,
    int deficit
  ) {
    this.dcPoh = dcPoh;
    this.dcRaw = dcRaw;
    this.outbound = outbound;
    this.dcSbkt = dcSbkt;
    this.dcRcpt = dcRcpt;
    this.deficit = deficit;
  }

  private DcSource() {
    this.dcPoh = 0;
    this.dcRaw = 0;
    this.outbound = 0;
    this.dcSbkt = 0;
    this.dcRcpt = 0;
    this.deficit = 0;
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

  public int getOutbound() {
    return outbound;
  }

  public int getDeficit() {
    return deficit;
  }
}
