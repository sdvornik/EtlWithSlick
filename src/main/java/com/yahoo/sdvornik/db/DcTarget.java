package com.yahoo.sdvornik.db;

public class DcTarget {
  public final static DcTarget Default = new DcTarget();

  private final int dcRcpt;
  private final int dcOhRsv;
  private final int dcAta;
  private final int aOut;

  public DcTarget(
    int dcRcpt,
    int dcOhRsv,
    int dcAta,
    int aOut
  ) {
    this.dcRcpt = dcRcpt;
    this.dcOhRsv = dcOhRsv;
    this.dcAta = dcAta;
    this.aOut = aOut;
  }

  private DcTarget() {
    this.dcRcpt = 0;
    this.dcOhRsv = 0;
    this.dcAta = 0;
    this.aOut = 0;
  }

  public int getDcOhRsv() {
    return dcOhRsv;
  }

  public int getDcAta() {
    return dcAta;
  }

  public int getaOut() {
    return aOut;
  }

  public int getDcRcpt() {
    return dcRcpt;
  }
}
