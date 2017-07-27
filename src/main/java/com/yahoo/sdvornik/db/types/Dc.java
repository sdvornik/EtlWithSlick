package com.yahoo.sdvornik.db.types;

public class Dc {

  public final static Dc Default = new Dc();

  private final int dcPoh;
  private final int dcRaw;
  private final int outbound;
  private final int dcSbkt;
  private int dcRcpt;
  private final int deficit;


  private int dcOhRsv;
  private int dcAta;
  private int aOut;

  public Dc(
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

    this.dcOhRsv = 0;
    this.dcAta = 0;
    this.aOut = 0;
  }

  private Dc() {
    this.dcPoh = 0;
    this.dcRaw = 0;
    this.outbound = 0;
    this.dcSbkt = 0;
    this.dcRcpt = 0;
    this.deficit = 0;

    this.dcOhRsv = 0;
    this.dcAta = 0;
    this.aOut = 0;
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

  public int getOutbound() {
    return outbound;
  }

  public int getDeficit() {
    return deficit;
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

  public void setDcRcpt(int dcRcpt) {
    this.dcRcpt = dcRcpt;
  }

  public void setDcOhRsv(int dcOhRsv) {
    this.dcOhRsv = dcOhRsv;
  }

  public void setDcAta(int dcAta) {
    this.dcAta = dcAta;
  }

  public void setaOut(int aOut) {
    this.aOut = aOut;
  }
}
