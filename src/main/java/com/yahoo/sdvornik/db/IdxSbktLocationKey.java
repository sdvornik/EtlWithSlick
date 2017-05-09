package com.yahoo.sdvornik.db;


public final class IdxSbktLocationKey {
  private final int idx;
  private final int sbkt;
  private final String location;

  public IdxSbktLocationKey(int idx, int sbkt, String location) {
    this.idx = idx;
    this.sbkt = sbkt;
    this.location = location;
  }

  public int getIdx() {
    return idx;
  }

  public String getLocation() {
    return location;
  }

  public int getSbkt() {
    return sbkt;
  }

  @Override
  public int hashCode() {
    StringBuilder builder = new StringBuilder();
    builder.append(location);
    builder.append('\u0009');
    builder.append(sbkt);
    builder.append('\u0009');
    builder.append(idx);
    return builder.toString().hashCode();
  }
}
