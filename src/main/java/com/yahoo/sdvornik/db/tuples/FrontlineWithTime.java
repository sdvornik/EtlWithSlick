package com.yahoo.sdvornik.db.tuples;

import java.util.Objects;

public final class FrontlineWithTime {
  private final int dbtwkIndx;

  private final int erlstmkdnwkIndx;

  private final int exitDateIndx;

  public FrontlineWithTime(int dbtwkIndx, int erlstmkdnwkIndx, int exitDateIndx) {
    this.dbtwkIndx = dbtwkIndx;
    this.erlstmkdnwkIndx = erlstmkdnwkIndx;
    this.exitDateIndx = exitDateIndx;
  }

  public int getDbtwkIndx() {
    return dbtwkIndx;
  }

  public int getErlstmkdnwkIndx() {
    return erlstmkdnwkIndx;
  }

  public int getExitDateIndx() {
    return exitDateIndx;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FrontlineWithTime that = (FrontlineWithTime) o;
    return dbtwkIndx == that.dbtwkIndx &&
      erlstmkdnwkIndx == that.erlstmkdnwkIndx &&
      exitDateIndx == that.exitDateIndx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbtwkIndx, erlstmkdnwkIndx, exitDateIndx);
  }
}
