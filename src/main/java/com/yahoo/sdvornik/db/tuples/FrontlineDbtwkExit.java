package com.yahoo.sdvornik.db.tuples;

public final class FrontlineDbtwkExit {

  private final int up;

  private final int dbtwkBottom;

  public FrontlineDbtwkExit(int up, int dbtwkBottom) {
    this.up = up;
    this.dbtwkBottom = dbtwkBottom;
  }

  public int getUp() {
    return up;
  }

  public int getDbtwkBottom() {
    return dbtwkBottom;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FrontlineDbtwkExit that = (FrontlineDbtwkExit) o;

    return (up == that.up) && (dbtwkBottom == that.dbtwkBottom);
  }

  @Override
  public int hashCode() {
    return 31 * up + dbtwkBottom;
  }
}
