package com.yahoo.sdvornik.db.tuples;

public final class FrontlineExit {

  private final int up;

  private final int bottom;

  public FrontlineExit(int up, int bottom) {
    this.up = up;
    this.bottom = bottom;
  }

  public int getUp() {
    return up;
  }

  public int getBottom() {
    return bottom;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FrontlineExit that = (FrontlineExit) o;

    return (up == that.up) && (bottom == that.bottom);
  }

  @Override
  public int hashCode() {
    return 31 * up + bottom;
  }
}
