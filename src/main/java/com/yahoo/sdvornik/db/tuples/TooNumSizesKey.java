package com.yahoo.sdvornik.db.tuples;

import java.util.Objects;

public final class TooNumSizesKey {

  private final int too;

  private final int numSizes;

  public TooNumSizesKey(int too, int numSizes) {
    this.too = too;
    this.numSizes = numSizes;
  }

  public int getToo() {
    return too;
  }

  public int getNumSizes() {
    return numSizes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TooNumSizesKey that = (TooNumSizesKey) o;
    return too == that.too &&
      numSizes == that.numSizes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(too, numSizes);
  }
}
