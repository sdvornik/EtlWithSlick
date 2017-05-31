package com.yahoo.sdvornik.db.tuples;

import com.yahoo.sdvornik.db.LocationIndxKey;

import java.util.Objects;

public final class LocBaseFcstKey {

  private final LocationIndxKey key;

  private final int fcst;

  public LocBaseFcstKey(LocationIndxKey key, int fcst) {
    this.key = key;
    this.fcst = fcst;
  }

  public LocationIndxKey getKey() {
    return key;
  }

  public int getFcst() {
    return fcst;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LocBaseFcstKey that = (LocBaseFcstKey) o;
    return fcst == that.fcst && Objects.equals(key, that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, fcst);
  }
}
