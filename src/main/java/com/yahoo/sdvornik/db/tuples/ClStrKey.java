package com.yahoo.sdvornik.db.tuples;

import java.util.Objects;

public final class ClStrKey {

  private final String location;

  private final String climate;

  public ClStrKey(String location, String climate) {
    this.location = location;
    this.climate = climate;
  }

  public String getLocation() {
    return location;
  }

  public String getClimate() {
    return climate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ClStrKey clStrKey = (ClStrKey) o;
    return Objects.equals(location, clStrKey.location) &&
      Objects.equals(climate, clStrKey.climate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, climate);
  }
}
