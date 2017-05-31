package com.yahoo.sdvornik.db.tuples;

import com.yahoo.sdvornik.db.toh_input.PreTohInput;

import java.util.Objects;

public final class LocationPreTohInputKey {

  private final String location;

  private final PreTohInput preTohInput;

  public LocationPreTohInputKey(String location, PreTohInput preTohInput) {
    this.location = location;
    this.preTohInput = preTohInput;
  }

  public String getLocation() {
    return location;
  }

  public PreTohInput getPreTohInput() {
    return preTohInput;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LocationPreTohInputKey that = (LocationPreTohInputKey) o;
    return Objects.equals(location, that.location) &&
      Objects.equals(preTohInput, that.preTohInput);
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, preTohInput);
  }
}
