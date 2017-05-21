package com.yahoo.sdvornik.db;

public final class LocationKey implements Comparable<LocationKey>{
  private final String value;

  public LocationKey(String value) {
    this.value = value.trim();
  }

  public String getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof LocationKey)) return false;
    LocationKey other = (LocationKey) obj;
    return value.equals(other.value);
  }

  @Override
  public int compareTo(LocationKey o) {
    return value.compareTo(o.value);
  }
}
