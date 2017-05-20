package com.yahoo.sdvornik.db;

public final class Location implements Comparable<Location>{
  private final String value;

  public Location(String value) {
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
    if(!(obj instanceof Location)) return false;
    Location other = (Location) obj;
    return value.equals(other.value);
  }

  @Override
  public int compareTo(Location o) {
    return value.compareTo(o.value);
  }
}
