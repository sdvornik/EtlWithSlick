package com.yahoo.sdvornik.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LocationIndxKey {
  public final static String INDEX_NAME = "indx";
  public final static String LOCATION_NAME = "location";

  private final int indx;
  private final LocationKey location;

  public LocationIndxKey(LocationKey location, int indx) {
    this.location = location;
    this.indx = indx;
  }

  public LocationIndxKey(ResultSet rs) throws SQLException {
    this.location = new LocationKey(rs.getString(LOCATION_NAME));
    this.indx = rs.getInt(INDEX_NAME);
  }

  public int getIndx() {
    return indx;
  }

  public LocationKey getLocation() {
    return location;
  }

  @Override
  public int hashCode() {
    StringBuilder sb = new StringBuilder();
    sb.append(indx);
    sb.append('\u0009');
    sb.append(location);
    return sb.toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof LocationIndxKey)) return false;
    LocationIndxKey other = (LocationIndxKey)obj;
    return location.equals(other.location)&& indx == other.indx;
  }
}
