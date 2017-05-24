package com.yahoo.sdvornik.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LocationIndxKey {
  public final static String INDEX_NAME = "indx";
  public final static String LOCATION_NAME = "location";

  private final int indx;
  private final LocationKey location;
  private final int hash;

  public LocationIndxKey(LocationKey location, int indx) {
    this.location = location;
    this.indx = indx;
    this.hash = 31*location.hashCode()+indx;
  }

  public LocationIndxKey(ResultSet rs) throws SQLException {
    this.location = new LocationKey(rs.getString(LOCATION_NAME));
    this.indx = rs.getInt(INDEX_NAME);
    this.hash = 31*location.hashCode()+indx;
  }

  public int getIndx() {
    return indx;
  }

  public LocationKey getLocation() {
    return location;
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof LocationIndxKey)) return false;
    LocationIndxKey other = (LocationIndxKey)obj;
    return location.equals(other.location)&& indx == other.indx;
  }
}
