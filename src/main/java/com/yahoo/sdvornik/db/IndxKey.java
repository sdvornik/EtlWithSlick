package com.yahoo.sdvornik.db;


import java.sql.ResultSet;
import java.sql.SQLException;

public final class IndxKey implements Comparable<IndxKey>{
  public final static String INDEX_NAME = "indx";

  private final int value;

  public IndxKey(int value) {
    this.value = value;
  }

  public IndxKey(ResultSet rs) throws SQLException {
    this.value = rs.getInt(INDEX_NAME);
  }

  public int getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof IndxKey)) return false;
    IndxKey other = (IndxKey) obj;
    return value == other.value;
  }

  @Override
  public int compareTo(IndxKey o) {
    return value - o.value;
  }
}
