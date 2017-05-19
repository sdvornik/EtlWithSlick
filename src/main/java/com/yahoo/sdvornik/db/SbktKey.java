package com.yahoo.sdvornik.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SbktKey implements Comparable<SbktKey>{

  public final static String SBKT_NAME = "sbkt";

    private final int value;

    public SbktKey(int value) {
      this.value = value;
    }

    public SbktKey(ResultSet rs) throws SQLException {
      this.value = rs.getInt(SBKT_NAME);
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
      if(!(obj instanceof SbktKey)) return false;
      SbktKey other = (SbktKey) obj;
      return value == other.value;
    }

    @Override
    public int compareTo(SbktKey o) {
      return value - o.value;
    }
  }


