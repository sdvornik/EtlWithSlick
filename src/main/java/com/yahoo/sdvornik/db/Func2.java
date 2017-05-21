package com.yahoo.sdvornik.db;


import org.h2.tools.SimpleResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class Func2 {

  private final static Logger logger = LoggerFactory.getLogger(Func2.class);

  private Func2() {}

  private final static Map<LocationKey,Map<IndxKey,Integer>> UPDATE_TOH_MAP = new HashMap<>();
  private final static Map<LocationKey,Map<IndxKey,Integer>> REC_LOCATION_EXT_MAP = new HashMap<>();
  private final static Map<LocationIndxKey, Integer> AGG_MAP = new HashMap<>();

  private final static String LOCATION_NAME = "location";
  private final static String INDEX_NAME = "indx";
  private final static String UNC_FCST_NAME = "unc_fcst";
  private final static String MAX_INDEX_NAME = "max_index";



  public static ResultSet custom_join_tables(Connection conn, String UPDATE_TOH_NAME, String REC_LOCATION_EXT) throws SQLException {
    SimpleResultSet rs = createOutputResultSet();
    if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) return rs;

    try {
      readFromUpdateTohTable(conn, UPDATE_TOH_NAME);
      readFromRecLocationExtTable(conn, REC_LOCATION_EXT);

      Iterator<Map.Entry<LocationKey, Map<IndxKey, Integer>>> iterator = UPDATE_TOH_MAP.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<LocationKey, Map<IndxKey, Integer>> updateTohEntry = iterator.next();
        LocationKey loc = updateTohEntry.getKey();
        Map<IndxKey, Integer> updateTohValue = updateTohEntry.getValue();

        Map<IndxKey, Integer> recLocationExtValue = REC_LOCATION_EXT_MAP.get(loc);
        if (recLocationExtValue == null) {
          iterator.remove();
          continue;
        }

        Set<IndxKey> recLocationExtIndxSet = recLocationExtValue.keySet();
        IndxKey[] recLocationExtIndxArr = recLocationExtIndxSet.toArray(new IndxKey[recLocationExtIndxSet.size()]);

        IndexSearcher<IndxKey> searcher = new IndexSearcher<>(recLocationExtIndxArr);

        for (Map.Entry<IndxKey, Integer> indxFcstEntry : updateTohValue.entrySet()) {
          IndxKey updateTohIndex = indxFcstEntry.getKey();
          int fcstValue = indxFcstEntry.getValue();

          int upperBoundIndex = searcher.binarySearch(updateTohIndex);
          if (upperBoundIndex < 0) continue;
          for (int i = 0; i <= upperBoundIndex; ++i) {
            IndxKey recLocationIndxKey = recLocationExtIndxArr[i];
            int recLocationMaxValue = recLocationExtValue.get(recLocationIndxKey);
            if (recLocationMaxValue <= updateTohIndex.getValue()) continue;
            LocationIndxKey aggKey = new LocationIndxKey(loc, recLocationIndxKey.getValue());
            int curSum = AGG_MAP.getOrDefault(aggKey, 0);
            curSum += fcstValue;
            AGG_MAP.put(aggKey, curSum);
          }

        }
        iterator.remove();
        REC_LOCATION_EXT_MAP.remove(loc);
      }

      for (Map.Entry<LocationIndxKey, Integer> entry : AGG_MAP.entrySet()) {
        rs.addRow(
          entry.getKey().getLocation().getValue(),
          entry.getKey().getIndx(),
          entry.getValue()
        );
      }
    }
    catch(Exception e) {
      e.printStackTrace();
    }

    return rs;
  }



  private static SimpleResultSet createOutputResultSet() {
    SimpleResultSet rs = new SimpleResultSet();

    rs.addColumn("location", Types.VARCHAR, 8, 0);
    rs.addColumn("indx", Types.INTEGER, 10, 0);
    rs.addColumn("toh", Types.INTEGER, 10, 0);

    return rs;
  }

  private static void readFromUpdateTohTable(Connection conn, String UPDATE_TOH_NAME) throws SQLException {
    String GET_UPDATE_TOH = "select "+
      LOCATION_NAME+", "+
      INDEX_NAME+", "+
      UNC_FCST_NAME+" from "+UPDATE_TOH_NAME;
    try(Statement st = conn.createStatement()) {
      ResultSet updateTohRs = st.executeQuery(GET_UPDATE_TOH);
      while (updateTohRs.next()) {
        LocationKey loc = new LocationKey(updateTohRs.getString(LOCATION_NAME));
        IndxKey indxKey = new IndxKey(updateTohRs.getInt(INDEX_NAME));
        Integer value = updateTohRs.getInt(UNC_FCST_NAME);
        Map<IndxKey,Integer> map = UPDATE_TOH_MAP.computeIfAbsent(loc, key -> new TreeMap<>());
        map.put(indxKey, value);
      }
    }
    logger.info("UPDATE_TOH_MAP size: " + UPDATE_TOH_MAP.size());
  }

  private static void readFromRecLocationExtTable(Connection conn, String REC_LOCATION_EXT) throws SQLException {
    String GET_REC_LOCATION_EXT = "select "+
      LOCATION_NAME+", "+
      INDEX_NAME+", "+
      MAX_INDEX_NAME+" from "+REC_LOCATION_EXT;
    try(Statement st = conn.createStatement()) {
      ResultSet recLocationExtRs = st.executeQuery(GET_REC_LOCATION_EXT);
      while (recLocationExtRs.next()) {
        LocationKey loc = new LocationKey(recLocationExtRs.getString(LOCATION_NAME));
        IndxKey indxKey = new IndxKey(recLocationExtRs.getInt(INDEX_NAME));
        Integer value = recLocationExtRs.getInt(MAX_INDEX_NAME);
        Map<IndxKey,Integer> map = REC_LOCATION_EXT_MAP.computeIfAbsent(loc, key -> new TreeMap<>());
        map.put(indxKey, value);
      }
    }
    logger.info("REC_LOCATION_EXT_MAP size: " + REC_LOCATION_EXT_MAP.size());

  }


}
