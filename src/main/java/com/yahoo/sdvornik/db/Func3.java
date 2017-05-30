package com.yahoo.sdvornik.db;

import com.yahoo.sdvornik.db.toh_input.PreTohInput;
import fj.P2;
import fj.P3;
import org.h2.tools.SimpleResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;
import java.util.*;

public class Func3 {
  private final static Logger logger = LoggerFactory.getLogger(Func3.class);

  private final static String LOC_BASE_FCST_PREFIX = "LOC_BASE_FCST_";

  private final static int DEPARTMENT_SET_INITIAL_CAPACITY = 1024;


  private Func3() {}

  private static Set<String> readDepartmentSet(Connection conn, String product, String DEPARTMENT_TABLE_NAME) throws SQLException {
    String sql =
      "select "+
        FieldName.DEPARTMENT + " "+
      "from "+DEPARTMENT_TABLE_NAME+" "+
      "where "+FieldName.PRODUCT+"='"+product+'\'';

    Set<String> DEPARTMENT_SET = new HashSet<>(DEPARTMENT_SET_INITIAL_CAPACITY);
    try(Statement st = conn.createStatement()) {
      ResultSet departmentSetRs = st.executeQuery(sql);
      while (departmentSetRs.next()) {
        String department = departmentSetRs.getString(FieldName.DEPARTMENT);
        DEPARTMENT_SET.add(department);
      }
    }
    logger.info("DEPARTMENT_SET size: " + DEPARTMENT_SET.size());
    return DEPARTMENT_SET;
  }

  private static void readFrontline(
    Connection conn,
    String product,
    int v_plancurrent,
    int v_planend,
    Set<P2<Integer, Integer>> FRONTLINE_EXIT_SET,
    Set<P2<Integer, Integer>> FRONTLINE_DBTWK_EXIT_SET,
    Set<P3<Integer, Integer, Integer>> FRONTLINE_WITH_TIME,
    Map<String, Set<P2<String, String>>> FRONT_CLIMATE_MAP,
    String FRONTLINE_NAME
  ) throws SQLException {

    String sql =
      "select "+
        FieldName.LOCATION +','+
        FieldName.FLRSET +','+
        FieldName.GRADE +','+
        FieldName.STRCLIMATE +','+
        FieldName.DBTWK_INDX +','+
        FieldName.ERLSTMKDNWK_INDX +','+
        FieldName.INITRCPTWK_INDX +','+
        FieldName.EXITDATE_INDX +" "+
      "from "+FRONTLINE_NAME;

    try(Statement st = conn.createStatement()) {
      ResultSet frontlineRs = st.executeQuery(sql);
      while (frontlineRs.next()) {
        int bottom = Math.max(
          frontlineRs.getInt(FieldName.INITRCPTWK_INDX),
          v_plancurrent
        );
        int up = Math.min(
          frontlineRs.getInt(FieldName.EXITDATE_INDX),
          v_planend+1
        );
        P2<Integer, Integer> frontlineExitElm = new  P2<Integer, Integer>(){
          @Override
          public Integer _1() {
            return bottom;
          }
          @Override
          public Integer _2() {
            return up;
          }
        };
        //TODO need only one string
        FRONTLINE_EXIT_SET.add(frontlineExitElm);

        int dbtwkBottom = Math.max(
          frontlineRs.getInt(FieldName.DBTWK_INDX),
          v_plancurrent
        );
        P2<Integer, Integer> frontlineDbtwkExitElm = new  P2<Integer, Integer>(){
          @Override
          public Integer _1() {
            return dbtwkBottom;
          }
          @Override
          public Integer _2() {
            return up;
          }
        };
        //TODO need only one string
        FRONTLINE_DBTWK_EXIT_SET.add(frontlineDbtwkExitElm);

        String flrset = frontlineRs.getString(FieldName.FLRSET);
        String grade = frontlineRs.getString(FieldName.GRADE);
        String strclimate = frontlineRs.getString(FieldName.STRCLIMATE);
        P2<String, String> frontlineClimateElm = new P2<String, String>() {
          @Override
          public String _1() {
            return grade;
          }
          @Override
          public String _2() {
            return strclimate;
          }
        };
        Set<P2<String, String>> elmSet = FRONT_CLIMATE_MAP.computeIfAbsent(flrset, key -> new HashSet<>());
        elmSet.add(frontlineClimateElm);

        Integer dbtwkIndx = frontlineRs.getInt(FieldName.DBTWK_INDX);
        Integer erlstmkdnwkIndx = frontlineRs.getInt(FieldName.ERLSTMKDNWK_INDX);
        Integer exitDateIndx = frontlineRs.getInt(FieldName.EXITDATE_INDX);

        P3<Integer, Integer, Integer> frontlineWithTimeElm = new P3<Integer, Integer, Integer>(){
          @Override
          public Integer _1() {
            return dbtwkIndx;
          }
          @Override
          public Integer _2() {
            return erlstmkdnwkIndx;
          }
          @Override
          public Integer _3() {
            return exitDateIndx;
          }
        };
        FRONTLINE_WITH_TIME.add(frontlineWithTimeElm);


        LocationKey locKey = new LocationKey(frontlineRs.getString(FieldName.LOCATION));


      }
    }
    logger.info("FRONTLINE_EXIT_SET size: " + FRONTLINE_EXIT_SET.size());
    logger.info("FRONTLINE_DBTWK_EXIT_SET size: "+ FRONTLINE_DBTWK_EXIT_SET.size());
    logger.info("FRONT_CLIMATE_MAP size: "+FRONT_CLIMATE_MAP.size());
    logger.info("FRONTLINE_WITH_TIME size: "+FRONTLINE_WITH_TIME.size());
  }

  private static Map<P2<Integer, String>, List<String>>  readAttrTime(
    Connection conn,
    String product,
    Set<String> DEPARTMENT_SET,
    Map<String, Set<P2<String, String>>> FRONT_CLIMATE_MAP,
    String ATTR_TIME_NAME
  ) throws SQLException {

    String sql =
      "select "+
        FieldName.INDX +','+
        FieldName.FLRSET +','+
        FieldName.DEPARTMENT +" "+
      "from "+ATTR_TIME_NAME;

    Map<P2<Integer, String>, List<String>> AM_WK_MAP = new HashMap<>();

    try(Statement st = conn.createStatement()) {
      ResultSet attrTimeRs = st.executeQuery(sql);
      while (attrTimeRs.next()) {
        String department = attrTimeRs.getString(FieldName.DEPARTMENT);
        if(!DEPARTMENT_SET.contains(department)) continue;
        String flrset = attrTimeRs.getString(FieldName.FLRSET);
        if(flrset == null) continue;
        Integer week_indx = attrTimeRs.getInt(FieldName.INDX);

        Set<P2<String, String>> elmSet = FRONT_CLIMATE_MAP.get(flrset);
        if(elmSet == null) continue;
        for(P2<String, String> elm : elmSet) {
          String grade = elm._1();
          String strclimate = elm._2();
          P2<Integer, String> amWkKey = new P2<Integer, String>() {
            @Override
            public Integer _1() {
              return week_indx;
            }
            @Override
            public String _2() {
              return grade;
            }
          };

          List<String> strClimateList = AM_WK_MAP.computeIfAbsent(amWkKey, k -> new ArrayList<>());
          strClimateList.add(strclimate);
        }

      }
    }
    int totalSize=0;
    for(List<String>list : AM_WK_MAP.values()) {
      totalSize+=list.size();
    }
    logger.info("AM_WK_MAP size: " + totalSize);
    return AM_WK_MAP;
  }

  private static Set<P2<String, String>> readClStr(Connection conn, String CL_STR_NAME) throws SQLException {
    String sql =
      "select "+
        FieldName.LOCATION +','+
        FieldName.STRCLIMATE +" "+
      "from "+CL_STR_NAME;

    Set<P2<String, String>> result = new HashSet<>();
    try(Statement st = conn.createStatement()) {

      ResultSet clStrRs = st.executeQuery(sql);
      while (clStrRs.next()) {
        String location = clStrRs.getString(FieldName.LOCATION);
        String climate = clStrRs.getString(FieldName.STRCLIMATE);
        P2<String, String> locationClimate = new P2<String, String>() {
          @Override
          public String _1() {
            return location;
          }
          @Override
          public String _2() {
            return climate;
          }
        };

        result.add(locationClimate);

      }
    }
    logger.info("CL_STR_SET size: "+result.size());
    return result;
  }

  private static Map<String, Set<Integer>> readStoreLookup(
    Connection conn,
    String product,
    Set<P2<String, String>> CL_STR_SET,
    Set<String> DEPARTMENT_SET,
    Map<P2<Integer, String>, List<String>> AM_WK_MAP,
    Set<P2<Integer, Integer>> FRONTLINE_EXIT_SET,
    String STORE_LOOKUP_NAME
  ) throws SQLException {

    String sql =
      "select " +
        FieldName.INDX +','+
        FieldName.LOCATION +','+
        FieldName.GRADE +','+
        FieldName.DEPARTMENT +" "+
      "from "+STORE_LOOKUP_NAME;

    Map<String, Set<Integer>> STORE_LIST_MAP = new HashMap<>();

    try(Statement st = conn.createStatement()) {

      ResultSet storeLookupRs = st.executeQuery(sql);
      Map<P2<String, String>, String> PRE_STORE_LIST = new HashMap<>();
      P2<Integer, Integer> frontlineExit = FRONTLINE_EXIT_SET.iterator().next();
      int bottom = frontlineExit._1();
      int up = frontlineExit._2();
      while (storeLookupRs.next()) {
        String department = storeLookupRs.getString(FieldName.DEPARTMENT);
        if(!DEPARTMENT_SET.contains(department)) continue;
        int idIndx = storeLookupRs.getInt(FieldName.INDX);
        if(idIndx < bottom || idIndx >= up) continue;

        String location = storeLookupRs.getString(FieldName.LOCATION);
        String grade = storeLookupRs.getString(FieldName.GRADE);

        P2<Integer, String> amWkKey = new P2<Integer, String>() {
          @Override
          public Integer _1() {
            return idIndx;
          }
          @Override
          public String _2() {
            return grade;
          }
        };
        List<String> strClimateList = AM_WK_MAP.get(amWkKey);
        if(strClimateList == null) continue;

        for(String strclimate : strClimateList) {
          P2<String, String> locationClimate = new P2<String, String>() {
            @Override
            public String _1() {
              return location;
            }
            @Override
            public String _2() {
              return strclimate;
            }
          };
          if(! CL_STR_SET.contains(locationClimate)) continue;
          Set<Integer> indxSet = STORE_LIST_MAP.computeIfAbsent(location, k -> new TreeSet<Integer>());
          indxSet.add(idIndx);
        }
      }
    }
    int totalSize = 0;
    for(Set<Integer> intSet : STORE_LIST_MAP.values()) {
      totalSize+=intSet.size();
    }
    logger.info("STORE_LIST_MAP total size: "+totalSize);
    return STORE_LIST_MAP;
  }

  private static Set<P2<String, PreTohInput>> createStoreLyfecyle(
    Map<String, Integer> MIN_INDEX_BY_LOCATION,
    Set<P3<Integer, Integer, Integer>> FRONTLINE_WITH_TIME
  ) {
    Set<P2<String, PreTohInput>> STORE_LYFECYLE_SET = new HashSet<>();

    for(Map.Entry<String, Integer> entry : MIN_INDEX_BY_LOCATION.entrySet()) {
      String location = entry.getKey();
      Integer flrset_min = entry.getValue();
      for(P3<Integer, Integer, Integer> frontLineWithTime : FRONTLINE_WITH_TIME) {
        Integer dbtwk_indx = frontLineWithTime._1();
        Integer erlstmkdnwk_indx = frontLineWithTime._2();
        Integer exitdate_indx = frontLineWithTime._3();

        int debut = Math.max(dbtwk_indx, flrset_min);
        int md_start = Math.max(erlstmkdnwk_indx, flrset_min);
        int too = Math.max(erlstmkdnwk_indx, flrset_min) - Math.max(dbtwk_indx, flrset_min);
        int exit = Math.max(exitdate_indx, flrset_min);
        PreTohInput preTohInput = new PreTohInput(debut, md_start, too, exit);
        P2<String, PreTohInput> setElm = new P2<String, PreTohInput>() {
          @Override
          public String _1() {
            return location;
          }
          @Override
          public PreTohInput _2() {
            return preTohInput;
          }
        };
        STORE_LYFECYLE_SET.add(setElm);

      }
    }
    logger.info("STORE_LYFECYLE_SET size: "+STORE_LYFECYLE_SET.size());
    return STORE_LYFECYLE_SET;
  }

  private static Map<LocationIndxKey, PreTohInput> joinStoreAndLifecyle(
    Set<P2<String, PreTohInput>> STORE_LYFECYLE_SET,
    Map<String, Set<Integer>> STORE_LIST_MAP
  ) {

    Map<LocationIndxKey, PreTohInput> STORE_AND_LIFECYLE_MAP = new HashMap<>();
    for(P2<String, PreTohInput> storeLyfecyleElm : STORE_LYFECYLE_SET) {
      String location = storeLyfecyleElm._1();
      PreTohInput preTohInput = storeLyfecyleElm._2();

      Set<Integer> list = STORE_LIST_MAP.get(location);
      if(list == null) continue;
      Integer[] indxArr = list.toArray(new Integer[list.size()]);
      IndexSearcher<Integer> searcher = new IndexSearcher(indxArr);
      int start = searcher.binarySearch(/*debut*/ preTohInput.getDebut());
      int end = searcher.binarySearch(/*exit*/ preTohInput.getExit());

      for(int week_indx = start; week_indx <= end; ++week_indx) {
        LocationIndxKey key = new LocationIndxKey(new LocationKey(location), week_indx);

        Integer toh_calc = (week_indx < /*md_start*/ preTohInput.getMdStart()) ? 1 : 0;
        preTohInput.setTohCalc(toh_calc);
        STORE_AND_LIFECYLE_MAP.put(key, preTohInput);

      }
    }
    logger.info("STORE_AND_LIFECYLE_MAP size: "+STORE_AND_LIFECYLE_MAP.size());
    return STORE_AND_LIFECYLE_MAP;
  }

  private static Integer readFromFrontSizes(
    Connection conn,
    String product,
    String FRONT_SIZES_NAME
  ) throws SQLException {
    String sql =
      "select " +
        FieldName.NUM_SIZES +' '+
      "from "+FRONT_SIZES_NAME+' '+
      "where "+FieldName.PRODUCT+"='"+product+'\'';

    Integer numSizes = null;
    try(Statement st = conn.createStatement()) {
      ResultSet numSizesRs = st.executeQuery(sql);
      numSizesRs.next();
      numSizes = numSizesRs.getInt(FieldName.NUM_SIZES);
    }
    logger.info("NUM_SIZES: "+numSizes);
    return numSizes;
  }

  private static void readLocBaseFcst(
    Connection conn,
    String product,
    List<P2<LocationIndxKey, Integer>> LOC_BASE_FCST,
    Map<Integer, Map<LocationIndxKey, Integer>> LOC_BASE_FCST_LIST_BY_INDX
  ) throws SQLException {
    String sql =
      "select " +
        FieldName.INDX +','+
        FieldName.LOCATION +','+
        FieldName.FCST +' '+
      "from "+LOC_BASE_FCST_PREFIX+product.replaceAll("-","_");

    try(Statement st = conn.createStatement()) {
      ResultSet locBaseFcstRs = st.executeQuery(sql);
      while (locBaseFcstRs.next()) {
        String location = locBaseFcstRs.getString(FieldName.LOCATION);
        Integer indx = locBaseFcstRs.getInt(FieldName.INDX);
        Integer fcst = locBaseFcstRs.getInt(FieldName.FCST);

        LocationIndxKey key = new LocationIndxKey(new LocationKey(location), indx);
        P2<LocationIndxKey, Integer> locBaseFcstElm = new P2<LocationIndxKey, Integer>() {
          @Override
          public LocationIndxKey _1() {
            return key;
          }
          @Override
          public Integer _2() {
            return fcst;
          }
        };
        LOC_BASE_FCST.add(locBaseFcstElm);
        Map<LocationIndxKey, Integer> locBaseFcst = LOC_BASE_FCST_LIST_BY_INDX.computeIfAbsent(indx, k -> new HashMap<>());
        locBaseFcst.put(key, fcst);
      }
      logger.info("LOC_BASE_FCST size: "+LOC_BASE_FCST.size());
      logger.info("LOC_BASE_FCST_LIST_BY_INDX size: "+LOC_BASE_FCST_LIST_BY_INDX.size());
    }

  }

  private static Map<LocationIndxKey, PreTohInput> createPreTempTohInput(
    Map<LocationIndxKey, PreTohInput> STORE_AND_LIFECYLE_MAP,
    List<P2<LocationIndxKey, Integer>> LOC_BASE_FCST,
    Set<P2<Integer, Integer>> FRONTLINE_DBTWK_EXIT_SET,
    Integer numSizes
  ) {
    Map<LocationIndxKey, PreTohInput> PRE_TEMP_TOH_INPUT = new HashMap<>();
    for(P2<LocationIndxKey, Integer> locBaseFcst : LOC_BASE_FCST) {
      LocationIndxKey key = locBaseFcst._1();
      Integer fcst = locBaseFcst._2();
      PreTohInput preTohInput = STORE_AND_LIFECYLE_MAP.get(key);
      if(preTohInput != null) {
        preTohInput.setUncFcst(fcst);
        preTohInput.setToh(0);
        PRE_TEMP_TOH_INPUT.put(key, preTohInput);
      }
      else {
        int indx = key.getIndx();
        P2<Integer, Integer> dbtwkExit = FRONTLINE_DBTWK_EXIT_SET.iterator().next();
        if(indx >= dbtwkExit._1() && indx < dbtwkExit._2()) {
          preTohInput = new PreTohInput(0, fcst);
          PRE_TEMP_TOH_INPUT.put(key, preTohInput);
        }
      }

    }
    logger.info("PRE_TEMP_TOH_INPUT size: "+PRE_TEMP_TOH_INPUT.size());
    return PRE_TEMP_TOH_INPUT;
  }



  public static ResultSet create_toh_input_table(
    Connection conn,
    String product,
    int v_plancurrent,
    int v_planend,
    String DEPARTMENT_TABLE_NAME,
    String ATTR_TIME_NAME,
    String FRONTLINE_NAME,
    String STORE_LOOKUP_NAME,
    String CL_STR_NAME,
    String FRONT_SIZES_NAME
  ) throws SQLException {

    SimpleResultSet rs = createOutputResultSet();
    if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) return rs;

    try {

      Set<P2<Integer, Integer>> FRONTLINE_EXIT_SET = new HashSet<>();
      Set<P3<Integer, Integer, Integer>> FRONTLINE_WITH_TIME = new HashSet<>();
      Map<String, Set<P2<String, String>>> FRONT_CLIMATE_MAP = new HashMap<>();
      Set<P2<Integer, Integer>> FRONTLINE_DBTWK_EXIT_SET = new HashSet<>();
      readFrontline(conn, product, v_plancurrent, v_planend, FRONTLINE_EXIT_SET, FRONTLINE_DBTWK_EXIT_SET, FRONTLINE_WITH_TIME, FRONT_CLIMATE_MAP, FRONTLINE_NAME);

      Set<String> DEPARTMENT_SET = readDepartmentSet(conn, product, DEPARTMENT_TABLE_NAME);

      Set<P2<String, String>> CL_STR_SET = readClStr(conn, CL_STR_NAME);

      Map<P2<Integer, String>, List<String>> AM_WK_MAP = readAttrTime(conn, product, DEPARTMENT_SET, FRONT_CLIMATE_MAP, ATTR_TIME_NAME);


      Map<String, Set<Integer>> STORE_LIST_MAP =
        readStoreLookup(conn, product, CL_STR_SET, DEPARTMENT_SET, AM_WK_MAP, FRONTLINE_EXIT_SET, STORE_LOOKUP_NAME);

      Map<String, Integer> MIN_INDEX_BY_LOCATION = new HashMap<>();
      for(Map.Entry<String, Set<Integer>> entry : STORE_LIST_MAP.entrySet()) {
        MIN_INDEX_BY_LOCATION.put(entry.getKey(), entry.getValue().iterator().next());
      }

      Set<P2<String, PreTohInput>> STORE_LYFECYLE_SET =
        createStoreLyfecyle(MIN_INDEX_BY_LOCATION, FRONTLINE_WITH_TIME);

      Map<LocationIndxKey, PreTohInput> STORE_AND_LIFECYLE_MAP =
        joinStoreAndLifecyle(STORE_LYFECYLE_SET, STORE_LIST_MAP);

      Integer numSizes = readFromFrontSizes(conn, product, FRONT_SIZES_NAME);

      List<P2<LocationIndxKey, Integer>> LOC_BASE_FCST = new ArrayList<>();
      Map<Integer, Map<LocationIndxKey, Integer>> LOC_BASE_FCST_LIST_BY_INDX = new TreeMap<>();
      readLocBaseFcst(conn, product, LOC_BASE_FCST, LOC_BASE_FCST_LIST_BY_INDX);

      Map<LocationIndxKey, PreTohInput> PRE_TEMP_TOH_INPUT =
        createPreTempTohInput(STORE_AND_LIFECYLE_MAP, LOC_BASE_FCST, FRONTLINE_DBTWK_EXIT_SET, numSizes);




      logger.info("Successfully execute function.");
    }
    catch(Exception e) {
      e.printStackTrace();
    }

    rs.addRow(
      "DC",
      1,
      1
    );
    rs.addRow(
      "DC",
      2,
      2
    );

    return rs;
  }

  private static SimpleResultSet createOutputResultSet() {
    SimpleResultSet rs = new SimpleResultSet();

    rs.addColumn("location", Types.VARCHAR, 8, 0);
    rs.addColumn("indx", Types.INTEGER, 10, 0);
    rs.addColumn("toh", Types.INTEGER, 10, 0);

    return rs;
  }
}
