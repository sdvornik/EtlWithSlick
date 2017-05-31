package com.yahoo.sdvornik.db;

import com.yahoo.sdvornik.db.toh_input.PreTohInput;
//import fj.P2;
import com.yahoo.sdvornik.db.toh_input.TohInput;
import com.yahoo.sdvornik.db.tuples.*;
//import fj.P3;
import org.h2.tools.SimpleResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;
import java.util.*;

public class Func3 {
  private Func3() {}
  private final static Logger logger = LoggerFactory.getLogger(Func3.class);

  private final static String LOC_BASE_FCST_PREFIX = "LOC_BASE_FCST_";

  private final static int DEPARTMENT_SET_INITIAL_CAPACITY = 1024;

  private final static String PRODUCT  = "\""+FieldName.PRODUCT+"\"";
  private final static String DEPARTMENT  = "\""+FieldName.DEPARTMENT+"\"";
  private final static String INDX  = "\""+FieldName.INDX+"\"";
  private final static String LOCATION  = "\""+FieldName.LOCATION+"\"";
  private final static String FLRSET  = "\""+FieldName.FLRSET+"\"";
  private final static String GRADE  = "\""+FieldName.GRADE+"\"";
  private final static String STRCLIMATE  = "\""+FieldName.STRCLIMATE+"\"";
  private final static String DBTWK_INDX  = "\""+FieldName.DBTWK_INDX+"\"";
  private final static String ERLSTMKDNWK_INDX  = "\""+FieldName.ERLSTMKDNWK_INDX+"\"";
  private final static String INITRCPTWK_INDX  = "\""+FieldName.INITRCPTWK_INDX+"\"";
  private final static String EXITDATE_INDX  = "\""+FieldName.EXITDATE_INDX+"\"";
  private final static String NUM_SIZES  = "\""+FieldName.NUM_SIZES+"\"";
  private final static String FCST  = "\""+FieldName.FCST+"\"";
  private final static String TOO  = "\""+FieldName.TOO+"\"";
  private final static String APS  = "\""+FieldName.APS+"\"";
  private final static String APS_LOWER  = "\""+FieldName.APS_LOWER+"\"";
  private final static String WOC  = "\""+FieldName.WOC+"\"";
  private final static String EOH  = "\""+FieldName.EOH+"\"";
  private final static String BOD  = "\""+FieldName.BOD+"\"";

  private static Set<String> readDepartmentSet(Connection conn, String product, String DEPARTMENT_TABLE_NAME) throws SQLException {
    String sql =
      "select "+
        DEPARTMENT + " "+
      "from "+DEPARTMENT_TABLE_NAME+" "+
      "where "+PRODUCT+"='"+product+'\'';

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
    Set<FrontlineExit> FRONTLINE_EXIT_SET,
    Set<FrontlineDbtwkExit> FRONTLINE_DBTWK_EXIT_SET,
    Set<FrontlineWithTime> FRONTLINE_WITH_TIME,
    Map<String, Set<FrontlineClimate>> FRONT_CLIMATE_MAP,
    String FRONTLINE_NAME
  ) throws SQLException {

    String sql =
      "select "+
        LOCATION +','+
        FLRSET +','+
        GRADE +','+
        STRCLIMATE +','+
        DBTWK_INDX +','+
        ERLSTMKDNWK_INDX +','+
        INITRCPTWK_INDX +','+
        EXITDATE_INDX +' '+
      "from "+FRONTLINE_NAME+ ' '+
      "where "+PRODUCT+"='"+product+'\'';;

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
        FrontlineExit frontlineExitElm = new FrontlineExit(up, bottom);

        //TODO need only one string
        FRONTLINE_EXIT_SET.add(frontlineExitElm);

        int dbtwkBottom = Math.max(
          frontlineRs.getInt(FieldName.DBTWK_INDX),
          v_plancurrent
        );
        FrontlineDbtwkExit frontlineDbtwkExitElm = new FrontlineDbtwkExit(up, dbtwkBottom);

        //TODO need only one string
        FRONTLINE_DBTWK_EXIT_SET.add(frontlineDbtwkExitElm);

        String flrset = frontlineRs.getString(FieldName.FLRSET);
        String grade = frontlineRs.getString(FieldName.GRADE);
        String strclimate = frontlineRs.getString(FieldName.STRCLIMATE);
        FrontlineClimate frontlineClimateElm = new FrontlineClimate(grade, strclimate);

        Set<FrontlineClimate> elmSet = FRONT_CLIMATE_MAP.computeIfAbsent(flrset, key -> new HashSet<>());
        elmSet.add(frontlineClimateElm);

        Integer dbtwkIndx = frontlineRs.getInt(FieldName.DBTWK_INDX);
        Integer erlstmkdnwkIndx = frontlineRs.getInt(FieldName.ERLSTMKDNWK_INDX);
        Integer exitDateIndx = frontlineRs.getInt(FieldName.EXITDATE_INDX);

        FrontlineWithTime frontlineWithTimeElm = new FrontlineWithTime(dbtwkIndx, erlstmkdnwkIndx, exitDateIndx);
        FRONTLINE_WITH_TIME.add(frontlineWithTimeElm);


        LocationKey locKey = new LocationKey(frontlineRs.getString(FieldName.LOCATION));


      }
    }
    logger.info("FRONTLINE_EXIT_SET size: " + FRONTLINE_EXIT_SET.size());
    logger.info("FRONTLINE_WITH_TIME size: "+FRONTLINE_WITH_TIME.size());
    logger.info("FRONTLINE_DBTWK_EXIT_SET size: "+ FRONTLINE_DBTWK_EXIT_SET.size());
    int totalSize=0;
    for(Map.Entry<String,Set<FrontlineClimate>> entry : FRONT_CLIMATE_MAP.entrySet()) {
      totalSize+=entry.getValue().size();
    }
    logger.info("FRONT_CLIMATE_MAP total size: "+totalSize);

  }

  private static Map<AmWkKey, List<String>>  readAttrTime(
    Connection conn,
    String product,
    Set<String> DEPARTMENT_SET,
    Map<String, Set<FrontlineClimate>> FRONT_CLIMATE_MAP,
    String ATTR_TIME_NAME
  ) throws SQLException {

    String sql =
      "select "+
        INDX +','+
        FLRSET +','+
        DEPARTMENT +' '+
      "from "+ATTR_TIME_NAME;

    Map<AmWkKey, List<String>> AM_WK_MAP = new HashMap<>();

    try(Statement st = conn.createStatement()) {
      ResultSet attrTimeRs = st.executeQuery(sql);
      while (attrTimeRs.next()) {
        String department = attrTimeRs.getString(FieldName.DEPARTMENT);
        if(!DEPARTMENT_SET.contains(department)) continue;
        String flrset = attrTimeRs.getString(FieldName.FLRSET);
        if(flrset == null) continue;
        Integer week_indx = attrTimeRs.getInt(FieldName.INDX);

        Set<FrontlineClimate> elmSet = FRONT_CLIMATE_MAP.get(flrset);
        if(elmSet == null) continue;
        for(FrontlineClimate elm : elmSet) {
          String grade = elm.getGrade();
          String strclimate = elm.getStrClimate();
          AmWkKey amWkKey = new AmWkKey(week_indx, grade);
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

  private static Set<ClStrKey> readClStr(Connection conn, String CL_STR_NAME) throws SQLException {
    String sql =
      "select "+
        LOCATION +','+
        STRCLIMATE +" "+
      "from "+CL_STR_NAME;

    Set<ClStrKey> result = new HashSet<>();
    try(Statement st = conn.createStatement()) {

      ResultSet clStrRs = st.executeQuery(sql);
      while (clStrRs.next()) {
        String location = clStrRs.getString(FieldName.LOCATION);
        String climate = clStrRs.getString(FieldName.STRCLIMATE);
        ClStrKey locationClimate = new ClStrKey(location, climate);

        result.add(locationClimate);

      }
    }
    logger.info("CL_STR_SET size: "+result.size());
    return result;
  }

  private static Map<String, Set<Integer>> readStoreLookup(
    Connection conn,
    String product,
    Set<ClStrKey> CL_STR_SET,
    Set<String> DEPARTMENT_SET,
    Map<AmWkKey, List<String>> AM_WK_MAP,
    Set<FrontlineExit> FRONTLINE_EXIT_SET,
    String STORE_LOOKUP_NAME
  ) throws SQLException {

    String sql =
      "select " +
        INDX +','+
        LOCATION +','+
        GRADE +','+
        DEPARTMENT +' '+
      "from "+STORE_LOOKUP_NAME;

    Map<String, Set<Integer>> STORE_LIST_MAP = new HashMap<>();

    try(Statement st = conn.createStatement()) {
      ResultSet storeLookupRs = st.executeQuery(sql);

      FrontlineExit frontlineExit = FRONTLINE_EXIT_SET.iterator().next();
      int bottom = frontlineExit.getBottom();
      int up = frontlineExit.getUp();

      while (storeLookupRs.next()) {
        String department = storeLookupRs.getString(FieldName.DEPARTMENT);
        if(!DEPARTMENT_SET.contains(department)) continue;

        int idIndx = storeLookupRs.getInt(FieldName.INDX);
        if(idIndx < bottom || idIndx >= up) continue;

        String location = storeLookupRs.getString(FieldName.LOCATION);
        String grade = storeLookupRs.getString(FieldName.GRADE);

        AmWkKey amWkKey = new AmWkKey(idIndx, grade);
        List<String> strClimateList = AM_WK_MAP.get(amWkKey);
        if(strClimateList == null) continue;

        for(String strclimate : strClimateList) {
          ClStrKey locationClimate = new ClStrKey(location, strclimate);

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

  private static Set<LocationPreTohInputKey> createStoreLyfecyle(
    Map<String, Integer> MIN_INDEX_BY_LOCATION,
    Set<FrontlineWithTime> FRONTLINE_WITH_TIME,
    Integer numSizes
  ) {
    Set<LocationPreTohInputKey> STORE_LYFECYLE_SET = new HashSet<>();

    for(Map.Entry<String, Integer> entry : MIN_INDEX_BY_LOCATION.entrySet()) {
      String location = entry.getKey();
      Integer flrset_min = entry.getValue();
      for(FrontlineWithTime frontLineWithTime : FRONTLINE_WITH_TIME) {
        Integer dbtwk_indx = frontLineWithTime.getDbtwkIndx();
        Integer erlstmkdnwk_indx = frontLineWithTime.getErlstmkdnwkIndx();
        Integer exitdate_indx = frontLineWithTime.getExitDateIndx();

        int debut = Math.max(dbtwk_indx, flrset_min);
        int md_start = Math.max(erlstmkdnwk_indx, flrset_min);
        int too = Math.max(erlstmkdnwk_indx, flrset_min) - Math.max(dbtwk_indx, flrset_min);
        int exit = Math.max(exitdate_indx, flrset_min);
        PreTohInput preTohInput = new PreTohInput(debut, md_start, exit, too, numSizes);
        LocationPreTohInputKey setElm = new LocationPreTohInputKey(location, preTohInput);
        STORE_LYFECYLE_SET.add(setElm);

      }
    }
    logger.info("STORE_LYFECYLE_SET size: "+STORE_LYFECYLE_SET.size());
    return STORE_LYFECYLE_SET;
  }

  private static Map<LocationIndxKey, TohInput> joinStoreAndLifecyle(
    Set<LocationPreTohInputKey> STORE_LYFECYLE_SET,
    Map<String, Set<Integer>> STORE_LIST_MAP
  ) {

    Map<LocationIndxKey, TohInput> STORE_AND_LIFECYLE_MAP = new HashMap<>();
    for(LocationPreTohInputKey storeLyfecyleElm : STORE_LYFECYLE_SET) {
      String location = storeLyfecyleElm.getLocation();
      PreTohInput preTohInput = storeLyfecyleElm.getPreTohInput();

      Set<Integer> list = STORE_LIST_MAP.get(location);
      if(list == null) continue;
      Integer[] indxArr = list.toArray(new Integer[list.size()]);
      IndexSearcher<Integer> searcher = new IndexSearcher(indxArr);
      int start = searcher.binarySearch(preTohInput.getDebut());
      int end = searcher.binarySearch(preTohInput.getExit());

      for(int i = start; i <= end; ++i) {
        LocationIndxKey key = new LocationIndxKey(new LocationKey(location), indxArr[i]);

        Integer tohCalc = (indxArr[i] < preTohInput.getMdStart()) ? 1 : 0;
        TohInput tohInput = new TohInput(preTohInput, tohCalc);

        STORE_AND_LIFECYLE_MAP.put(key, tohInput);

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
        NUM_SIZES +' ' +
      "from "+FRONT_SIZES_NAME+' '+
      "where "+PRODUCT+"='"+product+'\'';

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
    List<LocBaseFcstKey> LOC_BASE_FCST,
    Map<Integer, Map<LocationIndxKey, Integer>> LOC_BASE_FCST_LIST_BY_INDX
  ) throws SQLException {
    String sql =
      "select " +
        INDX +','+
        LOCATION +','+
        FCST +' '+
      "from "+LOC_BASE_FCST_PREFIX+product.replaceAll("-","_");

    try(Statement st = conn.createStatement()) {
      ResultSet locBaseFcstRs = st.executeQuery(sql);
      while (locBaseFcstRs.next()) {
        String location = locBaseFcstRs.getString(FieldName.LOCATION);
        Integer indx = locBaseFcstRs.getInt(FieldName.INDX);
        Integer fcst = locBaseFcstRs.getInt(FieldName.FCST);

        LocationIndxKey key = new LocationIndxKey(new LocationKey(location), indx);
        LocBaseFcstKey locBaseFcstElm = new LocBaseFcstKey(key, fcst);
        LOC_BASE_FCST.add(locBaseFcstElm);
        Map<LocationIndxKey, Integer> locBaseFcst = LOC_BASE_FCST_LIST_BY_INDX.computeIfAbsent(indx, k -> new HashMap<>());
        locBaseFcst.put(key, fcst);
      }
      logger.info("LOC_BASE_FCST size: "+LOC_BASE_FCST.size());
      logger.info("LOC_BASE_FCST_LIST_BY_INDX size: "+LOC_BASE_FCST_LIST_BY_INDX.size());
    }

  }

  private static void createTohInput(
    Map<LocationIndxKey, TohInput> STORE_AND_LIFECYLE_MAP,
    List<LocBaseFcstKey> LOC_BASE_FCST,
    Set<FrontlineDbtwkExit> FRONTLINE_DBTWK_EXIT_SET,
    Map<LocationIndxKey, TohInput> TOH_INPUT,
    Map<LocationKey, Map<IndxKey, Integer>> UPDATE_TOH_INPUT,
    Map<TooNumSizesKey, Map<LocationIndxKey, TohInput>> REC_LOCATION
  ) {

    for(LocBaseFcstKey locBaseFcst : LOC_BASE_FCST) {
      LocationIndxKey key = locBaseFcst.getKey();
      IndxKey indxKey = new IndxKey(key.getIndx());
      LocationKey locKey = key.getLocation();
      Integer fcst = locBaseFcst.getFcst();
      TohInput tohInput = STORE_AND_LIFECYLE_MAP.get(key);

      if(tohInput != null) {
        tohInput.setUncFcst(fcst);
        TOH_INPUT.put(key, tohInput);

        Map<IndxKey, Integer> curMap = UPDATE_TOH_INPUT.computeIfAbsent(locKey, k-> new TreeMap<>());
        curMap.put(indxKey, fcst);

        if(tohInput.getTohCalc() == 1) {
          TooNumSizesKey tooNumSizesKey = tohInput.getTooNumSizesKey();

          Map<LocationIndxKey, TohInput> map = REC_LOCATION.computeIfAbsent(tooNumSizesKey, k -> new HashMap<>());

          map.put(key, tohInput);
        }

      }
      else {
        int indx = key.getIndx();
        FrontlineDbtwkExit dbtwkExit = FRONTLINE_DBTWK_EXIT_SET.iterator().next();
        if(indx >= dbtwkExit.getDbtwkBottom() && indx < dbtwkExit.getUp()) {
          TohInput tohInputDefault = new TohInput(PreTohInput.Default, 0);
          tohInputDefault.setUncFcst(fcst);
          TOH_INPUT.put(key, tohInputDefault);

          Map<IndxKey, Integer> curMap = UPDATE_TOH_INPUT.computeIfAbsent(locKey, k-> new TreeMap<>());
          curMap.put(indxKey, fcst);
        }
      }

    }
    int totalSize = 0;
    for(Map<LocationIndxKey, TohInput> curList : REC_LOCATION.values()) {
      totalSize+=curList.size();
    }
    logger.info("PRE_TEMP_TOH_INPUT size: "+TOH_INPUT.size());
    logger.info("REC_LOCATION size: "+totalSize);
  }

  private static Map<LocationIndxKey, Integer> readInvModel(
    Connection conn,
    Set<String> DEPARTMENT_SET,
    Map<TooNumSizesKey, Map<LocationIndxKey, TohInput>> REC_LOCATION,
    String INV_MODEL_NAME
  ) throws SQLException {
    String sql =
      "select " +
        TOO + ',' +
        NUM_SIZES + ',' +
        DEPARTMENT + ',' +
        APS + ',' +
        APS_LOWER + ',' +
        WOC + ' '+
      "from "+INV_MODEL_NAME;
    Map<LocationIndxKey, Integer> positiveMap = new HashMap<>();
    Map<LocationIndxKey, Integer> negativeMap = new HashMap<>();

    try(Statement st = conn.createStatement()) {
      ResultSet invModelRs = st.executeQuery(sql);
      while (invModelRs.next()) {
        String department = invModelRs.getString(FieldName.DEPARTMENT);
        if(!DEPARTMENT_SET.contains(department)) continue;

        int too = invModelRs.getInt(FieldName.TOO);
        int numSizes = invModelRs.getInt(FieldName.NUM_SIZES);
        TooNumSizesKey key = new TooNumSizesKey(too, numSizes);
        Map<LocationIndxKey, TohInput> recLocation= REC_LOCATION.get(key);
        if(recLocation == null) continue;

        int aps = invModelRs.getInt(FieldName.APS);
        int apsLower = invModelRs.getInt(FieldName.APS_LOWER);
        for(Map.Entry<LocationIndxKey, TohInput> entry : recLocation.entrySet()) {
          int fcst = entry.getValue().getUncFcst();
          if(fcst <= apsLower || fcst > aps) continue;

          LocationIndxKey locIndxKey = entry.getKey();
          int woc = invModelRs.getInt(FieldName.WOC);
          if(fcst > 0) {
            int positiveMin = positiveMap.getOrDefault(locIndxKey, Integer.MAX_VALUE);
            if(woc < positiveMin) positiveMap.put(locIndxKey, woc);
          }
          else {
            int negativeMin = negativeMap.getOrDefault(locIndxKey, Integer.MAX_VALUE);
            if(woc < negativeMin) negativeMap.put(locIndxKey, woc);
          }
        }
      }
    }
    logger.info("POSITIVE_MIN size: "+positiveMap.size());
    logger.info("NEGATIVE_MIN size: "+negativeMap.size());
    positiveMap.putAll(negativeMap);
    return positiveMap;
  }

  private static Map<LocationKey,Map<IndxKey,Integer>> createRecLocationMap(
    Map<TooNumSizesKey, Map<LocationIndxKey, TohInput>> REC_LOCATION,
    Map<LocationIndxKey, Integer> LKP_REC
  ) {

    Map<LocationKey,Map<IndxKey,Integer>> REC_LOCATION_EXT = new HashMap<>();
    for(Map<LocationIndxKey, TohInput> mapValue : REC_LOCATION.values()) {
      for(LocationIndxKey key : mapValue.keySet()) {
        int indx = key.getIndx();
        LocationKey locKey = key.getLocation();
        Integer woc = LKP_REC.get(key);
        Integer res = (woc != null) ? indx+woc : null;
        Map<IndxKey,Integer> curMap = REC_LOCATION_EXT.computeIfAbsent(locKey, k -> new TreeMap<>());
        curMap.put(new IndxKey(indx), res);
      }
    }
    logger.info("REC_LOCATION_EXT size: "+REC_LOCATION_EXT.size());
    return REC_LOCATION_EXT;
  }

  public static void createTohInputFinal(
    Map<LocationIndxKey, TohInput> TOH_INPUT,
    Map<LocationKey, Map<IndxKey, Integer>> UPDATE_TOH_MAP,
    Map<LocationKey,Map<IndxKey,Integer>> REC_LOCATION_EXT,
    Set<LocationKey> LOCATION_SET
  ) throws SQLException {

    Map<LocationIndxKey, Integer> V_TOH_MOD = new HashMap<>();

    Iterator<Map.Entry<LocationKey, Map<IndxKey, Integer>>> iterator = UPDATE_TOH_MAP.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<LocationKey, Map<IndxKey, Integer>> updateTohEntry = iterator.next();
      LocationKey loc = updateTohEntry.getKey();
      Map<IndxKey, Integer> updateTohValue = updateTohEntry.getValue();

      Map<IndxKey, Integer> recLocationExtValue = REC_LOCATION_EXT.get(loc);
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
          int curSum = V_TOH_MOD.getOrDefault(aggKey, 0);
          curSum += fcstValue;
          V_TOH_MOD.put(aggKey, curSum);
        }
      }

      for(Map.Entry<LocationIndxKey, TohInput> entry : TOH_INPUT.entrySet()) {
        LocationIndxKey key = entry.getKey();
        TohInput tohInput = entry.getValue();

        Integer tohValue = V_TOH_MOD.get(key);
        if(tohValue != null) tohInput.setToh(tohValue);
      }
    }
  }

  private static Map<LocationKey, Integer> readFromBod(
    Connection conn,
    Set<String> DEPARTMENT_SET,
    Set<LocationKey> LOCATION_SET,
    String BOD_NAME
  ) throws SQLException {
    String sql =
      "select " +
        LOCATION + ',' +
        DEPARTMENT + ',' +
        BOD + ' ' +
      "from " + BOD_NAME;
    Map<LocationKey, Integer> V_LT_MAP = new HashMap<>();
    try(Statement st = conn.createStatement()) {
      ResultSet bodRs = st.executeQuery(sql);
      while (bodRs.next()) {
          String department = bodRs.getString(FieldName.DEPARTMENT);
          if(! DEPARTMENT_SET.contains(department)) continue;
          LocationKey locKey = new LocationKey(bodRs.getString(FieldName.LOCATION));
          if(! LOCATION_SET.contains(locKey)) continue;
          Integer bod = bodRs.getInt(FieldName.BOD);
          V_LT_MAP.put(locKey, bod);
      }
    }
    return V_LT_MAP;
  }

  private static Map<LocationKey, Integer> readFromEohTable(Connection conn, String product, String EOH_NAME) throws SQLException {
    String sql =
      "select " +
        LOCATION + ',' +
        EOH + ' ' +
      "from "+ EOH_NAME + ' ' +
      "where "+PRODUCT+"='"+product+'\'';

    Map<LocationKey, Integer> EOH_BY_PRODUCT = new HashMap<>();
    try(Statement st = conn.createStatement()) {
      ResultSet eohByProductRs = st.executeQuery(sql);
      while (eohByProductRs.next()) {
        LocationKey loc = new LocationKey(eohByProductRs.getString(FieldName.LOCATION));
        Integer value = eohByProductRs.getInt(FieldName.EOH);
        EOH_BY_PRODUCT.put(loc, value);
      }
    }
    logger.info("EOH_BY_PRODUCT size: " + EOH_BY_PRODUCT.size());
    return EOH_BY_PRODUCT;
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
    String FRONT_SIZES_NAME,
    String INV_MODEL_NAME,
    String EOH_NAME,
    String BOD_NAME
  ) throws SQLException {

    SimpleResultSet rs = createOutputResultSet();
    if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) return rs;

    try {

      Set<FrontlineExit> FRONTLINE_EXIT_SET = new HashSet<>();
      Set<FrontlineDbtwkExit> FRONTLINE_DBTWK_EXIT_SET = new HashSet<>();
      Set<FrontlineWithTime> FRONTLINE_WITH_TIME = new HashSet<>();
      Map<String, Set<FrontlineClimate>> FRONT_CLIMATE_MAP = new HashMap<>();
      readFrontline(conn, product, v_plancurrent, v_planend, FRONTLINE_EXIT_SET, FRONTLINE_DBTWK_EXIT_SET, FRONTLINE_WITH_TIME, FRONT_CLIMATE_MAP, FRONTLINE_NAME);

      Integer numSizes = readFromFrontSizes(conn, product, FRONT_SIZES_NAME);

      Set<String> DEPARTMENT_SET = readDepartmentSet(conn, product, DEPARTMENT_TABLE_NAME);

      Set<ClStrKey> CL_STR_SET = readClStr(conn, CL_STR_NAME);

      Map<AmWkKey, List<String>> AM_WK_MAP = readAttrTime(conn, product, DEPARTMENT_SET, FRONT_CLIMATE_MAP, ATTR_TIME_NAME);

      Map<String, Set<Integer>> STORE_LIST_MAP =
        readStoreLookup(conn, product, CL_STR_SET, DEPARTMENT_SET, AM_WK_MAP, FRONTLINE_EXIT_SET, STORE_LOOKUP_NAME);

      Map<String, Integer> MIN_INDEX_BY_LOCATION = new HashMap<>();
      for(Map.Entry<String, Set<Integer>> entry : STORE_LIST_MAP.entrySet()) {
        MIN_INDEX_BY_LOCATION.put(entry.getKey(), entry.getValue().iterator().next());
      }
      logger.info("MIN_INDEX_BY_LOCATION size: "+MIN_INDEX_BY_LOCATION.size());

      Set<LocationPreTohInputKey> STORE_LYFECYLE_SET =
        createStoreLyfecyle(MIN_INDEX_BY_LOCATION, FRONTLINE_WITH_TIME, numSizes);

      Map<LocationIndxKey, TohInput> STORE_AND_LIFECYLE_MAP =
        joinStoreAndLifecyle(STORE_LYFECYLE_SET, STORE_LIST_MAP);



      List<LocBaseFcstKey> LOC_BASE_FCST = new ArrayList<>();
      Map<Integer, Map<LocationIndxKey, Integer>> LOC_BASE_FCST_LIST_BY_INDX = new TreeMap<>();
      readLocBaseFcst(conn, product, LOC_BASE_FCST, LOC_BASE_FCST_LIST_BY_INDX);

      Map<LocationIndxKey, TohInput> TOH_INPUT = new HashMap<>();
      Map<LocationKey, Map<IndxKey, Integer>> UPDATE_TOH_INPUT = new HashMap<>();
      Map<TooNumSizesKey, Map<LocationIndxKey, TohInput>> REC_LOCATION = new HashMap<>();
      createTohInput(STORE_AND_LIFECYLE_MAP, LOC_BASE_FCST, FRONTLINE_DBTWK_EXIT_SET, TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION);

      Map<LocationIndxKey, Integer> LKP_REC = readInvModel(conn, DEPARTMENT_SET, REC_LOCATION, INV_MODEL_NAME);

      Map<LocationKey,Map<IndxKey,Integer>> REC_LOCATION_EXT = createRecLocationMap(REC_LOCATION, LKP_REC);

      Set<LocationKey> LOCATION_SET = new HashSet<>();
      createTohInputFinal(TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION_EXT, LOCATION_SET);

      Map<LocationKey, Integer> V_LT_MAP = readFromBod(conn, DEPARTMENT_SET, LOCATION_SET, BOD_NAME);





      Map<LocationKey, Integer> EOH_BY_PRODUCT = readFromEohTable(conn, product, EOH_NAME);


      for (Map.Entry<LocationIndxKey, TohInput> entry : TOH_INPUT.entrySet()) {
        rs.addRow(
          entry.getKey().getLocation().getValue(),
          entry.getKey().getIndx(),
          entry.getValue().getUncFcst(),
          entry.getValue().getToh()
        );
      }


      logger.info("Successfully execute function.");
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
    rs.addColumn("unc_fcst", Types.INTEGER, 10, 0);
    rs.addColumn("toh", Types.INTEGER, 10, 0);

    return rs;
  }
}
