package com.yahoo.sdvornik.db;

import com.yahoo.sdvornik.db.dc.Dc;
import com.yahoo.sdvornik.db.rcpt.Rcpt;
import com.yahoo.sdvornik.db.toh_input.PreTohInput;
import com.yahoo.sdvornik.db.toh_input.TohInput;
import com.yahoo.sdvornik.db.tuples.*;

import com.yahoo.sdvornik.db.vrp_test.VrpTestSource;
import fj.P2;
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

  private static Set<String> readFromDepartment(Connection conn, String product, String DEPARTMENT_TABLE_NAME) throws SQLException {
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

  private static Map<AmWkKey, List<String>> readFromAttrTime(
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

  private static Set<ClStrKey> readFromClStr(Connection conn, String CL_STR_NAME) throws SQLException {
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

  private static Map<String, Set<Integer>> readFromStoreLookup(
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
        LOCATION_SET.add(key.getLocation());
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

  private static P2<Integer, Integer> readFromVrpTest(
    Connection conn,
    Map<IndxKey, VrpTestSource> VRP_TEST_SOURCE_MAP,
    Map<SbktKey, Set<IndxKey>> SBKT_MAP,
    Map<IndxKey, SbktKey> MIN_SBKT_BY_INDEX
  ) throws SQLException {
    Set<IndxKey> FRST_SBKT_FINAL_VRP_SET = new TreeSet<>();
    try(Statement st = conn.createStatement()) {

      ResultSet vrpTestRs = st.executeQuery(VrpTestSource.GET_VRP_TEST_SOURCE);

      while (vrpTestRs.next()) {
        IndxKey indxKey = new IndxKey(vrpTestRs);
        SbktKey sbkt = new SbktKey(vrpTestRs);

        VrpTestSource vrpTestSource = new VrpTestSource(vrpTestRs);
        VRP_TEST_SOURCE_MAP.put(indxKey, vrpTestSource);

        Set<IndxKey> indxKeySet = SBKT_MAP.computeIfAbsent(
          sbkt,
          key -> new TreeSet<>()
        );
        indxKeySet.add(indxKey);
        SbktKey dcSbkt = MIN_SBKT_BY_INDEX.computeIfAbsent(
          indxKey,
          key -> new SbktKey(Integer.MAX_VALUE)
        );
        if (dcSbkt.compareTo(sbkt) > 0) MIN_SBKT_BY_INDEX.put(indxKey, sbkt);

        if (vrpTestSource.getFinalVrp() > 0) FRST_SBKT_FINAL_VRP_SET.add(indxKey);
      }
    }


    Integer frstSbktFinalVrp = FRST_SBKT_FINAL_VRP_SET.iterator().next().getValue();
    FRST_SBKT_FINAL_VRP_SET.clear();

    SbktKey minSbkt = SBKT_MAP.keySet().iterator().next();
    Set<IndxKey> indxKeySet = SBKT_MAP.get(minSbkt);

    Integer vFrstSbkt = indxKeySet.iterator().next().getValue();

    P2<Integer, Integer> firstSbkt = new P2<Integer, Integer>() {
      @Override
      public Integer _1() {
        return frstSbktFinalVrp;
      }
      @Override
      public Integer _2() {
        return vFrstSbkt;
      }
    };
    logger.info("vFrstSbkt: " + vFrstSbkt);
    logger.info("frstSbktFinalVrp: " + frstSbktFinalVrp);
    logger.info("VRP_TEST size: " + VRP_TEST_SOURCE_MAP.size());

    return firstSbkt;
  }

  private static Map<LocationKey, Integer> readFromEoh(Connection conn, String product, String EOH_NAME) throws SQLException {
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

  private static void fillRcptMap(
    int v_plancurrent,
    int v_planend,
    Set<LocationKey> LOCATION_SET,
    Map<LocationKey, Integer> V_LT,
    Map<LocationKey, Integer> EOH_BY_PRODUCT,
    Map<LocationIndxKey, TohInput> TOH_INPUT,
    Map<LocationIndxKey, Rcpt> RCPT_MAP,
    Map<IndxKey, List<Rcpt>> RCPT_MAP_BY_INDX
  ) {

    for(LocationKey str : LOCATION_SET) {
      int vLtValue = V_LT.getOrDefault(str,0);
      int eohValue = EOH_BY_PRODUCT.getOrDefault(str, 0);

      for(int t = v_plancurrent; t <= v_planend; ++t) {  //Change this to Max (Plan_Current, Debut_Week) to Min (Exit_Week, Plan_End)
        LocationIndxKey key = new LocationIndxKey(str, t);
        LocationIndxKey prevKey = new LocationIndxKey(str, t-1);
        TohInput TOH_INPUT_CUR = TOH_INPUT.getOrDefault(key, TohInput.Default);
        Rcpt RCPT_PREV = RCPT_MAP.getOrDefault(prevKey, Rcpt.Default);


        int vUncFcst = TOH_INPUT_CUR.getUncFcst();
        int vToh = TOH_INPUT_CUR.getToh();
        int vUncBoh = (t == v_plancurrent) ? eohValue : Math.max(0, RCPT_PREV.getUncBoh() + RCPT_PREV.getUncNeed() - RCPT_PREV.getUncFcst());
        int vExistInv = (t == v_plancurrent) ? eohValue : Math.max(0, RCPT_PREV.getUncBoh() - RCPT_PREV.getUncFcst());
        int vUncNeed = (t < v_plancurrent + vLtValue) ? 0 : Math.max(0, vToh - vUncBoh);

        int vExistInvTemp = (t == v_plancurrent) ? eohValue : Math.max(0,RCPT_PREV.getExistInv() - RCPT_PREV.getExistSlsu());
        int vConsSlsu = (t < v_plancurrent + vLtValue) ? 0 : Math.min(vExistInvTemp, vUncFcst);
        int vConsEoh = (t < v_plancurrent + vLtValue) ? 0 : vExistInvTemp - vConsSlsu;
        int vExistSlsu = Math.min(vExistInv,vUncFcst);

        Rcpt rcpt = new Rcpt(vLtValue, vToh, vUncBoh, vUncNeed, vUncFcst, vExistInv, vExistSlsu, vConsSlsu, vConsEoh);
        RCPT_MAP.put(key, rcpt);

        List<Rcpt> list = RCPT_MAP_BY_INDX.computeIfAbsent(
          new IndxKey(t),
          a -> new ArrayList<>()
        );
        list.add(rcpt);

        LocationIndxKey laggedKey = new LocationIndxKey(str, t - vLtValue);
        Rcpt sourceLagged = RCPT_MAP.get(laggedKey);
        if(sourceLagged == null) {
          Rcpt rcptLagged = new Rcpt(vLtValue, vUncFcst, vToh, vUncNeed, vConsEoh);

          RCPT_MAP.put(
            laggedKey,
            rcptLagged
          );
          List<Rcpt> list2 = RCPT_MAP_BY_INDX.computeIfAbsent(
            new IndxKey(t),
            a -> new ArrayList<>()
          );
          list2.add(rcptLagged);
        }
        else {
          sourceLagged.setConsEohLt(vConsEoh);
          sourceLagged.setTohLt(vToh);
          sourceLagged.setUncFcstLt(vUncFcst);
          sourceLagged.setUncNeedLt(vUncNeed);
        }
      }
    }
    logger.info("RCPT_MAP size: " + RCPT_MAP.size());
    logger.info("RCPT_MAP_BY_INDX size: " + RCPT_MAP_BY_INDX.size());
  }

  private static int fillDcMap(
    int v_plancurrent,
    int v_planend,
    Map<LocationKey, Integer> EOH_BY_PRODUCT,
    Map<IndxKey, VrpTestSource> VRP_TEST_SOURCE_MAP,
    Map<IndxKey, SbktKey> MIN_SBKT_BY_INDEX,
    Map<IndxKey, List<Rcpt>> RCPT_MAP_BY_INDX,
    int frstSbktFinalVrp,
    Map<IndxKey, Dc> DC_MAP,
    Map<SbktKey, List<Dc>> DC_MAP_BY_SBKT
  ) {
    int EOH_BY_PRODUCT_DC = EOH_BY_PRODUCT.getOrDefault(new LocationKey("DC"), 0);

    int Ttl_DC_Rcpt = 0;
    for (int t = v_plancurrent; t < v_planend; ++t) {  //Change this to Max (Plan_Current, Debut_Week) to Min (Exit_Week, Plan_End)
      IndxKey indxKey = new IndxKey(t);
      IndxKey prevIndxKey = new IndxKey(t - 1);

      VrpTestSource VRP_TEST = VRP_TEST_SOURCE_MAP.getOrDefault(indxKey, VrpTestSource.Default);
      SbktKey sbktKey = MIN_SBKT_BY_INDEX.get(indxKey);

      List<Rcpt> rcptList = RCPT_MAP_BY_INDX.getOrDefault(indxKey, new ArrayList<>());
      int RCPT_CUR_Agg_Unc_Need_DC = 0;
      int RCPT_CUR_Agg_Unc_TOH_DC = 0;
      for (Rcpt rcpt : rcptList) {
        RCPT_CUR_Agg_Unc_Need_DC += rcpt.getUncNeedLt();
        RCPT_CUR_Agg_Unc_TOH_DC += rcpt.getTohLt();
      }
      rcptList.clear();


      Dc DC_PREV = DC_MAP.getOrDefault(prevIndxKey, Dc.Default);

      int DC_POH = (t == v_plancurrent) ? DC_POH = EOH_BY_PRODUCT_DC :
        DC_PREV.getDcPoh() + DC_PREV.getDcRaw() - DC_PREV.getOutbound();

      int Used_Need = (t == v_plancurrent) ? RCPT_CUR_Agg_Unc_Need_DC :
        Math.min(RCPT_CUR_Agg_Unc_TOH_DC, RCPT_CUR_Agg_Unc_Need_DC + DC_PREV.getDeficit());

      int DC_Raw = (t < frstSbktFinalVrp) ? 0 :
        (VRP_TEST.getCons() == 1) ? VRP_TEST.getFinalQty() : Math.max(0, Used_Need - DC_POH);
      Ttl_DC_Rcpt += DC_Raw;

      int Outbound = Math.min(Used_Need, DC_POH + DC_Raw);
      int Deficit = Math.max(0, Used_Need - Outbound);


      Dc dcSource = new Dc(
        DC_POH,
        DC_Raw,
        Outbound,
        sbktKey.getValue(),
        0,
        Deficit
      );

      DC_MAP.put(indxKey, dcSource);
      List<Dc> list = DC_MAP_BY_SBKT.computeIfAbsent(
        sbktKey,
        key -> new ArrayList<Dc>()
      );
      list.add(dcSource);
    }
    logger.info("DC_MAP size: " + DC_MAP.size());
    logger.info("DC_MAP_BY_SBKT size: " + DC_MAP_BY_SBKT.size());
    logger.info("Ttl_DC_Rcpt: " + Ttl_DC_Rcpt);
    return Ttl_DC_Rcpt;
  }

  private static Map<LocationKey, Integer> calculateInitMaxCons(
    int Ttl_DC_Rcpt,
    Map<LocationIndxKey, Rcpt> RCPT_MAP,
    Map<LocationKey, Integer> EOH_BY_PRODUCT
  ) {
    Map<LocationKey, Integer> INIT_MAX_CONS = new HashMap<>();

    int Ttl_Str_Unc_Need = 0;
    Map<LocationKey, Integer> Str_Unc_Need = new HashMap<>();
    for (Map.Entry<LocationIndxKey, Rcpt> rcptEntry : RCPT_MAP.entrySet()) {
      LocationIndxKey key = rcptEntry.getKey();
      LocationKey locationKey = key.getLocation();
      Rcpt rcpt = rcptEntry.getValue();

      int localStr_Unc_Need = Str_Unc_Need.getOrDefault(locationKey, 0);
      localStr_Unc_Need += rcpt.getUncNeedLt();
      Str_Unc_Need.put(locationKey, localStr_Unc_Need);
      Ttl_Str_Unc_Need += rcpt.getUncNeedLt();

    }

    int EOH_BY_PRODUCT_DC = EOH_BY_PRODUCT.getOrDefault(new LocationKey("DC"), 0);
    for (Map.Entry<LocationKey, Integer> StrUncNeedEntry : Str_Unc_Need.entrySet()) {
      LocationKey locationKey = StrUncNeedEntry.getKey();
      int StrUncNeedValue = StrUncNeedEntry.getValue();

      int value;
      if (Ttl_Str_Unc_Need == 0) value = 0;
      else value = half_round((Ttl_DC_Rcpt + EOH_BY_PRODUCT_DC) * StrUncNeedValue / (double) Ttl_Str_Unc_Need);

      INIT_MAX_CONS.put(locationKey, value);
    }
    logger.info("INIT_MAX_CONS size: " + INIT_MAX_CONS.size());
    return INIT_MAX_CONS;
  }

  private static void finalCalculation(
    int v_plancurrent,
    int vFrstSbkt,
    Map<SbktKey, Set<IndxKey>> SBKT_MAP,
    Set<LocationKey> LOCATION_SET,
    Map<LocationKey, Integer> INIT_MAX_CONS,
    Map<LocationKey, Integer> EOH_BY_PRODUCT,
    Map<IndxKey, Dc> DC_MAP,
    Map<SbktKey, List<Dc>> DC_MAP_BY_SBKT,
    Map<LocationIndxKey, Rcpt> RCPT_MAP
  ) {
    for (Map.Entry<SbktKey, Set<IndxKey>> entry : SBKT_MAP.entrySet()) {

      SbktKey v_sbkt_id = entry.getKey();

      Set<IndxKey> INDX_SET = entry.getValue();

      int v_sbkt_start = -1;
      int temp_need_sbkt_dc = 0;
      Map<LocationKey, Integer> tempNeedSbktByLocation = new HashMap<>();

      for (IndxKey indxKey : INDX_SET) {
        int t = indxKey.getValue();
        if (v_sbkt_start < 0) v_sbkt_start = t;
        for (LocationKey str : LOCATION_SET) {
          LocationIndxKey key = new LocationIndxKey(str, t);
          LocationIndxKey prevKey = new LocationIndxKey(str, t - 1);

          Rcpt RCPT_CUR = RCPT_MAP.getOrDefault(key, Rcpt.Default);
          Rcpt RCPT_PREV = RCPT_MAP.getOrDefault(prevKey, Rcpt.Default);

          int Temp_BOH;
          int Temp_Cons;
          if (t == v_sbkt_start) {
            Temp_BOH = (t == v_plancurrent && RCPT_CUR.getLeadTime() == 0) ? EOH_BY_PRODUCT.getOrDefault(str, 0) :
              RCPT_PREV.getConsEohLt();

            Temp_Cons = (t == vFrstSbkt) ? INIT_MAX_CONS.getOrDefault(str, 0) :
              Math.max(0, RCPT_PREV.getMaxConsLt() - RCPT_PREV.getConsRcptLt());
          }
          else {
            Temp_BOH = RCPT_PREV.getTempEoh();
            Temp_Cons = Math.max(0, RCPT_PREV.getTempCons() - RCPT_PREV.getTempRcpt());
          }

          int Temp_Need = Temp_Cons > 0 ? Math.max(0, RCPT_CUR.getTohLt() - Temp_BOH) : 0;
          int Temp_Rcpt = Temp_Need;
          int Temp_SlsU = Math.min(RCPT_CUR.getUncFcstLt(), Temp_BOH + Temp_Rcpt);
          int Temp_EOH = Temp_BOH + Temp_Rcpt - Temp_SlsU;

          RCPT_CUR.setTempBoh(Temp_BOH);
          RCPT_CUR.setTempNeed(Temp_Need);
          RCPT_CUR.setTempRcpt(Temp_Rcpt);
          RCPT_CUR.setTempEoh(Temp_EOH);
          RCPT_CUR.setTempSlsu(Temp_SlsU);
          RCPT_CUR.setTempCons(Temp_Cons);
          RCPT_CUR.setDcSbkt(v_sbkt_id.getValue());


          temp_need_sbkt_dc += Temp_Need;
          int temp_need_sbkt = tempNeedSbktByLocation.getOrDefault(str, 0) + Temp_Need;
          tempNeedSbktByLocation.put(str, temp_need_sbkt);
        }
      }

      Map<LocationKey, Double> ratioMap = new HashMap<>();
      for (Map.Entry<LocationKey, Integer> temp_need_sbkt_entry : tempNeedSbktByLocation.entrySet()) {
        Double ratio = (temp_need_sbkt_dc == 0) ? 0.0 :
          temp_need_sbkt_entry.getValue() / Double.valueOf(temp_need_sbkt_dc);
        ratioMap.put(temp_need_sbkt_entry.getKey(), ratio);
      }

      tempNeedSbktByLocation.clear();

      List<Dc> listBySbkt = DC_MAP_BY_SBKT.getOrDefault(v_sbkt_id, new ArrayList<>());
      int sum = 0;
      for (Dc dc : listBySbkt) {
        sum += dc.getDcRaw();
      }
      DC_MAP_BY_SBKT.remove(v_sbkt_id);

      Dc dcSource = DC_MAP.get(new IndxKey(v_sbkt_start));
      dcSource.setDcRcpt(sum);

      for (IndxKey indxKey : INDX_SET) {
        int t = indxKey.getValue();
        IndxKey prevIndxKey = new IndxKey(t - 1);

        Dc DC_CUR = DC_MAP.getOrDefault(indxKey, Dc.Default);
        Dc DC_PREV = DC_MAP.getOrDefault(prevIndxKey, Dc.Default);

        int DC_OH_Rsv = (t == v_plancurrent) ? DC_CUR.getDcPoh() :
          DC_PREV.getDcOhRsv() + DC_PREV.getDcRcpt() - DC_PREV.getaOut();

        int DC_ATA = (t == v_sbkt_start) ? (DC_CUR.getDcRcpt() + DC_OH_Rsv) : DC_OH_Rsv;

        int A_OUT = 0;

        for (LocationKey str : LOCATION_SET) {
          LocationIndxKey key = new LocationIndxKey(str, t);
          LocationIndxKey prevKey = new LocationIndxKey(str, t - 1);

          Rcpt RCPT_CUR = RCPT_MAP.getOrDefault(key, Rcpt.Default);
          Rcpt RCPT_PREV = RCPT_MAP.getOrDefault(prevKey, Rcpt.Default);

          int Cons_BOH_LT = (t == v_plancurrent) ? ((RCPT_CUR.getLeadTime() == 0) ?
            EOH_BY_PRODUCT.getOrDefault(str, 0) : RCPT_PREV.getConsEohLt()) :
            RCPT_PREV.getConsBohLt() + RCPT_PREV.getConsRcptLt() - RCPT_PREV.getConsSlsuLt();
          RCPT_CUR.setConsBohLt(Cons_BOH_LT);


          int PO_Alloc_LT = (t == v_sbkt_start) ? half_round(DC_ATA * ratioMap.getOrDefault(str, 0.0)) : 0;
          RCPT_CUR.setPoAllocLt(PO_Alloc_LT);

          int Cons_Need_LT = Math.max(0,RCPT_CUR.getTohLt() - Cons_BOH_LT);
          RCPT_CUR.setConsNeedLt(Cons_Need_LT);

          int Max_Cons_LT = (t == vFrstSbkt) ? INIT_MAX_CONS.getOrDefault(str, 0) :
            Math.max(0, RCPT_PREV.getMaxConsLt() - RCPT_PREV.getConsRcptLt());
          RCPT_CUR.setMaxConsLt(Max_Cons_LT);

          int Cons_Avl_LT = (t == v_sbkt_start) ? Math.min(Max_Cons_LT, PO_Alloc_LT) : RCPT_PREV.getUnAllocLt();
          RCPT_CUR.setConsAvlLt(Cons_Avl_LT);

          int Cons_Rcpt_LT = Math.min(Cons_Need_LT, Cons_Avl_LT);
          RCPT_CUR.setConsRcptLt(Cons_Rcpt_LT);

          int UnAlloc_LT = Math.max(0, Cons_Avl_LT - Cons_Rcpt_LT);
          RCPT_CUR.setUnAllocLt(UnAlloc_LT);

          int Cons_SlsU_LT = Math.min(RCPT_CUR.getUncFcstLt(), Cons_BOH_LT + Cons_Rcpt_LT);
          RCPT_CUR.setConsSlsuLt(Cons_SlsU_LT);

          int Cons_EOH_LT = Math.max(0, Cons_BOH_LT + Cons_Rcpt_LT - Cons_SlsU_LT);

          RCPT_CUR.setConsEohLt(Cons_EOH_LT);

          A_OUT += Cons_Rcpt_LT;

        }
        DC_CUR.setaOut(A_OUT);
        DC_CUR.setDcAta(DC_ATA);
        DC_CUR.setDcOhRsv(DC_OH_Rsv);
      }
    }
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

      long start = System.currentTimeMillis();
      long globalStart = start;
      Set<FrontlineExit> FRONTLINE_EXIT_SET = new HashSet<>();
      Set<FrontlineDbtwkExit> FRONTLINE_DBTWK_EXIT_SET = new HashSet<>();
      Set<FrontlineWithTime> FRONTLINE_WITH_TIME = new HashSet<>();
      Map<String, Set<FrontlineClimate>> FRONT_CLIMATE_MAP = new HashMap<>();
      readFrontline(conn, product, v_plancurrent, v_planend, FRONTLINE_EXIT_SET, FRONTLINE_DBTWK_EXIT_SET, FRONTLINE_WITH_TIME, FRONT_CLIMATE_MAP, FRONTLINE_NAME);
      long end = System.currentTimeMillis();
      logger.info("readFrontline() "+(end-start));

      start=end;
      Integer numSizes = readFromFrontSizes(conn, product, FRONT_SIZES_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromFrontSizes() "+(end-start));

      start=end;
      Set<String> DEPARTMENT_SET = readFromDepartment(conn, product, DEPARTMENT_TABLE_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromDepartment() "+(end-start));

      start=end;
      Set<ClStrKey> CL_STR_SET = readFromClStr(conn, CL_STR_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromClStr() "+(end-start));

      start=end;
      Map<AmWkKey, List<String>> AM_WK_MAP = readFromAttrTime(conn, product, DEPARTMENT_SET, FRONT_CLIMATE_MAP, ATTR_TIME_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromAttrTime() "+(end-start));

      start=end;
      Map<String, Set<Integer>> STORE_LIST_MAP =
        readFromStoreLookup(conn, product, CL_STR_SET, DEPARTMENT_SET, AM_WK_MAP, FRONTLINE_EXIT_SET, STORE_LOOKUP_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromStoreLookup() "+(end-start));

      start=end;
      Map<String, Integer> MIN_INDEX_BY_LOCATION = new HashMap<>();
      for(Map.Entry<String, Set<Integer>> entry : STORE_LIST_MAP.entrySet()) {
        MIN_INDEX_BY_LOCATION.put(entry.getKey(), entry.getValue().iterator().next());
      }
      end = System.currentTimeMillis();
      logger.info("create MIN_INDEX_BY_LOCATION "+(end-start));
      logger.info("MIN_INDEX_BY_LOCATION size: "+MIN_INDEX_BY_LOCATION.size());

      start=end;
      Set<LocationPreTohInputKey> STORE_LYFECYLE_SET =
        createStoreLyfecyle(MIN_INDEX_BY_LOCATION, FRONTLINE_WITH_TIME, numSizes);
      end = System.currentTimeMillis();
      logger.info("createStoreLyfecyle() "+(end-start));

      start=end;
      Map<LocationIndxKey, TohInput> STORE_AND_LIFECYLE_MAP =
        joinStoreAndLifecyle(STORE_LYFECYLE_SET, STORE_LIST_MAP);
      end = System.currentTimeMillis();
      logger.info("joinStoreAndLifecyle() "+(end-start));

      start=end;
      List<LocBaseFcstKey> LOC_BASE_FCST = new ArrayList<>();
      Map<Integer, Map<LocationIndxKey, Integer>> LOC_BASE_FCST_LIST_BY_INDX = new TreeMap<>();
      readLocBaseFcst(conn, product, LOC_BASE_FCST, LOC_BASE_FCST_LIST_BY_INDX);
      end = System.currentTimeMillis();
      logger.info("readLocBaseFcst() "+(end-start));

      start=end;
      Map<LocationIndxKey, TohInput> TOH_INPUT = new HashMap<>();
      Map<LocationKey, Map<IndxKey, Integer>> UPDATE_TOH_INPUT = new HashMap<>();
      Map<TooNumSizesKey, Map<LocationIndxKey, TohInput>> REC_LOCATION = new HashMap<>();
      createTohInput(STORE_AND_LIFECYLE_MAP, LOC_BASE_FCST, FRONTLINE_DBTWK_EXIT_SET, TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION);
      end = System.currentTimeMillis();
      logger.info("createTohInput() "+(end-start));

      start=end;
      Map<LocationIndxKey, Integer> LKP_REC = readInvModel(conn, DEPARTMENT_SET, REC_LOCATION, INV_MODEL_NAME);
      end = System.currentTimeMillis();
      logger.info("readInvModel() "+(end-start));

      start=end;
      Map<LocationKey,Map<IndxKey,Integer>> REC_LOCATION_EXT = createRecLocationMap(REC_LOCATION, LKP_REC);
      end = System.currentTimeMillis();
      logger.info("createRecLocationMap() "+(end-start));

      start=end;
      Set<LocationKey> LOCATION_SET = new HashSet<>();
      createTohInputFinal(TOH_INPUT, UPDATE_TOH_INPUT, REC_LOCATION_EXT, LOCATION_SET);
      logger.info("LOCATION_SET size: "+LOCATION_SET.size());
      end = System.currentTimeMillis();
      logger.info("createTohInputFinal() "+(end-start));

      start=end;
      Map<LocationKey, Integer> V_LT = readFromBod(conn, DEPARTMENT_SET, LOCATION_SET, BOD_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromBod() "+(end-start));

      start=end;
      Map<IndxKey, VrpTestSource> VRP_TEST_SOURCE_MAP = new HashMap<>();
      Map<SbktKey, Set<IndxKey>> SBKT_MAP = new TreeMap<>();
      Map<IndxKey, SbktKey> MIN_SBKT_BY_INDEX = new TreeMap<>();
      P2<Integer, Integer> firstSbkt = readFromVrpTest(conn, VRP_TEST_SOURCE_MAP, SBKT_MAP, MIN_SBKT_BY_INDEX);
      int frstSbktFinalVrp = firstSbkt._1();
      int vFrstSbkt = firstSbkt._2();
      end = System.currentTimeMillis();
      logger.info("readFromVrpTest() "+(end-start));

      start=end;
      Map<LocationKey, Integer> EOH_BY_PRODUCT = readFromEoh(conn, product, EOH_NAME);
      end = System.currentTimeMillis();
      logger.info("readFromEoh() "+(end-start));

      start=end;
      Map<LocationIndxKey, Rcpt> RCPT_MAP = new HashMap<>();
      Map<IndxKey, List<Rcpt>> RCPT_MAP_BY_INDX = new HashMap<>();
      fillRcptMap(v_plancurrent, v_planend, LOCATION_SET, V_LT, EOH_BY_PRODUCT, TOH_INPUT, RCPT_MAP, RCPT_MAP_BY_INDX);
      end = System.currentTimeMillis();
      logger.info("fillRcptMap() "+(end-start));

      start=end;
      Map<IndxKey, Dc> DC_MAP = new HashMap<>();
      Map<SbktKey, List<Dc>> DC_MAP_BY_SBKT = new TreeMap<>();
      int Ttl_DC_Rcpt = fillDcMap(v_plancurrent, v_planend, EOH_BY_PRODUCT, VRP_TEST_SOURCE_MAP, MIN_SBKT_BY_INDEX, RCPT_MAP_BY_INDX, frstSbktFinalVrp, DC_MAP, DC_MAP_BY_SBKT);
      end = System.currentTimeMillis();
      logger.info("fillDcMap() "+(end-start));

      start=end;
      Map<LocationKey, Integer> INIT_MAX_CONS = calculateInitMaxCons(Ttl_DC_Rcpt, RCPT_MAP, EOH_BY_PRODUCT);
      end = System.currentTimeMillis();
      logger.info("calculateInitMaxCons() "+(end-start));

      start=end;
      finalCalculation(v_plancurrent, vFrstSbkt, SBKT_MAP, LOCATION_SET, INIT_MAX_CONS, EOH_BY_PRODUCT, DC_MAP, DC_MAP_BY_SBKT, RCPT_MAP);
      end = System.currentTimeMillis();
      logger.info("finalCalculation() "+(end-start));
      logger.error("Total execution time: "+(end-globalStart));


      start=end;
      for (Map.Entry<LocationIndxKey, Rcpt> rcptEntry : RCPT_MAP.entrySet()) {
        LocationIndxKey key = rcptEntry.getKey();
        Rcpt target = rcptEntry.getValue();

        Dc dcTarget = DC_MAP.getOrDefault(new IndxKey(key.getIndx()), Dc.Default);
        rs.addRow(
          key.getIndx(),
          key.getLocation().getValue(),
          target.getDcSbkt(),
          target.getUncNeedLt(),

          target.getTempBoh(),
          target.getTempNeed(),
          target.getTempRcpt(),
          target.getTempEoh(),
          target.getTempSlsu(),
          target.getTempCons(),

          target.getConsBohLt(),
          target.getConsNeedLt(),
          target.getConsRcptLt(),
          target.getConsAvlLt(),
          target.getMaxConsLt(),
          target.getPoAllocLt(),
          target.getUnAllocLt(),
          target.getConsEohLt(),
          target.getConsSlsuLt(),
          dcTarget.getDcRcpt(),
          dcTarget.getDcOhRsv(),
          dcTarget.getDcAta(),
          dcTarget.getaOut(),
          dcTarget.getDcRaw(),
          dcTarget.getOutbound(),
          dcTarget.getDcPoh(),
          dcTarget.getDeficit()
        );
      }
      end = System.currentTimeMillis();
      logger.info("Output() "+(end-start));
      logger.info("Successfully execute function.");
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    return rs;
  }

  private static SimpleResultSet createOutputResultSet() {
    SimpleResultSet rs = new SimpleResultSet();

    rs.addColumn("idx", Types.INTEGER, 10, 0);
    rs.addColumn("location", Types.VARCHAR, 8, 0);
    rs.addColumn("sbkt", Types.INTEGER, 10, 0);

    rs.addColumn("unc_need_lt", Types.INTEGER, 10, 0);

    rs.addColumn("temp_boh", Types.INTEGER, 10, 0);
    rs.addColumn("temp_need", Types.INTEGER, 10, 0);
    rs.addColumn("temp_rcpt", Types.INTEGER, 10, 0);
    rs.addColumn("temp_eoh", Types.INTEGER, 10, 0);
    rs.addColumn("temp_slsu", Types.INTEGER, 10, 0);
    rs.addColumn("temp_cons", Types.INTEGER, 10, 0);

    rs.addColumn("cons_boh_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_need_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_rcpt_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_avl_lt", Types.INTEGER, 10, 0);
    rs.addColumn("max_cons_lt", Types.INTEGER, 10, 0);
    rs.addColumn("po_alloc_lt", Types.INTEGER, 10, 0);
    rs.addColumn("unalloc_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_eoh_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_slsu_lt", Types.INTEGER, 10, 0);
    rs.addColumn("dc_rcpt", Types.INTEGER, 10, 0);
    rs.addColumn("dc_oh_rsv", Types.INTEGER, 10, 0);
    rs.addColumn("dc_ata", Types.INTEGER, 10, 0);
    rs.addColumn("a_out", Types.INTEGER, 10, 0);

    rs.addColumn("dc_raw", Types.INTEGER, 10, 0);
    rs.addColumn("outbound", Types.INTEGER, 10, 0);
    rs.addColumn("dc_poh", Types.INTEGER, 10, 0);
    rs.addColumn("deficit", Types.INTEGER, 10, 0);


    return rs;
  }

  public static int half_round(double v) {
    long rounded_number = Math.round(v);
    if (rounded_number == v + 0.5) rounded_number -= 1;
    return (int) rounded_number;
  }
}
