package com.yahoo.sdvornik.db;

import com.yahoo.sdvornik.db.dc.DcSource;
import com.yahoo.sdvornik.db.rcpt.RcptSource;
import com.yahoo.sdvornik.db.rcpt.RcptTarget;
import com.yahoo.sdvornik.db.toh_input.TohInputSource;
import com.yahoo.sdvornik.db.vrp_test.VrpTestSource;
import org.h2.tools.SimpleResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public final class Func {

  private final static Logger logger = LoggerFactory.getLogger(Func.class);

  private final static String LOCATION_NAME = "location";
  private final static String VALUE_NAME = "value";

  private final static String EOH_NAME = "EOH_BY_PRODUCT";
  private final static String GET_EOH = "SELECT " +
    LOCATION_NAME + ", " +
    VALUE_NAME +
    " FROM " + EOH_NAME;
  private final static Map<Location, Integer> EOH_BY_PRODUCT = new HashMap<>();

  private final static String V_LT_NAME = "V_LT";
  private final static String GET_V_LT = "SELECT " +
    LOCATION_NAME + ", " +
    VALUE_NAME +
    " FROM " + V_LT_NAME;
  private final static Map<Location, Integer> V_LT = new HashMap<>();

  private final static Map<IndxKey, VrpTestSource> VRP_TEST_SOURCE_MAP = new HashMap<>();

  private final static Map<LocationIndxKey, TohInputSource> TOH_INPUT_MAP = new HashMap<>();
  private final static Set<Location> TOH_INPUT_LOCATION_SET = new TreeSet<>();

  private final static Map<SbktKey, Set<IndxKey>> SBKT_MAP = new TreeMap<>();
  private final static Map<IndxKey, SbktKey> MIN_SBKT_BY_INDEX = new TreeMap<>();
  private static int frstSbktFinalVrp;
  private static int vFrstSbkt;

  private final static Map<LocationIndxKey, RcptSource> RCPT_SOURCE_MAP = new HashMap<>();
  private final static Map<IndxKey, List<RcptSource>> RCPT_SOURCE_MAP_BY_INDX = new HashMap<>();

  private final static Map<IndxKey, DcSource> DC_SOURCE_MAP = new HashMap<>();
  private final static Map<SbktKey, List<DcSource>> DC_SOURCE_MAP_BY_SBKT = new TreeMap<>();

  private Func() {}

  public static ResultSet final_uncons_mod(Connection conn, int v_plancurrent, int v_planend) throws SQLException {

    SimpleResultSet rs = createOutputResultSet();

    if (conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) return rs;

    try {
      Map<LocationIndxKey, RcptTarget> RCPT_TARGET_MAP = new HashMap<>();

      readFromEohTable(conn);

      readFromVLtTable(conn);

      readFromTohInputTable(conn);

      readFromVrpTestTable(conn);

      fillRcptSourceMap(v_plancurrent, v_planend);

      int Ttl_DC_Rcpt = fillDcSourceMap(v_plancurrent, v_planend);

      Map<Location, Integer> INIT_MAX_CONS = calculateInitMaxCons(Ttl_DC_Rcpt);


      for (Map.Entry<SbktKey, Set<IndxKey>> entry : SBKT_MAP.entrySet()) {

        SbktKey v_sbkt_id = entry.getKey();

        Set<IndxKey> INDX_SET = entry.getValue();

        int v_sbkt_start = -1;
        int temp_need_sbkt_dc = 0;
        Map<Location, Integer> tempNeedSbktByLocation = new HashMap<>();

        for (IndxKey indxKey : INDX_SET) {
          int t = indxKey.getValue();
          if (v_sbkt_start < 0) v_sbkt_start = t;
          for (Location str : TOH_INPUT_LOCATION_SET) {
            LocationIndxKey key = new LocationIndxKey(str, t);
            LocationIndxKey prevKey = new LocationIndxKey(str, t - 1);

            RcptSource RCPT_CUR = RCPT_SOURCE_MAP.getOrDefault(key, RcptSource.Default);
            RcptTarget RCPT_PREV = RCPT_TARGET_MAP.getOrDefault(prevKey, RcptTarget.Default);

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

            RcptTarget target = new RcptTarget(
              Temp_BOH,
              Temp_Need,
              Temp_Rcpt,
              Temp_EOH,
              Temp_SlsU,
              Temp_Cons,
              v_sbkt_id.getValue(),
              RCPT_CUR.getUncNeedLt(),
              RCPT_CUR.getConsEohLt()
            );
            RCPT_TARGET_MAP.put(key, target);
            temp_need_sbkt_dc += Temp_Need;
            int temp_need_sbkt = tempNeedSbktByLocation.getOrDefault(str, 0) + Temp_Need;
            tempNeedSbktByLocation.put(str, temp_need_sbkt);
          }
        }

        Map<Location, Double> ratioMap = new HashMap<>();
        for (Map.Entry<Location, Integer> temp_need_sbkt_entry : tempNeedSbktByLocation.entrySet()) {
          Double ratio = (temp_need_sbkt_dc == 0) ? 0.0 :
            temp_need_sbkt_entry.getValue() / Double.valueOf(temp_need_sbkt_dc);
          ratioMap.put(temp_need_sbkt_entry.getKey(), ratio);
        }

        tempNeedSbktByLocation.clear();

        List<DcSource> listBySbkt = DC_SOURCE_MAP_BY_SBKT.get(v_sbkt_id);
        int sum = 0;
        for (DcSource dc : listBySbkt) {
          sum += dc.getDcRaw();
        }
        DC_SOURCE_MAP_BY_SBKT.remove(v_sbkt_id);

        DcSource dcSource = DC_SOURCE_MAP.get(new IndxKey(v_sbkt_start));
        dcSource.setDcRcpt(sum);

        for (IndxKey indxKey : INDX_SET) {
          int t = indxKey.getValue();
          IndxKey prevIndxKey = new IndxKey(t - 1);

          DcSource DC_CUR = DC_SOURCE_MAP.getOrDefault(indxKey, DcSource.Default);
          DcSource DC_PREV = DC_SOURCE_MAP.getOrDefault(prevIndxKey, DcSource.Default);

          int DC_OH_Rsv = (t == v_plancurrent) ? DC_CUR.getDcPoh() :
            DC_PREV.getDcOhRsv() + DC_PREV.getDcRcpt() - DC_PREV.getaOut();

          int DC_ATA = (t == v_sbkt_start) ? DC_CUR.getDcRcpt() + DC_OH_Rsv : DC_OH_Rsv;

          int A_OUT = 0;

          for (Location str : TOH_INPUT_LOCATION_SET) { // TODO LOCATION_SET) {
            LocationIndxKey key = new LocationIndxKey(str, t);
            LocationIndxKey prevKey = new LocationIndxKey(str, t - 1);

            RcptSource RCPT_CUR = RCPT_SOURCE_MAP.getOrDefault(key, RcptSource.Default);
            RcptTarget RCPT_PREV = RCPT_TARGET_MAP.getOrDefault(prevKey, RcptTarget.Default);

            RcptTarget RCPT_TARGET = RCPT_TARGET_MAP.getOrDefault(key, RcptTarget.Default);

            int Cons_BOH_LT = (t == v_plancurrent) ? ((RCPT_CUR.getLeadTime() == 0) ?
              EOH_BY_PRODUCT.getOrDefault(str, 0) : RCPT_PREV.getConsEohLt()) :
              RCPT_PREV.getConsBohLt() + RCPT_PREV.getConsRcptLt() - RCPT_PREV.getConsSlsuLt();
            RCPT_TARGET.setConsBohLt(Cons_BOH_LT);


            int PO_Alloc_LT = (t == v_sbkt_start) ? half_round(DC_ATA * ratioMap.getOrDefault(str, 0.0)) : 0;
            RCPT_TARGET.setPoAllocLt(PO_Alloc_LT);

            int Cons_Need_LT = Math.max(0,RCPT_CUR.getTohLt() - Cons_BOH_LT);
            RCPT_TARGET.setConsNeedLt(Cons_Need_LT);

            int Max_Cons_LT = (t == vFrstSbkt) ? INIT_MAX_CONS.getOrDefault(str, 0) :
              Math.max(0, RCPT_PREV.getMaxConsLt() - RCPT_PREV.getConsRcptLt());
            RCPT_TARGET.setMaxConsLt(Max_Cons_LT);

            int Cons_Avl_LT = (t == v_sbkt_start) ? Math.min(Max_Cons_LT, PO_Alloc_LT) : RCPT_PREV.getUnAllocLt();
            RCPT_TARGET.setConsAvlLt(Cons_Avl_LT);

            int Cons_Rcpt_LT = Math.min(Cons_Need_LT, Cons_Avl_LT);
            RCPT_TARGET.setConsRcptLt(Cons_Rcpt_LT);

            int UnAlloc_LT = Math.max(0, Cons_Avl_LT - Cons_Rcpt_LT);
            RCPT_TARGET.setUnAllocLt(UnAlloc_LT);

            int Cons_SlsU_LT = Math.min(RCPT_CUR.getUncFcstLt(), Cons_BOH_LT + Cons_Rcpt_LT);
            RCPT_TARGET.setConsSlsuLt(Cons_SlsU_LT);

            int Cons_EOH_LT = Math.max(0, Cons_BOH_LT + Cons_Rcpt_LT - Cons_SlsU_LT);

            RCPT_TARGET.setConsEohLt(Cons_EOH_LT);

            A_OUT += Cons_Rcpt_LT;

          }
          DC_CUR.setaOut(A_OUT);
          DC_CUR.setDcAta(DC_ATA);
          DC_CUR.setDcOhRsv(DC_OH_Rsv);
        }
      }


      for (Map.Entry<LocationIndxKey, RcptTarget> rcptEntry : RCPT_TARGET_MAP.entrySet()) {
        LocationIndxKey key = rcptEntry.getKey();
        RcptTarget target = rcptEntry.getValue();

        DcSource dcTarget = DC_SOURCE_MAP.getOrDefault(new IndxKey(key.getIndx()), DcSource.Default);
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
          dcTarget.getaOut()
        );
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return rs;
  }

  private static Map<Location, Integer> calculateInitMaxCons(int Ttl_DC_Rcpt) {
    Map<Location, Integer> INIT_MAX_CONS = new HashMap<>();

    int Ttl_Str_Unc_Need = 0;
    Map<Location, Integer> Str_Unc_Need = new HashMap<>();
    for (Map.Entry<LocationIndxKey, RcptSource> rcptEntry : RCPT_SOURCE_MAP.entrySet()) {
      LocationIndxKey key = rcptEntry.getKey();
      Location locationKey = key.getLocation();
      RcptSource rcptSource = rcptEntry.getValue();

      int localStr_Unc_Need = Str_Unc_Need.getOrDefault(locationKey, 0);
      localStr_Unc_Need += rcptSource.getUncNeedLt();
      Str_Unc_Need.put(locationKey, localStr_Unc_Need);
      Ttl_Str_Unc_Need += rcptSource.getUncNeedLt();

    }

    int EOH_BY_PRODUCT_DC = EOH_BY_PRODUCT.getOrDefault("DC", 0);
    for (Map.Entry<Location, Integer> StrUncNeedEntry : Str_Unc_Need.entrySet()) {
      Location locationKey = StrUncNeedEntry.getKey();
      int StrUncNeedValue = StrUncNeedEntry.getValue();

      int value;
      if (Ttl_Str_Unc_Need == 0) value = 0;
      else value = half_round((Ttl_DC_Rcpt + EOH_BY_PRODUCT_DC) * StrUncNeedValue / (double) Ttl_Str_Unc_Need);

      INIT_MAX_CONS.put(locationKey, value);
    }
    return INIT_MAX_CONS;
  }

  private static int fillDcSourceMap(int v_plancurrent, int v_planend) {
    int EOH_BY_PRODUCT_DC = EOH_BY_PRODUCT.getOrDefault("DC", 0);

    int Ttl_DC_Rcpt = 0;
    for (int t = v_plancurrent; t < v_planend; ++t) {  //Change this to Max (Plan_Current, Debut_Week) to Min (Exit_Week, Plan_End)
      IndxKey indxKey = new IndxKey(t);
      IndxKey prevIndxKey = new IndxKey(t - 1);

      VrpTestSource VRP_TEST = VRP_TEST_SOURCE_MAP.getOrDefault(indxKey, VrpTestSource.Default);
      SbktKey sbktKey = MIN_SBKT_BY_INDEX.get(indxKey);

      List<RcptSource> rcptList = RCPT_SOURCE_MAP_BY_INDX.get(indxKey);
      int RCPT_CUR_Agg_Unc_Need_DC = 0;
      int RCPT_CUR_Agg_Unc_TOH_DC = 0;
      for (RcptSource rcpt : rcptList) {
        RCPT_CUR_Agg_Unc_Need_DC += rcpt.getUncNeedLt();
        RCPT_CUR_Agg_Unc_TOH_DC += rcpt.getTohLt();
      }
      rcptList.clear();


      DcSource DC_PREV = DC_SOURCE_MAP.getOrDefault(prevIndxKey, DcSource.Default);

      int DC_POH = (t == v_plancurrent) ? DC_POH = EOH_BY_PRODUCT_DC :
        DC_PREV.getDcPoh() + DC_PREV.getDcRaw() - DC_PREV.getOutbound();

      int Used_Need = (t == v_plancurrent) ? RCPT_CUR_Agg_Unc_Need_DC :
        Math.min(RCPT_CUR_Agg_Unc_TOH_DC, RCPT_CUR_Agg_Unc_Need_DC + DC_PREV.getDeficit());

      int DC_Raw = (t < frstSbktFinalVrp) ? 0 :
        (VRP_TEST.getCons() == 1) ? VRP_TEST.getFinalQty() : Math.max(0, Used_Need - DC_POH);
      Ttl_DC_Rcpt += DC_Raw;

      int Outbound = Math.min(Used_Need, DC_POH + DC_Raw);
      int Deficit = Math.max(0, Used_Need - Outbound);


      DcSource dcSource = new DcSource(
        DC_POH,
        DC_Raw,
        Outbound,
        sbktKey.getValue(),
        0,
        Deficit
      );

      DC_SOURCE_MAP.put(indxKey, dcSource);
      List<DcSource> list = DC_SOURCE_MAP_BY_SBKT.computeIfAbsent(
        sbktKey,
        key -> new ArrayList<DcSource>()
      );
      list.add(dcSource);
    }
    return Ttl_DC_Rcpt;
  }

  private static void fillRcptSourceMap(int v_plancurrent, int v_planend) {

    for(Location str : TOH_INPUT_LOCATION_SET ) {
      int vLtValue = V_LT.getOrDefault(str,0);
      int eohValue = EOH_BY_PRODUCT.getOrDefault(str, 0);

      for(int t = v_plancurrent; t <= v_planend; ++t) {  //Change this to Max (Plan_Current, Debut_Week) to Min (Exit_Week, Plan_End)
        LocationIndxKey key = new LocationIndxKey(str, t);
        LocationIndxKey prevKey = new LocationIndxKey(str, t-1);
        TohInputSource TOH_INPUT_CUR = TOH_INPUT_MAP.getOrDefault(key, TohInputSource.Default);
        RcptSource RCPT_PREV = RCPT_SOURCE_MAP.getOrDefault(prevKey, RcptSource.Default);


        int vUncFcst = TOH_INPUT_CUR.getUncFcst();
        int vToh = TOH_INPUT_CUR.getToh();
        int vUncBoh = (t == v_plancurrent) ? eohValue : Math.max(0, RCPT_PREV.getUncBoh() + RCPT_PREV.getUncNeed() - RCPT_PREV.getUncFcst());
        int vExistInv = (t == v_plancurrent) ? eohValue : Math.max(0, RCPT_PREV.getUncBoh() - RCPT_PREV.getUncFcst());
        int vUncNeed = (t < v_plancurrent + vLtValue) ? 0 : Math.max(0, vToh - vUncBoh);

        int vExistInvTemp = (t == v_plancurrent) ? eohValue : Math.max(0,RCPT_PREV.getExistInv() - RCPT_PREV.getExistSlsu());
        int vConsSlsu = (t < v_plancurrent + vLtValue) ? 0 : Math.min(vExistInvTemp, vUncFcst);
        int vConsEoh = (t < v_plancurrent + vLtValue) ? 0 : vExistInvTemp - vConsSlsu;
        int vExistSlsu = Math.min(vExistInv,vUncFcst);

        RcptSource rcpt = new RcptSource(vLtValue, vToh, vUncBoh, vUncNeed, vUncFcst, vExistInv, vExistSlsu, vConsSlsu, vConsEoh);
        RCPT_SOURCE_MAP.put(key, rcpt);

        List<RcptSource> list = RCPT_SOURCE_MAP_BY_INDX.computeIfAbsent(
          new IndxKey(t),
          a -> new ArrayList<>()
        );
        list.add(rcpt);

        LocationIndxKey laggedKey = new LocationIndxKey(str, t - vLtValue);
        RcptSource sourceLagged = RCPT_SOURCE_MAP.get(laggedKey);
        if(sourceLagged == null) {
          RcptSource rcptLagged = new RcptSource(vLtValue, vUncFcst, vToh, vUncNeed, vConsEoh);

          RCPT_SOURCE_MAP.put(
            laggedKey,
            rcptLagged
          );
          List<RcptSource> list2 = RCPT_SOURCE_MAP_BY_INDX.computeIfAbsent(
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
  }

  private static void readFromVrpTestTable(Connection conn) throws SQLException {
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


    frstSbktFinalVrp = FRST_SBKT_FINAL_VRP_SET.iterator().next().getValue();
    FRST_SBKT_FINAL_VRP_SET.clear();

    SbktKey minSbkt = SBKT_MAP.keySet().iterator().next();
    Set<IndxKey> indxKeySet = SBKT_MAP.get(minSbkt);

    vFrstSbkt = indxKeySet.iterator().next().getValue();
    logger.info("vFrstSbkt: " + vFrstSbkt);
    logger.info("frstSbktFinalVrp: " + frstSbktFinalVrp);
    logger.info("VRP_TEST size: " + VRP_TEST_SOURCE_MAP.size());
  }

  private static void readFromTohInputTable(Connection conn) throws SQLException {
    try(Statement st = conn.createStatement()) {
      ResultSet tohInputRs = st.executeQuery(TohInputSource.GET_TOH_INPUT_SOURCE);

      while (tohInputRs.next()) {
        LocationIndxKey key = new LocationIndxKey(tohInputRs);
        TohInputSource tohInputSource = new TohInputSource(tohInputRs);
        TOH_INPUT_MAP.put(key, tohInputSource);
        TOH_INPUT_LOCATION_SET.add(key.getLocation());
      }
    }
    logger.info("TOH_INPUT_MAP size: " + TOH_INPUT_MAP.size());
  }

  private static void readFromVLtTable(Connection conn) throws SQLException {
    try(Statement st = conn.createStatement()) {
      ResultSet vLtRs = st.executeQuery(GET_V_LT);

      while (vLtRs.next()) {
        Location loc = new Location(vLtRs.getString(LOCATION_NAME));
        Integer value = vLtRs.getInt(VALUE_NAME);
        V_LT.put(loc, value);
      }
    }
    logger.info("V_LT size: " + V_LT.size());
  }

  private static void readFromEohTable(Connection conn) throws SQLException {
    try(Statement st = conn.createStatement()) {
      ResultSet eohByProductRs = st.executeQuery(GET_EOH);
      while (eohByProductRs.next()) {
        Location loc = new Location(eohByProductRs.getString(LOCATION_NAME));
        Integer value = eohByProductRs.getInt(VALUE_NAME);
        EOH_BY_PRODUCT.put(loc, value);
      }
    }
    logger.info("EOH_BY_PRODUCT size: " + EOH_BY_PRODUCT.size());
  }

  public static int half_round(double v) {
    long rounded_number = Math.round(v);
    if (rounded_number == v + 0.5) rounded_number -= 1;
    return (int) rounded_number;
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

    return rs;
  }

}
