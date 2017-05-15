package com.yahoo.sdvornik.db;

import org.h2.tools.SimpleResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public final class Func {
  private final static Logger logger = LoggerFactory.getLogger(Func.class);

  private final static String VRP_TEST_NAME = "VRP_TEST";
  private final static String INDX_NAME = "indx";
  private final static String SBKT_NAME="sbkt";
  private final static String GET_SBKT_SET = "SELECT DISTINCT "+
    SBKT_NAME+", "+
    "min("+INDX_NAME+") AS "+INDX_NAME+
    " FROM "+VRP_TEST_NAME+
    " GROUP BY "+ SBKT_NAME;

  private final static String GET_V_FRST_SBKT = "SELECT MIN(indx) AS value FROM VRP_TEST JOIN ("+
    "SELECT min(sbkt) AS sbkt FROM vrp_test ) AS a ON vrp_test.sbkt = a.sbkt";

  private final static String GET_INDX = "SELECT "+
    INDX_NAME+
    " FROM "+VRP_TEST_NAME+" where sbkt = ?";

  private final static String MAX_CONS_NAME = "MAX_CONS";
  private final static String LOCATION_NAME = "location";
  private final static String VALUE_NAME = "value";
  private final static String GET_INIT_MAX_CONS =  "SELECT "+
    LOCATION_NAME +", "+
    VALUE_NAME +
    " FROM "+MAX_CONS_NAME;

  private final static String EOH_NAME = "EOH_BY_PRODUCT";
  private final static String GET_EOH =  "SELECT "+
    LOCATION_NAME +", "+
    VALUE_NAME +
    " FROM "+EOH_NAME;


  private Func() {}

  public static ResultSet final_uncons_mod(Connection conn, int v_plancurrent) throws SQLException {
    //output
    SimpleResultSet rs = new SimpleResultSet();

    rs.addColumn("idx", Types.INTEGER, 10, 0);
    rs.addColumn("location", Types.VARCHAR, 8, 0);
    rs.addColumn("sbkt", Types.INTEGER, 10, 0);

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

    if(conn.getMetaData().getURL().equals("jdbc:columnlist:connection")) return rs;

    System.out.println("RUN "+conn.getMetaData().getURL());

    Map<LocationIndxKey, RcptTarget> RCPT_TARGET_MAP = new HashMap<>();
    Map<IndxKey, DcTarget> DC_TARGET_MAP = new HashMap<>();

    Statement st = conn.createStatement();
    ResultSet sbktSetRs = st.executeQuery(GET_SBKT_SET);
    Map<Integer,Integer> SBKT_MAP = new TreeMap<>();
    while(sbktSetRs.next()) {
      Integer sbkt = sbktSetRs.getInt(SBKT_NAME);
      Integer indx = sbktSetRs.getInt(INDX_NAME);
      SBKT_MAP.put(sbkt, indx);
    }
    st.close();
    logger.info("SBKT_MAP size: "+SBKT_MAP.size());


    st = conn.createStatement();
    ResultSet frstSbktRs = st.executeQuery(GET_V_FRST_SBKT);
    frstSbktRs.next();
    int vFrstSbkt = frstSbktRs.getInt("value");
    st.close();
    logger.info("vFrstSbkt: "+vFrstSbkt);

    st = conn.createStatement();
    ResultSet initMaxConsRs = st.executeQuery(GET_INIT_MAX_CONS);
    Map<Location, Integer> INIT_MAX_CONS = new HashMap<>();
    while(initMaxConsRs.next()) {
      Location loc = new Location(initMaxConsRs.getString(LOCATION_NAME));
      Integer value = initMaxConsRs.getInt(VALUE_NAME);
      INIT_MAX_CONS.put(loc, value);
    }
    st.close();
    logger.info("INIT_MAX_CONS size: "+INIT_MAX_CONS.size());

    st = conn.createStatement();
    ResultSet eohByProductRs = st.executeQuery(GET_EOH);
    Map<Location, Integer> EOH_BY_PRODUCT = new HashMap<>();
    while(eohByProductRs.next()) {
      Location loc = new Location(eohByProductRs.getString(LOCATION_NAME));
      Integer value = eohByProductRs.getInt(VALUE_NAME);
      EOH_BY_PRODUCT.put(loc, value);
    }
    st.close();
    logger.info("EOH_BY_PRODUCT size: "+EOH_BY_PRODUCT.size());

    st = conn.createStatement();
    ResultSet rcptSourceRs = st.executeQuery(RcptSource.GET_RCPT_SOURCE);
    Map<LocationIndxKey, RcptSource> RCPT_SOURCE_MAP = new HashMap<>();
    while(rcptSourceRs.next()) {
      LocationIndxKey key = new LocationIndxKey(rcptSourceRs);
      RcptSource rcpt = new RcptSource(rcptSourceRs);
      RCPT_SOURCE_MAP.put(key, rcpt);
    }
    st.close();
    logger.info("RCPT_SOURCE_MAP size: "+RCPT_SOURCE_MAP.size());

    st = conn.createStatement();
    ResultSet dcSourceRs = st.executeQuery(DcSource.GET_DC_SOURCE);
    Map<IndxKey, DcSource> DC_SOURCE_MAP = new HashMap<>();
    Map<Integer, Map<IndxKey, DcSource>> dcSourceMapBySbkt = new TreeMap<>();
    while(dcSourceRs.next()) {
      IndxKey key = new IndxKey(dcSourceRs);
      DcSource dc = new DcSource(dcSourceRs);
      DC_SOURCE_MAP.put(key, dc);

      Integer dc_sbkt = dc.getDcSbkt();
      Map<IndxKey, DcSource> map = dcSourceMapBySbkt.get(dc_sbkt);
      if(map == null) {
        map = new TreeMap<>();
        dcSourceMapBySbkt.put(dc_sbkt, map);
      }
      map.put(key, dc);
    }
    st.close();
    logger.info("DC_SOURCE_MAP size: "+RCPT_SOURCE_MAP.size());


    Set<Location> LOCATION_SET = new TreeSet<>();
    RCPT_SOURCE_MAP.keySet().forEach(
      key -> {
        LOCATION_SET.add(key.getLocation());
      }
    );
    logger.info("LOCATION_SET size: "+LOCATION_SET.size());
    try {
      SBKT_MAP.entrySet().forEach(
        entry -> {
          int v_sbkt_id = entry.getKey();
          int v_sbkt_start = entry.getValue();

          Set<Integer> INDX_SET = new TreeSet<>();
          try {
            PreparedStatement pSt = conn.prepareStatement(GET_INDX);
            pSt.setInt(1, v_sbkt_id);
            ResultSet indxRs = pSt.executeQuery();

            while (indxRs.next()) {
              Integer indx = indxRs.getInt(INDX_NAME);
              INDX_SET.add(indx);
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }

          int temp_need_sbkt_dc = 0;
          Map<Location, Integer> tempNeedSbktByLocation = new HashMap<>();

          for (int t : INDX_SET) {
            for (Location str : LOCATION_SET) {
              int Temp_BOH;
              int Temp_Cons;
              int Temp_Need;
              int Temp_Rcpt;
              int Temp_SlsU;
              int Temp_EOH;

              LocationIndxKey key = new LocationIndxKey(str, t);
              LocationIndxKey prevKey = new LocationIndxKey(str, t - 1);

              RcptSource RCPT_CUR = RCPT_SOURCE_MAP.getOrDefault(key, RcptSource.Default);
              RcptTarget RCPT_PREV = RCPT_TARGET_MAP.getOrDefault(prevKey, RcptTarget.Default);

              if (t == v_sbkt_start) {
                if (t == v_plancurrent && RCPT_CUR.getLeadTime() == 0) Temp_BOH = EOH_BY_PRODUCT.getOrDefault(str, 0);
                else Temp_BOH = RCPT_PREV.getConsEohLt();

                if (t == vFrstSbkt) Temp_Cons = INIT_MAX_CONS.getOrDefault(str, 0);
                else Temp_Cons = Math.max(0, RCPT_PREV.getMaxConsLt() - RCPT_PREV.getConsRcptLt());
              } else {
                Temp_BOH = RCPT_PREV.getTempEoh();
                Temp_Cons = Math.max(0, RCPT_PREV.getTempCons() - RCPT_PREV.getTempRcpt());
              }

              if (Temp_Cons > 0) Temp_Need = Math.max(0, RCPT_CUR.getTohLt() - Temp_BOH);
              else Temp_Need = 0;

              Temp_Rcpt = Temp_Need;
              Temp_SlsU = Math.min(RCPT_CUR.getUncFcstLt(), Temp_BOH + Temp_Rcpt);
              Temp_EOH = Temp_BOH + Temp_Rcpt - Temp_SlsU;

              RcptTarget target = new RcptTarget(
                Temp_BOH,
                Temp_Need,
                Temp_Rcpt,
                Temp_EOH,
                Temp_SlsU,
                Temp_Cons,
                v_sbkt_id,

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

          Map<IndxKey, DcSource> map = dcSourceMapBySbkt.get(v_sbkt_id);
          //TODO v_sbkt_start = minIndex
          int minIndex = Integer.MAX_VALUE;
          int sum = 0;
          for (Map.Entry<IndxKey, DcSource> dcSourceEntry : map.entrySet()) {
            IndxKey indxKey = dcSourceEntry.getKey();
            DcSource dc = dcSourceEntry.getValue();
            sum += dc.getDcRaw();
            if (indxKey.getValue() < minIndex) minIndex = indxKey.getValue();
          }
          dcSourceMapBySbkt.remove(v_sbkt_id);
          DcSource dcSource = DC_SOURCE_MAP.get(new IndxKey(minIndex));
          dcSource.setDcRcpt(sum);

          for (int t : INDX_SET) {
            IndxKey indxKey = new IndxKey(t);
            IndxKey prevIndxKey = new IndxKey(t - 1);

            DcSource DC_CUR = DC_SOURCE_MAP.getOrDefault(indxKey, DcSource.Default);
            DcTarget DC_PREV = DC_TARGET_MAP.getOrDefault(prevIndxKey, DcTarget.Default);

            int DC_OH_Rsv = (t == v_plancurrent) ? DC_CUR.getDcPoh() :
              DC_PREV.getDcOhRsv() + DC_PREV.getDcRcpt() - DC_PREV.getaOut();

            int DC_ATA = (t == v_sbkt_start) ? DC_CUR.getDcRcpt() + DC_OH_Rsv : DC_OH_Rsv;

            int A_OUT = 0;

            for (Location str : LOCATION_SET) {
              LocationIndxKey key = new LocationIndxKey(str, t);
              LocationIndxKey prevKey = new LocationIndxKey(str, t-1);

              RcptSource RCPT_CUR = RCPT_SOURCE_MAP.getOrDefault(key, RcptSource.Default);
              RcptTarget RCPT_PREV = RCPT_TARGET_MAP.getOrDefault(prevKey, RcptTarget.Default);

              RcptTarget RCPT_TARGET = RCPT_TARGET_MAP.getOrDefault(key, RcptTarget.Default);

              int Cons_BOH_LT = (t == v_plancurrent) ? ((RCPT_CUR.getLeadTime() == 0) ?
                EOH_BY_PRODUCT.getOrDefault(str, 0) : RCPT_PREV.getConsEohLt()) :
                RCPT_PREV.getConsBohLt() + RCPT_PREV.getConsRcptLt() - RCPT_PREV.getConsSlsuLt();
              RCPT_TARGET.setConsBohLt(Cons_BOH_LT);

              int PO_Alloc_LT = (t == v_sbkt_start) ? half_round(DC_ATA * ratioMap.getOrDefault(str, 0.0)) : 0;
              RCPT_TARGET.setPoAllocLt(PO_Alloc_LT);

              int Cons_Need_LT = Math.abs(RCPT_CUR.getTohLt() - Cons_BOH_LT);
              RCPT_TARGET.setConsNeedLt(Cons_Need_LT);

              int Max_Cons_LT = (t == vFrstSbkt) ? INIT_MAX_CONS.getOrDefault(str, 0) :
                Math.abs(RCPT_PREV.getMaxConsLt() - RCPT_PREV.getConsRcptLt());
              RCPT_TARGET.setMaxConsLt(Max_Cons_LT);

              int Cons_Avl_LT = (t == v_sbkt_start) ? Math.min(Max_Cons_LT, PO_Alloc_LT) : RCPT_PREV.getUnAllocLt();
              RCPT_TARGET.setConsAvlLt(Cons_Avl_LT);

              int Cons_Rcpt_LT = Math.min(Cons_Need_LT, Cons_Avl_LT);
              RCPT_TARGET.setConsRcptLt(Cons_Rcpt_LT);

              int UnAlloc_LT = Math.abs(Cons_Avl_LT - Cons_Rcpt_LT);
              RCPT_TARGET.setUnAllocLt(UnAlloc_LT);

              int Cons_SlsU_LT = Math.min(RCPT_CUR.getUncFcstLt(), Cons_BOH_LT + Cons_Rcpt_LT);
              RCPT_TARGET.setConsSlsuLt(Cons_SlsU_LT);

              int Cons_EOH_LT = Math.max(0, Cons_BOH_LT + Cons_Rcpt_LT - Cons_SlsU_LT);
              RCPT_TARGET.setConsEohLt(Cons_EOH_LT);

              A_OUT += Cons_Rcpt_LT;

            }
            DC_TARGET_MAP.put(indxKey, new DcTarget(
              DC_CUR.getDcRcpt(),
              DC_OH_Rsv,
              DC_ATA,
              A_OUT
            ));
          }
        }
      );
    }
    catch(Exception e) {
      e.printStackTrace();
    }

  try {
    for (Map.Entry<LocationIndxKey, RcptTarget> rcptEntry : RCPT_TARGET_MAP.entrySet()) {
      LocationIndxKey key = rcptEntry.getKey();
      RcptTarget target = rcptEntry.getValue();

      DcTarget dcTarget = DC_TARGET_MAP.getOrDefault(new IndxKey(key.getIndx()), DcTarget.Default);
      rs.addRow(
        key.getIndx(),
        key.getLocation().getValue(),

        target.getDcSbkt(),
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
  }
  catch(Exception e){
    e.printStackTrace();
  }
    return rs;
  }

  public static int half_round(double v) {
    long rounded_number = Math.round(v);
    if(rounded_number == v + 0.5) rounded_number -= 1;
    return (int)rounded_number;
  }

}
