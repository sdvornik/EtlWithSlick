package com.yahoo.sdvornik.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import org.h2.tools.SimpleResultSet;

public class Functions {
  private final static String GET_V_FRST_SBKT = "SELECT MIN(indx) AS value FROM VRP_TEST JOIN ("+
    "SELECT min(sbkt) AS sbkt FROM vrp_test ) AS a ON vrp_test.sbkt = a.sbkt";

  private final static String GET_SOURCE = "SELECT * FROM DC_RCPT_SOURCE";

  private Functions(){}

  private final static Comparator<IdxSbktLocationKey> keyComparator = new Comparator<IdxSbktLocationKey>() {
    @Override
    public int compare(IdxSbktLocationKey o1, IdxSbktLocationKey o2) {
      if(o1.getIdx()-o2.getIdx()!=0) return o1.getIdx()-o2.getIdx();
      else if (!o1.getLocation().equals(o2.getLocation())) return o1.getLocation().compareTo(o2.getLocation());
      else return o1.getSbkt()-o2.getSbkt();
    }
  };

  public static ResultSet query(Connection conn, int v_plancurrent) throws SQLException {

    ResultSet frstSbktRs = conn.createStatement().executeQuery(GET_V_FRST_SBKT);
    frstSbktRs.next();
    int vFrstSbkt = frstSbktRs.getInt("value");
    frstSbktRs.close();

    ResultSet dcRcptSourceRs = conn.createStatement().executeQuery(GET_SOURCE);

    Map<IdxSbktLocationKey, DcRcptSource> sourceMap = new TreeMap<>(keyComparator);
    Map<IdxSbktLocationKey, DcRcptTarget> targetMap = new TreeMap<>(keyComparator);

    while (dcRcptSourceRs.next()) {
      int sbkt = dcRcptSourceRs.getInt("sbkt");
      int idx = dcRcptSourceRs.getInt("idx");
      String location = dcRcptSourceRs.getString("location");
      int lead_time = dcRcptSourceRs.getInt("lead_time");
		  int unc_fcst_lt = dcRcptSourceRs.getInt("unc_fcst_lt");
      int toh_lt = dcRcptSourceRs.getInt("toh_lt");
		  int dc_poh = dcRcptSourceRs.getInt("dc_poh");
		  int dc_rcpt = dcRcptSourceRs.getInt("dc_rcpt");
		  int eoh = dcRcptSourceRs.getInt("eoh");
		  int init_max_cons = dcRcptSourceRs.getInt("init_max_cons");
		  double ratio = dcRcptSourceRs.getDouble("ratio");
		  int v_sbkt_start = dcRcptSourceRs.getInt("v_sbkt_start");

      IdxSbktLocationKey key = new IdxSbktLocationKey(idx, sbkt, location);

		  DcRcptSource source = new DcRcptSource(
        sbkt,
        lead_time,
        unc_fcst_lt,
        toh_lt,
        dc_poh,
        dc_rcpt,
        eoh,
        init_max_cons,
        ratio,
        v_sbkt_start
      );
      sourceMap.put(key, source);
    }

    SimpleResultSet rs = new SimpleResultSet();
    rs.addColumn("idx", Types.INTEGER, 10, 0);
    rs.addColumn("location", Types.VARCHAR, 8, 0);
    rs.addColumn("cons_boh_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_need_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_rcpt_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_avl_lt", Types.INTEGER, 10, 0);
    rs.addColumn("max_cons_lt", Types.INTEGER, 10, 0);
    rs.addColumn("po_alloc_lt", Types.INTEGER, 10, 0);
    rs.addColumn("unalloc_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_eoh_lt", Types.INTEGER, 10, 0);
    rs.addColumn("cons_slsu_lt", Types.INTEGER, 10, 0);
    rs.addColumn("dc_oh_rsv", Types.INTEGER, 10, 0);
    rs.addColumn("dc_ata", Types.INTEGER, 10, 0);
    rs.addColumn("a_out", Types.INTEGER, 10, 0);



    int prevIdx=-1;
    int A_OUT = 0;
    for(Map.Entry<IdxSbktLocationKey, DcRcptSource> entry : sourceMap.entrySet()) {
      IdxSbktLocationKey key = entry.getKey();
      int t = key.getIdx();
      int sbkt = key.getSbkt();
      String location = key.getLocation();

      IdxSbktLocationKey prevKey = new IdxSbktLocationKey(t-1, sbkt, location);

      DcRcptSource source = entry.getValue();

      if(t!=prevIdx) A_OUT = 0;


      DcRcptSource prevSource = sourceMap.get(prevKey);
      if(prevSource == null) prevSource = DcRcptSource.getEmpty();

      DcRcptTarget prevTarget = targetMap.get(prevKey);
      if(prevTarget == null) prevTarget = DcRcptTarget.getEmpty();

      int DC_OH_Rsv;
      if(t == v_plancurrent) DC_OH_Rsv = source.getDc_poh();
      else DC_OH_Rsv = prevTarget.getDc_oh_rsv() + prevSource.getDc_rcpt() - prevTarget.getA_out();

      int DC_ATA;
      if(t == source.getV_sbkt_start()) DC_ATA =	source.getDc_rcpt() + DC_OH_Rsv;
      else DC_ATA =	DC_OH_Rsv;

      int Cons_BOH_LT;
      if(t == v_plancurrent) {
        if(source.getLead_time() == 0) Cons_BOH_LT = source.getEoh();
        else Cons_BOH_LT = prevTarget.getCons_eoh_lt();
      }
      else Cons_BOH_LT = prevTarget.getCons_boh_lt() + prevTarget.getCons_rcpt_lt() - prevTarget.getCons_slsu_lt();

      int PO_Alloc_LT = 0;
      if(t == source.getV_sbkt_start()) PO_Alloc_LT	= half_round(DC_ATA * source.getRatio());


      int Cons_Need_LT	= Math.max(0, source.getToh_lt() - Cons_BOH_LT);

      int Max_Cons_LT;
      if(t == vFrstSbkt) Max_Cons_LT = source.getInit_max_cons();
      else Max_Cons_LT = Math.max(0, prevTarget.getMax_cons_lt() - prevTarget.getCons_rcpt_lt());

      int Cons_Avl_LT;
      if(t == source.getV_sbkt_start()) Cons_Avl_LT = Math.min(Max_Cons_LT, PO_Alloc_LT);
      else Cons_Avl_LT = prevTarget.getUnalloc_lt();

      int Cons_Rcpt_LT	=  Math.min(Cons_Need_LT, Cons_Avl_LT);
      int UnAlloc_LT = Math.max(0,Cons_Avl_LT - Cons_Rcpt_LT);
      int Cons_SlsU_LT = Math.min(source.getUnc_fcst_lt(), Cons_BOH_LT + Cons_Rcpt_LT);
      int Cons_EOH_LT = Math.max(0, Cons_BOH_LT + Cons_Rcpt_LT - Cons_SlsU_LT);

      A_OUT += Cons_Rcpt_LT;
      //int A_OUTPUT = (prevIdx != t) ? A_OUT : -1;
      rs.addRow(
        entry.getKey().getIdx(),
        entry.getKey().getLocation(),
        Cons_BOH_LT,
        Cons_Need_LT,
        Cons_Rcpt_LT,
        Cons_Avl_LT,
        Max_Cons_LT,
        PO_Alloc_LT,
        UnAlloc_LT,
        Cons_EOH_LT,
        Cons_SlsU_LT,
        DC_OH_Rsv,
        DC_ATA,
        A_OUT
      );

      DcRcptTarget target = new DcRcptTarget(
        Cons_BOH_LT,
        Cons_Need_LT,
        Cons_Rcpt_LT,
        Cons_Avl_LT,
        Max_Cons_LT,
        PO_Alloc_LT,
        UnAlloc_LT,
        Cons_EOH_LT,
        Cons_SlsU_LT,
        DC_OH_Rsv,
        DC_ATA,
        A_OUT
      );

      targetMap.put(key, target);

      prevIdx = t;
    }

    return rs;

  }

  private static int half_round(double v) {
    long rounded_number = Math.round(v);
    if(rounded_number == v + 0.5) rounded_number -= 1;
    return (int)rounded_number;
  }
}

