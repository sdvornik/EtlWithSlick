package com.yahoo.sdvornik.db;


public final class DcRcptTarget {
  private final int cons_boh_lt;
  private final int cons_need_lt;
  private final int cons_rcpt_lt;
  private final int cons_avl_lt;
  private final int max_cons_lt;
  private final int po_alloc_lt;
  private final int unalloc_lt;
  private final int cons_eoh_lt;
  private final int cons_slsu_lt;

  private final int dc_oh_rsv;
  private final int dc_ata;
  private final int a_out;


  private static DcRcptTarget INSTANCE = new DcRcptTarget();

  public static DcRcptTarget getEmpty() {
    return INSTANCE;
  }

  private DcRcptTarget() {
    this.cons_boh_lt = 0;
    this.cons_need_lt = 0;
    this.cons_rcpt_lt = 0;
    this.cons_avl_lt = 0;
    this.max_cons_lt = 0;
    this.po_alloc_lt = 0;
    this.unalloc_lt = 0;
    this.cons_eoh_lt = 0;
    this.cons_slsu_lt = 0;
    this.dc_oh_rsv = 0;
    this.dc_ata = 0;
    this.a_out = 0;
  }

  public DcRcptTarget(
    int cons_boh_lt,
    int cons_need_lt,
    int cons_rcpt_lt,
    int cons_avl_lt,
    int max_cons_lt,
    int po_alloc_lt,
    int unalloc_lt,
    int cons_eoh_lt,
    int cons_slsu_lt,
    int dc_oh_rsv,
    int dc_ata,
    int a_out
  ) {
      this.cons_boh_lt = cons_boh_lt;
      this.cons_need_lt = cons_need_lt;
      this.cons_rcpt_lt = cons_rcpt_lt;
      this.cons_avl_lt = cons_avl_lt;
      this.max_cons_lt = max_cons_lt;
      this.po_alloc_lt = po_alloc_lt;
      this.unalloc_lt = unalloc_lt;
      this.cons_eoh_lt = cons_eoh_lt;
      this.cons_slsu_lt = cons_slsu_lt;
      this.dc_oh_rsv = dc_oh_rsv;
      this.dc_ata = dc_ata;
      this.a_out = a_out;

  }

  public int getCons_boh_lt() {
    return cons_boh_lt;
  }

  public int getCons_need_lt() {
    return cons_need_lt;
  }

  public int getCons_rcpt_lt() {
    return cons_rcpt_lt;
  }

  public int getCons_avl_lt() {
    return cons_avl_lt;
  }

  public int getMax_cons_lt() {
    return max_cons_lt;
  }

  public int getPo_alloc_lt() {
    return po_alloc_lt;
  }

  public int getUnalloc_lt() {
    return unalloc_lt;
  }

  public int getCons_eoh_lt() {
    return cons_eoh_lt;
  }

  public int getCons_slsu_lt() {
    return cons_slsu_lt;
  }

  public int getDc_oh_rsv() {
    return dc_oh_rsv;
  }

  public int getDc_ata() {
    return dc_ata;
  }

  public int getA_out() {
    return a_out;
  }
}
