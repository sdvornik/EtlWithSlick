package com.yahoo.sdvornik.mem_tables


import slick.jdbc.H2Profile.api._

class AttrTime(tag: Tag) extends Table[(String, String, Int)](tag, "ATTR_TIME") {
  def department: Rep[String] = column[String]("DEPARTMENT")
  def flrset: Rep[String] = column[String]("FLRSET")
  def week_indx: Rep[Int] = column[Int]("WEEK_INDX")
  def *  = (department, flrset, week_indx)
}

class Bod(tag: Tag) extends Table[(String, String, Int)](tag, "BOD") {
  def department: Rep[String] = column[String]("DEPARTMENT")
  def location: Rep[String] = column[String]("LOCATION")
  def bod: Rep[Int] = column[Int]("BOD")
  def *  = (department, location, bod)
}

class ClStr(tag: Tag) extends Table[(String, String)](tag, "CL_STR") {
  def location: Rep[String] = column[String]("LOCATION")
  def strClimate: Rep[String] = column[String]("STR_CLIMATE")
  def *  = (location, strClimate)
}

class DcAdj(tag: Tag) extends Table[(String, Int, Int, Int, Int, Int, Int, Double)](tag, "DC_ADJ") {
  def product: Rep[String] = column[String]("PRODUCT")
  def time_indx: Rep[Int] = column[Int]("TIME_INDX")
  def user_vrp: Rep[Int] = column[Int]("USER_VRP")
  def locked_qty: Rep[Int] = column[Int]("LOCKED_QTY")
  def user_adj_qty: Rep[Int] = column[Int]("USER_ADJ_QTY")
  def on_order_qty: Rep[Int] = column[Int]("ON_ORDER_QTY")
  def oo_revision_qty: Rep[Int] = column[Int]("OO_REVISION_QTY")
  def adj_cost: Rep[Double] = column[Double]("ADJ_COST")

  def * =
    (product, time_indx, user_vrp, locked_qty, user_adj_qty, on_order_qty, oo_revision_qty, adj_cost)
}

class Department(tag: Tag) extends Table[(String, String)](tag, "DEPARTMENT") {
  def product: Rep[String] = column[String]("PRODUCT")
  def department: Rep[String] = column[String]("DEPARTMENT")
  def *  = (product, department)
}

class Eoh(tag: Tag) extends Table[(String, String, Int)](tag, "EOH") {
  def product: Rep[String] = column[String]("PRODUCT")
  def location: Rep[String] = column[String]("LOCATION")
  def eoh: Rep[Int] = column[Int]("EOH")
  def * = (product, location, eoh)
}

class Frontline(tag: Tag) extends Table[(String, String, String, String, String, Int, Int, Int, Int, String, Int)](tag, "FRONTLINE") {
  def product: Rep[String] = column[String]("PRODUCT")
  def location: Rep[String] = column[String]("LOCATION")
  def flrset: Rep[String] = column[String]("FLRSET")
  def grade: Rep[String] = column[String]("GRADE")
  def strclimate: Rep[String] = column[String]("STRCLIMATE")
  def initrcptwk_indx: Rep[Int] = column[Int]("INITRCPTWK_INDX")
  def exitdate_indx: Rep[Int] = column[Int]("EXITDATE_INDX")
  def dbtwk_indx: Rep[Int] = column[Int]("DBTWK_INDX")
  def erlstmkdnwk_indx: Rep[Int] = column[Int]("ERLSTMKDNWK_INDX")
  def validsizes: Rep[String] = column[String]("VALIDSIZES")
  def lastdcrcpt_indx: Rep[Int] = column[Int]("LASTDCRCPT_INDX")
  def *  =
    (product, location, flrset, grade, strclimate, initrcptwk_indx, exitdate_indx, dbtwk_indx, erlstmkdnwk_indx, validsizes, lastdcrcpt_indx)
}

class InvModel(tag: Tag) extends Table[(String, Int, Int, Long, Long, Int)](tag, "INV_MODEL") {
  def product: Rep[String] = column[String]("PRODUCT")
  def sizes: Rep[Int] = column[Int]("SIZES")
  def too: Rep[Int] = column[Int]("TOO")
  def aps_lower: Rep[Long] = column[Long]("APS_LOWER")
  def aps: Rep[Long] = column[Long]("APS")
  def woc: Rep[Int] = column[Int]("WOC")

  def *  = (product, sizes, too, aps_lower, aps, woc)
}

class StoreLookup(tag: Tag) extends Table[(String, String, String, String)](tag, "STORE_LOOKUP") {
  def department: Rep[String] = column[String]("DEPARTMENT")
  def time: Rep[String] = column[String]("TIME")
  def location: Rep[String] = column[String]("LOCATION")
  def grade: Rep[String] = column[String]("EOH")
  def *  = (department, time, location, grade)
}

class TimeIndx(tag: Tag) extends Table[(Int, String)](tag, "TIME_INDX") {
  def indx: Rep[Int] = column[Int]("INDX")
  def id: Rep[String] = column[String]("ID")
  def * = (indx, id)
}

class TimeStd(tag: Tag) extends Table[(Int, String)](tag, "TIME_STD") {
  def id_indx: Rep[Int] = column[Int]("INDX")
  def time: Rep[String] = column[String]("ID")
  def * = (id_indx, time)
}

class VrcptInt(tag: Tag) extends Table[(Int, String)](tag, "V_RCPT_INT") {
  def v_rcpt_int: Rep[Int] = column[Int]("V_RCPT_INT")
  def product: Rep[String] = column[String]("PRODUCT")
  def * = (v_rcpt_int, product)
}

class LocBaseFcst(tag: Tag) extends Table[(String, Int, Int)](tag, "LOC_BASE_FCST_") {
  def location: Rep[String] = column[String]("LOCATION")
  def week_indx: Rep[Int] = column[Int]("WEEK_INDX")
  def fcst: Rep[Int] = column[Int]("FCST")
  def *  = (location, week_indx, fcst)
}

