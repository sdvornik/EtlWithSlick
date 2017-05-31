package com.yahoo.sdvornik.mem_tables

import com.yahoo.sdvornik.db.FieldName
import slick.jdbc.H2Profile.api._
import slick.sql.SqlProfile.ColumnOption.Nullable

class AttrTime(tag: Tag) extends Table[(String, String, Int)](tag, "ATTR_TIME") {
  def department: Rep[String] = column[String](FieldName.DEPARTMENT, O.SqlType("VARCHAR(8)"))
  def flrset: Rep[String] = column[String](FieldName.FLRSET, O.SqlType("VARCHAR(32)"))
  def week_indx: Rep[Int] = column[Int](FieldName.INDX)
  def *  = (department, flrset, week_indx)
}

class Bod(tag: Tag) extends Table[(String, String, Int)](tag, "BOD") {
  def department: Rep[String] = column[String](FieldName.DEPARTMENT, O.SqlType("VARCHAR(8)"))
  def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)"))
  def bod: Rep[Int] = column[Int](FieldName.BOD)
  def *  = (department, location, bod)
}

class ClStr(tag: Tag) extends Table[(String, String)](tag, "CL_STR") {
  def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)"))
  def strClimate: Rep[String] = column[String](FieldName.STRCLIMATE, O.SqlType("VARCHAR(8)"))
  def *  = (location, strClimate)
}

class DcAdj(tag: Tag) extends Table[(String, Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Double])](tag, "DC_ADJ") {
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def time_indx: Rep[Option[Int]] = column[Option[Int]]("TIME_INDX", Nullable)
  def user_vrp: Rep[Option[Int]] = column[Option[Int]]("USER_VRP", Nullable)
  def locked_qty: Rep[Option[Int]] = column[Option[Int]]("LOCKED_QTY", Nullable)
  def user_adj_qty: Rep[Option[Int]] = column[Option[Int]]("USER_ADJ_QTY", Nullable)
  def on_order_qty: Rep[Option[Int]] = column[Option[Int]]("ON_ORDER_QTY", Nullable)
  def oo_revision_qty: Rep[Option[Int]] = column[Option[Int]]("OO_REVISION_QTY", Nullable)
  def adj_cost: Rep[Option[Double]] = column[Option[Double]]("ADJ_COST", Nullable)

  def * =
    (product, time_indx, user_vrp, locked_qty, user_adj_qty, on_order_qty, oo_revision_qty, adj_cost)
}

class Department(tag: Tag) extends Table[(String, String)](tag, "DEPARTMENT") {
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def department: Rep[String] = column[String](FieldName.DEPARTMENT, O.SqlType("VARCHAR(8)"))
  def *  = (product, department)
}

class Eoh(tag: Tag) extends Table[(String, String, Int)](tag, "EOH") {
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)"))
  def eoh: Rep[Int] = column[Int](FieldName.EOH)
  def * = (product, location, eoh)
}

class Frontline(tag: Tag) extends Table[(String, String, String, String, String, String, Int, Int, Int, Int, Int)](tag, "FRONTLINE") {
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)"))
  def flrset: Rep[String] = column[String](FieldName.FLRSET, O.SqlType("VARCHAR(32)"))
  def grade: Rep[String] = column[String](FieldName.GRADE, O.SqlType("VARCHAR(8)"))
  def strclimate: Rep[String] = column[String](FieldName.STRCLIMATE, O.SqlType("VARCHAR(8)"))
  def validsizes: Rep[String] = column[String]("VALIDSIZES")
  def initrcptwk_indx: Rep[Int] = column[Int](FieldName.INITRCPTWK_INDX)
  def exitdate_indx: Rep[Int] = column[Int](FieldName.EXITDATE_INDX)
  def dbtwk_indx: Rep[Int] = column[Int](FieldName.DBTWK_INDX)
  def erlstmkdnwk_indx: Rep[Int] = column[Int](FieldName.ERLSTMKDNWK_INDX)
  def lastdcrcpt_indx: Rep[Int] = column[Int]("LASTDCRCPT_INDX")
  def *  =
    (product, location, flrset, grade, strclimate, validsizes, initrcptwk_indx, exitdate_indx, dbtwk_indx, erlstmkdnwk_indx, lastdcrcpt_indx)
}

class InvModel(tag: Tag) extends Table[(String, Int, Int, Long, Long, Int)](tag, "INV_MODEL") {
  def product: Rep[String] = column[String](FieldName.DEPARTMENT, O.SqlType("VARCHAR(8)"))
  def sizes: Rep[Int] = column[Int](FieldName.NUM_SIZES)
  def too: Rep[Int] = column[Int](FieldName.TOO)
  def aps_lower: Rep[Long] = column[Long](FieldName.APS_LOWER)
  def aps: Rep[Long] = column[Long](FieldName.APS)
  def woc: Rep[Int] = column[Int](FieldName.WOC)

  def *  = (product, sizes, too, aps_lower, aps, woc)
}

class StoreLookup(tag: Tag) extends Table[(String, Int, String, String)](tag, "STORE_LOOKUP") {
  def department: Rep[String] = column[String](FieldName.DEPARTMENT, O.SqlType("VARCHAR(8)"))
  def indx: Rep[Int] = column[Int](FieldName.INDX)
  def location: Rep[String] = column[String](FieldName.LOCATION, O.SqlType("VARCHAR(16)"))
  def grade: Rep[String] = column[String](FieldName.GRADE, O.SqlType("VARCHAR(8)"))
  def *  = (department, indx, location, grade)
}

class TimeIndx(tag: Tag) extends Table[(Int, String)](tag, "TIME_INDX") {
  def indx: Rep[Int] = column[Int](FieldName.INDX)
  def id: Rep[String] = column[String]("ID")
  def * = (indx, id)
}

class TimeStd(tag: Tag) extends Table[(Int, String)](tag, "TIME_STD") {
  def id_indx: Rep[Int] = column[Int](FieldName.INDX)
  def time: Rep[String] = column[String]("ID")
  def * = (id_indx, time)
}

class VrcptInt(tag: Tag) extends Table[(Int, String)](tag, "V_RCPT_INT") {
  def v_rcpt_int: Rep[Int] = column[Int]("V_RCPT_INT")
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def * = (v_rcpt_int, product)
}

class FrontSizes(tag: Tag) extends Table[(Int, String)](tag, "FRONT_SIZES") {
  def num_sizes: Rep[Int] = column[Int](FieldName.NUM_SIZES)
  def product: Rep[String] = column[String](FieldName.PRODUCT, O.SqlType("VARCHAR(16)"))
  def * = (num_sizes, product)
}



