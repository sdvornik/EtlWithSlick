package com.yahoo.sdvornik.db

import com.sun.rowset.CachedRowSetImpl
import java.sql._

import com.typesafe.scalalogging.Logger

import scala.io.Codec

object DBUtil {

  private val POSTGRES_DRIVER: String  = "org.postgresql.Driver"

  private val H2_DRIVER: String = "org.h2.Driver"

  private val log: Logger = Logger(getClass)

  def getSqlString(pathToFile: String ): Option[String] = {
    try {
      val codec = Codec.UTF8
      import scala.io.Source
      Some(Source.fromResource(pathToFile)).map(_.mkString)
    }
    catch {
      case e: Exception =>
        log.error("Can't read sql string", e)
        None
    }
  }

  def getPreparedStatement(optCon: Option[Connection], optSqlString: Option[String]): Option[PreparedStatement] = {
    optCon.flatMap(
      conn => optSqlString.flatMap(
        sqlString => try {
          Some(conn.prepareStatement(sqlString))
        }
        catch {
          case e: Exception =>
            log.error("Can't create PreparedStatement", e)
            None
        }
      )
    )
  }

  def closePreparedStatement(optPS: Option[PreparedStatement]) {
    optPS.foreach(ps =>
      try {
        ps.close()
      }
      catch {
        case e: Exception => log.error("Can't close PreparedStatement", e)
      }
    )
  }

  private def getConnection(driver:String) (str: String, user: String, pwd: String): Option[Connection] = {
    try{
      Class.forName(driver)
      Some(DriverManager.getConnection(str, user, pwd))
    }
    catch {
      case e: Exception =>
        log.error("Can't get connection",e)
        None
    }
  }

  def getPostgresConnection(str: String, user: String, pwd: String): Option[Connection] =
    getConnection(POSTGRES_DRIVER) (str, user, pwd)

  def getEmbeddedConnection(str: String, user: String, pwd: String): Option[Connection] =
    getConnection(H2_DRIVER) (str, user, pwd)

  def closeConnection(optConn: Option[Connection]) {
    try {
      optConn.filter(!_.isClosed).foreach(_.close)
    }
    catch {
      case e: Exception => log.error("Can't close connection",e)
    }
  }

  def execQuery(query: String,optConn: Option[Connection]): Option[ResultSet] = {
    try {
      optConn.map(_.createStatement()).map(
        statement => new CachedRowSetImpl()
          .populate(statement.executeQuery(query))
          .asInstanceOf[ResultSet]
      )
    }
    catch{
      case e: Exception =>
        log.error("Can't execute query",e)
        None
    }
  }

  def execUpdate(query: String, optConn: Option[Connection]): Option[Integer] = {
    try {
      optConn.map(_.createStatement()).map(_.executeUpdate(query))
    }
    catch {
      case e: Exception =>
        log.error("Can't execute update",e)
        None
    }
  }

  def toImmutableList[A](toInstance: ResultSet => Option[A]): ResultSet => List[A] =
    (resultSet: ResultSet) => new ResultSetIterable(resultSet, toInstance).toList.flatten[A]


}
