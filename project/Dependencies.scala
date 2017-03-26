/*
 *  Copyright (c) Realine Technology, Inc. 2017
 */
import sbt._
object Dependencies {

  lazy val scalaTestVersion = "3.0.1"
  lazy val log4jScalaVersion = "2.8.1"
  lazy val h2DbVersion = "1.4.193"
  lazy val postgresDbVersion = "9.1-901-1.jdbc4"
  lazy val scalaLoggingVersion = "3.5.0"
  lazy val slickVersion = "3.2.0"
  lazy val jooqVersion = "3.9.1"
  lazy val slf4jNopVersion = "1.6.4"

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % scalaTestVersion

  val scalaLogging: ModuleID = "com.typesafe.scala-logging" % "scala-logging_2.12" % scalaLoggingVersion

  val h2Db: ModuleID = "com.h2database" % "h2" % h2DbVersion

  val postgresDb: ModuleID = "postgresql" % "postgresql" % postgresDbVersion

  val slick: ModuleID = "com.typesafe.slick" % "slick_2.12" % slickVersion

  val slf4jNop: ModuleID = "org.slf4j" % "slf4j-simple" % slf4jNopVersion

  val slickHikaricp: ModuleID = "com.typesafe.slick" %% "slick-hikaricp" % slickVersion

  //val jooq: ModuleID = "org.jooq" % "jooq" % jooqVersion

  val dependencies: Seq[ModuleID] = Seq(
    scalaTest % Test,
    scalaLogging,
    h2Db,
    postgresDb,
    slick,
    slf4jNop,
    slickHikaricp
  )
}
