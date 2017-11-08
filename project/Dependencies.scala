import sbt._

object Dependencies {

  lazy val scalaTestVersion = "3.0.1"
  lazy val slf4jVersion = "1.7.25"
  lazy val h2DbVersion = "1.4.193"
  lazy val postgresDbVersion = "9.1-901-1.jdbc4"
  lazy val scalaLoggingVersion = "3.5.0"
  lazy val slickVersion = "3.2.0"

  val scalaTest: ModuleID = "org.scalatest" %% "scalatest" % scalaTestVersion

  val scalaLogging: ModuleID = "com.typesafe.scala-logging" % "scala-logging_2.12" % scalaLoggingVersion

  val h2Db: ModuleID = "com.h2database" % "h2" % h2DbVersion

  val postgresDb: ModuleID = "postgresql" % "postgresql" % postgresDbVersion

  val slick: ModuleID = "com.typesafe.slick" % "slick_2.12" % slickVersion

  val slickHikaricp: ModuleID = "com.typesafe.slick" %% "slick-hikaricp" % slickVersion

  val slf4jApi: ModuleID = "org.slf4j" % "slf4j-api" % slf4jVersion

  val slf4jImpl: ModuleID = "org.slf4j" % "slf4j-log4j12" % slf4jVersion

  val dependencies: Seq[ModuleID] = Seq(
    scalaTest % Test,
    scalaLogging,
    h2Db,
    postgresDb,
    slick,
    slickHikaricp,
    slf4jApi,
    slf4jImpl
  )
}
