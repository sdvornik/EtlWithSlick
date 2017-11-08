import Dependencies._

Revolver.settings

val projectName = "EtlWithSlick"

name := "EtlWithSlick"

scalaVersion in ThisBuild := "2.12.1"

scalacOptions in ThisBuild ++= Seq(
  "-unchecked"
  , "-deprecation"
  , "-encoding"
  , "utf8"
  , "-Ybreak-cycles"
)

organization in ThisBuild := "com.yahoo.sdvornik"

version in ThisBuild := "1.0"

test in assembly in ThisBuild := {}

lazy val main = (project in file("."))
  .settings(

    logLevel in assembly := Level.Debug,

    assemblyJarName in assembly := "common.jar",
	  
	  mainClass in assembly := Some("com.yahoo.sdvornik.EntryPoint"),
	  
		scalaSource in Compile := baseDirectory.value / "src/main/scala",

		scalaSource in Test := baseDirectory.value / "src/test/scala",

		javaSource in Compile := baseDirectory.value / "src/main/java",

		javaSource in Test := baseDirectory.value / "src/test/java",

    libraryDependencies ++= dependencies,

      assemblyMergeStrategy in assembly := {
        case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      }
  )




