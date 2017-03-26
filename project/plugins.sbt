/*
 *  Copyright (c) Realine Technology, Inc. 2017
 */
resolvers ++= Seq(

  Resolver.mavenLocal,

  "JBoss" at "https://repository.jboss.org/",

  "Apache public" at "https://repository.apache.org/content/groups/public/",

  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",

  "Bintray sbt plugin releases" at "http://dl.bintray.com/sbt/sbt-plugin-releases/"
)

addSbtPlugin("io.spray" % "sbt-revolver" % "0.8.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.0-M8")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")


