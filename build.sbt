organization := "com.github.fujohnwang"

name := """lite-kafka"""

version := "1.0"

scalaVersion := "2.11.1"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

publishMavenStyle := true

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.3.5"

libraryDependencies += "ch.qos.logback" % "logback-core" % "1.1.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1.1" excludeAll(ExclusionRule(organization = "com.sun.jdmk"), ExclusionRule(organization = "com.sun.jmx"))

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.6" % "test"



