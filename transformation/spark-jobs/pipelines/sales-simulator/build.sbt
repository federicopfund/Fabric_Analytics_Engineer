import sbt._

val sparkVersion  = "3.3.1"
val hadoopVersion = "3.3.4"

lazy val root = (project in file("."))
  .settings(
    organization := "Vortex",
    name         := "sales-simulator",
    scalaVersion := "2.12.13",
    version      := "1.0.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "io.delta"         %% "delta-core" % "2.2.0"      % "provided",
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided",
      "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262" % "provided"
    ),

    assembly / assemblyJarName := "sales-simulator-assembly-1.0.0.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)          => MergeStrategy.concat
      case PathList("META-INF", _*)                      => MergeStrategy.discard
      case "reference.conf"                              => MergeStrategy.concat
      case _                                             => MergeStrategy.first
    },

    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.protobuf.**" -> "shaded.protobuf.@1").inAll
    ),

    javacOptions ++= Seq("-source", "11", "-target", "11"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
