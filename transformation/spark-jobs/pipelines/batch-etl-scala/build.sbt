import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

val sparkVersion = "3.3.1"
val hadoopVersion = "3.3.4"
val hiveVersion   = "3.1.3"

lazy val commonSettings = Seq(
  organization := "Vortex",
  scalaVersion := "2.12.13",
  version := "1.0.0"
)

lazy val libraryDeps = Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  // Delta Lake
  "io.delta" %% "delta-core" % "2.2.0",

  // Hadoop Client
  "org.apache.hadoop" % "hadoop-client"  % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common"  % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs"    % hadoopVersion,

  // Hive (for metastore & querying)
  "org.apache.hive" % "hive-metastore" % hiveVersion excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j")
  ),
  "org.apache.hive" % "hive-exec" % hiveVersion classifier "core" excludeAll(
    ExclusionRule(organization = "org.apache.logging.log4j"),
    ExclusionRule(organization = "org.pentaho")
  ),

  // Logging
  "log4j" % "log4j" % "1.2.17",

  // Charting — JFreeChart para generación de gráficos PNG headless
  "org.jfree" % "jfreechart" % "1.5.4"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= libraryDeps,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)  => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
      case "reference.conf"                      => MergeStrategy.concat
      case x if x.endsWith(".proto")             => MergeStrategy.first
      case x => MergeStrategy.first
    }
  )
  .enablePlugins(AssemblyPlugin)
  .settings(
    Compile / run / fork := true,
    Compile / runMain / fork := true,
    javaOptions ++= Seq("-Xmx1g", "-Xms256m", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200", "-XX:+UseStringDeduplication"),
    run / outputStrategy := Some(StdoutOutput)
  )
