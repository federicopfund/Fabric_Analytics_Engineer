import sbt._

val sparkVersion = "3.3.1"
val hadoopVersion = "3.3.4"
val hiveVersion   = "3.1.3"

lazy val commonSettings = Seq(
  organization := "Vortex",
  scalaVersion := "2.12.13",
  version := "2.0.0"
)

lazy val libraryDeps = Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,

  // Delta Lake
  "io.delta" %% "delta-core" % "2.2.0",

  // Hadoop Client + S3A (IBM COS via S3A protocol)
  "org.apache.hadoop" % "hadoop-client"  % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common"  % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs"    % hadoopVersion,
  "org.apache.hadoop" % "hadoop-aws"     % hadoopVersion,
  "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262",

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
  "org.jfree" % "jfreechart" % "1.5.4",

  // JDBC — Db2 on Cloud driver para exportación Gold → Db2
  "com.ibm.db2" % "jcc" % "11.5.9.0",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
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
  .settings(
    Compile / run / fork := true,
    Compile / runMain / fork := true,
    javaOptions ++= Seq(
      "-Xmx1g", "-Xms256m", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200", "-XX:+UseStringDeduplication",
      // JDK 11+ compatibility — Spark 3.3.1 / Hadoop 3.3.4 / Hive 3.1.3
      "--add-opens", "java.base/java.lang=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens", "java.base/java.io=ALL-UNNAMED",
      "--add-opens", "java.base/java.net=ALL-UNNAMED",
      "--add-opens", "java.base/java.nio=ALL-UNNAMED",
      "--add-opens", "java.base/java.util=ALL-UNNAMED",
      "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens", "java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens", "java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    run / outputStrategy := Some(StdoutOutput)
  )
