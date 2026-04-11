import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

val sparkVersion = "3.3.1"

lazy val commonSettings = Seq(
  organization := "Vortex",
  scalaVersion := "2.12.13",
  version := "0.0.1"
)

lazy val libraryDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql"  % sparkVersion ,
  "io.delta" %% "delta-core" % "2.2.0",
  "log4j" % "log4j" % "1.2.17"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    libraryDependencies ++= libraryDeps,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)  => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
  .enablePlugins(AssemblyPlugin)
