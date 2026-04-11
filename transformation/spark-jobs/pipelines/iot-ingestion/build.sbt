name := "DeviceDataProducer"

version := "1.0"

scalaVersion := "2.13.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.14.1"
)
