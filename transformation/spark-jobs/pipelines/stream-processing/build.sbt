name := "SparkKafkaStreamingApp"

version := "1.0"

scalaVersion := "2.11.12"  // Asegúrate de especificar la versión de Scala que estás utilizando

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-streaming" % "2.4.8",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.8",
  "org.apache.kafka" % "kafka-clients" % "2.0.0"
)

// Opcional: Configuración para el uso de log4j (puedes ajustar el nivel de registro según tus necesidades)
logLevel := Level.Warn

// Opcional: Configuración para el uso de la consola de Spark (puedes ajustar según tus necesidades)
javaOptions += "-Dspark.master=local[*]"

// Opcional: Configuración para el uso de Spark SQL (si es necesario)
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
