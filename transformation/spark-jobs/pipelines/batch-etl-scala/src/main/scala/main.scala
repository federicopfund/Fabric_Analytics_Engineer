package main

import scala.common.sparkSession.SparkSessionSingleton
import scala.ingest.hdfs
import scala.workflow.Workflow
import org.apache.log4j.PropertyConfigurator

object MainETL {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))

    val hdfsUriRoot = sys.env.getOrElse("HDFS_URI", "hdfs://localhost:9001")
    val localCsvPath = sys.env.getOrElse("CSV_PATH",
      new java.io.File("./src/main/resources/csv").getCanonicalPath)

    val useLocal = !hdfs.isAvailable(hdfsUriRoot)

    // Inicializar datalake según el entorno
    val config = if (useLocal) {
      println(">>> Modo LOCAL — HDFS no disponible")
      Workflow.initLocalDatalake("./datalake", localCsvPath)
    } else {
      println(">>> Modo HDFS")
      hdfs.createDatalakeStructure(hdfsUriRoot)
      hdfs.uploadToRaw(hdfsUriRoot, localCsvPath)
      Workflow.initHdfsDatalake(hdfsUriRoot)
    }

    // Crear SparkSession y ejecutar pipeline
    val spark = SparkSessionSingleton.getSparkSession(useLocal)

    Workflow.run(spark, config)

    spark.stop()
  }
}

