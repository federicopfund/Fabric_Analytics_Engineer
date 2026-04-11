package scala.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

object DataLakeIO {

  private val logger = Logger.getLogger(getClass.getName)

  // ============================================================
  // READ — Lee CSV desde raw con schema explícito
  // ============================================================
  def readCsv(spark: SparkSession, basePath: String, fileName: String, schema: StructType): DataFrame = {
    val fullPath = s"$basePath/$fileName"
    logger.info(s"Leyendo: $fullPath")
    spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
      .schema(schema)
      .csv(fullPath)
  }

  // ============================================================
  // WRITE — Escribe Parquet en la capa indicada
  // ============================================================
  def writeParquet(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    val outputPath = s"$basePath/$tableName"
    logger.info(s"Escribiendo Parquet: $outputPath")

    val writer = df.write.mode("overwrite")
    partitionCol match {
      case Some(col) => writer.partitionBy(col).parquet(outputPath)
      case None      => writer.parquet(outputPath)
    }
    logger.info(s"✔ $tableName escrito correctamente (${df.count()} filas)")
  }

  // ============================================================
  // WRITE DELTA — Escribe Delta Lake en la capa Gold
  // ============================================================
  def writeDelta(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    val outputPath = s"$basePath/$tableName"
    logger.info(s"Escribiendo Delta: $outputPath")

    val writer = df.write.mode("overwrite").format("delta")
    partitionCol match {
      case Some(col) => writer.partitionBy(col).save(outputPath)
      case None      => writer.save(outputPath)
    }
    logger.info(s"✔ $tableName (Delta) escrito correctamente")
  }

  // ============================================================
  // PATH EXISTS — Verificar existencia en local o HDFS
  // ============================================================
  def pathExists(filePath: String): Boolean = {
    if (filePath.startsWith("hdfs://")) {
      try {
        val conf = new Configuration()
        conf.set("fs.defaultFS", filePath.split("/").take(3).mkString("/"))
        val fs = FileSystem.get(conf)
        fs.exists(new Path(filePath))
      } catch {
        case _: Exception => false
      }
    } else {
      java.nio.file.Files.exists(java.nio.file.Paths.get(filePath))
    }
  }
}
