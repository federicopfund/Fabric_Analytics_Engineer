package medallion.infra

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import io.delta.tables.DeltaTable

/**
 * Utilidades de lectura/escritura para el Datalake.
 * Soporta CSV (ingesta), Parquet (Bronze/Silver) y Delta Lake (Gold).
 * Detecta automáticamente rutas locales vs HDFS.
 */
object DataLakeIO {

  private val logger = Logger.getLogger(getClass.getName)

  def readCsv(spark: SparkSession, basePath: String, fileName: String, schema: StructType): DataFrame = {
    val fullPath = s"$basePath/$fileName"
    logger.info(s"Leyendo: $fullPath")
    spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
      .schema(schema)
      .csv(fullPath)
  }

  def writeParquet(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    val outputPath = s"$basePath/$tableName"
    logger.info(s"Escribiendo Parquet: $outputPath")
    val writer = df.write.mode("overwrite")
    partitionCol match {
      case Some(col) => writer.partitionBy(col).parquet(outputPath)
      case None      => writer.parquet(outputPath)
    }
    logger.info(s"✔ $tableName escrito correctamente")
  }

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

  def writeDeltaOrParquet(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    try {
      writeDelta(df, basePath, tableName, partitionCol)
    } catch {
      case _: Exception =>
        logger.warn(s"Delta write falló para $tableName, usando Parquet fallback")
        writeParquet(df, basePath, tableName, partitionCol)
    }
  }

  /**
   * Incremental MERGE (upsert) en tabla Delta existente.
   * Si la tabla no existe, hace un write completo.
   *
   * @param df           DataFrame con datos nuevos/actualizados
   * @param basePath     Ruta base del datalake
   * @param tableName    Nombre de la tabla
   * @param mergeKeys    Columnas que forman la clave de merge (AND condition)
   * @param partitionCol Columna opcional de partición
   */
  def mergeDelta(
    df: DataFrame,
    basePath: String,
    tableName: String,
    mergeKeys: Seq[String],
    partitionCol: Option[String] = None
  ): Unit = {
    val outputPath = s"$basePath/$tableName"

    if (!pathExists(outputPath)) {
      logger.info(s"Delta MERGE: $tableName no existe, haciendo write completo")
      writeDelta(df, basePath, tableName, partitionCol)
      return
    }

    logger.info(s"Delta MERGE: $tableName — upsert por [${mergeKeys.mkString(", ")}]")

    val deltaTable = DeltaTable.forPath(outputPath)

    val mergeCondition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

    deltaTable.as("target")
      .merge(df.as("source"), mergeCondition)
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

    logger.info(s"✔ $tableName (Delta MERGE) completado")
  }

  /**
   * Ejecuta VACUUM en una tabla Delta para limpiar archivos obsoletos.
   * @param basePath      Ruta base del datalake
   * @param tableName     Nombre de la tabla
   * @param retentionHours Horas de retención (default 168 = 7 días)
   */
  def vacuumDelta(basePath: String, tableName: String, retentionHours: Double = 168.0): Unit = {
    val outputPath = s"$basePath/$tableName"
    if (!pathExists(outputPath)) {
      logger.warn(s"VACUUM: $tableName no existe, skip")
      return
    }

    logger.info(f"VACUUM: $tableName — retención ${retentionHours}%.0f horas")
    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.vacuum(retentionHours)
    logger.info(s"✔ $tableName (VACUUM) completado")
  }

  def pathExists(filePath: String): Boolean = {
    if (filePath.startsWith("hdfs://") || filePath.startsWith("s3a://") || filePath.startsWith("s3://")) {
      try {
        val uri = new java.net.URI(filePath)
        val conf = SparkSession.active.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(uri, conf)
        val p = new Path(filePath)
        if (!fs.exists(p)) return false
        if (fs.isFile(p)) return true
        fs.exists(new Path(filePath, "_SUCCESS")) ||
          fs.exists(new Path(filePath, "_delta_log"))
      } catch {
        case _: Exception => false
      }
    } else {
      val p = java.nio.file.Paths.get(filePath)
      if (!java.nio.file.Files.exists(p)) return false
      if (java.nio.file.Files.isRegularFile(p)) return true
      java.nio.file.Files.exists(p.resolve("_SUCCESS")) ||
        java.nio.file.Files.exists(p.resolve("_delta_log"))
    }
  }
}
