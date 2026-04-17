package medallion.layer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger
import medallion.infra.DataLakeIO
import medallion.config.TableRegistry
import scala.util.control.NonFatal

/**
 * BRONZE LAYER — Ingesta y limpieza de datos crudos (RAW → Bronze).
 * Aplica schema enforcement, deduplicación por claves naturales,
 * filtrado de nulos y columnas de auditoría.
 *
 * Mejoras v4:
 *   - Iteración declarativa desde TableRegistry (sin hardcoded processTable calls)
 *   - Dead Letter Queue: filas rechazadas se escriben en _rejected/ para auditoría
 *   - NonFatal error handling
 */
object BronzeLayer {

  private val logger = Logger.getLogger(getClass.getName)

  def process(spark: SparkSession, rawPath: String, bronzePath: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  BRONZE LAYER — Data Cleansing")
    logger.info("═══════════════════════════════════════")

    TableRegistry.bronze.foreach { td =>
      (td.csvFileName, td.csvSchema) match {
        case (Some(fileName), Some(schema)) =>
          processTable(spark, rawPath, bronzePath, fileName, schema, td.deduplicateKeys)
        case _ =>
          logger.warn(s"⚠ Bronze/${td.name} — sin csvFileName o csvSchema en TableRegistry, skip")
      }
    }

    logger.info("✔ BRONZE LAYER completada")
  }

  private def processTable(
    spark: SparkSession,
    rawPath: String,
    bronzePath: String,
    fileName: String,
    schema: StructType,
    deduplicateKeys: Seq[String]
  ): Unit = {

    val tableName = fileName.stripSuffix(".csv").toLowerCase
    val outputPath = s"$bronzePath/$tableName"

    if (DataLakeIO.shouldSkip(outputPath)) {
      logger.info(s"⏭ Bronze/$tableName ya existe — skip")
      return
    }

    logger.info(s"▶ Procesando raw → bronze: $tableName")

    try {
      val dfRaw = DataLakeIO.readCsv(spark, rawPath, fileName, schema)
      val keyColumns = deduplicateKeys.map(col)
      val nullFilter = keyColumns.map(_.isNotNull).reduce(_ && _)

      // Separar filas válidas de rechazadas (Dead Letter Queue)
      val dfValid = dfRaw.filter(nullFilter)
      val dfRejected = dfRaw.filter(!nullFilter)

      // Escribir filas rechazadas si las hay
      val rejectedPath = s"$bronzePath/_rejected/$tableName"
      if (!dfRejected.head(1).isEmpty) {
        val dfRejectedAudit = dfRejected
          .withColumn("_rejected_at", current_timestamp())
          .withColumn("_rejected_reason", lit(s"null key: ${deduplicateKeys.mkString(",")}"))
          .withColumn("_source_file", lit(fileName))
        DataLakeIO.writeParquet(dfRejectedAudit, s"$bronzePath/_rejected", tableName)
        logger.warn(s"  ⚠ bronze/_rejected/$tableName — filas con claves nulas escritas en DLQ")
      }

      val dfDeduped = dfValid.dropDuplicates(deduplicateKeys)
      val dfBronze = dfDeduped
        .withColumn("_bronze_ingested_at", current_timestamp())
        .withColumn("_bronze_source_file", lit(fileName))

      DataLakeIO.writeParquet(dfBronze, bronzePath, tableName)
      println(s"  ✔ bronze/$tableName")
    } catch {
      case NonFatal(e) =>
        logger.error(s"  ✗ bronze/$tableName — error: ${e.getMessage}")
        throw e
    }
  }
}
