package medallion.layer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger
import medallion.infra.DataLakeIO
import medallion.schema.CsvSchemas

/**
 * BRONZE LAYER — Ingesta y limpieza de datos crudos (RAW → Bronze).
 * Aplica schema enforcement, deduplicación por claves naturales,
 * filtrado de nulos y columnas de auditoría.
 */
object BronzeLayer {

  private val logger = Logger.getLogger(getClass.getName)

  def process(spark: SparkSession, rawPath: String, bronzePath: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  BRONZE LAYER — Data Cleansing")
    logger.info("═══════════════════════════════════════")

    processTable(spark, rawPath, bronzePath, "Categoria.csv",       CsvSchemas.categoriaSchema,       Seq("Cod_Categoria"))
    processTable(spark, rawPath, bronzePath, "Subcategoria.csv",    CsvSchemas.subcategoriaSchema,    Seq("Cod_SubCategoria"))
    processTable(spark, rawPath, bronzePath, "Producto.csv",        CsvSchemas.productoSchema,        Seq("Cod_Producto"))
    processTable(spark, rawPath, bronzePath, "VentasInternet.csv",  CsvSchemas.ventasInternetSchema,  Seq("NumeroOrden", "Cod_Producto"))
    processTable(spark, rawPath, bronzePath, "Sucursales.csv",      CsvSchemas.sucursalesSchema,      Seq("Cod_Sucursal"))
    processTable(spark, rawPath, bronzePath, "FactMine.csv",        CsvSchemas.factMineSchema,        Seq("TruckID", "ProjectID", "Date"))
    processTable(spark, rawPath, bronzePath, "Mine.csv",            CsvSchemas.mineSchema,            Seq("TruckID", "ProjectID", "OperatorID", "Date"))

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

    if (DataLakeIO.pathExists(outputPath)) {
      logger.info(s"⏭ Bronze/$tableName ya existe — skip")
      return
    }

    logger.info(s"▶ Procesando raw → bronze: $tableName")

    val dfRaw = DataLakeIO.readCsv(spark, rawPath, fileName, schema)
    val keyColumns = deduplicateKeys.map(col)
    val dfCleaned = dfRaw.filter(keyColumns.map(_.isNotNull).reduce(_ && _))
    val dfDeduped = dfCleaned.dropDuplicates(deduplicateKeys)
    val dfBronze = dfDeduped
      .withColumn("_bronze_ingested_at", current_timestamp())
      .withColumn("_bronze_source_file", lit(fileName))

    DataLakeIO.writeParquet(dfBronze, bronzePath, tableName)
    println(s"  ✔ bronze/$tableName")
  }
}
