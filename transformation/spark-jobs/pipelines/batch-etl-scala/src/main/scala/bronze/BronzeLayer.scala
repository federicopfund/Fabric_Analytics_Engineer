package scala.bronze

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger
import scala.common.{DataLakeIO, Schemas}

object BronzeLayer {

  private val logger = Logger.getLogger(getClass.getName)

  // ============================================================
  // ORQUESTADOR BRONZE — Procesa todos los archivos raw → bronze
  // ============================================================
  def process(spark: SparkSession, rawPath: String, bronzePath: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  BRONZE LAYER — Data Cleansing")
    logger.info("═══════════════════════════════════════")

    processTable(spark, rawPath, bronzePath, "Categoria.csv",       Schemas.categoriaSchema,       Seq("Cod_Categoria"))
    processTable(spark, rawPath, bronzePath, "Subcategoria.csv",    Schemas.subcategoriaSchema,    Seq("Cod_SubCategoria"))
    processTable(spark, rawPath, bronzePath, "Producto.csv",        Schemas.productoSchema,        Seq("Cod_Producto"))
    processTable(spark, rawPath, bronzePath, "VentasInternet.csv",  Schemas.ventasInternetSchema,  Seq("NumeroOrden", "Cod_Producto"))
    processTable(spark, rawPath, bronzePath, "Sucursales.csv",      Schemas.sucursalesSchema,      Seq("Cod_Sucursal"))
    processTable(spark, rawPath, bronzePath, "FactMine.csv",        Schemas.factMineSchema,        Seq("TruckID", "ProjectID", "Date"))
    processTable(spark, rawPath, bronzePath, "Mine.csv",            Schemas.mineSchema,            Seq("TruckID", "ProjectID", "OperatorID", "Date"))

    logger.info("✔ BRONZE LAYER completada")
  }

  // ============================================================
  // PROCESAMIENTO GENÉRICO POR TABLA
  // ============================================================
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

    // 1. Leer con schema explícito
    val dfRaw = DataLakeIO.readCsv(spark, rawPath, fileName, schema)

    // 2. Limpieza: eliminar filas donde TODAS las columnas clave sean null
    val keyColumns = deduplicateKeys.map(col)
    val dfCleaned = dfRaw.filter(keyColumns.map(_.isNotNull).reduce(_ && _))

    // 3. Deduplicar por claves naturales
    val dfDeduped = dfCleaned.dropDuplicates(deduplicateKeys)

    // 4. Agregar metadatos de auditoría
    val dfBronze = dfDeduped
      .withColumn("_bronze_ingested_at", current_timestamp())
      .withColumn("_bronze_source_file", lit(fileName))

    // 5. Escribir Parquet
    DataLakeIO.writeParquet(dfBronze, bronzePath, tableName)

    println(s"  ✔ bronze/$tableName: ${dfBronze.count()} filas")
  }
}
