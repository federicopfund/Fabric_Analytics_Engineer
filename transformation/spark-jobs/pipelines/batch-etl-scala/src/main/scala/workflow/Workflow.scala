package scala.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.common.DataLakeIO
import scala.bronze.BronzeLayer
import scala.silver.SilverLayer
import scala.gold.GoldLayer

object Workflow {

  private val logger = Logger.getLogger(getClass.getName)

  case class DatalakeConfig(
    rawPath: String,
    bronzePath: String,
    silverPath: String,
    goldPath: String
  )

  // ============================================================
  // INICIALIZAR ESTRUCTURA DEL DATALAKE (local)
  // ============================================================
  def initLocalDatalake(basePath: String, localCsvPath: String): DatalakeConfig = {
    val base = new java.io.File(basePath).getCanonicalPath
    val config = DatalakeConfig(
      rawPath    = s"$base/raw",
      bronzePath = s"$base/bronze",
      silverPath = s"$base/silver",
      goldPath   = s"$base/gold"
    )

    // Crear directorios
    Seq(config.rawPath, config.bronzePath, config.silverPath, config.goldPath).foreach { dir =>
      new java.io.File(dir).mkdirs()
    }

    // Copiar CSVs a raw si no existen
    val csvSource = new java.io.File(localCsvPath)
    if (csvSource.exists()) {
      val rawDir = new java.io.File(config.rawPath)
      csvSource.listFiles().filter(_.getName.endsWith(".csv")).foreach { f =>
        val dest = new java.io.File(rawDir, f.getName)
        if (!dest.exists()) {
          java.nio.file.Files.copy(f.toPath, dest.toPath)
          logger.info(s"  ✔ Copiado a raw: ${f.getName}")
        }
      }
    }

    config
  }

  // ============================================================
  // INICIALIZAR ESTRUCTURA DEL DATALAKE (HDFS)
  // ============================================================
  def initHdfsDatalake(hdfsUri: String): DatalakeConfig = {
    val base = s"$hdfsUri/hive/warehouse/datalake"
    DatalakeConfig(
      rawPath    = s"$base/raw",
      bronzePath = s"$base/bronze",
      silverPath = s"$base/silver",
      goldPath   = s"$base/gold"
    )
  }

  // ============================================================
  // EJECUTAR PIPELINE COMPLETO: RAW → BRONZE → SILVER → GOLD
  // ============================================================
  def run(spark: SparkSession, config: DatalakeConfig): Unit = {
    val startTime = System.currentTimeMillis()

    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║     DATA ENGINEERING PIPELINE v0.3       ║")
    logger.info("╚══════════════════════════════════════════╝")
    println()

    // ── STAGE 1: BRONZE ──
    println("┌──────────────────────────────────────────┐")
    println("│  STAGE 1: BRONZE — Data Cleansing        │")
    println("└──────────────────────────────────────────┘")
    BronzeLayer.process(spark, config.rawPath, config.bronzePath)
    println()

    // ── STAGE 2: SILVER ──
    println("┌──────────────────────────────────────────┐")
    println("│  STAGE 2: SILVER — Business Logic        │")
    println("└──────────────────────────────────────────┘")
    SilverLayer.process(spark, config.bronzePath, config.silverPath)
    println()

    // ── STAGE 3: GOLD ──
    println("┌──────────────────────────────────────────┐")
    println("│  STAGE 3: GOLD — BI & Analytics Models   │")
    println("└──────────────────────────────────────────┘")
    GoldLayer.process(spark, config.silverPath, config.goldPath)
    println()

    // ── RESUMEN ──
    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    println("┌──────────────────────────────────────────┐")
    println("│  PIPELINE COMPLETADO                     │")
    println("└──────────────────────────────────────────┘")
    println(s"  Tiempo total: ${elapsed}s")

    printDatalakeSummary(spark, config)
  }

  // ============================================================
  // RESUMEN DEL DATALAKE
  // ============================================================
  private def printDatalakeSummary(spark: SparkSession, config: DatalakeConfig): Unit = {
    println()
    println("  ═══ DATALAKE SUMMARY ═══")

    val layers = Seq(
      ("BRONZE",  config.bronzePath,  "parquet"),
      ("SILVER",  config.silverPath,  "parquet"),
      ("GOLD",    config.goldPath,    "delta")
    )

    layers.foreach { case (layer, path, format) =>
      val dir = new java.io.File(path)
      if (dir.exists()) {
        val tables = dir.listFiles().filter(_.isDirectory).map(_.getName).sorted
        println(s"  $layer (${tables.length} tablas — $format):")
        tables.foreach { t =>
          try {
            val count = spark.read.format(format).load(s"$path/$t").count()
            println(f"    ├── $t%-35s $count%,d filas")
          } catch {
            case _: Exception => println(s"    ├── $t (error al leer)")
          }
        }
      }
    }
    println()
  }
}
