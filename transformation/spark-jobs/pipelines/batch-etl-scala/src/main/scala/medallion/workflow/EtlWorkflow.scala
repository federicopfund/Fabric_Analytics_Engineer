package medallion.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import medallion.config.{DatalakeConfig, TableRegistry}
import medallion.infra.{DataLakeIO, HdfsManager}
import medallion.layer.{BronzeLayer, SilverLayer, GoldLayer}
import scala.util.control.NonFatal

/**
 * WORKFLOW 1: ETL Pipeline — RAW → BRONZE → SILVER → GOLD
 *
 * Orquesta las tres capas del medallion architecture con soporte
 * para Hive catalog registration y auditoría de esquemas.
 */
object EtlWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  // ── Inicializadores de entorno ──

  def setupHadoopEnvironment(hdfsUri: String, user: String = "fede"): Option[HdfsManager.HadoopConfig] = {
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   HADOOP ENVIRONMENT SETUP               ║")
    logger.info("╚══════════════════════════════════════════╝")
    if (!HdfsManager.isAvailable(hdfsUri)) { logger.warn("HDFS no disponible"); return None }
    val config = HdfsManager.HadoopConfig(hdfsUri = hdfsUri, user = user, replication = 1,
      blockSize = 134217728L, hiveWarehouse = "/hive/warehouse", hiveMetastoreUris = "thrift://localhost:9083")
    HdfsManager.buildHadoopConfiguration(config)
    println(HdfsManager.getConfigSummary(config))
    logger.info("▶ Creando estructura del Datalake en HDFS...")
    HdfsManager.createDatalakeStructure(hdfsUri)
    logger.info("▶ Validando estructura del Datalake...")
    if (!HdfsManager.validateDatalake(hdfsUri)) { logger.error("✗ Validación falló"); return None }
    logger.info("✔ Hadoop environment configurado correctamente")
    Some(config)
  }

  def ingestRawData(hdfsUri: String, localCsvPath: String): Int = {
    logger.info("▶ Ingesting raw data to HDFS...")
    val count = HdfsManager.uploadToRaw(hdfsUri, localCsvPath)
    logger.info(s"✔ $count archivos subidos a RAW")
    count
  }

  def initLocalDatalake(basePath: String, localCsvPath: String): DatalakeConfig = {
    val base = new java.io.File(basePath).getCanonicalPath
    val chartsDir = new java.io.File("./src/main/resources/analytics").getCanonicalPath
    val lineageDir = new java.io.File(s"$basePath/lineage").getCanonicalPath
    val metricsDir = new java.io.File(s"$basePath/metrics").getCanonicalPath
    val checkpointDir = new java.io.File(s"$basePath/.checkpoints").getCanonicalPath
    val config = DatalakeConfig(
      rawPath = s"$base/raw", bronzePath = s"$base/bronze",
      silverPath = s"$base/silver", goldPath = s"$base/gold",
      chartsPath = chartsDir, lineagePath = lineageDir, metricsPath = metricsDir,
      checkpointPath = checkpointDir
    )
    Seq(config.rawPath, config.bronzePath, config.silverPath, config.goldPath,
        config.lineagePath, config.metricsPath, config.checkpointPath).foreach(dir => new java.io.File(dir).mkdirs())

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

  def initHdfsDatalake(hdfsUri: String, hadoopConfig: Option[HdfsManager.HadoopConfig] = None): DatalakeConfig = {
    val base = s"$hdfsUri${HdfsManager.DATALAKE_BASE}"
    val chartsDir = new java.io.File("./src/main/resources/analytics").getCanonicalPath
    DatalakeConfig(
      rawPath = s"$base/raw", bronzePath = s"$base/bronze",
      silverPath = s"$base/silver", goldPath = s"$base/gold",
      chartsPath = chartsDir, lineagePath = s"$base/lineage", metricsPath = s"$base/metrics",
      checkpointPath = new java.io.File("./datalake/.checkpoints").getCanonicalPath,
      hadoopConfig = hadoopConfig, hiveEnabled = hadoopConfig.isDefined
    )
  }

  // ── Pipeline principal ──

  def run(spark: SparkSession, config: DatalakeConfig): Unit = {
    val startTime = System.currentTimeMillis()

    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║     DATA ENGINEERING PIPELINE v2.0       ║")
    logger.info("║     Lakehouse: HDFS + Hive + Delta Lake  ║")
    logger.info("╚══════════════════════════════════════════╝")
    println()

    // ── STAGE 0: HIVE SETUP ──
    if (config.hiveEnabled) {
      println("┌──────────────────────────────────────────┐")
      println("│  STAGE 0: HIVE — Metastore Registration  │")
      println("└──────────────────────────────────────────┘")
      setupHiveTables(spark, config)
      ensureHiveCatalog(spark, config)
      println()
    }

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

    // ── STAGE 4: HIVE CATALOG ──
    if (config.hiveEnabled) {
      println("┌──────────────────────────────────────────┐")
      println("│  STAGE 4: HIVE — Catalog Registration    │")
      println("└──────────────────────────────────────────┘")
      registerHiveCatalog(spark, config)
      println()
    }

    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    println("┌──────────────────────────────────────────┐")
    println("│  ETL PIPELINE COMPLETADO                 │")
    println("└──────────────────────────────────────────┘")
    println(s"  Tiempo ETL: ${elapsed}s")

    printDatalakeSummary(spark, config)
  }

  // ── Hive helpers ──

  private def setupHiveTables(spark: SparkSession, config: DatalakeConfig): Unit = {
    try {
      spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")
      spark.sql("USE lakehouse")
      logger.info("✔ Hive database 'lakehouse' creada/seleccionada")
    } catch { case NonFatal(e) => logger.warn(s"Hive metastore no disponible: ${e.getMessage}") }
  }

  private def registerHiveCatalog(spark: SparkSession, config: DatalakeConfig): Unit = {
    try {
      registerHiveTables(spark, config)
      logger.info("✔ Todas las tablas registradas en Hive Catalog")
      println("  ═══ HIVE CATALOG ═══")
      spark.sql("SHOW TABLES IN lakehouse").show(20, truncate = false)
    } catch { case NonFatal(e) => logger.warn(s"Error registrando en Hive: ${e.getMessage}") }
  }

  private def ensureHiveCatalog(spark: SparkSession, config: DatalakeConfig): Unit = {
    try {
      spark.sql("USE lakehouse")
      val existingTables = spark.sql("SHOW TABLES IN lakehouse").collect().map(_.getString(1)).toSet
      val expectedGold = TableRegistry.goldNames
      val expectedSilver = TableRegistry.silverNames.map(t => s"silver_$t")
      val allExpected = expectedGold ++ expectedSilver
      val missing = allExpected.filterNot(existingTables.contains)
      if (missing.isEmpty) {
        logger.info(s"  ✔ Hive catalog OK — ${existingTables.size} tablas registradas")
      } else {
        logger.info(s"  ⚠ Recuperando ${missing.size} tablas no registradas...")
        registerHiveTables(spark, config)
      }
    } catch { case NonFatal(e) => logger.warn(s"  Verificación Hive no disponible: ${e.getMessage}") }
  }

  private def registerHiveTables(spark: SparkSession, config: DatalakeConfig): Unit = {
    TableRegistry.goldNames.foreach { table =>
      val tablePath = s"${config.goldPath}/$table"
      try {
        if (DataLakeIO.pathExists(tablePath)) {
          spark.sql(s"CREATE TABLE IF NOT EXISTS lakehouse.$table USING DELTA LOCATION '$tablePath'")
          logger.info(s"  ✔ Hive: lakehouse.$table registrada")
        }
      } catch { case NonFatal(e) => logger.warn(s"  ✗ Hive: lakehouse.$table falló: ${e.getMessage}") }
    }
    TableRegistry.silverNames.foreach { table =>
      val tablePath = s"${config.silverPath}/$table"
      try {
        if (DataLakeIO.pathExists(tablePath)) {
          spark.sql(s"CREATE TABLE IF NOT EXISTS lakehouse.silver_$table USING PARQUET LOCATION '$tablePath'")
          logger.info(s"  ✔ Hive: lakehouse.silver_$table registrada")
        }
      } catch { case NonFatal(e) => logger.warn(s"  ✗ Hive: lakehouse.silver_$table falló: ${e.getMessage}") }
    }
  }

  // ── Summary ──

  private def printDatalakeSummary(spark: SparkSession, config: DatalakeConfig): Unit = {
    println()
    println("  ═══ DATALAKE SUMMARY ═══")
    val layers = Seq(("BRONZE", config.bronzePath, "parquet"), ("SILVER", config.silverPath, "parquet"), ("GOLD", config.goldPath, "delta"))
    layers.foreach { case (layer, path, format) =>
      try {
        if (path.startsWith("hdfs://")) {
          val conf = new org.apache.hadoop.conf.Configuration()
          conf.set("fs.defaultFS", path.split("/").take(3).mkString("/"))
          conf.set("dfs.client.use.datanode.hostname", "true")
          val fs = org.apache.hadoop.fs.FileSystem.get(conf)
          val p = new org.apache.hadoop.fs.Path(path)
          if (fs.exists(p)) {
            val tables = fs.listStatus(p).filter(_.isDirectory).map(_.getPath.getName).sorted
            println(s"  $layer (${tables.length} tablas — $format):")
            tables.foreach(t => println(s"    ├── $t"))
          }
        } else {
          val dir = new java.io.File(path)
          if (dir.exists()) {
            val tables = dir.listFiles().filter(_.isDirectory).map(_.getName).sorted
            println(s"  $layer (${tables.length} tablas — $format):")
            tables.foreach(t => println(s"    ├── $t"))
          }
        }
      } catch { case NonFatal(e) => println(s"  $layer — no disponible: ${e.getMessage}") }
    }
    println()
  }
}
