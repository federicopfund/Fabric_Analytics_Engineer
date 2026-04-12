package medallion

import medallion.config.{DatalakeConfig, SparkFactory}
import medallion.infra.HdfsManager
import medallion.workflow._
import org.apache.log4j.{Logger, PropertyConfigurator}
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.Executors

/**
 * Pipeline v3.0 — Orquestador con ejecución paralela de workflows.
 *
 * Arquitectura:
 *   1. WF1: ETL Pipeline         (secuencial — Bronze → Silver → Gold)
 *   2. WF4+WF5+WF2: Paralelos   (Quality || Lineage || Analytics) — son read-only
 *   3. WF3: Hive Audit           (secuencial — solo si HDFS)
 *   4. WF6: Metrics Report       (secuencial — barrera final)
 *
 * Features:
 *   - Retry con backoff exponencial por workflow
 *   - Checkpoint por stage para reanudación
 *   - Thread pool controlado para workflows paralelos
 *   - MetricsWorkflow thread-safe con ConcurrentHashMap
 *
 * Ejecutar:
 *   sbt "runMain medallion.Pipeline"
 */
object Pipeline {

  private val logger = Logger.getLogger(getClass.getName)

  private val BRONZE_TABLES = Seq("categoria", "subcategoria", "producto", "ventasinternet", "sucursales", "factmine", "mine")
  private val SILVER_TABLES = Seq("catalogo_productos", "ventas_enriquecidas", "resumen_ventas_mensuales",
    "rentabilidad_producto", "segmentacion_clientes", "produccion_operador", "eficiencia_minera", "produccion_por_pais")
  private val GOLD_TABLES = Seq("dim_producto", "dim_cliente", "fact_ventas", "kpi_ventas_mensuales",
    "dim_operador", "fact_produccion_minera", "kpi_mineria")

  private val MAX_RETRIES = 3
  private val PARALLEL_TIMEOUT = 10.minutes

  // ════════════════════════════════════════════════════════
  // Retry con backoff exponencial
  // ════════════════════════════════════════════════════════

  /**
   * Ejecuta un bloque con retry y backoff exponencial.
   * Si falla después de maxRetries, lanza la última excepción.
   * @param name      Nombre del workflow para logging
   * @param critical  Si es false, los fallos se loguean como warning sin re-throw
   * @param maxRetries Número máximo de reintentos
   * @param body      Bloque a ejecutar
   */
  private def withRetry[T](name: String, critical: Boolean = true, maxRetries: Int = MAX_RETRIES)(body: => T): T = {
    var lastError: Throwable = null
    var attempt = 0

    while (attempt < maxRetries) {
      try {
        return body
      } catch {
        case e: Throwable =>
          attempt += 1
          lastError = e
          if (attempt < maxRetries) {
            val backoffMs = 2000L * attempt
            logger.warn(s"⚠ $name — intento $attempt/$maxRetries falló: ${e.getMessage}. Retry en ${backoffMs}ms")
            Thread.sleep(backoffMs)
          }
      }
    }

    if (critical) {
      logger.error(s"✗ $name — falló después de $maxRetries intentos: ${lastError.getMessage}")
      throw lastError
    } else {
      logger.warn(s"⚠ $name — falló después de $maxRetries intentos (no crítico): ${lastError.getMessage}")
      null.asInstanceOf[T]
    }
  }

  // ════════════════════════════════════════════════════════
  // Checkpoint: skip stages ya completados
  // ════════════════════════════════════════════════════════

  private def isCheckpointed(checkpointPath: String, stage: String): Boolean = {
    if (checkpointPath.isEmpty) return false
    new java.io.File(s"$checkpointPath/.checkpoint_$stage").exists()
  }

  private def writeCheckpoint(checkpointPath: String, stage: String): Unit = {
    if (checkpointPath.isEmpty) return
    val dir = new java.io.File(checkpointPath)
    if (!dir.exists()) dir.mkdirs()
    new java.io.PrintWriter(s"$checkpointPath/.checkpoint_$stage").close()
    logger.info(s"  ✔ Checkpoint: $stage")
  }

  // ════════════════════════════════════════════════════════
  // Main
  // ════════════════════════════════════════════════════════

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))

    MetricsWorkflow.startPipeline()

    println("╔══════════════════════════════════════════════════════════════╗")
    println("║        PIPELINE ORCHESTRATOR v3.0 — Data Engineering        ║")
    println("║        Parallel Workflows | Retry | Checkpoint              ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    val hdfsUriRoot = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
    val localCsvPath = sys.env.getOrElse("CSV_PATH",
      new java.io.File("./src/main/resources/csv").getCanonicalPath)

    val useLocal = !HdfsManager.isAvailable(hdfsUriRoot)

    // ════════════════════════════════════════════════════════
    // SETUP: Inicializar datalake según entorno
    // ════════════════════════════════════════════════════════
    val config = if (useLocal) {
      println(">>> Modo LOCAL — HDFS no disponible")
      EtlWorkflow.initLocalDatalake("./datalake", localCsvPath)
    } else {
      println(">>> Modo HDFS + Hive Lakehouse")
      val hadoopConfig = EtlWorkflow.setupHadoopEnvironment(hdfsUriRoot)
      EtlWorkflow.ingestRawData(hdfsUriRoot, localCsvPath)
      EtlWorkflow.initHdfsDatalake(hdfsUriRoot, hadoopConfig)
    }

    val spark = SparkFactory.getOrCreate(useLocal)

    // Thread pool controlado para workflows parallel (2 threads)
    val threadPool = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

    try {
      val totalTables = BRONZE_TABLES.length + SILVER_TABLES.length + GOLD_TABLES.length

      // ══════════════════════════════════════════════════════
      // FASE 1: ETL Pipeline (secuencial — con retry)
      // ══════════════════════════════════════════════════════
      if (!isCheckpointed(config.checkpointPath, "ETL")) {
        println("┌──────────────────────────────────────────────────────┐")
        println("│  WORKFLOW 1: ETL Pipeline                            │")
        println("│  RAW → BRONZE → SILVER → GOLD                       │")
        println("└──────────────────────────────────────────────────────┘")

        MetricsWorkflow.startStage("ETL")
        withRetry("WF1:ETL") {
          EtlWorkflow.run(spark, config)
        }
        MetricsWorkflow.endStage("ETL", totalTables)
        writeCheckpoint(config.checkpointPath, "ETL")
      } else {
        println("  ⏭ WF1:ETL — checkpoint encontrado, skip")
      }

      // ══════════════════════════════════════════════════════
      // FASE 2: Workflows paralelos (Quality || Lineage || Analytics)
      // Todos son READ-ONLY sobre el datalake ya escrito
      // ══════════════════════════════════════════════════════
      println()
      println("┌──────────────────────────────────────────────────────┐")
      println("│  PARALLEL PHASE: Quality || Lineage || Analytics     │")
      println("│  3 workflows ejecutándose concurrentemente           │")
      println("└──────────────────────────────────────────────────────┘")

      // WF4: Data Quality (paralelo)
      val qualityFuture = Future {
        if (!isCheckpointed(config.checkpointPath, "QUALITY")) {
          MetricsWorkflow.startStage("QUALITY")
          withRetry("WF4:Quality", critical = false) {
            val bq = DataQualityWorkflow.validateLayer(spark, "BRONZE", config.bronzePath, BRONZE_TABLES, "parquet")
            val sq = DataQualityWorkflow.validateLayer(spark, "SILVER", config.silverPath, SILVER_TABLES, "parquet")
            val gq = DataQualityWorkflow.validateLayer(spark, "GOLD", config.goldPath, GOLD_TABLES, "delta")
            DataQualityWorkflow.printConsolidatedReport(bq ++ sq ++ gq)
          }
          MetricsWorkflow.endStage("QUALITY", totalTables)
          writeCheckpoint(config.checkpointPath, "QUALITY")
        } else {
          println("  ⏭ WF4:Quality — checkpoint encontrado, skip")
        }
      }

      // WF5: Lineage (paralelo)
      val lineageFuture = Future {
        if (!isCheckpointed(config.checkpointPath, "LINEAGE")) {
          MetricsWorkflow.startStage("LINEAGE")
          withRetry("WF5:Lineage", critical = false) {
            val bl = LineageWorkflow.captureLayerLineage(spark, "BRONZE", config.bronzePath, BRONZE_TABLES, "parquet")
            val sl = LineageWorkflow.captureLayerLineage(spark, "SILVER", config.silverPath, SILVER_TABLES, "parquet")
            val gl = LineageWorkflow.captureLayerLineage(spark, "GOLD", config.goldPath, GOLD_TABLES, "delta")
            val all = bl ++ sl ++ gl
            LineageWorkflow.printLineageGraph(all)
            if (config.lineagePath.nonEmpty) LineageWorkflow.exportManifest(all, config.lineagePath)
          }
          MetricsWorkflow.endStage("LINEAGE", totalTables)
          writeCheckpoint(config.checkpointPath, "LINEAGE")
        } else {
          println("  ⏭ WF5:Lineage — checkpoint encontrado, skip")
        }
      }

      // WF2: BI Analytics (paralelo)
      val analyticsFuture = Future {
        if (!isCheckpointed(config.checkpointPath, "ANALYTICS")) {
          MetricsWorkflow.startStage("ANALYTICS")
          withRetry("WF2:Analytics", critical = false) {
            val chartsDir = if (config.chartsPath.nonEmpty) config.chartsPath
              else new java.io.File("./src/main/resources/analytics").getCanonicalPath
            AnalyticsWorkflow.run(spark, config.goldPath, chartsDir)
          }
          MetricsWorkflow.endStage("ANALYTICS", 10)
          writeCheckpoint(config.checkpointPath, "ANALYTICS")
        } else {
          println("  ⏭ WF2:Analytics — checkpoint encontrado, skip")
        }
      }

      // Barrera: esperar a que los 3 workflows paralelos terminen
      val parallelPhase = Future.sequence(Seq(qualityFuture, lineageFuture, analyticsFuture))
      Await.result(parallelPhase, PARALLEL_TIMEOUT)

      println()
      println("  ✔ PARALLEL PHASE completada — Quality, Lineage, Analytics")
      println()

      // ══════════════════════════════════════════════════════
      // FASE 3: Hive Audit (secuencial — solo si HDFS)
      // ══════════════════════════════════════════════════════
      if (config.hiveEnabled && !isCheckpointed(config.checkpointPath, "HIVE_AUDIT")) {
        println("┌──────────────────────────────────────────────────────┐")
        println("│  WORKFLOW 3: Hive Audit                              │")
        println("│  Verificación de tablas Delta en catálogo Hive       │")
        println("└──────────────────────────────────────────────────────┘")
        MetricsWorkflow.startStage("HIVE_AUDIT")
        withRetry("WF3:HiveAudit", critical = false) {
          val basePath = s"$hdfsUriRoot/hive/warehouse/datalake"
          HiveWorkflow.run(spark, basePath, hdfsUriRoot)
        }
        MetricsWorkflow.endStage("HIVE_AUDIT", GOLD_TABLES.length + SILVER_TABLES.length)
        writeCheckpoint(config.checkpointPath, "HIVE_AUDIT")
      }

      // ══════════════════════════════════════════════════════
      // FASE 4: Metrics Report (barrera final)
      // ══════════════════════════════════════════════════════
      MetricsWorkflow.generateReport()
      if (config.metricsPath.nonEmpty) MetricsWorkflow.exportMetrics(config.metricsPath)

      // ══════════════════════════════════════════════════════
      // RESUMEN FINAL
      // ══════════════════════════════════════════════════════
      println("╔══════════════════════════════════════════════════════════════╗")
      println("║              ORCHESTRATOR v3.0 COMPLETADO                   ║")
      println("╠══════════════════════════════════════════════════════════════╣")
      println(s"║  Modo: ${if (useLocal) "LOCAL" else "HDFS + Hive"}")
      println(s"║  Parallel: Quality || Lineage || Analytics")
      println(s"║  Retry: ${MAX_RETRIES}x con backoff exponencial")
      println(s"║  Checkpoint: ${if (config.checkpointPath.nonEmpty) config.checkpointPath else "disabled"}")
      println(s"║  Workflows: ETL → [Quality||Lineage||Analytics] → ${if (config.hiveEnabled) "Hive → " else ""}Metrics")
      println(s"║  Tablas: Bronze=${BRONZE_TABLES.length} Silver=${SILVER_TABLES.length} Gold=${GOLD_TABLES.length}")
      println("╚══════════════════════════════════════════════════════════════╝")

    } finally {
      threadPool.shutdown()
      spark.stop()
    }
  }
}
