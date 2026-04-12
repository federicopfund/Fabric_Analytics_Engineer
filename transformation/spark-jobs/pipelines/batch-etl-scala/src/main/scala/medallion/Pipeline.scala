package medallion

import medallion.config.{DatalakeConfig, SparkFactory, TableRegistry}
import medallion.engine.{DagExecutor, DagTask}
import medallion.infra.HdfsManager
import medallion.workflow._
import org.apache.log4j.{Logger, PropertyConfigurator}
import scala.util.control.NonFatal

/**
 * Pipeline v4.0 — Orquestador declarativo basado en DAG.
 *
 * Arquitectura:
 *   El pipeline se define como un grafo acíclico dirigido (DAG) donde cada
 *   task declara sus dependencias. El DagExecutor resuelve el orden óptimo,
 *   paraleliza tasks sin dependencias pendientes, y maneja retry/checkpoint.
 *
 *   ETL → [QUALITY || LINEAGE || ANALYTICS] → HIVE → METRICS
 *
 * Mejoras v4 sobre v3:
 *   - Declarativo: ~20 líneas de definición de tasks vs ~190 imperativas
 *   - DagExecutor con errores tipados (Transient/Fatal/Skippable)
 *   - NonFatal guards (no captura OOM/StackOverflow)
 *   - Checkpoint con metadatos JSON
 *   - TableRegistry como fuente única de tablas
 *   - Tasks no-críticas (critical=false) no bloquean dependientes
 *
 * Ejecutar:
 *   sbt "runMain medallion.Pipeline"
 */
object Pipeline {

  private val logger = Logger.getLogger(getClass.getName)

  // ════════════════════════════════════════════════════════
  // Main
  // ════════════════════════════════════════════════════════

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))

    MetricsWorkflow.startPipeline()

    println("╔══════════════════════════════════════════════════════════════╗")
    println("║        PIPELINE ORCHESTRATOR v4.0 — Data Engineering        ║")
    println("║        Declarative DAG | Typed Errors | Checkpoint          ║")
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

    try {
      // ══════════════════════════════════════════════════════
      // DECLARACIÓN DEL DAG
      // Cada task declara sus dependencias; el DagExecutor
      // resuelve la ejecución óptima automáticamente.
      // ══════════════════════════════════════════════════════
      val totalTables = TableRegistry.totalTables

      val dagTasks = Seq(
        // WF1: ETL Pipeline (sin dependencias)
        DagTask.fromUnit("ETL", Set.empty, () => {
          MetricsWorkflow.startStage("ETL")
          EtlWorkflow.run(spark, config)
          MetricsWorkflow.endStage("ETL", totalTables)
        }, description = "RAW → Bronze → Silver → Gold"),

        // WF4: Data Quality (después de ETL, no-crítico)
        DagTask.fromUnit("QUALITY", Set("ETL"), () => {
          MetricsWorkflow.startStage("QUALITY")
          val bq = DataQualityWorkflow.validateLayer(spark, "BRONZE", config.bronzePath, TableRegistry.bronzeNames, "parquet")
          val sq = DataQualityWorkflow.validateLayer(spark, "SILVER", config.silverPath, TableRegistry.silverNames, "parquet")
          val gq = DataQualityWorkflow.validateLayer(spark, "GOLD", config.goldPath, TableRegistry.goldNames, "delta")
          DataQualityWorkflow.printConsolidatedReport(bq ++ sq ++ gq)
          MetricsWorkflow.endStage("QUALITY", totalTables)
        }, critical = false, description = "Validación de calidad por capa"),

        // WF5: Lineage (después de ETL, no-crítico)
        DagTask.fromUnit("LINEAGE", Set("ETL"), () => {
          MetricsWorkflow.startStage("LINEAGE")
          val bl = LineageWorkflow.captureLayerLineage(spark, "BRONZE", config.bronzePath, TableRegistry.bronzeNames, "parquet")
          val sl = LineageWorkflow.captureLayerLineage(spark, "SILVER", config.silverPath, TableRegistry.silverNames, "parquet")
          val gl = LineageWorkflow.captureLayerLineage(spark, "GOLD", config.goldPath, TableRegistry.goldNames, "delta")
          val all = bl ++ sl ++ gl
          LineageWorkflow.printLineageGraph(all)
          if (config.lineagePath.nonEmpty) LineageWorkflow.exportManifest(all, config.lineagePath)
          MetricsWorkflow.endStage("LINEAGE", totalTables)
        }, critical = false, description = "Captura de linaje de datos"),

        // WF2: BI Analytics (después de ETL, no-crítico)
        DagTask.fromUnit("ANALYTICS", Set("ETL"), () => {
          MetricsWorkflow.startStage("ANALYTICS")
          val chartsDir = if (config.chartsPath.nonEmpty) config.chartsPath
            else new java.io.File("./src/main/resources/analytics").getCanonicalPath
          AnalyticsWorkflow.run(spark, config.goldPath, chartsDir)
          MetricsWorkflow.endStage("ANALYTICS", 10)
        }, critical = false, description = "Generación de gráficos BI"),

        // WF3: Hive Audit (después de Quality+Lineage, solo si HDFS)
        DagTask(
          id = "HIVE_AUDIT",
          dependencies = Set("QUALITY", "LINEAGE"),
          execute = () => {
            if (config.hiveEnabled) {
              MetricsWorkflow.startStage("HIVE_AUDIT")
              val basePath = s"$hdfsUriRoot/hive/warehouse/datalake"
              HiveWorkflow.run(spark, basePath, hdfsUriRoot)
              MetricsWorkflow.endStage("HIVE_AUDIT", TableRegistry.goldNames.length + TableRegistry.silverNames.length)
            }
            Right(())
          },
          critical = false,
          description = "Verificación de tablas en catálogo Hive"
        ),

        // WF6: Metrics Report (barrera final — depende de todo)
        DagTask(
          id = "METRICS",
          dependencies = Set("QUALITY", "LINEAGE", "ANALYTICS", "HIVE_AUDIT"),
          execute = () => {
            MetricsWorkflow.generateReport()
            if (config.metricsPath.nonEmpty) MetricsWorkflow.exportMetrics(config.metricsPath)
            Right(())
          },
          description = "Reporte final de métricas"
        )
      )

      // ══════════════════════════════════════════════════════
      // EJECUCIÓN DEL DAG
      // ══════════════════════════════════════════════════════
      val executor = new DagExecutor(
        tasks = dagTasks,
        parallelism = 3,  // Quality || Lineage || Analytics en paralelo
        checkpointPath = config.checkpointPath
      )

      val results = executor.execute()

      // ══════════════════════════════════════════════════════
      // RESUMEN FINAL
      // ══════════════════════════════════════════════════════
      println("╔══════════════════════════════════════════════════════════════╗")
      println("║              ORCHESTRATOR v4.0 COMPLETADO                   ║")
      println("╠══════════════════════════════════════════════════════════════╣")
      println(s"║  Modo: ${if (useLocal) "LOCAL" else "HDFS + Hive"}")
      println(s"║  DAG: ${dagTasks.length} tasks, parallelism=3")
      println(s"║  Checkpoint: ${if (config.checkpointPath.nonEmpty) config.checkpointPath else "disabled"}")
      println(s"║  Tablas: Bronze=${TableRegistry.bronzeNames.length} Silver=${TableRegistry.silverNames.length} Gold=${TableRegistry.goldNames.length}")
      println("╚══════════════════════════════════════════════════════════════╝")

    } catch {
      case NonFatal(e) =>
        logger.error(s"Pipeline falló: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }
}
