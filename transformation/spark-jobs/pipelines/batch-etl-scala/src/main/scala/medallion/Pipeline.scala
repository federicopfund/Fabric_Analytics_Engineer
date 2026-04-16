package medallion

import medallion.config.{DatalakeConfig, Db2Config, IbmCloudConfig, SparkFactory, TableRegistry}
import medallion.config.IbmCloudConfig.{ExecutionMode, IbmAnalyticsEngine, HdfsCluster, LocalMode}
import medallion.engine.{DagExecutor, DagTask, DistributedStateStore, LocalStateStore, PipelineStateStore}
import medallion.infra.HdfsManager
import medallion.workflow._
import org.apache.log4j.{Logger, PropertyConfigurator}
import scala.util.control.NonFatal

/**
 * Pipeline v6.0 — Orquestador declarativo con soporte IBM Cloud.
 *
 * Arquitectura:
 *   El pipeline se define como un grafo acíclico dirigido (DAG) donde cada
 *   task declara sus dependencias. El DagExecutor resuelve el orden óptimo,
 *   paraleliza tasks sin dependencias pendientes, y maneja retry/checkpoint.
 *
 *   COS_UPLOAD → ETL → [QUALITY || LINEAGE || ANALYTICS || DB2_EXPORT] → HIVE → METRICS
 *
 * Modos de ejecución (auto-detectados):
 *   1. IBM Analytics Engine Serverless (Spark 3.5 + IBM COS)
 *   2. HDFS + Hive Lakehouse
 *   3. Local filesystem
 *
 * Ejecutar:
 *   sbt "runMain medallion.Pipeline"                         # auto-detect
 *   AE_INSTANCE_ID=xxx AE_API_KEY=yyy sbt "runMain medallion.Pipeline"  # force AE
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
    println("║        PIPELINE ORCHESTRATOR v6.0 — Data Engineering        ║")
    println("║  Declarative DAG | COS Upload | ETL | Db2 Export | CI/CD    ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    val hdfsUriRoot = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
    val localCsvPath = sys.env.getOrElse("CSV_PATH",
      new java.io.File("./src/main/resources/csv").getCanonicalPath)

    // ════════════════════════════════════════════════════════
    // DETECCIÓN DE MODO: AE > HDFS > LOCAL
    // ════════════════════════════════════════════════════════
    val executionMode = IbmCloudConfig.resolveExecutionMode(hdfsUriRoot)

    println(s">>> Modo detectado: $executionMode")
    println()

    // ════════════════════════════════════════════════════════
    // SETUP: Inicializar datalake según entorno
    // ════════════════════════════════════════════════════════
    val config = executionMode match {
      case IbmAnalyticsEngine =>
        initCosDataLake(localCsvPath)

      case HdfsCluster =>
        println(">>> Modo HDFS + Hive Lakehouse")
        val hadoopConfig = EtlWorkflow.setupHadoopEnvironment(hdfsUriRoot)
        EtlWorkflow.ingestRawData(hdfsUriRoot, localCsvPath)
        EtlWorkflow.initHdfsDatalake(hdfsUriRoot, hadoopConfig)

      case LocalMode =>
        println(">>> Modo LOCAL — HDFS no disponible, AE no configurado")
        EtlWorkflow.initLocalDatalake("./datalake", localCsvPath)
    }

    val spark = SparkFactory.getOrCreate(executionMode)

    try {
      // ══════════════════════════════════════════════════════
      // DECLARACIÓN DEL DAG
      // ══════════════════════════════════════════════════════
      val totalTables = TableRegistry.totalTables

      val dagTasks = Seq(
        // WF0: COS Upload — Carga inicial de CSV locales al bucket raw
        // NOTA: En modo AE los archivos CSV deben subirse al bucket ANTES
        //       de ejecutar el pipeline (ver submit-to-ae.sh upload_csv_data).
        //       Este task solo aplica en modo local/dev con ibmcloud CLI disponible.
        DagTask.fromUnit("COS_UPLOAD", Set.empty, () => {
          if (config.cosEnabled && executionMode != IbmAnalyticsEngine) {
            MetricsWorkflow.startStage("COS_UPLOAD")
            val cos = IbmCloudConfig.loadCosConfig()
            val uploaded = CosUploadWorkflow.uploadMissingFiles(localCsvPath, cos.bucketRaw)
            println(s"  📦 COS Upload: $uploaded archivos nuevos subidos a ${cos.bucketRaw}")
            MetricsWorkflow.endStage("COS_UPLOAD", uploaded)
          } else if (executionMode == IbmAnalyticsEngine) {
            println("  ⏭ COS Upload: modo AE — datos pre-cargados via submit-to-ae.sh")
          } else {
            println("  ⏭ COS Upload: modo local — skip")
          }
        }, critical = false, description = "Carga inicial CSV → COS datalake-raw"),

        // WF1: ETL Pipeline (depende de COS_UPLOAD si COS está habilitado)
        DagTask.fromUnit("ETL", Set("COS_UPLOAD"), () => {
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

        // WF-DB2: Exportar Gold → Db2 on Cloud (después de ETL, no-crítico)
        DagTask.fromUnit("DB2_EXPORT", Set("ETL"), () => {
          if (config.db2Enabled) {
            MetricsWorkflow.startStage("DB2_EXPORT")
            val exported = Db2ExportWorkflow.run(spark, config)
            MetricsWorkflow.endStage("DB2_EXPORT", exported)
          } else {
            println("  ⏭ Db2 Export: no configurado — skip")
          }
        }, critical = false, description = "Gold → Db2 on Cloud (JDBC bulk insert)"),

        // WF3: Hive Audit (después de Quality+Lineage, solo si HDFS)
        DagTask(
          id = "HIVE_AUDIT",
          dependencies = Set("QUALITY", "LINEAGE"),
          execute = () => {
            if (config.hiveEnabled) {
              MetricsWorkflow.startStage("HIVE_AUDIT")
              val hdfsUriForHive = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
              val basePath = s"$hdfsUriForHive/hive/warehouse/datalake"
              HiveWorkflow.run(spark, basePath, hdfsUriForHive)
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
          dependencies = Set("QUALITY", "LINEAGE", "ANALYTICS", "DB2_EXPORT", "HIVE_AUDIT"),
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
      val stateStore: PipelineStateStore = executionMode match {
        case IbmAnalyticsEngine =>
          val cos = IbmCloudConfig.loadCosConfig()
          new DistributedStateStore(spark, s"${cos.goldBase}/pipeline-state")
        case HdfsCluster =>
          val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
          new DistributedStateStore(spark, s"$hdfsUri/hive/warehouse/pipeline-state")
        case LocalMode if config.checkpointPath.nonEmpty =>
          new LocalStateStore(config.checkpointPath)
        case _ =>
          new LocalStateStore(config.checkpointPath)
      }

      val executor = new DagExecutor(
        tasks = dagTasks,
        parallelism = 4,  // Quality || Lineage || Analytics || Db2Export en paralelo
        stateStore = stateStore
      )

      val results = executor.execute()

      // ══════════════════════════════════════════════════════
      // RESUMEN FINAL
      // ══════════════════════════════════════════════════════
      println("╔══════════════════════════════════════════════════════════════╗")
      println("║              ORCHESTRATOR v6.0 COMPLETADO                   ║")
      println("╠══════════════════════════════════════════════════════════════╣")
      println(s"║  Modo: $executionMode")
      println(s"║  DAG: ${dagTasks.length} tasks, parallelism=4")
      println(s"║  Checkpoint: ${stateStore.getClass.getSimpleName}")
      println(s"║  COS: ${if (config.cosEnabled) "✔ IBM Cloud Object Storage" else "✗"}")
      println(s"║  Db2: ${if (config.db2Enabled) "✔ Gold → Db2 on Cloud" else "✗ no configurado"}")
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

  /**
   * Inicializa DatalakeConfig para IBM COS (S3A).
   * Los datos RAW ya están en el bucket COS; no se copian localmente.
   */
  private def initCosDataLake(localCsvPath: String): DatalakeConfig = {
    val cos = IbmCloudConfig.loadCosConfig()
    println(s">>> IBM COS — RAW: ${cos.bucketRaw}  Bronze: ${cos.bucketBronze}")
    println(s">>> IBM COS — Silver: ${cos.bucketSilver}  Gold: ${cos.bucketGold}")
    println(s">>> IBM COS — Endpoint: ${cos.endpoint}")

    val db2 = Db2Config.fromEnv()
    if (db2.isDefined) println(s">>> Db2 on Cloud — Configurado (${db2.get.hostname})")
    else println(">>> Db2 on Cloud — No configurado (exportación deshabilitada)")

    // En AE Serverless, usar /tmp solo para escritura local efímera (charts)
    val chartsDir  = "/tmp/analytics-charts"
    new java.io.File(chartsDir).mkdirs()

    DatalakeConfig(
      rawPath        = cos.rawBase,
      bronzePath     = cos.bronzeBase,
      silverPath     = cos.silverBase,
      goldPath       = cos.goldBase,
      chartsPath     = chartsDir,
      lineagePath    = s"${cos.goldBase}/lineage",
      metricsPath    = s"${cos.goldBase}/metrics",
      checkpointPath = "", // checkpoints via DistributedStateStore en COS
      cosEnabled     = true,
      db2Enabled     = db2.isDefined,
      db2Config      = db2
    )
  }
}
