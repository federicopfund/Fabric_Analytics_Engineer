package medallion.engine

import medallion.config.{DatalakeConfig, Db2Config, IbmCloudConfig, SparkFactory, TableRegistry}
import medallion.config.IbmCloudConfig.{IbmAnalyticsEngine, HdfsCluster, LocalMode}
import medallion.workflow.EtlWorkflow
import org.apache.log4j.Logger

/**
 * ContextBuilder — Construye el ExecutionContext completo.
 *
 * Encapsula todo el setup imperativo que actualmente vive en Pipeline.main():
 *   - Detección del modo de ejecución (AE > HDFS > LOCAL)
 *   - Inicialización del datalake según el modo
 *   - Construcción del SparkSession
 *   - Selección del PipelineStateStore correcto
 *   - Lectura de FeatureFlags desde env vars
 *
 * Antes (Pipeline.main() — ~60 líneas de setup):
 *   val executionMode = IbmCloudConfig.resolveExecutionMode(hdfsUriRoot)
 *   val config = executionMode match { case IbmAnalyticsEngine => ... case HdfsCluster => ... }
 *   val spark = SparkFactory.getOrCreate(executionMode)
 *   val stateStore = executionMode match { ... }
 *
 * Después (Pipeline.main() — 1 línea):
 *   val ctx = ContextBuilder.build()
 */
object ContextBuilder {

  private val logger = Logger.getLogger(getClass.getName)

  /**
   * Construye el ExecutionContext detectando el ambiente automáticamente.
   * Equivale a todo el bloque de setup de Pipeline.main().
   */
  def build(): ExecutionContext = {
    val hdfsUriRoot  = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
    val localCsvPath = sys.env.getOrElse("CSV_PATH",
      new java.io.File("./src/main/resources/csv").getCanonicalPath)

    val mode     = IbmCloudConfig.resolveExecutionMode(hdfsUriRoot)
    val features = FeatureFlags.fromEnv()
    val config   = buildDatalakeConfig(mode, hdfsUriRoot, localCsvPath, features)
    val spark    = SparkFactory.getOrCreate(mode)
    val store    = buildStateStore(mode, config, spark)

    logger.info(s"  ContextBuilder: $mode | $features")

    ExecutionContext(
      mode       = mode,
      config     = config,
      spark      = spark,
      stateStore = store,
      features   = features
    )
  }

  /**
   * Construye el DatalakeConfig según el modo.
   * Extrae la lógica de initCosDataLake / initHdfsDatalake / initLocalDatalake
   * que antes estaba inline en Pipeline.main().
   */
  private def buildDatalakeConfig(
    mode:         IbmCloudConfig.ExecutionMode,
    hdfsUriRoot:  String,
    localCsvPath: String,
    features:     FeatureFlags
  ): DatalakeConfig = mode match {

    case IbmAnalyticsEngine =>
      val cos = IbmCloudConfig.loadCosConfig()
      val db2 = Db2Config.fromEnv()
      logger.info(s"  COS — RAW: ${cos.bucketRaw}  Gold: ${cos.bucketGold}")
      if (db2.isDefined) logger.info(s"  Db2 — ${db2.get.hostname}")

      // En AE Serverless, charts se escriben en /tmp (efímero, no necesitan persistir)
      val chartsDir = "/tmp/analytics-charts"
      new java.io.File(chartsDir).mkdirs()

      DatalakeConfig(
        rawPath        = cos.rawBase,
        bronzePath     = cos.bronzeBase,
        silverPath     = cos.silverBase,
        goldPath       = cos.goldBase,
        chartsPath     = if (features.chartsEnabled) chartsDir else "",
        lineagePath    = s"${cos.goldBase}/lineage",
        metricsPath    = s"${cos.goldBase}/metrics",
        checkpointPath = "",          // checkpoints van al DistributedStateStore
        cosEnabled     = true,
        db2Enabled     = db2.isDefined,
        db2Config      = db2
      )

    case HdfsCluster =>
      logger.info("  Modo HDFS + Hive Lakehouse")
      val hadoopConfig = EtlWorkflow.setupHadoopEnvironment(hdfsUriRoot)
      EtlWorkflow.ingestRawData(hdfsUriRoot, localCsvPath)
      val config = EtlWorkflow.initHdfsDatalake(hdfsUriRoot, hadoopConfig)
      // Aplicar feature flags: deshabilitar charts si no están habilitados
      config.copy(
        chartsPath  = if (features.chartsEnabled) config.chartsPath else "",
        hiveEnabled = features.hiveEnabled || hadoopConfig.isDefined
      )

    case LocalMode =>
      logger.info("  Modo LOCAL — filesystem local")
      val config = EtlWorkflow.initLocalDatalake("./datalake", localCsvPath)
      config.copy(
        chartsPath = if (features.chartsEnabled) config.chartsPath else ""
      )
  }

  /**
   * Selecciona el PipelineStateStore correcto según el modo.
   * En AE e HDFS: DistributedStateStore (checkpoints persisten entre pods).
   * En local: LocalStateStore.
   */
  private def buildStateStore(
    mode:   IbmCloudConfig.ExecutionMode,
    config: DatalakeConfig,
    spark:  org.apache.spark.sql.SparkSession
  ): PipelineStateStore = mode match {

    case IbmAnalyticsEngine =>
      // Checkpoints en el bucket gold, subcarpeta pipeline-state
      val path = s"${config.goldPath}/pipeline-state"
      logger.info(s"  StateStore: DistributedStateStore → $path")
      new DistributedStateStore(spark, path)

    case HdfsCluster =>
      val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
      val path    = s"$hdfsUri/hive/warehouse/pipeline-state"
      logger.info(s"  StateStore: DistributedStateStore → $path")
      new DistributedStateStore(spark, path)

    case LocalMode =>
      val path = if (config.checkpointPath.nonEmpty) config.checkpointPath
                 else "./datalake/.pipeline-state"
      logger.info(s"  StateStore: LocalStateStore → $path")
      new LocalStateStore(path)
  }
}
