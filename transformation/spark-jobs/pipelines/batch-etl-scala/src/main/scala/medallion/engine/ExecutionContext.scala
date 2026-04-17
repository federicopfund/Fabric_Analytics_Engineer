package medallion.engine

import medallion.config.{DatalakeConfig, IbmCloudConfig}
import medallion.config.IbmCloudConfig.{ExecutionMode, IbmAnalyticsEngine, HdfsCluster, LocalMode}
import org.apache.spark.sql.SparkSession

/**
 * ExecutionContext — Contenedor inmutable de todas las dependencias del pipeline.
 *
 * Reemplaza el estado global mutable disperso en Pipeline.main():
 *   - Antes: executionMode, config, spark declarados por separado y mutados con match
 *   - Ahora: un único objeto inmutable construido una vez y pasado a todos los workflows
 *
 * Ventajas:
 *   - Thread-safe por diseño: es un case class, no hay mutación posible
 *   - Testeable: se puede construir con mocks sin tocar env vars
 *   - Extensible: agregar un nuevo flag = agregar un campo a FeatureFlags
 *
 * Uso:
 *   val ctx = ContextBuilder.build()
 *   WorkflowRegistry.buildDag(ctx)   // cada workflow recibe ctx
 */
case class ExecutionContext(
  mode:       ExecutionMode,
  config:     DatalakeConfig,
  spark:      SparkSession,
  stateStore: PipelineStateStore,
  features:   FeatureFlags
) {
  /** Path de checkpoints resuelto según el modo */
  def checkpointBasePath: String = mode match {
    case IbmAnalyticsEngine => s"${config.goldPath}/../pipeline-state"
    case HdfsCluster        =>
      val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
      s"$hdfsUri/hive/warehouse/pipeline-state"
    case LocalMode          => config.checkpointPath
  }

  /** Indica si los checkpoints están en almacenamiento distribuido */
  def isDistributed: Boolean = mode == IbmAnalyticsEngine || mode == HdfsCluster

  override def toString: String =
    s"ExecutionContext(mode=$mode, features=$features, distributed=$isDistributed)"
}

/**
 * FeatureFlags — Controla el comportamiento del pipeline por ambiente.
 *
 * Leídos de variables de entorno con defaults seguros por ambiente.
 * Permite activar/desactivar features sin cambiar código ni hacer deploy.
 *
 * Variables de entorno:
 *   PIPELINE_CHARTS_ENABLED     → true/false  (default: true)
 *   PIPELINE_HIVE_ENABLED       → true/false  (default: false)
 *   PIPELINE_QUALITY_MIN_SCORE  → 0-100       (default: 70 dev, 80 staging, 90 prod)
 *   PIPELINE_NOTIFY_SLACK       → true/false  (default: false)
 *   PIPELINE_ENV                → dev/staging/prod (default: dev)
 */
case class FeatureFlags(
  chartsEnabled:    Boolean = true,
  hiveEnabled:      Boolean = false,
  qualityMinScore:  Double  = 70.0,
  notifySlack:      Boolean = false,
  environment:      String  = "dev"
) {
  /** En prod, el QualityGate es crítico y bloquea la exportación */
  def qualityGateIsCritical: Boolean = environment == "prod"

  /** En prod, el score mínimo es más exigente */
  def effectiveMinScore: Double = environment match {
    case "prod"    => math.max(qualityMinScore, 90.0)
    case "staging" => math.max(qualityMinScore, 80.0)
    case _         => qualityMinScore
  }

  override def toString: String =
    s"FeatureFlags(env=$environment, charts=$chartsEnabled, hive=$hiveEnabled, " +
    s"qualityMin=${effectiveMinScore}, notify=$notifySlack)"
}

object FeatureFlags {
  /** Parsea un String a Double de forma segura (Scala 2.12 no tiene toDoubleOption nativo) */
  private def parseDouble(s: String): Option[Double] =
    try Some(s.toDouble) catch { case _: NumberFormatException => None }

  /** Construye FeatureFlags desde variables de entorno */
  def fromEnv(): FeatureFlags = {
    val env       = sys.env.getOrElse("PIPELINE_ENV", "dev")
    val minScore  = parseDouble(sys.env.getOrElse("PIPELINE_QUALITY_MIN_SCORE",
      if (env == "prod") "90" else if (env == "staging") "80" else "70"
    )).getOrElse(70.0)

    FeatureFlags(
      chartsEnabled   = sys.env.getOrElse("PIPELINE_CHARTS_ENABLED", "true").trim.toLowerCase != "false",
      hiveEnabled     = sys.env.getOrElse("PIPELINE_HIVE_ENABLED",   "false").trim.toLowerCase == "true",
      qualityMinScore = minScore,
      notifySlack     = sys.env.getOrElse("PIPELINE_NOTIFY_SLACK",   "false").trim.toLowerCase == "true",
      environment     = env
    )
  }
}
