package medallion.engine

import medallion.config.{TableRegistry}
import medallion.config.IbmCloudConfig.IbmAnalyticsEngine
import medallion.workflow._
import org.apache.log4j.Logger

/**
 * Workflow — Interfaz que todo workflow del pipeline debe implementar.
 *
 * Cada workflow declara de forma explícita:
 *   - Su identidad (id único en el DAG)
 *   - Sus dependencias (IDs de workflows que deben completar primero)
 *   - Si es crítico (su fallo bloquea dependientes)
 *   - Si está habilitado según los FeatureFlags del ambiente
 *   - Cómo se convierte en un DagTask concreto dado un ExecutionContext
 *
 * Esto invierte el control: en lugar de que Pipeline.main() conozca cada
 * workflow y construya sus lambdas inline, cada workflow se auto-describe
 * y el WorkflowRegistry lo ensambla automáticamente.
 *
 * Antes:
 *   DagTask.fromUnit("QUALITY", Set("ETL"), () => { ... lógica inline ... }, critical = false)
 *
 * Ahora:
 *   object QualityWorkflow extends Workflow {
 *     val id           = "QUALITY"
 *     val dependencies = Set("ETL")
 *     val critical     = false
 *     def asDagTask(ctx: ExecutionContext): DagTask = ...
 *   }
 */
trait Workflow {
  /** Identificador único en el DAG — debe coincidir exactamente con las dependencias de otros workflows */
  def id: String

  /** IDs de los workflows que deben completarse antes de ejecutar éste */
  def dependencies: Set[String]

  /** Si true, un fallo Fatal bloquea todos los workflows dependientes */
  def critical: Boolean = true

  /** Número de reintentos ante errores Transient */
  def retryCount: Int = 3

  /**
   * Determina si este workflow debe ejecutarse dado el ambiente actual.
   * Por defecto siempre se ejecuta. Override para workflows condicionales.
   */
  def isEnabledFor(features: FeatureFlags): Boolean = true

  /** Convierte este workflow en un DagTask ejecutable */
  def asDagTask(ctx: ExecutionContext): DagTask
}

/**
 * WorkflowRegistry — Catálogo centralizado de todos los workflows del pipeline.
 *
 * Para agregar un nuevo workflow al pipeline:
 *   1. Crear el objeto que extiende Workflow
 *   2. Agregarlo al Seq `all` aquí
 *   3. Listo — Pipeline.main() no se toca
 *
 * El WorkflowRegistry también valida que el grafo sea coherente:
 *   - No hay dependencias rotas (un workflow declara dep a un id que no existe)
 *   - No hay duplicados de id
 */
object WorkflowRegistry {

  private val logger = Logger.getLogger(getClass.getName)

  /**
   * Registro completo de los 8 workflows del pipeline Medallion.
   * El orden en este Seq es irrelevante — el DagExecutor resuelve el orden topológico.
   */
  private val all: Seq[Workflow] = Seq(
    CosUploadWorkflowDef,
    EtlWorkflowDef,
    QualityWorkflowDef,
    LineageWorkflowDef,
    AnalyticsWorkflowDef,
    Db2ExportWorkflowDef,
    HiveAuditWorkflowDef,
    MetricsWorkflowDef
  )

  /**
   * Construye el Seq[DagTask] completo para el DagExecutor.
   * Filtra por FeatureFlags y convierte cada Workflow en su DagTask.
   */
  def buildDag(ctx: ExecutionContext): Seq[DagTask] = {
    validateRegistry()

    val enabled = all.filter(_.isEnabledFor(ctx.features))
    val skipped = all.filterNot(_.isEnabledFor(ctx.features))

    if (skipped.nonEmpty)
      logger.info(s"  WorkflowRegistry: skipped by feature flags: ${skipped.map(_.id).mkString(", ")}")

    logger.info(s"  WorkflowRegistry: building DAG with ${enabled.length} workflows: " +
      enabled.map(_.id).mkString(", "))

    enabled.map(_.asDagTask(ctx))
  }

  /** Valida que el registry no tenga ids duplicados ni dependencias rotas */
  private def validateRegistry(): Unit = {
    val ids = all.map(_.id)

    // Detectar duplicados
    val dups = ids.groupBy(identity).filter(_._2.size > 1).keys
    if (dups.nonEmpty)
      throw new IllegalStateException(s"WorkflowRegistry: IDs duplicados: ${dups.mkString(", ")}")

    // Detectar dependencias rotas
    val idSet = ids.toSet
    all.foreach { w =>
      w.dependencies.foreach { dep =>
        if (!idSet.contains(dep))
          throw new IllegalStateException(
            s"WorkflowRegistry: ${w.id} declara dependencia '$dep' que no existe en el registry")
      }
    }

    logger.info(s"  WorkflowRegistry: validated ${all.length} workflows, no issues")
  }

  /** Lista todos los workflows registrados con su estado de habilitación */
  def describe(features: FeatureFlags): String =
    all.map { w =>
      val enabled = if (w.isEnabledFor(features)) "✔" else "⏭"
      s"  $enabled ${w.id.padTo(15, ' ')} deps=[${w.dependencies.mkString(",")}] critical=${w.critical}"
    }.mkString("\n")
}

// ══════════════════════════════════════════════════════════════════════
// DEFINICIONES DE LOS 8 WORKFLOWS
// ══════════════════════════════════════════════════════════════════════

/** WF0: Carga de CSVs al bucket raw de COS. No-crítico, sin dependencias. */
private object CosUploadWorkflowDef extends Workflow {
  val id           = "COS_UPLOAD"
  val dependencies = Set.empty[String]
  override val critical   = false
  override val retryCount = 2

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      if (ctx.config.cosEnabled && ctx.mode != IbmAnalyticsEngine) {
        MetricsWorkflow.startStage(id)
        val cos      = medallion.config.IbmCloudConfig.loadCosConfig()
        val csvPath  = sys.env.getOrElse("CSV_PATH",
          new java.io.File("./src/main/resources/csv").getCanonicalPath)
        val uploaded = CosUploadWorkflow.uploadMissingFiles(csvPath, cos.bucketRaw)
        println(s"  📦 COS Upload: $uploaded archivos → ${cos.bucketRaw}")
        MetricsWorkflow.endStage(id, uploaded)
      } else {
        println(s"  ⏭ COS Upload: skip (modo=${ctx.mode})")
      }
    }, retryCount = retryCount, critical = critical,
       description = "Carga inicial CSV → COS datalake-raw")
}

/** WF1: Pipeline ETL principal RAW → Bronze → Silver → Gold. Crítico. */
private object EtlWorkflowDef extends Workflow {
  val id           = "ETL"
  val dependencies = Set("COS_UPLOAD")

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      MetricsWorkflow.startStage(id)
      EtlWorkflow.run(ctx.spark, ctx.config)
      MetricsWorkflow.endStage(id, TableRegistry.totalTables)
    }, retryCount = retryCount, description = "RAW → Bronze → Silver → Gold")
}

/**
 * WF4: Validación de calidad de datos.
 * critical depende del ambiente: en prod es crítico, en dev no.
 */
private object QualityWorkflowDef extends Workflow {
  val id           = "QUALITY"
  val dependencies = Set("ETL")

  // La criticidad se resuelve en tiempo de ejecución según el ambiente
  override def critical: Boolean = false  // override dinámico en asDagTask

  def asDagTask(ctx: ExecutionContext): DagTask = {
    val isCritical = ctx.features.qualityGateIsCritical
    DagTask(
      id           = id,
      dependencies = dependencies,
      critical     = isCritical,
      retryCount   = 1,
      description  = s"Quality gate (minScore=${ctx.features.effectiveMinScore}, critical=$isCritical)",
      execute      = () => {
        MetricsWorkflow.startStage(id)
        val bq = DataQualityWorkflow.validateLayer(ctx.spark, "BRONZE",
          ctx.config.bronzePath, TableRegistry.bronzeNames, "parquet")
        val sq = DataQualityWorkflow.validateLayer(ctx.spark, "SILVER",
          ctx.config.silverPath, TableRegistry.silverNames, "parquet")
        val gq = DataQualityWorkflow.validateLayer(ctx.spark, "GOLD",
          ctx.config.goldPath, TableRegistry.goldNames, "delta")
        val all = bq ++ sq ++ gq
        DataQualityWorkflow.printConsolidatedReport(all)
        MetricsWorkflow.endStage(id, TableRegistry.totalTables)

        // Quality Gate: en prod, score bajo el umbral → Fatal (bloquea EXPORT)
        if (all.nonEmpty) {
          val globalScore = all.map(_.qualityScore).sum / all.length
          val minScore    = ctx.features.effectiveMinScore
          if (globalScore < minScore && isCritical) {
            Left(TaskError.fatal(
              s"Quality gate failed: score=$globalScore < minScore=$minScore (env=${ctx.features.environment})",
              new RuntimeException(s"Quality SLA violation: $globalScore < $minScore")
            ))
          } else Right(())
        } else Right(())
      }
    )
  }
}

/** WF5: Captura de linaje de datos RAW → Gold. No-crítico. */
private object LineageWorkflowDef extends Workflow {
  val id           = "LINEAGE"
  val dependencies = Set("ETL")
  override val critical = false

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      MetricsWorkflow.startStage(id)
      val bl  = LineageWorkflow.captureLayerLineage(ctx.spark, "BRONZE",
        ctx.config.bronzePath, TableRegistry.bronzeNames, "parquet")
      val sl  = LineageWorkflow.captureLayerLineage(ctx.spark, "SILVER",
        ctx.config.silverPath, TableRegistry.silverNames, "parquet")
      val gl  = LineageWorkflow.captureLayerLineage(ctx.spark, "GOLD",
        ctx.config.goldPath, TableRegistry.goldNames, "delta")
      val all = bl ++ sl ++ gl
      LineageWorkflow.printLineageGraph(all)
      if (ctx.config.lineagePath.nonEmpty)
        LineageWorkflow.exportManifest(all, ctx.config.lineagePath)
      MetricsWorkflow.endStage(id, TableRegistry.totalTables)
    }, critical = false, description = "Linaje RAW → Gold")
}

/** WF2: Generación de 10 charts BI en PNG. No-crítico, condicional por feature flag. */
private object AnalyticsWorkflowDef extends Workflow {
  val id           = "ANALYTICS"
  val dependencies = Set("ETL")
  override val critical = false

  /** Solo se ejecuta si chartsEnabled=true en FeatureFlags */
  override def isEnabledFor(features: FeatureFlags): Boolean = features.chartsEnabled

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      MetricsWorkflow.startStage(id)
      val chartsDir = if (ctx.config.chartsPath.nonEmpty) ctx.config.chartsPath
                      else new java.io.File("./src/main/resources/analytics").getCanonicalPath
      AnalyticsWorkflow.run(ctx.spark, ctx.config.goldPath, chartsDir)
      MetricsWorkflow.endStage(id, 10)
    }, critical = false, description = "10 charts BI (JFreeChart)")
}

/** WF-DB2: Exportación Gold → Db2 on Cloud. No-crítico, condicional por db2Enabled. */
private object Db2ExportWorkflowDef extends Workflow {
  val id           = "DB2_EXPORT"
  val dependencies = Set("ETL")
  override val critical   = false
  override val retryCount = 3

  override def isEnabledFor(features: FeatureFlags): Boolean = true // filtrado en runtime

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      if (ctx.config.db2Enabled) {
        MetricsWorkflow.startStage(id)
        val exported = Db2ExportWorkflow.run(ctx.spark, ctx.config)
        MetricsWorkflow.endStage(id, exported)
      } else {
        println(s"  ⏭ Db2 Export: db2Enabled=false — skip")
      }
    }, retryCount = retryCount, critical = false,
       description = "Gold → Db2 on Cloud (JDBC)")
}

/** WF3: Auditoría del catálogo Hive. No-crítico, condicional por hiveEnabled. */
private object HiveAuditWorkflowDef extends Workflow {
  val id           = "HIVE_AUDIT"
  val dependencies = Set("QUALITY", "LINEAGE")
  override val critical = false

  override def isEnabledFor(features: FeatureFlags): Boolean = features.hiveEnabled

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask.fromUnit(id, dependencies, () => {
      MetricsWorkflow.startStage(id)
      val hdfsUri  = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
      val basePath = s"$hdfsUri/hive/warehouse/datalake"
      HiveWorkflow.run(ctx.spark, basePath, hdfsUri)
      MetricsWorkflow.endStage(id,
        TableRegistry.goldNames.length + TableRegistry.silverNames.length)
    }, critical = false, description = "Auditoría catálogo Hive")
}

/** WF6: Barrera final — reporte de métricas. Espera a todos los workflows anteriores. */
private object MetricsWorkflowDef extends Workflow {
  val id           = "METRICS"
  val dependencies = Set("QUALITY", "LINEAGE", "ANALYTICS", "DB2_EXPORT", "HIVE_AUDIT")
  // HIVE_AUDIT puede estar deshabilitado — el DagExecutor maneja el caso
  // donde una dep no-crítica fue skipped

  def asDagTask(ctx: ExecutionContext): DagTask =
    DagTask(
      id           = id,
      dependencies = dependencies,
      retryCount   = 1,
      description  = "Reporte final + persistencia de resultados",
      execute      = () => {
        MetricsWorkflow.generateReport()
        if (ctx.config.metricsPath.nonEmpty)
          MetricsWorkflow.exportMetrics(ctx.config.metricsPath)
        Right(())
      }
    )
}
