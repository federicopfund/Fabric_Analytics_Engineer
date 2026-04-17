package medallion

import medallion.engine.{ContextBuilder, DagExecutor, WorkflowRegistry}
import org.apache.log4j.{Logger, PropertyConfigurator}
import scala.util.control.NonFatal

/**
 * Pipeline v7.0 — Orquestador declarativo Sprint 2.
 *
 * main() pasa de ~150 líneas (v6.0) a 15 líneas limpias.
 * Todo el setup está en ContextBuilder.build().
 * Todos los workflows están en WorkflowRegistry.buildDag().
 *
 * Para agregar un nuevo workflow al pipeline:
 *   1. Crear objeto que extiende Workflow en WorkflowRegistry.scala
 *   2. Agregarlo al Seq `all` en WorkflowRegistry
 *   3. Este archivo NO se toca
 *
 * Topología del DAG (sin cambios):
 *   COS_UPLOAD → ETL → [QUALITY || LINEAGE || ANALYTICS || DB2_EXPORT] → HIVE_AUDIT → METRICS
 *
 * Modos de ejecución (auto-detectados por ContextBuilder):
 *   1. IBM Analytics Engine Serverless (AE_INSTANCE_ID env var presente)
 *   2. HDFS + Hive Lakehouse (HDFS_URI env var presente)
 *   3. Local filesystem (default)
 *
 * Feature flags (env vars):
 *   PIPELINE_ENV                 → dev | staging | prod
 *   PIPELINE_CHARTS_ENABLED      → true | false
 *   PIPELINE_HIVE_ENABLED        → true | false
 *   PIPELINE_QUALITY_MIN_SCORE   → 0-100
 *   PIPELINE_NOTIFY_SLACK        → true | false
 */
object Pipeline {

  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))

    println("╔══════════════════════════════════════════════════════════════╗")
    println("║        PIPELINE ORCHESTRATOR v7.0 — Data Engineering        ║")
    println("║  WorkflowRegistry | ContextBuilder | FeatureFlags | DAG     ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    // ── 1. Construir contexto de ejecución (antes: ~60 líneas) ──────────
    val ctx = ContextBuilder.build()

    // ── 1b. Limpiar checkpoints si se solicita (PIPELINE_CLEAR_CHECKPOINTS=true) ──
    if (sys.env.getOrElse("PIPELINE_CLEAR_CHECKPOINTS", "false").equalsIgnoreCase("true")) {
      println(">>> Limpiando checkpoints anteriores (PIPELINE_CLEAR_CHECKPOINTS=true)")
      ctx.stateStore.clearAll()
    }

    println(s">>> $ctx")
    println(s">>> DAG workflows registrados:")
    println(WorkflowRegistry.describe(ctx.features))
    println()

    // ── 2. Iniciar métricas ──────────────────────────────────────────────
    medallion.workflow.MetricsWorkflow.startPipeline()

    val spark = ctx.spark

    try {
      // ── 3. Construir el DAG declarativo (antes: ~80 líneas de DagTask.fromUnit) ──
      val tasks = WorkflowRegistry.buildDag(ctx)

      // ── 4. Ejecutar ──────────────────────────────────────────────────────
      // parallelism=2: evita contención de locks Delta en S3A (varios workflows
      // escribiendo al mismo _delta_log causa cuelgues en "Filtering files")
      val dagParallelism = sys.env.getOrElse("PIPELINE_DAG_PARALLELISM", "2").toInt
      val executor = new DagExecutor(
        tasks      = tasks,
        parallelism = dagParallelism,
        stateStore = ctx.stateStore
      )
      val results = executor.execute()

      // ── 5. Persistir resultados del run ──────────────────────────────────
      val completedCount = results.values.count(_ == medallion.engine.Completed)
      val failedCount    = results.values.count(_.isInstanceOf[medallion.engine.Failed])

      println("╔══════════════════════════════════════════════════════════════╗")
      println("║              PIPELINE v7.0 COMPLETADO                       ║")
      println("╠══════════════════════════════════════════════════════════════╣")
      println(s"║  Modo:        ${ctx.mode}")
      println(s"║  Ambiente:    ${ctx.features.environment}")
      println(s"║  StateStore:  ${ctx.stateStore.getClass.getSimpleName}")
      println(s"║  Tasks:       ${tasks.length} total | ✔$completedCount | ✗$failedCount")
      println(s"║  COS:         ${if (ctx.config.cosEnabled) "✔ habilitado" else "✗ deshabilitado"}")
      println(s"║  Db2:         ${if (ctx.config.db2Enabled) "✔ habilitado" else "✗ deshabilitado"}")
      println(s"║  Quality SLA: ${ctx.features.effectiveMinScore} (critical=${ctx.features.qualityGateIsCritical})")
      println("╚══════════════════════════════════════════════════════════════╝")

      if (failedCount > 0) {
        logger.error(s"Pipeline completado con $failedCount workflows fallidos")
        sys.exit(1)
      }

    } catch {
      case NonFatal(e) =>
        logger.error(s"Pipeline falló con excepción: ${e.getMessage}", e)
        try { spark.stop() } catch { case _: Throwable => () }
        sys.exit(1)
    }

    // ── Skip spark.stop() en AE ──────────────────────────────────────────
    // S3A cleanup + executor teardown tomaba ~7 min en AE serverless.
    // AE reclama recursos automáticamente al salir el driver.
    // En modos no-AE sí paramos limpiamente.
    val skipSparkStop = ctx.mode == medallion.config.IbmCloudConfig.IbmAnalyticsEngine ||
      sys.env.getOrElse("PIPELINE_SKIP_SPARK_STOP", "false").equalsIgnoreCase("true")
    if (!skipSparkStop) {
      try { spark.stop() } catch { case _: Throwable => () }
    } else {
      println(">>> Skip spark.stop() (AE teardown asíncrono)")
    }

    // Forzar salida del JVM (garantiza que ningún thread no-daemon bloquee el shutdown)
    println(">>> Pipeline finalizado. Terminando JVM.")
    sys.exit(0)
  }
}
