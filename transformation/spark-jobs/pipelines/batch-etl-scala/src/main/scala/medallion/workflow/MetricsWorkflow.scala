package medallion.workflow

import org.apache.log4j.Logger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.JavaConverters._

/**
 * WORKFLOW 6: Metrics — Captura y reporte de métricas de ejecución.
 *
 * Thread-safe: usa ConcurrentHashMap y ConcurrentLinkedQueue para
 * soportar registro de métricas desde workflows paralelos.
 *
 * Registra tiempos, contadores y throughput por cada stage del pipeline.
 * Permite hacer benchmarking entre ejecuciones y detectar regresiones.
 *
 * Uso:
 *   MetricsWorkflow.startStage("BRONZE")
 *   // ... proceso ...
 *   MetricsWorkflow.endStage("BRONZE", tablesProcessed = 7)
 *   MetricsWorkflow.generateReport()
 */
object MetricsWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  case class StageMetric(
    stage: String,
    startTimeMs: Long,
    endTimeMs: Long,
    durationSec: Double,
    tablesProcessed: Int
  )

  // Thread-safe collections para ejecución paralela de workflows
  private val stages = new ConcurrentHashMap[String, Long]()
  private val metrics = new ConcurrentLinkedQueue[StageMetric]()
  @volatile private var pipelineStartMs: Long = 0L

  /** Marca inicio del pipeline completo */
  def startPipeline(): Unit = {
    pipelineStartMs = System.currentTimeMillis()
    stages.clear()
    metrics.clear()
  }

  /** Marca inicio de un stage (thread-safe) */
  def startStage(stageName: String): Unit = {
    stages.put(stageName, System.currentTimeMillis())
  }

  /** Marca fin de un stage y registra métricas (thread-safe) */
  def endStage(stageName: String, tablesProcessed: Int = 0): Unit = {
    val endTime = System.currentTimeMillis()
    val startTime = Option(stages.get(stageName)).getOrElse(endTime)
    val duration = (endTime - startTime) / 1000.0

    val metric = StageMetric(stageName, startTime, endTime, Math.round(duration * 100) / 100.0, tablesProcessed)
    metrics.add(metric)

    logger.info(f"  ⏱ $stageName: ${metric.durationSec}%.2fs ($tablesProcessed tablas)")
  }

  /** Obtiene las métricas como lista inmutable ordenada por start time */
  private def orderedMetrics: Seq[StageMetric] =
    metrics.asScala.toSeq.sortBy(_.startTimeMs)

  /** Genera el reporte de métricas en consola */
  def generateReport(): Unit = {
    val totalDuration = if (pipelineStartMs > 0) (System.currentTimeMillis() - pipelineStartMs) / 1000.0 else 0.0

    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║              PIPELINE EXECUTION METRICS                     ║")
    println("╠══════════════════════════════════════════════════════════════╣")

    val sorted = orderedMetrics
    if (sorted.isEmpty) {
      println("║  No hay métricas registradas                               ║")
      println("╚══════════════════════════════════════════════════════════════╝")
      return
    }

    // Timeline de stages
    println("║                                                              ║")
    println("║  STAGE TIMELINE                                              ║")
    println("║  ─────────────────────────────────────────────────────       ║")

    val maxDuration = sorted.map(_.durationSec).max
    sorted.foreach { m =>
      val barLen = if (maxDuration > 0) Math.round(m.durationSec / maxDuration * 30).toInt else 0
      val bar = "█" * barLen + "░" * (30 - barLen)
      println(f"║  ${m.stage}%-25s ${m.durationSec}%7.2fs [$bar] ${m.tablesProcessed}%2d tablas")
    }

    println("║                                                              ║")
    println("║  SUMMARY                                                     ║")
    println("║  ─────────────────────────────────────────────────────       ║")

    val totalTables = sorted.map(_.tablesProcessed).sum
    val etlStages = sorted.filter(m => Seq("BRONZE", "SILVER", "GOLD", "ETL").contains(m.stage))
    val etlDuration = etlStages.map(_.durationSec).sum
    val etlTables = etlStages.map(_.tablesProcessed).sum
    val throughput = if (etlDuration > 0) etlTables / etlDuration else 0.0

    // Identificar si hubo workflows paralelos (overlap de tiempos)
    val parallelPairs = for {
      i <- sorted.indices
      j <- (i + 1) until sorted.length
      if sorted(i).endTimeMs > sorted(j).startTimeMs // overlap
    } yield (sorted(i).stage, sorted(j).stage)
    val hadParallel = parallelPairs.nonEmpty

    println(f"║  Total pipeline duration:   $totalDuration%7.2fs")
    println(f"║  ETL duration:              $etlDuration%7.2fs")
    println(f"║  Total tablas procesadas:   $totalTables%4d")
    println(f"║  ETL throughput:             $throughput%5.2f tablas/s")
    if (hadParallel) {
      println(s"║  Parallel workflows:        ${parallelPairs.map(p => s"${p._1}||${p._2}").mkString(", ")}")
    }

    // Identificar stage bottleneck
    val bottleneck = sorted.maxBy(_.durationSec)
    println(f"║  Bottleneck stage:          ${bottleneck.stage}%-15s (${bottleneck.durationSec}%.2fs)")

    // Memory snapshot
    val runtime = Runtime.getRuntime
    val usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    val maxMB = runtime.maxMemory() / (1024 * 1024)
    println(f"║  JVM Memory:                $usedMB%dMB / $maxMB%dMB")

    println("║                                                              ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()
  }

  /** Exporta métricas a archivo JSON */
  def exportMetrics(outputPath: String): Unit = {
    val dir = new java.io.File(outputPath)
    if (!dir.exists()) dir.mkdirs()

    val timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())
    val file = new java.io.File(s"$outputPath/metrics_$timestamp.json")

    val totalDuration = if (pipelineStartMs > 0) (System.currentTimeMillis() - pipelineStartMs) / 1000.0 else 0.0
    val runtime = Runtime.getRuntime
    val usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
    val maxMB = runtime.maxMemory() / (1024 * 1024)

    val sorted = orderedMetrics
    val stageJsons = sorted.map { m =>
      s"""    {"stage": "${m.stage}", "duration_sec": ${m.durationSec}, "tables": ${m.tablesProcessed}, "start_ms": ${m.startTimeMs}, "end_ms": ${m.endTimeMs}}"""
    }.mkString(",\n")

    val json =
      s"""{
  "generated_at": "$timestamp",
  "total_duration_sec": ${Math.round(totalDuration * 100) / 100.0},
  "total_tables": ${sorted.map(_.tablesProcessed).sum},
  "jvm_memory_used_mb": $usedMB,
  "jvm_memory_max_mb": $maxMB,
  "stages": [
$stageJsons
  ]
}"""

    val writer = new java.io.PrintWriter(file)
    try { writer.write(json) } finally { writer.close() }

    println(s"  ✔ Metrics exportado: ${file.getAbsolutePath}")
    logger.info(s"✔ Metrics: ${file.getAbsolutePath}")
  }
}
