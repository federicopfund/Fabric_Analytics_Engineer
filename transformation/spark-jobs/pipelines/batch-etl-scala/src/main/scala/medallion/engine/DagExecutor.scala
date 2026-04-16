package medallion.engine

import org.apache.log4j.Logger
import java.util.concurrent.{ConcurrentHashMap, Executors, CountDownLatch}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * DAG Executor — Motor de ejecución declarativa de tareas con dependencias.
 *
 * Estrategia:
 *   1. Valida que el DAG no tenga ciclos
 *   2. Identifica tasks sin dependencias pendientes (ready set)
 *   3. Las ejecuta en paralelo con un thread pool controlado
 *   4. Cuando una task completa, re-evalúa qué tasks se desbloquean
 *   5. Repite hasta que todas las tasks estén completadas o fallidas
 *
 * Soporta:
 *   - Retry con backoff exponencial (solo errores transitorios)
 *   - Checkpoint JSON con metadatos por task
 *   - Errores tipados (Transient/Fatal/Skippable)
 *   - Tasks no-críticas que no propagan fallo a dependientes
 *
 * Uso:
 *   val tasks = Seq(
 *     DagTask("bronze", Set.empty, () => Right(BronzeLayer.process(...))),
 *     DagTask("silver", Set("bronze"), () => Right(SilverLayer.process(...))),
 *   )
 *   val executor = new DagExecutor(tasks, parallelism = 2)
 *   val results = executor.execute()
 */
class DagExecutor(
  tasks: Seq[DagTask],
  parallelism: Int = 2,
  checkpointPath: String = "",
  stateStore: PipelineStateStore = null
) {

  private val store: PipelineStateStore = {
    if (stateStore != null) stateStore
    else if (checkpointPath.nonEmpty) new LocalStateStore(checkpointPath)
    else NoOpStateStore
  }

  private val logger = Logger.getLogger(getClass.getName)

  private val statusMap = new ConcurrentHashMap[String, TaskStatus]()
  private val taskMap = tasks.map(t => t.id -> t).toMap
  private val durations = new ConcurrentHashMap[String, Long]()

  /** Ejecuta el DAG completo. Retorna mapa de taskId → status */
  def execute(): Map[String, TaskStatus] = {
    // Validar DAG
    validateNoCycles()

    // Inicializar estados
    tasks.foreach { t =>
      if (store.isCompleted(t.id)) {
        statusMap.put(t.id, Skipped)
        logger.info(s"  ⏭ DAG: ${t.id} — checkpoint, skipped")
      } else {
        statusMap.put(t.id, Pending)
      }
    }

    val pool = Executors.newFixedThreadPool(parallelism)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)

    val completedCount = new AtomicInteger(0)
    val totalTasks = tasks.count(t => statusMap.get(t.id) != Skipped)
    val latch = new CountDownLatch(totalTasks)

    // Función recursiva que busca tasks listas y las lanza
    def scheduleReady(): Unit = {
      val readyTasks = tasks.filter { t =>
        statusMap.get(t.id) == Pending && t.dependencies.forall { dep =>
          val depStatus = statusMap.get(dep)
          depStatus == Completed || depStatus == Skipped ||
            depStatus.isInstanceOf[CompletedWithWarning] ||
            // Tasks no-críticas fallidas no bloquean dependientes
            (depStatus.isInstanceOf[Failed] && taskMap.get(dep).exists(!_.critical))
        }
      }

      readyTasks.foreach { task =>
        if (statusMap.replace(task.id, Pending, Running)) {
          Future {
            // Check if critical dependency failed
            val blockedByFailedDep = task.dependencies.exists { dep =>
              statusMap.get(dep).isInstanceOf[Failed] && taskMap.get(dep).exists(_.critical)
            }

            val result = if (blockedByFailedDep) {
              Skipped
            } else {
              val start = System.currentTimeMillis()
              val r = executeWithRetry(task)
              durations.put(task.id, System.currentTimeMillis() - start)
              r
            }

            statusMap.put(task.id, result)
            if (result == Completed || result.isInstanceOf[CompletedWithWarning]) {
              val desc = taskMap.get(task.id).map(_.description).getOrElse("")
              store.markCompleted(task.id, durations.getOrDefault(task.id, 0L), desc)
            }
            val done = completedCount.incrementAndGet()
            logger.info(s"  DAG progress: $done/$totalTasks — ${task.id}: $result")
            latch.countDown()

            // Re-schedule: ahora que esta task terminó, otras pueden desbloquearse
            this.synchronized { scheduleReady() }
          }
        }
      }
    }

    // Arrancar la primera ronda
    this.synchronized { scheduleReady() }

    // Esperar a que todas las tasks terminen (timeout 30 min)
    latch.await(30, java.util.concurrent.TimeUnit.MINUTES)
    pool.shutdown()

    // Reportar resultado
    printDagReport()

    statusMap.asScala.toMap
  }

  /** Ejecuta una task con retry y backoff exponencial. Respeta error tipado. */
  private def executeWithRetry(task: DagTask): TaskStatus = {
    var attempt = 0
    var lastError: Throwable = null

    while (attempt < task.retryCount) {
      try {
        task.execute() match {
          case Right(_) =>
            return Completed

          case Left(TaskError.Skippable(msg, _)) =>
            logger.warn(s"  ⚠ DAG: ${task.id} — completado con warning: $msg")
            return CompletedWithWarning(msg)

          case Left(TaskError.Fatal(msg, cause)) =>
            logger.error(s"  ✗ DAG: ${task.id} — error fatal (no retry): $msg")
            return Failed(cause.getOrElse(new RuntimeException(msg)))

          case Left(TaskError.Transient(msg, cause)) =>
            attempt += 1
            lastError = cause.getOrElse(new RuntimeException(msg))
            if (attempt < task.retryCount) {
              val backoffMs = 2000L * attempt
              logger.warn(s"  ⚠ DAG: ${task.id} — intento $attempt/${task.retryCount} falló (transient): $msg. Retry en ${backoffMs}ms")
              Thread.sleep(backoffMs)
            }
        }
      } catch {
        case NonFatal(e) =>
          attempt += 1
          lastError = e
          if (attempt < task.retryCount) {
            val backoffMs = 2000L * attempt
            logger.warn(s"  ⚠ DAG: ${task.id} — intento $attempt/${task.retryCount} excepción. Retry en ${backoffMs}ms")
            Thread.sleep(backoffMs)
          }
      }
    }

    logger.error(s"  ✗ DAG: ${task.id} — falló después de ${task.retryCount} intentos: ${lastError.getMessage}")
    Failed(lastError)
  }

  /** Valida que no haya ciclos en el DAG */
  private def validateNoCycles(): Unit = {
    val visited = scala.collection.mutable.Set.empty[String]
    val inStack = scala.collection.mutable.Set.empty[String]

    def visit(id: String): Unit = {
      if (inStack.contains(id))
        throw new IllegalArgumentException(s"DAG cycle detected involving task: $id")
      if (visited.contains(id)) return

      inStack += id
      taskMap.get(id).foreach { t =>
        t.dependencies.foreach(visit)
      }
      inStack -= id
      visited += id
    }

    tasks.foreach(t => visit(t.id))
    logger.info(s"  ✔ DAG validated: ${tasks.length} tasks, no cycles")
  }

  // Checkpoint logic delegated to PipelineStateStore

  /** Imprime resumen de ejecución del DAG */
  private def printDagReport(): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║              DAG EXECUTION REPORT                           ║")
    println("╠══════════════════════════════════════════════════════════════╣")

    val statuses = statusMap.asScala.toSeq.sortBy(_._1)
    val completed = statuses.count(_._2 == Completed)
    val warnings = statuses.count(_._2.isInstanceOf[CompletedWithWarning])
    val skipped = statuses.count(_._2 == Skipped)
    val failed = statuses.count { case (_, s) => s.isInstanceOf[Failed] }
    val pending = statuses.count(_._2 == Pending)

    statuses.foreach { case (id, status) =>
      val icon = status match {
        case Completed                => "✔"
        case CompletedWithWarning(_)  => "⚠"
        case Skipped                  => "⏭"
        case Failed(_)                => "✗"
        case Running                  => "⏳"
        case Pending                  => "○"
      }
      val durationStr = Option(durations.get(id)).map(d => f" (${d / 1000.0}%.2fs)").getOrElse("")
      val statusName = status match {
        case Completed                 => "Completed"
        case CompletedWithWarning(msg) => s"Warning($msg)"
        case Skipped                   => "Skipped"
        case Failed(e)                 => s"Failed(${e.getMessage.take(30)})"
        case Running                   => "Running"
        case Pending                   => "Pending"
      }
      println(s"║  $icon $id — $statusName$durationStr")
    }

    println("╠══════════════════════════════════════════════════════════════╣")
    println(f"║  Completed: $completed  |  Warnings: $warnings  |  Skipped: $skipped  |  Failed: $failed  |  Pending: $pending")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()
  }
}
