package medallion.engine

import org.apache.log4j.Logger
import java.util.concurrent.{ConcurrentHashMap, Executors, CountDownLatch}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

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
 * Soporta retry con backoff exponencial y checkpoint para skip.
 *
 * Uso:
 *   val tasks = Seq(
 *     DagTask("bronze", Set.empty, () => BronzeLayer.process(...)),
 *     DagTask("silver", Set("bronze"), () => SilverLayer.process(...)),
 *   )
 *   val executor = new DagExecutor(tasks, parallelism = 2)
 *   val results = executor.execute()
 */
class DagExecutor(
  tasks: Seq[DagTask],
  parallelism: Int = 2,
  checkpointPath: String = ""
) {

  private val logger = Logger.getLogger(getClass.getName)

  private val statusMap = new ConcurrentHashMap[String, TaskStatus]()
  private val taskMap = tasks.map(t => t.id -> t).toMap

  /** Ejecuta el DAG completo. Retorna mapa de taskId → status */
  def execute(): Map[String, TaskStatus] = {
    // Validar DAG
    validateNoCycles()

    // Inicializar estados
    tasks.foreach { t =>
      if (isCheckpointed(t.id)) {
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
          depStatus == Completed || depStatus == Skipped
        }
      }

      readyTasks.foreach { task =>
        if (statusMap.replace(task.id, Pending, Running)) {
          Future {
            val result = executeWithRetry(task)
            statusMap.put(task.id, result)
            if (result == Completed) {
              writeCheckpoint(task.id)
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

  /** Ejecuta una task con retry y backoff exponencial */
  private def executeWithRetry(task: DagTask): TaskStatus = {
    var attempt = 0
    var lastError: Throwable = null

    while (attempt < task.retryCount) {
      try {
        task.execute()
        return Completed
      } catch {
        case e: Throwable =>
          attempt += 1
          lastError = e
          if (attempt < task.retryCount) {
            val backoffMs = 2000L * attempt
            logger.warn(s"  ⚠ DAG: ${task.id} — intento $attempt/${task.retryCount} falló. Retry en ${backoffMs}ms")
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

  /** Checkpoint helpers */
  private def isCheckpointed(taskId: String): Boolean = {
    if (checkpointPath.isEmpty) return false
    new java.io.File(s"$checkpointPath/.dag_$taskId").exists()
  }

  private def writeCheckpoint(taskId: String): Unit = {
    if (checkpointPath.isEmpty) return
    val dir = new java.io.File(checkpointPath)
    if (!dir.exists()) dir.mkdirs()
    new java.io.PrintWriter(s"$checkpointPath/.dag_$taskId").close()
  }

  /** Imprime resumen de ejecución del DAG */
  private def printDagReport(): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║              DAG EXECUTION REPORT                           ║")
    println("╠══════════════════════════════════════════════════════════════╣")

    val statuses = statusMap.asScala.toSeq.sortBy(_._1)
    val completed = statuses.count(_._2 == Completed)
    val skipped = statuses.count(_._2 == Skipped)
    val failed = statuses.count { case (_, s) => s.isInstanceOf[Failed] }
    val pending = statuses.count(_._2 == Pending)

    statuses.foreach { case (id, status) =>
      val icon = status match {
        case Completed => "✔"
        case Skipped   => "⏭"
        case Failed(_) => "✗"
        case Running   => "⏳"
        case Pending   => "○"
      }
      val statusName = status match {
        case Completed => "Completed"
        case Skipped   => "Skipped"
        case Failed(e) => s"Failed(${e.getMessage.take(30)})"
        case Running   => "Running"
        case Pending   => "Pending"
      }
      println(s"║  $icon $id — $statusName")
    }

    println("╠══════════════════════════════════════════════════════════════╣")
    println(f"║  Completed: $completed  |  Skipped: $skipped  |  Failed: $failed  |  Pending: $pending")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()
  }
}
