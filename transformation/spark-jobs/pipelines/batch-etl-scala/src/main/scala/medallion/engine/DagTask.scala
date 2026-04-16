package medallion.engine

import org.apache.spark.sql.AnalysisException

/**
 * Nodo del DAG de ejecución del pipeline.
 *
 * Cada Task declara sus dependencias (IDs de tasks que deben completarse antes)
 * y su bloque de ejecución. El DagExecutor resuelve el orden óptimo y
 * paraleliza las tasks sin dependencias pendientes.
 *
 * @param id           Identificador único de la task (ej: "bronze_categoria")
 * @param dependencies IDs de las tasks que deben completar antes de ejecutar esta
 * @param execute      Bloque a ejecutar, retorna Either con canal de error tipado
 * @param retryCount   Número máximo de reintentos ante fallo
 * @param critical     Si es false, los fallos no propagan a dependientes
 * @param description  Descripción legible para reportes
 */
case class DagTask(
  id: String,
  dependencies: Set[String],
  execute: () => Either[TaskError, Unit],
  retryCount: Int = 3,
  critical: Boolean = true,
  description: String = ""
)

/** Companion para construcción simplificada de tasks */
object DagTask {
  /**
   * Crea una task desde un bloque Unit (compatibilidad con código existente).
   * Envuelve excepciones en TaskError automáticamente.
   */
  def fromUnit(
    id: String,
    dependencies: Set[String],
    body: () => Unit,
    retryCount: Int = 3,
    critical: Boolean = true,
    description: String = ""
  ): DagTask = {
    DagTask(
      id = id,
      dependencies = dependencies,
      execute = () => {
        try {
          body()
          Right(())
        } catch {
          case e: OutOfMemoryError =>
            Left(TaskError.fatal(s"JVM error (no retry): ${e.getMessage}", e))
          case e: StackOverflowError =>
            Left(TaskError.fatal(s"JVM error (no retry): ${e.getMessage}", e))
          case e: AnalysisException =>
            Left(TaskError.fatal(s"Schema/logic error (no retry): ${e.getMessage}", e))
          case e: IllegalArgumentException =>
            Left(TaskError.fatal(s"Invalid argument (no retry): ${e.getMessage}", e))
          case e: java.io.IOException =>
            Left(TaskError.transient(s"IO error (retryable): ${e.getMessage}", e))
          case e: Exception =>
            Left(TaskError.transient(e.getMessage, e))
        }
      },
      retryCount = retryCount,
      critical = critical,
      description = description
    )
  }
}

// ═══════════════════════════════════════════════════
// Error tipado para tasks del DAG
// ═══════════════════════════════════════════════════

/**
 * Canal de error tipado. Diferencia errores recuperables (Transient)
 * de errores permanentes (Fatal) y warnings no-bloqueantes (Skippable).
 */
sealed trait TaskError {
  def message: String
  def cause: Option[Throwable]
}

object TaskError {
  /** Error transitorio — el DagExecutor debe reintentar */
  case class Transient(message: String, cause: Option[Throwable] = None) extends TaskError
  /** Error fatal — no reintentar, propagar a dependientes */
  case class Fatal(message: String, cause: Option[Throwable] = None) extends TaskError
  /** Warning no-bloqueante — marcar como completado con advertencia */
  case class Skippable(message: String, cause: Option[Throwable] = None) extends TaskError

  def transient(msg: String, e: Throwable): TaskError = Transient(msg, Some(e))
  def fatal(msg: String, e: Throwable): TaskError = Fatal(msg, Some(e))
  def skippable(msg: String): TaskError = Skippable(msg, None)
}

// ═══════════════════════════════════════════════════
// Estado de ejecución de una task en el DAG
// ═══════════════════════════════════════════════════

sealed trait TaskStatus
case object Pending extends TaskStatus
case object Running extends TaskStatus
case object Completed extends TaskStatus
case class Failed(error: Throwable) extends TaskStatus
case object Skipped extends TaskStatus
case class CompletedWithWarning(warning: String) extends TaskStatus
