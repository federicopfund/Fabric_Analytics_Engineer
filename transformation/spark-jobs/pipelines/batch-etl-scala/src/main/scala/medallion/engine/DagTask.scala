package medallion.engine

/**
 * Nodo del DAG de ejecución del pipeline.
 *
 * Cada Task declara sus dependencias (IDs de tasks que deben completarse antes)
 * y su bloque de ejecución. El DagExecutor resuelve el orden óptimo y
 * paraleliza las tasks sin dependencias pendientes.
 *
 * @param id           Identificador único de la task (ej: "bronze_categoria")
 * @param dependencies IDs de las tasks que deben completar antes de ejecutar esta
 * @param execute      Bloque a ejecutar
 * @param retryCount   Número máximo de reintentos ante fallo
 */
case class DagTask(
  id: String,
  dependencies: Set[String],
  execute: () => Unit,
  retryCount: Int = 3
)

/** Estado de ejecución de una task en el DAG */
sealed trait TaskStatus
case object Pending extends TaskStatus
case object Running extends TaskStatus
case object Completed extends TaskStatus
case class Failed(error: Throwable) extends TaskStatus
case object Skipped extends TaskStatus
