package medallion.engine

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Abstracción para persistir estado del DAG (checkpoints).
 * Las implementaciones determinan el destino según el modo de ejecución:
 *   - LocalStateStore:        filesystem local (dev/test)
 *   - DistributedStateStore:  HDFS o COS/S3A (producción en K8s/AE)
 *   - NoOpStateStore:         checkpointing deshabilitado
 */
trait PipelineStateStore {
  def isCompleted(taskId: String): Boolean
  def markCompleted(taskId: String, durationMs: Long, description: String = ""): Unit
}

/** Checkpointing deshabilitado — operación nula */
object NoOpStateStore extends PipelineStateStore {
  def isCompleted(taskId: String): Boolean = false
  def markCompleted(taskId: String, durationMs: Long, description: String): Unit = ()
}

/**
 * Checkpoint en filesystem local. Solo válido para dev/test donde el
 * filesystem persiste entre ejecuciones del job.
 */
class LocalStateStore(basePath: String) extends PipelineStateStore {

  private val logger = Logger.getLogger(getClass.getName)

  private def dir: java.io.File = {
    val d = new java.io.File(basePath)
    if (!d.exists()) d.mkdirs()
    d
  }

  def isCompleted(taskId: String): Boolean = {
    new java.io.File(s"$basePath/.dag_$taskId.json").exists() ||
      new java.io.File(s"$basePath/.dag_$taskId").exists()
  }

  def markCompleted(taskId: String, durationMs: Long, description: String): Unit = {
    dir // ensure dir exists
    val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new java.util.Date())
    val json =
      s"""{
  "task": "$taskId",
  "status": "completed",
  "timestamp": "$timestamp",
  "duration_ms": $durationMs,
  "description": "$description"
}"""
    val writer = new java.io.PrintWriter(s"$basePath/.dag_$taskId.json")
    try { writer.write(json) } finally { writer.close() }
    logger.info(s"  ✔ Checkpoint (local): $taskId → $basePath/.dag_$taskId.json")
  }
}

/**
 * Checkpoint en almacenamiento distribuido (HDFS o COS/S3A).
 * Usa la API de Hadoop FileSystem, compatible con cualquier destino
 * configurado en el SparkSession (hdfs://, s3a://, etc.).
 *
 * Los checkpoints persisten entre runs y son accesibles desde cualquier pod.
 */
class DistributedStateStore(spark: SparkSession, basePath: String) extends PipelineStateStore {

  private val logger = Logger.getLogger(getClass.getName)

  private def fs: FileSystem = {
    val path = new Path(basePath)
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)
  }

  private def checkpointPath(taskId: String): Path =
    new Path(s"$basePath/.dag_$taskId.json")

  private def legacyPath(taskId: String): Path =
    new Path(s"$basePath/.dag_$taskId")

  def isCompleted(taskId: String): Boolean = {
    val filesystem = fs
    filesystem.exists(checkpointPath(taskId)) || filesystem.exists(legacyPath(taskId))
  }

  def markCompleted(taskId: String, durationMs: Long, description: String): Unit = {
    val filesystem = fs
    val dirPath = new Path(basePath)
    if (!filesystem.exists(dirPath)) filesystem.mkdirs(dirPath)

    val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new java.util.Date())
    val json =
      s"""{
  "task": "$taskId",
  "status": "completed",
  "timestamp": "$timestamp",
  "duration_ms": $durationMs,
  "description": "$description"
}"""

    val outPath = checkpointPath(taskId)
    val out = filesystem.create(outPath, true)
    try { out.writeBytes(json) } finally { out.close() }
    logger.info(s"  ✔ Checkpoint (distributed): $taskId → $outPath")
  }
}
