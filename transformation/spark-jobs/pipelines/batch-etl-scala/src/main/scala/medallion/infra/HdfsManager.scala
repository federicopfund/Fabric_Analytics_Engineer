package medallion.infra

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.log4j.Logger
import java.io.File

/**
 * Gestión centralizada de operaciones HDFS para el Lakehouse.
 * Verifica disponibilidad, configura Hadoop, crea estructura y sube archivos.
 */
object HdfsManager {

  private val logger = Logger.getLogger(getClass.getName)
  private var fs: FileSystem = _
  private var hadoopConf: Configuration = _

  val DATALAKE_BASE = "/hive/warehouse/datalake"
  val HIVE_WAREHOUSE = "/hive/warehouse"

  case class HadoopConfig(
    hdfsUri: String,
    user: String = "fede",
    replication: Int = 1,
    blockSize: Long = 134217728L,
    hiveWarehouse: String = "/hive/warehouse",
    hiveMetastoreUris: String = "thrift://localhost:9083"
  )

  def buildHadoopConfiguration(config: HadoopConfig): Configuration = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.hdfsUri)
    conf.set("hadoop.job.ugi", s"${config.user},supergroup")
    conf.set("hadoop.tmp.dir", "/opt/hadoop/tmp")
    conf.set("fs.permissions.umask-mode", "022")
    conf.setInt("dfs.replication", config.replication)
    conf.setLong("dfs.blocksize", config.blockSize)
    conf.setBoolean("dfs.webhdfs.enabled", true)
    conf.setBoolean("dfs.support.append", true)
    conf.set("dfs.permissions.superusergroup", "hadoop")
    conf.setBoolean("dfs.client.use.datanode.hostname", true)
    conf.set("hive.metastore.warehouse.dir", s"${config.hdfsUri}${config.hiveWarehouse}")
    conf.set("hive.metastore.uris", config.hiveMetastoreUris)
    conf.setBoolean("hive.metastore.schema.verification", false)
    conf.set("javax.jdo.option.ConnectionURL",
      "jdbc:derby:;databaseName=/opt/hive/metastore_db;create=true")
    conf.set("javax.jdo.option.ConnectionDriverName",
      "org.apache.derby.jdbc.EmbeddedDriver")
    conf.set("ipc.client.connect.timeout", "5000")
    conf.set("ipc.client.connect.max.retries", "3")
    conf.setInt("dfs.client.socket-timeout", 10000)
    hadoopConf = conf
    logger.info(s"✔ Hadoop configuration built for ${config.hdfsUri}")
    conf
  }

  def isAvailable(hdfsUri: String): Boolean = {
    try {
      val conf = new Configuration()
      conf.set("fs.defaultFS", hdfsUri)
      conf.set("ipc.client.connect.timeout", "3000")
      conf.set("ipc.client.connect.max.retries", "1")
      val testFs = FileSystem.get(new java.net.URI(hdfsUri), conf)
      val result = testFs.exists(new Path("/"))
      testFs.close()
      result
    } catch {
      case _: Exception =>
        logger.warn(s"HDFS no disponible en $hdfsUri")
        false
    }
  }

  def init(hdfsUri: String, user: String = "fede"): Configuration = {
    val conf = if (hadoopConf != null) hadoopConf
    else buildHadoopConfiguration(HadoopConfig(hdfsUri = hdfsUri, user = user))
    fs = FileSystem.get(new java.net.URI(hdfsUri), conf, user)
    logger.info(s"✔ HDFS conectado: $hdfsUri (usuario: $user)")
    conf
  }

  def createDatalakeStructure(hdfsUri: String): Unit = {
    init(hdfsUri)
    val folders = Seq(
      HIVE_WAREHOUSE, DATALAKE_BASE,
      s"$DATALAKE_BASE/raw", s"$DATALAKE_BASE/bronze",
      s"$DATALAKE_BASE/silver", s"$DATALAKE_BASE/gold",
      s"$HIVE_WAREHOUSE/tmp", s"$HIVE_WAREHOUSE/staging",
      "/user/spark/eventLog", "/user/spark/checkpoint"
    )
    folders.foreach { folder =>
      val path = new Path(folder)
      if (!fs.exists(path)) {
        fs.mkdirs(path)
        fs.setPermission(path, new FsPermission("770"))
        logger.info(s"✔ Creado: $folder")
      } else {
        logger.info(s"✓ Ya existe: $folder")
      }
    }
    fs.close()
  }

  def uploadToRaw(hdfsUri: String, localFolder: String): Int = {
    init(hdfsUri)
    val hdfsFolder = s"$DATALAKE_BASE/raw"
    val folderPath = new Path(hdfsFolder)
    if (!fs.exists(folderPath)) {
      fs.mkdirs(folderPath)
      fs.setPermission(folderPath, new FsPermission("770"))
    }
    val folder = new File(localFolder)
    if (!folder.exists() || !folder.isDirectory) {
      logger.error(s"Carpeta local no encontrada: $localFolder")
      fs.close()
      return 0
    }
    val archivos = folder.listFiles().filter(_.isFile)
    var uploaded = 0
    archivos.foreach { file =>
      val localPath = new Path(file.getAbsolutePath)
      val destPath = new Path(s"$hdfsFolder/${file.getName}")
      if (fs.exists(destPath)) {
        logger.info(s"✓ Ya existe en RAW: ${file.getName} — omitido")
      } else {
        fs.copyFromLocalFile(false, true, localPath, destPath)
        logger.info(s"✔ Archivo subido a RAW: ${file.getName} (${file.length()} bytes)")
      }
      uploaded += 1
    }
    list(hdfsFolder)
    fs.close()
    uploaded
  }

  def list(hdfsFolder: String): Seq[String] = {
    logger.info(s"Contenido de $hdfsFolder:")
    val files = fs.listStatus(new Path(hdfsFolder))
    val names = files.map { f =>
      val name = f.getPath.getName
      val sizeKb = f.getLen / 1024
      val typeStr = if (f.isDirectory) "DIR" else s"${sizeKb}KB"
      logger.info(s"  ├── $name ($typeStr)")
      name
    }
    names.toSeq
  }

  def delete(hdfsUri: String, pathToDelete: String): Boolean = {
    init(hdfsUri)
    val p = new Path(pathToDelete)
    val result = if (fs.exists(p)) {
      fs.delete(p, true)
      logger.info(s"✔ Eliminado: $pathToDelete")
      true
    } else {
      logger.warn(s"No existe: $pathToDelete")
      false
    }
    fs.close()
    result
  }

  def validateDatalake(hdfsUri: String): Boolean = {
    init(hdfsUri)
    val requiredPaths = Seq(
      s"$DATALAKE_BASE/raw", s"$DATALAKE_BASE/bronze",
      s"$DATALAKE_BASE/silver", s"$DATALAKE_BASE/gold"
    )
    val allExist = requiredPaths.forall { p =>
      val exists = fs.exists(new Path(p))
      if (!exists) logger.error(s"✗ Falta directorio: $p")
      exists
    }
    if (allExist) {
      logger.info("✔ Estructura del Datalake validada correctamente")
      val rawFiles = fs.listStatus(new Path(s"$DATALAKE_BASE/raw"))
      logger.info(s"  RAW: ${rawFiles.length} archivos")
    }
    fs.close()
    allExist
  }

  def getConfigSummary(config: HadoopConfig): String = {
    s"""
    |╔══════════════════════════════════════════════════╗
    |║         HADOOP CONFIGURATION SUMMARY            ║
    |╠══════════════════════════════════════════════════╣
    |║  HDFS URI:          ${config.hdfsUri}
    |║  User:              ${config.user}
    |║  Replication:       ${config.replication}
    |║  Block Size:        ${config.blockSize / (1024*1024)}MB
    |║  Hive Warehouse:    ${config.hiveWarehouse}
    |║  Metastore URIs:    ${config.hiveMetastoreUris}
    |╚══════════════════════════════════════════════════╝""".stripMargin
  }
}
