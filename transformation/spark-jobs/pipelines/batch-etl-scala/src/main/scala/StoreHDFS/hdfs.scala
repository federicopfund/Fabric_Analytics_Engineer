package scala.ingest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.log4j.Logger
import java.io.File

object hdfs {

  private val logger = Logger.getLogger(getClass.getName)
  private var fs: FileSystem = _

  // ============================================================
  // CHECK HDFS AVAILABILITY
  // ============================================================
  def isAvailable(hdfsUri: String): Boolean = {
    try {
      val conf = new Configuration()
      conf.set("fs.defaultFS", hdfsUri)
      conf.set("ipc.client.connect.timeout", "3000")
      conf.set("ipc.client.connect.max.retries", "1")
      val testFs = FileSystem.get(new java.net.URI(hdfsUri), conf)
      testFs.exists(new Path("/"))
      testFs.close()
      true
    } catch {
      case _: Exception =>
        logger.warn(s"HDFS no disponible en $hdfsUri")
        false
    }
  }

  // ============================================================
  // INIT HDFS — Se conecta como usuario 'fede'
  // ============================================================
  def init(hdfsUri: String, user: String = "fede"): Configuration = {
    val conf = new Configuration()

    conf.set("fs.defaultFS", hdfsUri)
    conf.set("hadoop.job.ugi", s"$user,supergroup")

    fs = FileSystem.get(new java.net.URI(hdfsUri), conf, user)

    conf
  }

  // ============================================================
  // CREAR LA ARQUITECTURA DEL DATALAKE
  // ============================================================
  def createDatalakeStructure(hdfsUri: String): Unit = {

    init(hdfsUri)

    val base = "/hive/warehouse/datalake"

    val folders = Seq(
      s"$base/raw",
      s"$base/bronze",
      s"$base/silver",
      s"$base/gold"
    )

    folders.foreach { folder =>
      val path = new Path(folder)
      if (!fs.exists(path)) {
        fs.mkdirs(path)
        fs.setPermission(path, new FsPermission("777"))   // cambiar a 770 en producción
        logger.info(s"✔ Creado: $folder")
      } else {
        logger.info(s"✓ Ya existe: $folder")
      }
    }

    fs.close()
  }

  // ============================================================
  // SUBIR CARPETA A RAW
  // ============================================================
  def uploadToRaw(hdfsUri: String, localFolder: String): Unit = {

    init(hdfsUri)
    val hdfsFolder = "/hive/warehouse/datalake/raw"

    val folderPath = new Path(hdfsFolder)
    if (!fs.exists(folderPath)) {
      fs.mkdirs(folderPath)
      fs.setPermission(folderPath, new FsPermission("777"))
    }

    val folder = new File(localFolder)
    val archivos = folder.listFiles()

    archivos.foreach { file =>
      val localPath = new Path(file.getAbsolutePath)
      val destPath = new Path(s"$hdfsFolder/${file.getName}")

      fs.copyFromLocalFile(false, true, localPath, destPath)
      logger.info(s"✔ Archivo subido a RAW: ${file.getName}")
    }

    list(hdfsFolder)
    fs.close()
  }

  // ============================================================
  // LISTAR DIRECTORIOS
  // ============================================================
  def list(hdfsFolder: String): Unit = {
    logger.info(s"Contenido de $hdfsFolder:")
    val files = fs.listStatus(new Path(hdfsFolder))
    files.foreach(f => logger.info(" - " + f.getPath.getName))
  }

  // ============================================================
  // ELIMINAR ARCHIVO O CARPETA
  // ============================================================
  def delete(hdfsUri: String, pathToDelete: String): Unit = {
    init(hdfsUri)

    val p = new Path(pathToDelete)
    if (fs.exists(p)) {
      fs.delete(p, true)
      logger.info(s"✔ Eliminado: $pathToDelete")
    } else {
      logger.warn(s"No existe: $pathToDelete")
    }

    fs.close()
  }
}
