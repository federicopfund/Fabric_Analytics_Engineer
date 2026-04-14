package medallion.workflow

import org.apache.log4j.Logger
import medallion.config.IbmCloudConfig
import scala.util.control.NonFatal

/**
 * WORKFLOW: Carga inicial de archivos CSV locales al bucket COS datalake-raw.
 *
 * Usa el CLI de IBM Cloud (ibmcloud cos put-object) para subir cada archivo
 * CSV desde el directorio local a s3://datalake-raw-us-south-dev/.
 *
 * Prerequisitos:
 *   - ibmcloud CLI instalado con plugin cloud-object-storage
 *   - Login activo (ibmcloud login)
 *   - Variables de entorno: COS_BUCKET_RAW (o default datalake-raw-us-south-dev)
 */
object CosUploadWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  /**
   * Sube todos los archivos CSV del directorio local al bucket COS raw.
   *
   * @param localRawPath  Ruta local con los CSV (ej: ./datalake/raw)
   * @param bucketName    Nombre del bucket COS destino
   * @return Número de archivos subidos exitosamente
   */
  def uploadRawFiles(localRawPath: String, bucketName: String): Int = {
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  COS UPLOAD — Carga inicial a Raw Bucket ║")
    logger.info("╚══════════════════════════════════════════╝")

    val rawDir = new java.io.File(localRawPath)
    if (!rawDir.exists() || !rawDir.isDirectory) {
      logger.error(s"✗ Directorio no encontrado: $localRawPath")
      return 0
    }

    val csvFiles = rawDir.listFiles().filter(f => f.isFile && f.getName.endsWith(".csv"))
    if (csvFiles.isEmpty) {
      logger.warn(s"⚠ No se encontraron archivos CSV en $localRawPath")
      return 0
    }

    println(s"  📦 Bucket destino: $bucketName")
    println(s"  📁 Archivos encontrados: ${csvFiles.length}")
    println()

    var uploaded = 0

    csvFiles.foreach { file =>
      try {
        val objectKey = file.getName
        val filePath = file.getCanonicalPath
        val sizeMB = file.length() / (1024.0 * 1024.0)

        println(f"  ☁️  Subiendo: $objectKey ($sizeMB%.2f MB)")

        val exitCode = uploadWithCli(bucketName, objectKey, filePath)

        if (exitCode == 0) {
          println(s"  ✔ $objectKey subido correctamente")
          uploaded += 1
        } else {
          logger.error(s"  ✗ $objectKey falló (exit code: $exitCode)")
        }
      } catch {
        case NonFatal(e) =>
          logger.error(s"  ✗ ${file.getName} — ${e.getMessage}")
      }
    }

    println()
    println(s"  ═══ RESULTADO: $uploaded/${csvFiles.length} archivos subidos ═══")
    logger.info(s"✔ COS Upload completado: $uploaded/${csvFiles.length}")
    uploaded
  }

  /**
   * Verifica qué archivos ya existen en el bucket COS.
   *
   * @param bucketName Nombre del bucket
   * @return Lista de object keys existentes
   */
  def listBucketObjects(bucketName: String): Seq[String] = {
    try {
      val process = new ProcessBuilder(
        "ibmcloud", "cos", "list-objects",
        "--bucket", bucketName,
        "--output", "json"
      ).redirectErrorStream(true).start()

      val output = scala.io.Source.fromInputStream(process.getInputStream).mkString
      process.waitFor()

      // Extraer nombres de objetos del JSON
      val keyPattern = """"Key"\s*:\s*"([^"]+)"""".r
      keyPattern.findAllMatchIn(output).map(_.group(1)).toSeq
    } catch {
      case NonFatal(e) =>
        logger.warn(s"No se pudo listar bucket $bucketName: ${e.getMessage}")
        Seq.empty
    }
  }

  /**
   * Sube solo archivos que no existen aún en el bucket (carga incremental).
   */
  def uploadMissingFiles(localRawPath: String, bucketName: String): Int = {
    val existingObjects = listBucketObjects(bucketName).toSet

    val rawDir = new java.io.File(localRawPath)
    if (!rawDir.exists()) return 0

    val csvFiles = rawDir.listFiles().filter(f => f.isFile && f.getName.endsWith(".csv"))
    val missingFiles = csvFiles.filterNot(f => existingObjects.contains(f.getName))

    if (missingFiles.isEmpty) {
      println(s"  ✔ Todos los archivos ya existen en $bucketName — skip")
      return 0
    }

    println(s"  📦 Archivos faltantes: ${missingFiles.length}/${csvFiles.length}")

    var uploaded = 0
    missingFiles.foreach { file =>
      try {
        val exitCode = uploadWithCli(bucketName, file.getName, file.getCanonicalPath)
        if (exitCode == 0) {
          println(s"  ✔ ${file.getName} subido")
          uploaded += 1
        }
      } catch {
        case NonFatal(e) => logger.error(s"  ✗ ${file.getName}: ${e.getMessage}")
      }
    }
    uploaded
  }

  /**
   * Ejecuta ibmcloud cos put-object para subir un archivo.
   */
  private def uploadWithCli(bucket: String, key: String, filePath: String): Int = {
    val process = new ProcessBuilder(
      "ibmcloud", "cos", "put-object",
      "--bucket", bucket,
      "--key", key,
      "--body", filePath
    ).redirectErrorStream(true).start()

    val output = scala.io.Source.fromInputStream(process.getInputStream).mkString
    val exitCode = process.waitFor()

    if (exitCode != 0) {
      logger.error(s"ibmcloud cos put-object falló: $output")
    }
    exitCode
  }
}
