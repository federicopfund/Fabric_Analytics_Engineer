package medallion.config

import org.apache.log4j.Logger
import scala.util.control.NonFatal

/**
 * Configuración y detección de servicios IBM Cloud.
 *
 * Detecta si Analytics Engine (AE) Serverless está activo,
 * provee configuración S3A para IBM COS, y resuelve el modo
 * de ejecución: IBM_AE > HDFS > LOCAL.
 */
object IbmCloudConfig {

  private val logger = Logger.getLogger(getClass.getName)

  // ═══════════════════════════════════════════════════
  // Modos de ejecución
  // ═══════════════════════════════════════════════════
  sealed trait ExecutionMode
  case object IbmAnalyticsEngine extends ExecutionMode
  case object HdfsCluster extends ExecutionMode
  case object LocalMode extends ExecutionMode

  // ═══════════════════════════════════════════════════
  // IBM COS (S3A) — leído de env vars con defaults
  // ═══════════════════════════════════════════════════
  case class CosConfig(
    accessKey:  String,
    secretKey:  String,
    endpoint:   String,
    region:     String = "us-south",
    bucketRaw:    String = "datalake-raw-us-south-dev",
    bucketBronze: String = "datalake-bronze-us-south-dev",
    bucketSilver: String = "datalake-silver-us-south-dev",
    bucketGold:   String = "datalake-gold-us-south-dev"
  ) {
    def s3aPath(bucket: String, table: String): String =
      s"s3a://$bucket/$table"

    def rawPath(table: String): String    = s3aPath(bucketRaw, table)
    def bronzePath(table: String): String = s3aPath(bucketBronze, table)
    def silverPath(table: String): String = s3aPath(bucketSilver, table)
    def goldPath(table: String): String   = s3aPath(bucketGold, table)

    def rawBase: String    = s"s3a://$bucketRaw"
    def bronzeBase: String = s"s3a://$bucketBronze"
    def silverBase: String = s"s3a://$bucketSilver"
    def goldBase: String   = s"s3a://$bucketGold"
  }

  // ═══════════════════════════════════════════════════
  // Analytics Engine config
  // ═══════════════════════════════════════════════════
  case class AnalyticsEngineConfig(
    instanceId:    String,
    apiKey:        String,
    apiEndpoint:   String,
    sparkVersion:  String = "3.5"
  )

  // ═══════════════════════════════════════════════════
  // Factory methods
  // ═══════════════════════════════════════════════════

  def loadCosConfig(): CosConfig = CosConfig(
    accessKey  = envRequired("COS_ACCESS_KEY"),
    secretKey  = envRequired("COS_SECRET_KEY"),
    endpoint   = env("COS_ENDPOINT",      "s3.us-south.cloud-object-storage.appdomain.cloud"),
    region     = env("COS_REGION",        "us-south"),
    bucketRaw    = env("COS_BUCKET_RAW",    "datalake-raw-us-south-dev"),
    bucketBronze = env("COS_BUCKET_BRONZE", "datalake-bronze-us-south-dev"),
    bucketSilver = env("COS_BUCKET_SILVER", "datalake-silver-us-south-dev"),
    bucketGold   = env("COS_BUCKET_GOLD",   "datalake-gold-us-south-dev")
  )

  def loadAeConfig(): Option[AnalyticsEngineConfig] = {
    val instanceId = sys.env.getOrElse("AE_INSTANCE_ID", "")
    val apiKey     = sys.env.getOrElse("AE_API_KEY", "")
    if (instanceId.nonEmpty && apiKey.nonEmpty) {
      Some(AnalyticsEngineConfig(
        instanceId   = instanceId,
        apiKey       = apiKey,
        apiEndpoint  = env("AE_API_ENDPOINT", s"https://api.us-south.ae.cloud.ibm.com/v3/analytics_engines/$instanceId"),
        sparkVersion = env("AE_SPARK_VERSION", "3.5")
      ))
    } else None
  }

  /**
   * Detecta si Analytics Engine está activo.
   * Intenta primero via Instance API con IAM token.
   * Si falla (403), intenta via ibmcloud CLI plugin.
   */
  def isAnalyticsEngineActive(aeConfig: AnalyticsEngineConfig): Boolean = {
    // Método 1: API directa con IAM token
    val apiResult = try {
      val iamToken = getIamToken(aeConfig.apiKey)
      if (iamToken.nonEmpty) {
        val url = aeConfig.apiEndpoint
        val conn = new java.net.URL(url).openConnection().asInstanceOf[java.net.HttpURLConnection]
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Authorization", s"Bearer $iamToken")
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(10000)

        val responseCode = conn.getResponseCode
        if (responseCode == 200) {
          val body = scala.io.Source.fromInputStream(conn.getInputStream).mkString
          conn.disconnect()
          body.contains("\"state\":\"active\"") || body.contains("\"state\": \"active\"")
        } else {
          conn.disconnect()
          logger.info(s"AE API respondió $responseCode, intentando CLI...")
          false
        }
      } else false
    } catch {
      case NonFatal(_) => false
    }

    if (apiResult) {
      logger.info("✔ Analytics Engine: ACTIVO (via API)")
      return true
    }

    // Método 2: ibmcloud CLI plugin
    try {
      val process = new ProcessBuilder("ibmcloud", "ae-v3", "instance", "show",
        "--id", aeConfig.instanceId, "--output", "json")
        .redirectErrorStream(true)
        .start()

      val output = scala.io.Source.fromInputStream(process.getInputStream).mkString
      val exitCode = process.waitFor()

      if (exitCode == 0 && (output.contains("\"state\":\"active\"") || output.contains("\"state\": \"active\""))) {
        logger.info("✔ Analytics Engine: ACTIVO (via CLI)")
        true
      } else {
        logger.warn(s"Analytics Engine CLI check: exit=$exitCode")
        false
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Analytics Engine no accesible via CLI: ${e.getMessage}")
        false
    }
  }

  /**
   * Obtiene IAM Bearer token usando API Key.
   */
  def getIamToken(apiKey: String): String = {
    try {
      val url = new java.net.URL("https://iam.cloud.ibm.com/identity/token")
      val conn = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      conn.setDoOutput(true)
      conn.setConnectTimeout(15000)
      conn.setReadTimeout(15000)

      val body = s"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=$apiKey"
      conn.getOutputStream.write(body.getBytes("UTF-8"))
      conn.getOutputStream.close()

      if (conn.getResponseCode == 200) {
        val response = scala.io.Source.fromInputStream(conn.getInputStream).mkString
        conn.disconnect()
        // Parse access_token from JSON (sin dependencia externa)
        val tokenPattern = """"access_token"\s*:\s*"([^"]+)"""".r
        tokenPattern.findFirstMatchIn(response).map(_.group(1)).getOrElse("")
      } else {
        conn.disconnect()
        logger.warn(s"IAM token request falló con código ${conn.getResponseCode}")
        ""
      }
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Error obteniendo IAM token: ${e.getMessage}")
        ""
    }
  }

  /**
   * Determina el modo de ejecución óptimo:
   *   0. Si EXECUTION_MODE env var está seteado → forzar ese modo
   *   1. Si AE está configurado y activo → IbmAnalyticsEngine
   *   2. Si HDFS está disponible → HdfsCluster
   *   3. Fallback → LocalMode
   */
  def resolveExecutionMode(hdfsUri: String): ExecutionMode = {
    // 0. Env var explícito (para AE Serverless donde no hay CLI ni API accesible)
    sys.env.get("EXECUTION_MODE") match {
      case Some(m) if m.equalsIgnoreCase("IBM_AE") || m.equalsIgnoreCase("AE") =>
        logger.info(">>> Modo: IBM Analytics Engine (forzado via EXECUTION_MODE)")
        return IbmAnalyticsEngine
      case Some(m) if m.equalsIgnoreCase("HDFS") =>
        logger.info(">>> Modo: HDFS (forzado via EXECUTION_MODE)")
        return HdfsCluster
      case Some(m) if m.equalsIgnoreCase("LOCAL") =>
        logger.info(">>> Modo: LOCAL (forzado via EXECUTION_MODE)")
        return LocalMode
      case _ => // auto-detect
    }

    // 1. Auto-detect: Verificar Analytics Engine
    loadAeConfig() match {
      case Some(ae) if isAnalyticsEngineActive(ae) =>
        logger.info(">>> Modo: IBM Analytics Engine (Serverless Spark)")
        IbmAnalyticsEngine
      case Some(_) =>
        logger.warn(">>> Analytics Engine configurado pero no activo, evaluando fallback...")
        checkHdfsOrLocal(hdfsUri)
      case None =>
        // 2. Fallback: si COS está configurado sin AE_API_KEY, asumir AE
        val cosKey = sys.env.getOrElse("COS_ACCESS_KEY", "")
        if (cosKey.nonEmpty) {
          logger.info(">>> Modo: IBM Analytics Engine (detectado via COS_ACCESS_KEY)")
          IbmAnalyticsEngine
        } else {
          logger.info(">>> Analytics Engine no configurado")
          checkHdfsOrLocal(hdfsUri)
        }
    }
  }

  private def checkHdfsOrLocal(hdfsUri: String): ExecutionMode = {
    if (medallion.infra.HdfsManager.isAvailable(hdfsUri)) {
      logger.info(">>> Modo: HDFS + Hive Lakehouse")
      HdfsCluster
    } else {
      logger.info(">>> Modo: LOCAL")
      LocalMode
    }
  }

  private def envRequired(key: String): String =
    sys.env.getOrElse(key,
      throw new IllegalStateException(
        s"Variable de entorno requerida no configurada: $key. " +
        s"Configurar en .env o como secret de Kubernetes/AE."
      )
    )

  private def env(key: String, default: String): String =
    sys.env.getOrElse(key, default)
}
