package simulator

import org.apache.spark.sql.SparkSession

/**
 * CosConfig — configuración de IBM Cloud Object Storage (COS) para el simulador.
 *
 * Soporta 3 modos de ejecución:
 *   - IBM Analytics Engine (IAE) — variables de entorno inyectadas por AE
 *   - K8s CronJob — variables inyectadas por secrets/configmaps
 *   - Local — variables de entorno o defaults para desarrollo
 */
object CosConfig {

  case class Config(
    accessKey:   String,
    secretKey:   String,
    endpoint:    String,
    bucketRaw:   String,
    bucketBronze: String,
    bucketSilver: String,
    bucketGold:   String
  )

  def load(): Config = Config(
    accessKey   = sys.env.getOrElse("COS_ACCESS_KEY",
                  sys.env.getOrElse("AWS_ACCESS_KEY_ID", "")),
    secretKey   = sys.env.getOrElse("COS_SECRET_KEY",
                  sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "")),
    endpoint    = sys.env.getOrElse("COS_ENDPOINT",
                  "s3.us-south.cloud-object-storage.appdomain.cloud"),
    bucketRaw   = sys.env.getOrElse("COS_BUCKET_RAW",    "datalake-raw-us-south-dev"),
    bucketBronze = sys.env.getOrElse("COS_BUCKET_BRONZE", "datalake-bronze-us-south-dev"),
    bucketSilver = sys.env.getOrElse("COS_BUCKET_SILVER", "datalake-silver-us-south-dev"),
    bucketGold   = sys.env.getOrElse("COS_BUCKET_GOLD",   "datalake-gold-us-south-dev")
  )

  def configureSparkS3A(spark: SparkSession, config: Config): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.access.key", config.accessKey)
    hadoopConf.set("fs.s3a.secret.key", config.secretKey)
    hadoopConf.set("fs.s3a.endpoint", s"https://${config.endpoint}")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoopConf.set("fs.s3a.fast.upload", "true")
    hadoopConf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
  }
}
