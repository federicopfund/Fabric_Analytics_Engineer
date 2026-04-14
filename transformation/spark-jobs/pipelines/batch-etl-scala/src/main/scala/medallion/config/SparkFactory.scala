package medallion.config

import org.apache.spark.sql.SparkSession
import medallion.config.IbmCloudConfig.{ExecutionMode, IbmAnalyticsEngine, HdfsCluster, LocalMode}

/**
 * Fábrica singleton de SparkSession.
 * Configura Spark según el modo de ejecución:
 *   - LOCAL:  filesystem local, in-memory catalog
 *   - HDFS:   Hadoop filesystem + Hive metastore
 *   - IBM_AE: IBM Analytics Engine Serverless (S3A → COS)
 */
object SparkFactory {

  @transient private var instance: SparkSession = _

  /**
   * Crea o retorna SparkSession según el modo detectado.
   * Mantiene compatibilidad con llamadas legacy (useLocal).
   */
  def getOrCreate(useLocal: Boolean = false): SparkSession =
    getOrCreate(if (useLocal) LocalMode else HdfsCluster)

  def getOrCreate(mode: ExecutionMode): SparkSession = {
    if (instance == null) {
      synchronized {
        if (instance == null) {

          var builder = SparkSession.builder()
            .appName("BatchETL-Lakehouse")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.autoBroadcastJoinThreshold", "5MB")
            .config("spark.memory.fraction", "0.5")
            .config("spark.memory.storageFraction", "0.2")
            .config("spark.executor.memory", "512m")
            .config("spark.cleaner.periodicGC.interval", "5min")
            // Delta Lake extensions
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

          mode match {
            case IbmAnalyticsEngine =>
              // IBM COS vía S3A — no requiere HDFS ni Hive local
              val cos = IbmCloudConfig.loadCosConfig()
              builder = builder
                .config("spark.hadoop.fs.s3a.access.key", cos.accessKey)
                .config("spark.hadoop.fs.s3a.secret.key", cos.secretKey)
                .config("spark.hadoop.fs.s3a.endpoint", s"https://${cos.endpoint}")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
                .config("spark.sql.catalogImplementation", "in-memory")

            case HdfsCluster =>
              val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")
              builder = builder
                .config("spark.sql.warehouse.dir", s"$hdfsUri/hive/warehouse/")
                .config("spark.hadoop.fs.defaultFS", hdfsUri)
                .config("hive.metastore.uris", sys.env.getOrElse("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
                .config("hive.metastore.warehouse.dir", s"$hdfsUri/hive/warehouse/")
                .config("spark.sql.hive.metastore.version", "3.1.3")
                .config("spark.sql.hive.metastore.jars", "builtin")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .config("spark.hadoop.dfs.replication", "1")
                .enableHiveSupport()

            case LocalMode =>
              val warehouseDir = new java.io.File("./spark-warehouse").getCanonicalPath
              builder = builder
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.sql.catalogImplementation", "in-memory")
          }

          val masterFromSubmit = System.getProperty("spark.master", "")

          mode match {
            case IbmAnalyticsEngine if masterFromSubmit.nonEmpty && !masterFromSubmit.startsWith("local") =>
              // En AE Serverless, Spark master lo inyecta el servicio
              println(s">>> SparkSession ejecutándose en IBM Analytics Engine: $masterFromSubmit")
            case IbmAnalyticsEngine =>
              // AE sin master explícito — no forzar local, dejar que AE lo resuelva
              println(s">>> SparkSession en IBM Analytics Engine (master será asignado por el servicio)")
            case _ if masterFromSubmit == "" || masterFromSubmit.startsWith("local") =>
              builder = builder.master("local[2]")
              println(s">>> SparkSession ejecutándose en LOCAL (2 threads — modo: $mode)")
            case _ =>
              println(s">>> SparkSession ejecutándose en CLUSTER: $masterFromSubmit")
          }

          instance = builder.getOrCreate()
        }
      }
    }
    instance
  }

  /** Reset para testing o re-inicialización con modo diferente. */
  def reset(): Unit = synchronized {
    if (instance != null) {
      instance.stop()
      instance = null
    }
  }
}
