package medallion.config

import org.apache.spark.sql.SparkSession

/**
 * Fábrica singleton de SparkSession.
 * Configura Spark para modo local (low-memory) o cluster HDFS + Hive.
 */
object SparkFactory {

  @transient private var instance: SparkSession = _

  def getOrCreate(useLocal: Boolean = false): SparkSession = {
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

          if (useLocal) {
            val warehouseDir = new java.io.File("./spark-warehouse").getCanonicalPath
            builder = builder
              .config("spark.sql.warehouse.dir", warehouseDir)
              .config("spark.sql.catalogImplementation", "in-memory")
          } else {
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
          }

          val masterFromSubmit = System.getProperty("spark.master", "")

          if (masterFromSubmit == "" || masterFromSubmit.startsWith("local")) {
            builder = builder.master("local[2]")
            println(">>> SparkSession ejecutándose en LOCAL (2 threads — parallel workflow mode)")
          } else {
            println(s">>> SparkSession ejecutándose en CLUSTER: $masterFromSubmit")
          }

          instance = builder.getOrCreate()
        }
      }
    }
    instance
  }
}
