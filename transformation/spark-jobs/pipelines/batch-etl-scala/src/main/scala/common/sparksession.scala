package scala.common.sparkSession

import org.apache.spark.sql.SparkSession

object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getSparkSession(useLocal: Boolean = false): SparkSession = {
    if (instance == null) {
      synchronized {
        if (instance == null) {

          var builder = SparkSession.builder()
            .appName("BatchETL-Scala")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

          if (useLocal) {
            val warehouseDir = new java.io.File("./spark-warehouse").getCanonicalPath
            builder = builder
              .config("spark.sql.warehouse.dir", warehouseDir)
          } else {
            val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://localhost:9001")
            builder = builder
              .config("spark.sql.warehouse.dir", s"$hdfsUri/hive/warehouse/")
              .config("spark.hadoop.fs.defaultFS", hdfsUri)
              .config("hive.metastore.uris", "")
          }

          // Detectamos si spark-submit ya definió spark.master
          val masterFromSubmit = System.getProperty("spark.master", "")

          if (masterFromSubmit == "" || masterFromSubmit.startsWith("local")) {
            builder = builder.master("local[*]")
            println(">>> SparkSession ejecutándose en LOCAL")
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
