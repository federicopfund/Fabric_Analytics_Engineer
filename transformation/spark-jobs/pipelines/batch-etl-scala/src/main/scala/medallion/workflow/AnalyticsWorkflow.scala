package medallion.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import medallion.analytics.BIChartGenerator

/**
 * WORKFLOW 2: BI Analytics — Generación de gráficos PNG desde Gold.
 *
 * Standalone: sbt "runMain medallion.workflow.AnalyticsWorkflow"
 */
object AnalyticsWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  def run(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║     WORKFLOW: BI ANALYTICS — Generación de Gráficos         ║")
    println("╠══════════════════════════════════════════════════════════════╣")
    println(s"║  Gold Path:  $goldPath")
    println(s"║  Output Dir: $outputDir")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    val dir = new java.io.File(outputDir)
    if (!dir.exists()) dir.mkdirs()

    BIChartGenerator.generate(spark, goldPath, outputDir)
  }

  def main(args: Array[String]): Unit = {
    val useHdfs = sys.env.get("HDFS_URI").exists(_.startsWith("hdfs://"))
    val goldPath = if (useHdfs) s"${sys.env("HDFS_URI")}/hive/warehouse/datalake/gold"
    else new java.io.File("./datalake/gold").getCanonicalPath
    val outputDir = sys.env.getOrElse("ANALYTICS_OUT", new java.io.File("./src/main/resources/analytics").getCanonicalPath)

    val spark = SparkSession.builder().appName("BI-Analytics").master("local[*]")
      .config("spark.driver.memory", "1g").config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try { run(spark, goldPath, outputDir) } finally { spark.stop() }
  }
}
