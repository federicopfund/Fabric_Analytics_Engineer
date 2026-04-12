package medallion.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
 * WORKFLOW 3: Hive Audit — Verificación de tablas Delta en catálogo Hive.
 *
 * Standalone: sbt "runMain medallion.workflow.HiveWorkflow"
 */
object HiveWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  def run(spark: SparkSession, basePath: String, hdfsUri: String): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║         HIVE + DELTA AUDIT — Evaluación por tabla           ║")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()

    val databases = spark.sql("SHOW DATABASES").collect().map(_.getString(0))
    val dbExists = databases.contains("lakehouse")
    println(s"  Database 'lakehouse': ${if (dbExists) "✔ EXISTE" else "✗ NO EXISTE"}")
    if (!dbExists) { println("  ⚠ No se puede continuar sin la database 'lakehouse'."); return }
    spark.sql("USE lakehouse")
    println()

    val hiveTables = spark.sql("SHOW TABLES IN lakehouse").collect().map(_.getString(1)).toSet
    println(s"  Tablas registradas en Hive: ${hiveTables.size}")
    hiveTables.toSeq.sorted.foreach(t => println(s"    ├── $t"))
    println()

    val goldTables = Seq("dim_producto", "dim_cliente", "fact_ventas", "kpi_ventas_mensuales", "dim_operador", "fact_produccion_minera", "kpi_mineria")
    println("  ═══ GOLD LAYER — Delta Tables ═══")
    println(s"  Esperadas: ${goldTables.length}")
    println()

    var goldOk = 0; var goldFail = 0
    goldTables.foreach { table =>
      val hdfsPath = s"$basePath/gold/$table"
      val inHive = hiveTables.contains(table)
      val hdfsExists = try {
        val conf = new org.apache.hadoop.conf.Configuration()
        conf.set("fs.defaultFS", hdfsUri); conf.set("dfs.client.use.datanode.hostname", "true")
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        fs.exists(new org.apache.hadoop.fs.Path(s"$hdfsPath/_delta_log"))
      } catch { case _: Exception => false }

      val status = (inHive, hdfsExists) match {
        case (true, true) => "✔ OK"
        case (true, false) => "⚠ REGISTRADA pero sin datos Delta en HDFS"
        case (false, true) => "✗ DATOS EXISTEN pero NO registrada en Hive"
        case (false, false) => "✗ NO EXISTE"
      }
      println(s"  ┌── $table: $status")
      if (inHive && hdfsExists) goldOk += 1 else goldFail += 1
    }
    println(s"  ─── GOLD: $goldOk/${goldTables.length} OK  |  $goldFail errores ───")
    println()

    val silverTables = Seq("catalogo_productos", "ventas_enriquecidas", "resumen_ventas_mensuales", "rentabilidad_producto", "segmentacion_clientes", "produccion_operador", "eficiencia_minera", "produccion_por_pais")
    println("  ═══ SILVER LAYER — Parquet Tables ═══")
    var silverOk = 0; var silverFail = 0
    silverTables.foreach { table =>
      val hdfsPath = s"$basePath/silver/$table"
      val hiveName = s"silver_$table"
      val inHive = hiveTables.contains(hiveName)
      val hdfsExists = try {
        val conf = new org.apache.hadoop.conf.Configuration()
        conf.set("fs.defaultFS", hdfsUri); conf.set("dfs.client.use.datanode.hostname", "true")
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        fs.exists(new org.apache.hadoop.fs.Path(s"$hdfsPath/_SUCCESS"))
      } catch { case _: Exception => false }
      val status = (inHive, hdfsExists) match {
        case (true, true) => "✔ OK"
        case (true, false) => "⚠ REGISTRADA pero sin datos"
        case (false, true) => "✗ DATOS EXISTEN pero NO registrada"
        case _ => "✗ NO EXISTE"
      }
      println(s"  ┌── $hiveName: $status")
      if (inHive && hdfsExists) silverOk += 1 else silverFail += 1
    }
    println(s"  ─── SILVER: $silverOk/${silverTables.length} OK  |  $silverFail errores ───")
  }

  def main(args: Array[String]): Unit = {
    val hdfsUri = sys.env.getOrElse("HDFS_URI", "hdfs://namenode:9000")

    // Verificar disponibilidad de HDFS antes de iniciar sesión Hive
    if (!medallion.infra.HdfsManager.isAvailable(hdfsUri)) {
      println()
      println("╔══════════════════════════════════════════════════════════════╗")
      println("║  ⚠ HIVE AUDIT — HDFS no disponible                         ║")
      println(s"║  URI: $hdfsUri")                                 
      println("║  HiveWorkflow requiere HDFS + Hive Metastore activos.       ║")
      println("║  Asegurate de ejecutar el entorno Hadoop antes:             ║")
      println("║    docker-compose up -d (infrastructure/hadoop/)            ║")
      println("╚══════════════════════════════════════════════════════════════╝")
      return
    }

    val basePath = s"$hdfsUri/hive/warehouse/datalake"
    val hiveMetastoreUri = sys.env.getOrElse("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    val spark = try {
      SparkSession.builder().appName("Hive-Audit").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", hdfsUri)
        .config("hive.metastore.uris", hiveMetastoreUri)
        .enableHiveSupport().getOrCreate()
    } catch {
      case e: Exception =>
        println()
        println("╔══════════════════════════════════════════════════════════════╗")
        println("║  ✗ ERROR — No se pudo inicializar Hive Metastore            ║")
        println("╚══════════════════════════════════════════════════════════════╝")
        val rootCause = Option(e.getCause).getOrElse(e)
        if (rootCause.isInstanceOf[ClassCastException] &&
            rootCause.getMessage.contains("URLClassLoader")) {
          println("  Causa: Incompatibilidad Hive 3.x con JDK 11+")
          println("  Solución: Ejecutar con JDK 8, o agregar JVM flags:")
          println("    --add-opens java.base/java.net=ALL-UNNAMED")
          println("    --add-opens java.base/java.lang=ALL-UNNAMED")
        } else {
          println(s"  Causa: ${rootCause.getMessage}")
        }
        println(s"  Hive Metastore URI: $hiveMetastoreUri")
        return
    }
    try { run(spark, basePath, hdfsUri) } finally { spark.stop() }
  }
}
