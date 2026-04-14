package simulator

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => F}

import java.time.LocalDate

/**
 * SimulatorApp — Genera ventas sintéticas a escala y escribe directamente a
 * s3a://{bucket}/VentasInternet.csv para que el pipeline Medallion las
 * procese en la siguiente ejecución (Bronze → Silver → Gold).
 *
 * La generación es DISTRIBUIDA (en workers vía mapPartitionsWithIndex)
 * para soportar 10M+ registros sin OOM en el driver.
 *
 * Parámetros (env vars inyectadas por submit-simulator.sh):
 *   SIM_ORDERS   Número de órdenes (default: 10000000)
 *   SIM_START    Fecha inicio YYYY-MM-DD
 *   SIM_END      Fecha fin YYYY-MM-DD
 *   SIM_SEED     Seed para reproducibilidad
 */
object SimulatorApp {

  val ventasSchema: StructType = StructType(Seq(
    StructField("Cod_Producto", IntegerType, nullable = false),
    StructField("Cod_Cliente", IntegerType, nullable = false),
    StructField("Cod_Territorio", IntegerType, nullable = false),
    StructField("NumeroOrden", StringType, nullable = false),
    StructField("Cantidad", IntegerType, nullable = false),
    StructField("PrecioUnitario", DoubleType, nullable = false),
    StructField("CostoUnitario", DoubleType, nullable = false),
    StructField("Impuesto", DoubleType, nullable = true),
    StructField("Flete", DoubleType, nullable = true),
    StructField("FechaOrden", TimestampType, nullable = true),
    StructField("FechaEnvio", TimestampType, nullable = true),
    StructField("FechaVencimiento", TimestampType, nullable = true),
    StructField("Cod_Promocion", IntegerType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val numOrders = params.getOrElse("orders",
      envOrDefault("SIM_ORDERS", "10000000")).toInt
    val startDate = LocalDate.parse(
      params.getOrElse("start",
        envOrDefault("SIM_START", LocalDate.now().minusDays(365).toString)))
    val endDate = LocalDate.parse(
      params.getOrElse("end",
        envOrDefault("SIM_END", LocalDate.now().toString)))
    val seed = params.getOrElse("seed",
      envOrDefault("SIM_SEED", System.currentTimeMillis().toString)).toLong

    println(
      s"""
         |╔══════════════════════════════════════════════════════╗
         |║  Simulador de Ventas — Medallion Pipeline           ║
         |╚══════════════════════════════════════════════════════╝
         |  Órdenes:    $numOrders
         |  Rango:      $startDate → $endDate
         |  Seed:       $seed
         |""".stripMargin)

    // ── Spark Session ──
    println("[1/4] Inicializando Spark y COS...")
    val spark = SparkSession.builder()
      .appName("SalesSimulator-Medallion")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val cosConfig = CosConfig.load()
    CosConfig.configureSparkS3A(spark, cosConfig)

    // ── Generación distribuida en workers ──
    println("[2/4] Generando ventas de forma distribuida...")
    val numPartitions = math.max(8, numOrders / 500000)
    val ordersPerPartition = numOrders / numPartitions
    val remainderOrders = numOrders % numPartitions

    // Broadcast params (no closures sobre objetos grandes)
    val startStr = startDate.toString
    val endStr = endDate.toString
    val seedVal = seed

    val rowsRDD = spark.sparkContext
      .parallelize(0 until numPartitions, numPartitions)
      .mapPartitionsWithIndex { (partIdx, _) =>
        val pStart = LocalDate.parse(startStr)
        val pEnd = LocalDate.parse(endStr)
        val pSeed = seedVal + partIdx
        val pOrders = ordersPerPartition + (if (partIdx < remainderOrders) 1 else 0)
        val orderOffset = partIdx * ordersPerPartition + math.min(partIdx, remainderOrders)

        val ventas = SalesGenerator.generate(pOrders, pStart, pEnd, orderOffset, pSeed)
        ventas.iterator.map { v =>
          Row(
            v.codProducto, v.codCliente, v.codTerritorio, v.numeroOrden,
            v.cantidad, v.precioUnitario, v.costoUnitario,
            v.impuesto, v.flete,
            v.fechaOrden, v.fechaEnvio, v.fechaVencimiento,
            v.codPromocion
          )
        }
      }

    val ventasDF = spark.createDataFrame(rowsRDD, ventasSchema)
    ventasDF.cache()

    val count = ventasDF.count()
    println(s"  ✓ $count registros generados en $numPartitions particiones")

    // ── Estadísticas ──
    println("[3/4] Calculando estadísticas...")
    printDistributedStats(ventasDF)

    // ── Escribir a VentasInternet.csv en la raíz del bucket RAW ──
    println("[4/4] Escribiendo VentasInternet.csv al bucket RAW...")
    val tmpPath = s"s3a://${cosConfig.bucketRaw}/_tmp_ventas_sim"
    val targetCsv = s"s3a://${cosConfig.bucketRaw}/VentasInternet.csv"

    ventasDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(tmpPath)

    // Spark escribe carpeta con part-xxx, renombramos a CSV plano
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(s"s3a://${cosConfig.bucketRaw}"), hadoopConf)
    val tmpDir = new org.apache.hadoop.fs.Path(tmpPath)
    val targetFile = new org.apache.hadoop.fs.Path(targetCsv)

    val partFile = fs.listStatus(tmpDir)
      .map(_.getPath)
      .find(_.getName.endsWith(".csv"))
      .getOrElse(throw new RuntimeException("No se generó archivo CSV"))

    if (fs.exists(targetFile)) fs.delete(targetFile, false)
    fs.rename(partFile, targetFile)
    fs.delete(tmpDir, true)

    println(s"  ✓ $count registros escritos en: $targetCsv")

    // ── Resúmenes ──
    println("\n── Resumen por Territorio ──")
    ventasDF.groupBy("Cod_Territorio")
      .agg(
        F.count("*").as("total_ventas"),
        F.sum(F.col("Cantidad") * F.col("PrecioUnitario")).as("ingreso_total")
      )
      .orderBy("Cod_Territorio")
      .show(false)

    println("── Resumen Mensual ──")
    ventasDF
      .withColumn("Mes", F.month(F.col("FechaOrden")))
      .groupBy("Mes")
      .agg(
        F.count("*").as("ordenes"),
        F.sum(F.col("Cantidad") * F.col("PrecioUnitario")).as("ingreso")
      )
      .orderBy("Mes")
      .show(false)

    ventasDF.unpersist()
    spark.stop()

    println(
      s"""
         |╔══════════════════════════════════════════════════════╗
         |║  ✔ Simulación completada exitosamente               ║
         |╚══════════════════════════════════════════════════════╝
         |  Registros:  $count
         |  Bucket:     ${cosConfig.bucketRaw}
         |  Archivo:    VentasInternet.csv
         |
         |  → El pipeline Medallion leerá estos datos automáticamente:
         |    Bronze → Silver → Gold
         |""".stripMargin)
  }

  private def printDistributedStats(df: org.apache.spark.sql.DataFrame): Unit = {
    val stats = df.agg(
      F.count("*").as("total"),
      F.sum(F.col("Cantidad") * F.col("PrecioUnitario")).as("ingreso"),
      F.sum(F.col("Cantidad") * F.col("CostoUnitario")).as("costo"),
      F.avg(F.col("Cantidad") * F.col("PrecioUnitario")).as("ticket_avg"),
      F.countDistinct("Cod_Producto").as("productos"),
      F.countDistinct("Cod_Cliente").as("clientes"),
      F.countDistinct("Cod_Territorio").as("territorios")
    ).collect()(0)

    val total = stats.getLong(0)
    val ingreso = stats.getDouble(1)
    val costo = stats.getDouble(2)
    val ticket = stats.getDouble(3)
    val prods = stats.getLong(4)
    val clients = stats.getLong(5)
    val terrs = stats.getLong(6)

    println(f"  Total órdenes:      $total%,d")
    println(f"  Ingreso bruto:      $$${ingreso}%,.2f")
    println(f"  Costo total:        $$${costo}%,.2f")
    println(f"  Margen bruto:       $$${ingreso - costo}%,.2f (${(ingreso - costo) / ingreso * 100}%.1f%%)")
    println(f"  Ticket promedio:    $$${ticket}%,.2f")
    println(f"  Productos únicos:   $prods%d")
    println(f"  Clientes únicos:    $clients%d")
    println(f"  Territorios:        $terrs%d")
  }

  private def parseArgs(args: Array[String]): Map[String, String] = {
    val m = scala.collection.mutable.Map[String, String]()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case key if key.startsWith("--") && i + 1 < args.length =>
          m(key.stripPrefix("--")) = args(i + 1)
          i += 2
        case _ => i += 1
      }
    }
    m.toMap
  }

  private def envOrDefault(key: String, default: String): String =
    Option(System.getenv(key)).filter(_.nonEmpty).getOrElse(default)
}
