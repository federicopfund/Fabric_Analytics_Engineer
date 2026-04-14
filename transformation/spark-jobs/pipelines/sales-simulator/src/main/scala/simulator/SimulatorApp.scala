package simulator

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * SimulatorApp — Punto de entrada de la aplicación.
 *
 * Genera ventas sintéticas realistas y las escribe directamente al bucket RAW
 * de IBM COS en formato CSV, compatible con el pipeline Medallion existente.
 *
 * Las ventas se appendean al archivo VentasInternet.csv de forma que el
 * pipeline batch-etl-scala las procese automáticamente en la siguiente
 * ejecución (Bronze → Silver → Gold).
 *
 * Uso:
 *   spark-submit --class simulator.SimulatorApp sales-simulator-assembly-1.0.0.jar \
 *     [--orders N] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--seed N]
 *
 * Args:
 *   --orders N          Número de órdenes a generar (default: 1000)
 *   --start YYYY-MM-DD  Fecha inicio (default: hace 30 días)
 *   --end YYYY-MM-DD    Fecha fin (default: hoy)
 *   --seed N            Seed para reproducibilidad (default: timestamp)
 *   --dry-run           Solo genera y muestra estadísticas, no escribe a COS
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

    // CLI args take priority, then env vars, then defaults
    val numOrders = params.getOrElse("orders",
      envOrDefault("SIM_ORDERS", "1000")).toInt
    val startDate = LocalDate.parse(
      params.getOrElse("start",
        envOrDefault("SIM_START", LocalDate.now().minusDays(30).toString)))
    val endDate = LocalDate.parse(
      params.getOrElse("end",
        envOrDefault("SIM_END", LocalDate.now().toString)))
    val seed = params.getOrElse("seed",
      envOrDefault("SIM_SEED", System.currentTimeMillis().toString)).toLong
    val dryRun = params.contains("dry-run")

    println(
      s"""
         |╔══════════════════════════════════════════════════════╗
         |║  Simulador de Ventas — Medallion Pipeline           ║
         |╚══════════════════════════════════════════════════════╝
         |  Órdenes:    $numOrders
         |  Rango:      $startDate → $endDate
         |  Seed:       $seed
         |  Dry-run:    $dryRun
         |""".stripMargin)

    // ── Generar ventas ──
    println("[1/4] Generando ventas sintéticas...")
    val ventas = SalesGenerator.generate(
      numOrders   = numOrders,
      startDate   = startDate,
      endDate     = endDate,
      orderOffset = 0,
      seed        = seed
    )
    println(s"  ✓ ${ventas.size} registros generados")

    // ── Estadísticas ──
    println("[2/4] Calculando estadísticas...")
    printStats(ventas)

    if (dryRun) {
      println("\n⚠ Modo dry-run: no se escriben datos a COS.")
      println("✔ Simulación completada.")
      return
    }

    // ── Spark Session ──
    println("[3/4] Inicializando Spark y COS...")
    val spark = SparkSession.builder()
      .appName("SalesSimulator-Medallion")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val cosConfig = CosConfig.load()
    CosConfig.configureSparkS3A(spark, cosConfig)

    import spark.implicits._

    // ── Convertir a DataFrame ──
    val ventasDF = spark.createDataFrame(
      spark.sparkContext.parallelize(ventas.map(v =>
        org.apache.spark.sql.Row(
          v.codProducto, v.codCliente, v.codTerritorio, v.numeroOrden,
          v.cantidad, v.precioUnitario, v.costoUnitario,
          v.impuesto, v.flete,
          v.fechaOrden, v.fechaEnvio, v.fechaVencimiento,
          v.codPromocion
        )
      )),
      ventasSchema
    )

    // ── Escribir al bucket RAW como CSV (append) ──
    println("[4/4] Escribiendo al bucket RAW de COS...")
    val rawPath = s"s3a://${cosConfig.bucketRaw}/VentasInternet_sim"
    val timestamp = java.time.Instant.now().toString.replace(":", "-").take(19)

    ventasDF
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv(s"$rawPath/batch_$timestamp")

    val count = ventasDF.count()
    println(s"  ✓ $count registros escritos en: $rawPath/batch_$timestamp")

    // ── Resumen por territorio ──
    println("\n── Resumen por Territorio ──")
    ventasDF.groupBy("Cod_Territorio")
      .agg(
        org.apache.spark.sql.functions.count("*").as("total_ventas"),
        org.apache.spark.sql.functions.sum(
          org.apache.spark.sql.functions.col("Cantidad") *
          org.apache.spark.sql.functions.col("PrecioUnitario")
        ).as("ingreso_total")
      )
      .orderBy("Cod_Territorio")
      .show(false)

    // ── Resumen mensual ──
    println("── Resumen Mensual ──")
    ventasDF
      .withColumn("Mes", org.apache.spark.sql.functions.month(
        org.apache.spark.sql.functions.col("FechaOrden")))
      .groupBy("Mes")
      .agg(
        org.apache.spark.sql.functions.count("*").as("ordenes"),
        org.apache.spark.sql.functions.sum(
          org.apache.spark.sql.functions.col("Cantidad") *
          org.apache.spark.sql.functions.col("PrecioUnitario")
        ).as("ingreso")
      )
      .orderBy("Mes")
      .show(false)

    spark.stop()

    println(
      s"""
         |╔══════════════════════════════════════════════════════╗
         |║  ✔ Simulación completada exitosamente               ║
         |╚══════════════════════════════════════════════════════╝
         |  Registros:  $count
         |  Bucket:     ${cosConfig.bucketRaw}
         |  Path:       VentasInternet_sim/batch_$timestamp
         |
         |  → Ejecutar pipeline Medallion para procesar:
         |    Bronze → Silver → Gold
         |""".stripMargin)
  }

  private def printStats(ventas: Seq[SalesGenerator.VentaRecord]): Unit = {
    val totalRevenue = ventas.map(v => v.cantidad * v.precioUnitario).sum
    val totalCost    = ventas.map(v => v.cantidad * v.costoUnitario).sum
    val avgOrder     = if (ventas.nonEmpty) totalRevenue / ventas.size else 0.0
    val uniqueProducts  = ventas.map(_.codProducto).distinct.size
    val uniqueCustomers = ventas.map(_.codCliente).distinct.size
    val uniqueTerritories = ventas.map(_.codTerritorio).distinct.size

    val bySegment = ventas.groupBy { v =>
      ProductCatalog.priceMap.get(v.codProducto).map(_._1) match {
        case Some(p) if p > 1000 => "Bicicletas"
        case Some(p) if p > 30   => "Prendas/Cascos"
        case _                   => "Accesorios"
      }
    }.map { case (seg, vs) => (seg, vs.size, vs.map(v => v.cantidad * v.precioUnitario).sum) }

    println(f"  Total órdenes:      ${ventas.size}%,d")
    println(f"  Ingreso bruto:      $$${totalRevenue}%,.2f")
    println(f"  Costo total:        $$${totalCost}%,.2f")
    println(f"  Margen bruto:       $$${totalRevenue - totalCost}%,.2f (${(totalRevenue - totalCost) / totalRevenue * 100}%.1f%%)")
    println(f"  Ticket promedio:    $$${avgOrder}%,.2f")
    println(f"  Productos únicos:   $uniqueProducts%d")
    println(f"  Clientes únicos:    $uniqueCustomers%d")
    println(f"  Territorios:        $uniqueTerritories%d")
    println("  ── Distribución por segmento ──")
    bySegment.toSeq.sortBy(-_._3).foreach { case (seg, count, rev) =>
      println(f"     $seg%-18s → $count%,5d órdenes | $$${rev}%,.2f")
    }
  }

  private def parseArgs(args: Array[String]): Map[String, String] = {
    val m = scala.collection.mutable.Map[String, String]()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--dry-run" =>
          m("dry-run") = "true"
          i += 1
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
