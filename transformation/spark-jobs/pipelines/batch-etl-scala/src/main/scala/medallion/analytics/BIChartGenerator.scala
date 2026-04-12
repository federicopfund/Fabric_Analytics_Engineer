package medallion.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

import org.jfree.chart.{ChartFactory, ChartUtils, JFreeChart}
import org.jfree.chart.plot.{PlotOrientation, CategoryPlot, PiePlot}
import org.jfree.chart.renderer.category.{BarRenderer, LineAndShapeRenderer}
import org.jfree.chart.title.TextTitle
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.general.DefaultPieDataset
import org.jfree.chart.axis.{CategoryAxis, CategoryLabelPositions}

import java.awt.{Color, Font, BasicStroke}
import java.io.File

/**
 * Generador de gráficos PNG para Business Intelligence.
 * Consume tablas Gold (Delta Lake) y produce 10 visualizaciones
 * cubriendo Retail (ventas) y Mining (producción mineral).
 */
object BIChartGenerator {

  private val logger = Logger.getLogger(getClass.getName)

  private val WIDTH = 1200
  private val HEIGHT = 700

  private val PALETTE = Array(
    new Color(41, 128, 185), new Color(39, 174, 96), new Color(231, 76, 60),
    new Color(243, 156, 18), new Color(142, 68, 173), new Color(44, 62, 80),
    new Color(26, 188, 156), new Color(192, 57, 43), new Color(52, 73, 94),
    new Color(241, 196, 15)
  )

  private val BG_COLOR = new Color(250, 250, 250)
  private val TITLE_FONT = new Font("SansSerif", Font.BOLD, 18)
  private val LABEL_FONT = new Font("SansSerif", Font.PLAIN, 12)

  def generate(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  BI ANALYTICS — Chart Generation")
    logger.info("═══════════════════════════════════════")

    val dir = new File(outputDir)
    if (!dir.exists()) dir.mkdirs()

    var chartsOk = 0
    var chartsFail = 0

    val charts: Seq[(String, () => Unit)] = Seq(
      ("01_ingresos_por_categoria",       () => chartIngresosPorCategoria(spark, goldPath, outputDir)),
      ("02_margen_mensual_tendencia",     () => chartMargenMensualTendencia(spark, goldPath, outputDir)),
      ("03_segmentacion_clientes",        () => chartSegmentacionClientes(spark, goldPath, outputDir)),
      ("04_top10_productos_revenue",      () => chartTop10ProductosRevenue(spark, goldPath, outputDir)),
      ("05_clasificacion_rentabilidad",   () => chartClasificacionRentabilidad(spark, goldPath, outputDir)),
      ("06_ventas_mom_variacion",         () => chartVentasMoMVariacion(spark, goldPath, outputDir)),
      ("07_produccion_minera_por_pais",   () => chartProduccionMineraPorPais(spark, goldPath, outputDir)),
      ("08_eficiencia_operadores",        () => chartEficienciaOperadores(spark, goldPath, outputDir)),
      ("09_desperdicio_vs_produccion",    () => chartDesperdicioVsProduccion(spark, goldPath, outputDir)),
      ("10_ticket_promedio_mensual",      () => chartTicketPromedioMensual(spark, goldPath, outputDir))
    )

    charts.foreach { case (name, fn) =>
      try {
        fn()
        println(s"  ✔ $name.png")
        chartsOk += 1
      } catch {
        case e: Exception =>
          println(s"  ✗ $name — ERROR: ${e.getMessage}")
          logger.warn(s"Chart $name falló: ${e.getMessage}")
          chartsFail += 1
      }
      spark.catalog.clearCache()
      System.gc()
    }

    println()
    println(s"  ─── Resumen Analytics ───")
    println(s"  Charts generados: $chartsOk / ${charts.length}  |  Errores: $chartsFail")
    println(s"  Output: $outputDir")
    logger.info(s"✔ BI Analytics completado: $chartsOk/${charts.length} gráficos")
  }

  // ── RETAIL CHARTS ──

  private def chartIngresosPorCategoria(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_ventas_mensuales")
    val data = df.groupBy("categoria").agg(sum("ingreso_bruto").as("total_ingreso")).orderBy(desc("total_ingreso")).collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach(row => dataset.addValue(row.getAs[Double]("total_ingreso"), "Ingreso Bruto", row.getAs[String]("categoria")))
    val chart = ChartFactory.createBarChart("Ingreso Bruto por Categoría de Producto", "Categoría", "Ingreso Bruto (USD)", dataset, PlotOrientation.HORIZONTAL, false, true, false)
    styleBarChart(chart)
    saveChart(chart, outputDir, "01_ingresos_por_categoria")
  }

  private def chartMargenMensualTendencia(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_ventas_mensuales")
    val data = df.select("periodo", "categoria", "margen_bruto").orderBy("periodo").collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach(row => dataset.addValue(row.getAs[Double]("margen_bruto"), row.getAs[String]("categoria"), row.getAs[String]("periodo")))
    val chart = ChartFactory.createLineChart("Tendencia de Margen Bruto Mensual por Categoría", "Período", "Margen Bruto (USD)", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleLineChart(chart)
    saveChart(chart, outputDir, "02_margen_mensual_tendencia")
  }

  private def chartSegmentacionClientes(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/dim_cliente")
    val data = df.groupBy("segmento").agg(count("*").as("total")).orderBy(desc("total")).collect()
    val dataset = new DefaultPieDataset[String]()
    data.foreach(row => dataset.setValue(s"${row.getAs[String]("segmento")} (${row.getAs[Long]("total")})", row.getAs[Long]("total")))
    val chart = ChartFactory.createPieChart("Segmentación de Clientes (RFM Analysis)", dataset, true, true, false)
    stylePieChart(chart)
    saveChart(chart, outputDir, "03_segmentacion_clientes")
  }

  private def chartTop10ProductosRevenue(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/dim_producto")
    val data = df.orderBy(desc("revenue_total")).limit(10).select("producto_nombre", "revenue_total", "categoria").collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach { row =>
      val label = { val name = row.getAs[String]("producto_nombre"); if (name.length > 25) name.substring(0, 22) + "..." else name }
      dataset.addValue(row.getAs[Double]("revenue_total"), row.getAs[String]("categoria"), label)
    }
    val chart = ChartFactory.createBarChart("Top 10 Productos por Revenue Total", "Producto", "Revenue (USD)", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleBarChart(chart)
    chart.getCategoryPlot.getDomainAxis.setCategoryLabelPositions(CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 4))
    saveChart(chart, outputDir, "04_top10_productos_revenue")
  }

  private def chartClasificacionRentabilidad(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/dim_producto")
    val data = df.groupBy("clasificacion_rentabilidad").agg(count("*").as("total")).orderBy(desc("total")).collect()
    val dataset = new DefaultPieDataset[String]()
    data.foreach(row => dataset.setValue(s"${row.getAs[String]("clasificacion_rentabilidad")} (${row.getAs[Long]("total")})", row.getAs[Long]("total")))
    val chart = ChartFactory.createPieChart("Clasificación de Rentabilidad del Portafolio", dataset, true, true, false)
    stylePieChart(chart)
    saveChart(chart, outputDir, "05_clasificacion_rentabilidad")
  }

  private def chartVentasMoMVariacion(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_ventas_mensuales")
    val data = df.filter(col("variacion_mom_pct").isNotNull).select("periodo", "categoria", "variacion_mom_pct").orderBy("periodo").collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach(row => dataset.addValue(row.getAs[Double]("variacion_mom_pct"), row.getAs[String]("categoria"), row.getAs[String]("periodo")))
    val chart = ChartFactory.createBarChart("Variación Mensual de Ingresos (MoM %)", "Período", "Variación (%)", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleBarChart(chart)
    val plot = chart.getCategoryPlot
    plot.getDomainAxis.setCategoryLabelPositions(CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 4))
    plot.getRangeAxis.setAutoRange(true)
    saveChart(chart, outputDir, "06_ventas_mom_variacion")
  }

  // ── MINING CHARTS ──

  private def chartProduccionMineraPorPais(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_mineria")
    val data = df.select("pais", "produccion_neta", "total_mineral", "total_desperdicio").orderBy(desc("produccion_neta")).collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach { row =>
      val pais = row.getAs[String]("pais")
      dataset.addValue(row.getAs[Double]("total_mineral"), "Mineral Extraído", pais)
      dataset.addValue(row.getAs[Double]("total_desperdicio"), "Desperdicio", pais)
      dataset.addValue(row.getAs[Double]("produccion_neta"), "Producción Neta", pais)
    }
    val chart = ChartFactory.createBarChart("Producción Minera por País: Mineral vs Desperdicio", "País", "Toneladas", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleBarChart(chart)
    val renderer = chart.getCategoryPlot.getRenderer.asInstanceOf[BarRenderer]
    renderer.setSeriesPaint(0, PALETTE(0)); renderer.setSeriesPaint(1, PALETTE(2)); renderer.setSeriesPaint(2, PALETTE(1))
    saveChart(chart, outputDir, "07_produccion_minera_por_pais")
  }

  private def chartEficienciaOperadores(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/dim_operador")
    val data = df.groupBy("clasificacion_eficiencia").agg(count("*").as("total_operadores"), avg("total_mineral_extraido").as("promedio_mineral")).orderBy(desc("total_operadores")).collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach(row => dataset.addValue(row.getAs[Long]("total_operadores"), "Cantidad de Operadores", row.getAs[String]("clasificacion_eficiencia")))
    val chart = ChartFactory.createBarChart("Distribución de Eficiencia de Operadores Mineros", "Clasificación", "Cantidad de Operadores", dataset, PlotOrientation.VERTICAL, false, true, false)
    styleBarChart(chart)
    saveChart(chart, outputDir, "08_eficiencia_operadores")
  }

  private def chartDesperdicioVsProduccion(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_mineria")
    val data = df.select("pais", "tasa_desperdicio_pct", "tasa_aprovechamiento_pct", "evaluacion_operativa").orderBy("pais").collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach { row =>
      val pais = s"${row.getAs[String]("pais")}\n(${row.getAs[String]("evaluacion_operativa")})"
      dataset.addValue(row.getAs[Double]("tasa_aprovechamiento_pct"), "Aprovechamiento %", pais)
      dataset.addValue(row.getAs[Double]("tasa_desperdicio_pct"), "Desperdicio %", pais)
    }
    val chart = ChartFactory.createStackedBarChart("Eficiencia Operativa por País: Aprovechamiento vs Desperdicio", "País", "Porcentaje (%)", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleBarChart(chart)
    val renderer = chart.getCategoryPlot.getRenderer.asInstanceOf[BarRenderer]
    renderer.setSeriesPaint(0, PALETTE(1)); renderer.setSeriesPaint(1, PALETTE(2))
    saveChart(chart, outputDir, "09_desperdicio_vs_produccion")
  }

  private def chartTicketPromedioMensual(spark: SparkSession, goldPath: String, outputDir: String): Unit = {
    val df = spark.read.format("delta").load(s"$goldPath/kpi_ventas_mensuales")
    val data = df.select("periodo", "categoria", "ticket_promedio").orderBy("periodo").collect()
    val dataset = new DefaultCategoryDataset()
    data.foreach(row => dataset.addValue(row.getAs[Double]("ticket_promedio"), row.getAs[String]("categoria"), row.getAs[String]("periodo")))
    val chart = ChartFactory.createLineChart("Evolución del Ticket Promedio Mensual", "Período", "Ticket Promedio (USD)", dataset, PlotOrientation.VERTICAL, true, true, false)
    styleLineChart(chart)
    saveChart(chart, outputDir, "10_ticket_promedio_mensual")
  }

  // ── STYLING UTILITIES ──

  private def styleBarChart(chart: JFreeChart): Unit = {
    chart.setBackgroundPaint(BG_COLOR); chart.getTitle.setFont(TITLE_FONT)
    val plot = chart.getCategoryPlot
    plot.setBackgroundPaint(Color.WHITE)
    plot.setDomainGridlinePaint(new Color(220, 220, 220))
    plot.setRangeGridlinePaint(new Color(220, 220, 220))
    plot.getDomainAxis.setLabelFont(LABEL_FONT); plot.getRangeAxis.setLabelFont(LABEL_FONT)
    val renderer = plot.getRenderer
    for (i <- PALETTE.indices) renderer.setSeriesPaint(i, PALETTE(i % PALETTE.length))
  }

  private def styleLineChart(chart: JFreeChart): Unit = {
    chart.setBackgroundPaint(BG_COLOR); chart.getTitle.setFont(TITLE_FONT)
    val plot = chart.getCategoryPlot
    plot.setBackgroundPaint(Color.WHITE)
    plot.setDomainGridlinePaint(new Color(220, 220, 220))
    plot.setRangeGridlinePaint(new Color(220, 220, 220))
    plot.getDomainAxis.setLabelFont(LABEL_FONT); plot.getRangeAxis.setLabelFont(LABEL_FONT)
    plot.getDomainAxis.setCategoryLabelPositions(CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 4))
    val renderer = plot.getRenderer.asInstanceOf[LineAndShapeRenderer]
    renderer.setDefaultShapesVisible(true)
    for (i <- PALETTE.indices) { renderer.setSeriesPaint(i, PALETTE(i % PALETTE.length)); renderer.setSeriesStroke(i, new BasicStroke(2.5f)) }
  }

  private def stylePieChart(chart: JFreeChart): Unit = {
    chart.setBackgroundPaint(BG_COLOR); chart.getTitle.setFont(TITLE_FONT)
    val plot = chart.getPlot.asInstanceOf[PiePlot[_]]
    plot.setBackgroundPaint(Color.WHITE); plot.setLabelFont(LABEL_FONT)
    plot.setCircular(true); plot.setOutlineVisible(false)
  }

  private def saveChart(chart: JFreeChart, outputDir: String, name: String): Unit = {
    val file = new File(s"$outputDir/$name.png")
    ChartUtils.saveChartAsPNG(file, chart, WIDTH, HEIGHT)
    logger.info(s"  ✔ Chart exportado: ${file.getAbsolutePath}")
  }
}
