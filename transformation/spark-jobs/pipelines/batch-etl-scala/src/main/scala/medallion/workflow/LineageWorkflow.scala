package medallion.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import medallion.infra.DataLakeIO

/**
 * WORKFLOW 5: Lineage — Trazabilidad de datos del pipeline.
 *
 * Captura y registra metadatos de linaje en cada capa:
 *   - Tablas fuente → tabla destino
 *   - Esquema producido (columnas y tipos)
 *   - Timestamps de creación
 *   - Volumen estimado (schema-only, sin Spark jobs pesados)
 *
 * Exporta un manifiesto JSON de linaje al directorio del datalake.
 */
object LineageWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  case class LineageRecord(
    layer: String,
    table: String,
    format: String,
    columns: Seq[String],
    columnTypes: Seq[String],
    sourceTables: Seq[String],
    timestamp: String
  )

  // Mapeo de fuentes por tabla (conocimiento del pipeline)
  private val BRONZE_SOURCES: Map[String, Seq[String]] = Map(
    "categoria" -> Seq("raw/Categoria.csv"), "subcategoria" -> Seq("raw/Subcategoria.csv"),
    "producto" -> Seq("raw/Producto.csv"), "ventasinternet" -> Seq("raw/VentasInternet.csv"),
    "sucursales" -> Seq("raw/Sucursales.csv"), "factmine" -> Seq("raw/FactMine.csv"),
    "mine" -> Seq("raw/Mine.csv")
  )

  private val SILVER_SOURCES: Map[String, Seq[String]] = Map(
    "catalogo_productos" -> Seq("bronze/producto", "bronze/subcategoria", "bronze/categoria"),
    "ventas_enriquecidas" -> Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria"),
    "resumen_ventas_mensuales" -> Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria"),
    "rentabilidad_producto" -> Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria"),
    "segmentacion_clientes" -> Seq("bronze/ventasinternet"),
    "produccion_operador" -> Seq("bronze/mine"),
    "eficiencia_minera" -> Seq("bronze/factmine"),
    "produccion_por_pais" -> Seq("bronze/mine")
  )

  private val GOLD_SOURCES: Map[String, Seq[String]] = Map(
    "dim_producto" -> Seq("silver/catalogo_productos", "silver/rentabilidad_producto"),
    "dim_cliente" -> Seq("silver/segmentacion_clientes"),
    "fact_ventas" -> Seq("silver/ventas_enriquecidas", "silver/segmentacion_clientes"),
    "kpi_ventas_mensuales" -> Seq("silver/resumen_ventas_mensuales"),
    "dim_operador" -> Seq("silver/produccion_operador"),
    "fact_produccion_minera" -> Seq("silver/eficiencia_minera", "silver/produccion_por_pais"),
    "kpi_mineria" -> Seq("silver/produccion_por_pais")
  )

  /** Captura el linaje de una capa completa */
  def captureLayerLineage(
    spark: SparkSession,
    layerName: String,
    layerPath: String,
    tables: Seq[String],
    format: String
  ): Seq[LineageRecord] = {
    val timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new java.util.Date())
    val sourcesMap = layerName.toUpperCase match {
      case "BRONZE" => BRONZE_SOURCES
      case "SILVER" => SILVER_SOURCES
      case "GOLD"   => GOLD_SOURCES
      case _        => Map.empty[String, Seq[String]]
    }

    println(s"  ╔═══ LINEAGE: $layerName ═══╗")

    val records = tables.flatMap { table =>
      val tablePath = s"$layerPath/$table"
      try {
        if (DataLakeIO.pathExists(tablePath)) {
          val df = spark.read.format(format).load(tablePath)
          val cols = df.columns.toSeq
          val types = df.schema.fields.map(_.dataType.simpleString).toSeq
          val sources = sourcesMap.getOrElse(table, Seq("unknown"))

          println(s"  ✔ $table — ${cols.length} cols ← [${sources.mkString(", ")}]")

          Some(LineageRecord(layerName, table, format, cols, types, sources, timestamp))
        } else {
          println(s"  ✗ $table — no encontrada")
          None
        }
      } catch {
        case e: Exception =>
          println(s"  ✗ $table — ERROR: ${e.getMessage}")
          None
      }
    }

    spark.catalog.clearCache()
    System.gc()

    println(s"  ─── $layerName: ${records.length}/${tables.length} tablas con linaje ───")
    println()
    records
  }

  /** Exporta manifiesto JSON de linaje */
  def exportManifest(records: Seq[LineageRecord], outputPath: String): Unit = {
    val dir = new java.io.File(outputPath)
    if (!dir.exists()) dir.mkdirs()

    val timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())
    val file = new java.io.File(s"$outputPath/lineage_$timestamp.json")

    val json = new StringBuilder()
    json.append("{\n")
    json.append(s"""  "generated_at": "$timestamp",\n""")
    json.append(s"""  "total_tables": ${records.length},\n""")
    json.append(s"""  "layers": {\n""")

    val byLayer = records.groupBy(_.layer)
    val layerEntries = byLayer.toSeq.sortBy(_._1).map { case (layer, recs) =>
      val tableEntries = recs.map { r =>
        val colsList = r.columns.zip(r.columnTypes).map { case (c, t) => s"""        {"name": "$c", "type": "$t"}""" }.mkString(",\n")
        val srcList = r.sourceTables.map(s => s""""$s"""").mkString(", ")
        s"""      "${r.table}": {
        "format": "${r.format}",
        "timestamp": "${r.timestamp}",
        "sources": [$srcList],
        "columns": [
$colsList
        ]
      }"""
      }.mkString(",\n")
      s"""    "$layer": {\n$tableEntries\n    }"""
    }.mkString(",\n")

    json.append(layerEntries)
    json.append("\n  }\n}")

    val writer = new java.io.PrintWriter(file)
    try { writer.write(json.toString()) } finally { writer.close() }

    println(s"  ✔ Lineage manifest exportado: ${file.getAbsolutePath}")
    logger.info(s"✔ Lineage manifest: ${file.getAbsolutePath}")
  }

  /** Imprime un resumen del grafo de linaje */
  def printLineageGraph(records: Seq[LineageRecord]): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║              DATA LINEAGE GRAPH                             ║")
    println("╚══════════════════════════════════════════════════════════════╝")

    val byLayer = records.groupBy(_.layer)
    Seq("BRONZE", "SILVER", "GOLD").foreach { layer =>
      byLayer.get(layer).foreach { recs =>
        println(s"\n  ═══ $layer (${recs.length} tablas) ═══")
        recs.foreach { r =>
          val arrow = r.sourceTables.mkString(" + ")
          println(s"  ${arrow} → $layer/${r.table} (${r.columns.length} cols)")
        }
      }
    }
    println()
  }
}
