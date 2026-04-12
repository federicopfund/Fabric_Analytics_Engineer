package medallion.config

import medallion.schema.CsvSchemas
import org.apache.spark.sql.types.StructType

/**
 * TableRegistry — Fuente única de verdad para todas las tablas del pipeline.
 *
 * Centraliza:
 *   - Nombres de tablas por capa (Bronze, Silver, Gold)
 *   - Formato de almacenamiento
 *   - Dependencias (sources) para linaje
 *   - Claves de deduplicación para Bronze
 *   - Esquemas CSV para ingesta
 *
 * Elimina duplicación de listas de tablas en Pipeline, EtlWorkflow,
 * HiveWorkflow, LineageWorkflow, SilverLayer, GoldLayer.
 */
object TableRegistry {

  // ═══════════════════════════════════════════════════
  // Enums
  // ═══════════════════════════════════════════════════

  sealed trait Layer
  case object Raw extends Layer
  case object Bronze extends Layer
  case object Silver extends Layer
  case object Gold extends Layer

  sealed trait Format
  case object Csv extends Format
  case object Parquet extends Format
  case object Delta extends Format

  // ═══════════════════════════════════════════════════
  // Table Definition
  // ═══════════════════════════════════════════════════

  case class TableDef(
    name: String,
    layer: Layer,
    format: Format,
    sources: Seq[String],
    deduplicateKeys: Seq[String] = Seq.empty,
    csvFileName: Option[String] = None,
    csvSchema: Option[StructType] = None
  )

  // ═══════════════════════════════════════════════════
  // BRONZE — 7 tablas (RAW CSV → Parquet)
  // ═══════════════════════════════════════════════════

  val bronze: Seq[TableDef] = Seq(
    TableDef("categoria",       Bronze, Parquet, Seq("raw/Categoria.csv"),
      Seq("Cod_Categoria"),       Some("Categoria.csv"),       Some(CsvSchemas.categoriaSchema)),
    TableDef("subcategoria",    Bronze, Parquet, Seq("raw/Subcategoria.csv"),
      Seq("Cod_SubCategoria"),    Some("Subcategoria.csv"),    Some(CsvSchemas.subcategoriaSchema)),
    TableDef("producto",        Bronze, Parquet, Seq("raw/Producto.csv"),
      Seq("Cod_Producto"),        Some("Producto.csv"),        Some(CsvSchemas.productoSchema)),
    TableDef("ventasinternet",  Bronze, Parquet, Seq("raw/VentasInternet.csv"),
      Seq("NumeroOrden", "Cod_Producto"), Some("VentasInternet.csv"), Some(CsvSchemas.ventasInternetSchema)),
    TableDef("sucursales",      Bronze, Parquet, Seq("raw/Sucursales.csv"),
      Seq("Cod_Sucursal"),        Some("Sucursales.csv"),      Some(CsvSchemas.sucursalesSchema)),
    TableDef("factmine",        Bronze, Parquet, Seq("raw/FactMine.csv"),
      Seq("TruckID", "ProjectID", "Date"), Some("FactMine.csv"), Some(CsvSchemas.factMineSchema)),
    TableDef("mine",            Bronze, Parquet, Seq("raw/Mine.csv"),
      Seq("TruckID", "ProjectID", "OperatorID", "Date"), Some("Mine.csv"), Some(CsvSchemas.mineSchema))
  )

  // ═══════════════════════════════════════════════════
  // SILVER — 8 tablas (Bronze Parquet → Parquet)
  // ═══════════════════════════════════════════════════

  val silver: Seq[TableDef] = Seq(
    TableDef("catalogo_productos",       Silver, Parquet,
      Seq("bronze/producto", "bronze/subcategoria", "bronze/categoria")),
    TableDef("ventas_enriquecidas",      Silver, Parquet,
      Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria")),
    TableDef("resumen_ventas_mensuales", Silver, Parquet,
      Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria")),
    TableDef("rentabilidad_producto",    Silver, Parquet,
      Seq("bronze/ventasinternet", "bronze/producto", "bronze/subcategoria", "bronze/categoria")),
    TableDef("segmentacion_clientes",    Silver, Parquet,
      Seq("bronze/ventasinternet")),
    TableDef("produccion_operador",      Silver, Parquet,
      Seq("bronze/mine")),
    TableDef("eficiencia_minera",        Silver, Parquet,
      Seq("bronze/factmine")),
    TableDef("produccion_por_pais",      Silver, Parquet,
      Seq("bronze/mine"))
  )

  // ═══════════════════════════════════════════════════
  // GOLD — 7 tablas (Silver Parquet → Delta Lake)
  // ═══════════════════════════════════════════════════

  val gold: Seq[TableDef] = Seq(
    TableDef("dim_producto",          Gold, Delta,
      Seq("silver/catalogo_productos", "silver/rentabilidad_producto")),
    TableDef("dim_cliente",           Gold, Delta,
      Seq("silver/segmentacion_clientes")),
    TableDef("fact_ventas",           Gold, Delta,
      Seq("silver/ventas_enriquecidas", "silver/segmentacion_clientes")),
    TableDef("kpi_ventas_mensuales",  Gold, Delta,
      Seq("silver/resumen_ventas_mensuales")),
    TableDef("dim_operador",          Gold, Delta,
      Seq("silver/produccion_operador")),
    TableDef("fact_produccion_minera", Gold, Delta,
      Seq("silver/eficiencia_minera", "silver/produccion_por_pais")),
    TableDef("kpi_mineria",           Gold, Delta,
      Seq("silver/produccion_por_pais"))
  )

  // ═══════════════════════════════════════════════════
  // Accessors
  // ═══════════════════════════════════════════════════

  def all: Seq[TableDef] = bronze ++ silver ++ gold

  def forLayer(layer: Layer): Seq[TableDef] = all.filter(_.layer == layer)

  def names(layer: Layer): Seq[String] = forLayer(layer).map(_.name)

  def bronzeNames: Seq[String] = names(Bronze)
  def silverNames: Seq[String] = names(Silver)
  def goldNames: Seq[String]   = names(Gold)

  def totalTables: Int = all.length

  /** Mapeo de fuentes por tabla para LineageWorkflow */
  def sourcesMap(layer: Layer): Map[String, Seq[String]] =
    forLayer(layer).map(t => t.name -> t.sources).toMap

  /** Formato string para lectura Spark */
  def formatString(f: Format): String = f match {
    case Csv     => "csv"
    case Parquet => "parquet"
    case Delta   => "delta"
  }

  def formatString(layer: Layer): String = layer match {
    case Raw    => "csv"
    case Bronze => "parquet"
    case Silver => "parquet"
    case Gold   => "delta"
  }
}
