package scala.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import scala.common.DataLakeIO

object SilverLayer {

  private val logger = Logger.getLogger(getClass.getName)

  // ============================================================
  // ORQUESTADOR SILVER — Lógica de negocio desde bronze → silver
  // ============================================================
  def process(spark: SparkSession, bronzePath: String, silverPath: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  SILVER LAYER — Business Logic")
    logger.info("═══════════════════════════════════════")

    // Cargar tablas bronze y registrar como vistas SQL
    registerBronzeTables(spark, bronzePath)

    // --- RETAIL DOMAIN ---
    buildCatalogoProductos(spark, silverPath)
    buildVentasEnriquecidas(spark, silverPath)
    buildResumenVentasMensuales(spark, silverPath)
    buildRentabilidadProducto(spark, silverPath)
    buildSegmentacionClientes(spark, silverPath)

    // --- MINING DOMAIN ---
    buildProduccionOperador(spark, silverPath)
    buildEficienciaMinera(spark, silverPath)
    buildProduccionPorPais(spark, silverPath)

    logger.info("✔ SILVER LAYER completada")
  }

  // ============================================================
  // REGISTRAR TABLAS BRONZE COMO VISTAS SQL
  // ============================================================
  private def registerBronzeTables(spark: SparkSession, bronzePath: String): Unit = {
    val tables = Seq("categoria", "subcategoria", "producto", "ventasinternet", "sucursales", "factmine", "mine")
    tables.foreach { table =>
      val path = s"$bronzePath/$table"
      if (DataLakeIO.pathExists(path)) {
        spark.read.parquet(path).createOrReplaceTempView(table)
        logger.info(s"  ✓ Vista registrada: $table")
      } else {
        logger.warn(s"  ✗ Tabla bronze no encontrada: $table")
      }
    }
  }

  // ============================================================
  // RETAIL: Catálogo completo de productos con jerarquía
  // JOIN Producto ← Subcategoria ← Categoria
  // ============================================================
  private def buildCatalogoProductos(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "catalogo_productos"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        p.Cod_Producto,
        p.Producto,
        p.Color,
        sc.Cod_SubCategoria,
        sc.SubCategoria,
        c.Cod_Categoria,
        c.Categoria
      FROM producto p
      INNER JOIN subcategoria sc ON p.Cod_SubCategoria = sc.Cod_SubCategoria
      INNER JOIN categoria c     ON sc.Cod_Categoria   = c.Cod_Categoria
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(10, truncate = false)
  }

  // ============================================================
  // RETAIL: Ventas enriquecidas con métricas financieras
  // JOIN VentasInternet + CatálogoProductos + cálculos
  // ============================================================
  private def buildVentasEnriquecidas(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "ventas_enriquecidas"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        v.NumeroOrden,
        v.Cod_Cliente,
        v.Cod_Territorio,
        v.Cod_Producto,
        p.Producto,
        sc.SubCategoria,
        c.Categoria,
        p.Color,
        v.Cantidad,
        v.PrecioUnitario,
        v.CostoUnitario,
        ROUND(v.Cantidad * v.PrecioUnitario, 4)                              AS Ingreso_Bruto,
        ROUND(v.Cantidad * v.CostoUnitario, 4)                               AS Costo_Total,
        ROUND(v.Cantidad * v.PrecioUnitario - v.Cantidad * v.CostoUnitario, 4) AS Margen_Bruto,
        ROUND((v.Cantidad * v.PrecioUnitario - v.Cantidad * v.CostoUnitario)
              / (v.Cantidad * v.PrecioUnitario) * 100, 2)                     AS Pct_Margen,
        v.Impuesto,
        v.Flete,
        ROUND(v.Cantidad * v.PrecioUnitario - v.Cantidad * v.CostoUnitario
              - COALESCE(v.Impuesto, 0) - COALESCE(v.Flete, 0), 4)           AS Ganancia_Neta,
        v.FechaOrden,
        v.FechaEnvio,
        v.FechaVencimiento,
        DATEDIFF(v.FechaEnvio, v.FechaOrden)                                  AS Dias_Envio,
        DATEDIFF(v.FechaVencimiento, v.FechaOrden)                            AS Dias_Vencimiento,
        CASE
          WHEN DATEDIFF(v.FechaEnvio, v.FechaOrden) <= 3 THEN 'Express'
          WHEN DATEDIFF(v.FechaEnvio, v.FechaOrden) <= 7 THEN 'Standard'
          ELSE 'Delayed'
        END                                                                    AS Tipo_Envio,
        v.Cod_Promocion,
        CASE WHEN v.Cod_Promocion > 1 THEN true ELSE false END                AS Tiene_Promocion
      FROM ventasinternet v
      INNER JOIN producto p      ON v.Cod_Producto    = p.Cod_Producto
      LEFT  JOIN subcategoria sc ON p.Cod_SubCategoria = sc.Cod_SubCategoria
      LEFT  JOIN categoria c     ON sc.Cod_Categoria   = c.Cod_Categoria
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(10, truncate = false)
  }

  // ============================================================
  // RETAIL: Resumen de ventas mensuales por categoría
  // ============================================================
  private def buildResumenVentasMensuales(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "resumen_ventas_mensuales"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        YEAR(v.FechaOrden)                                    AS Anio,
        MONTH(v.FechaOrden)                                   AS Mes,
        c.Categoria,
        COUNT(DISTINCT v.NumeroOrden)                          AS Total_Ordenes,
        COUNT(DISTINCT v.Cod_Cliente)                          AS Clientes_Unicos,
        SUM(v.Cantidad)                                        AS Unidades_Vendidas,
        ROUND(SUM(v.Cantidad * v.PrecioUnitario), 2)           AS Ingreso_Bruto,
        ROUND(SUM(v.Cantidad * v.CostoUnitario), 2)            AS Costo_Total,
        ROUND(SUM(v.Cantidad * v.PrecioUnitario
                 - v.Cantidad * v.CostoUnitario), 2)           AS Margen_Bruto,
        ROUND(AVG(v.Cantidad * v.PrecioUnitario), 2)           AS Ticket_Promedio
      FROM ventasinternet v
      INNER JOIN producto p      ON v.Cod_Producto    = p.Cod_Producto
      LEFT  JOIN subcategoria sc ON p.Cod_SubCategoria = sc.Cod_SubCategoria
      LEFT  JOIN categoria c     ON sc.Cod_Categoria   = c.Cod_Categoria
      GROUP BY YEAR(v.FechaOrden), MONTH(v.FechaOrden), c.Categoria
      ORDER BY Anio, Mes, Categoria
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(20, truncate = false)
  }

  // ============================================================
  // RETAIL: Rentabilidad por producto (top performers)
  // ============================================================
  private def buildRentabilidadProducto(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "rentabilidad_producto"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        p.Cod_Producto,
        p.Producto,
        p.Color,
        sc.SubCategoria,
        c.Categoria,
        COUNT(*)                                                                AS Veces_Vendido,
        SUM(v.Cantidad)                                                         AS Unidades_Totales,
        ROUND(SUM(v.Cantidad * v.PrecioUnitario), 2)                            AS Revenue_Total,
        ROUND(SUM(v.Cantidad * v.CostoUnitario), 2)                             AS Costo_Total,
        ROUND(SUM(v.Cantidad * v.PrecioUnitario - v.Cantidad * v.CostoUnitario), 2) AS Margen_Total,
        ROUND(AVG((v.PrecioUnitario - v.CostoUnitario) / v.PrecioUnitario * 100), 2) AS Pct_Margen_Promedio,
        ROUND(AVG(v.PrecioUnitario), 2)                                         AS Precio_Promedio
      FROM ventasinternet v
      INNER JOIN producto p      ON v.Cod_Producto    = p.Cod_Producto
      LEFT  JOIN subcategoria sc ON p.Cod_SubCategoria = sc.Cod_SubCategoria
      LEFT  JOIN categoria c     ON sc.Cod_Categoria   = c.Cod_Categoria
      GROUP BY p.Cod_Producto, p.Producto, p.Color, sc.SubCategoria, c.Categoria
      ORDER BY Margen_Total DESC
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(15, truncate = false)
  }

  // ============================================================
  // RETAIL: Segmentación de clientes (RFM simplificado)
  // Recency / Frequency / Monetary
  // ============================================================
  private def buildSegmentacionClientes(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "segmentacion_clientes"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      WITH cliente_metricas AS (
        SELECT
          Cod_Cliente,
          COUNT(DISTINCT NumeroOrden)                             AS Frecuencia,
          ROUND(SUM(Cantidad * PrecioUnitario), 2)                AS Monetary,
          MAX(FechaOrden)                                         AS Ultima_Compra,
          MIN(FechaOrden)                                         AS Primera_Compra,
          DATEDIFF(MAX(FechaOrden), MIN(FechaOrden))              AS Dias_Como_Cliente,
          ROUND(AVG(Cantidad * PrecioUnitario), 2)                AS Ticket_Promedio,
          SUM(Cantidad)                                           AS Unidades_Totales
        FROM ventasinternet
        GROUP BY Cod_Cliente
      )
      SELECT
        Cod_Cliente,
        Frecuencia,
        Monetary,
        Ticket_Promedio,
        Unidades_Totales,
        Ultima_Compra,
        Primera_Compra,
        Dias_Como_Cliente,
        CASE
          WHEN Frecuencia >= 10 AND Monetary >= 5000 THEN 'VIP'
          WHEN Frecuencia >= 5  AND Monetary >= 1000 THEN 'Premium'
          WHEN Frecuencia >= 2                        THEN 'Regular'
          ELSE 'Ocasional'
        END AS Segmento
      FROM cliente_metricas
      ORDER BY Monetary DESC
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(15, truncate = false)
  }

  // ============================================================
  // MINING: Producción por operador
  // ============================================================
  private def buildProduccionOperador(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "produccion_operador"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        OperatorID,
        FirstName,
        LastName,
        Country,
        COUNT(DISTINCT TruckID)                          AS Trucks_Operados,
        COUNT(DISTINCT ProjectID)                        AS Proyectos,
        COUNT(*)                                         AS Total_Operaciones,
        ROUND(SUM(TotalOreMined), 2)                     AS Total_Mineral,
        ROUND(SUM(TotalWasted), 2)                       AS Total_Desperdicio,
        ROUND(SUM(TotalOreMined) / COUNT(*), 2)          AS Promedio_Mineral_Por_Operacion,
        ROUND(SUM(TotalWasted) / SUM(TotalOreMined) * 100, 2) AS Pct_Desperdicio
      FROM mine
      GROUP BY OperatorID, FirstName, LastName, Country
      ORDER BY Total_Mineral DESC
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(15, truncate = false)
  }

  // ============================================================
  // MINING: Eficiencia minera por proyecto y truck
  // ============================================================
  private def buildEficienciaMinera(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "eficiencia_minera"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        f.TruckID,
        f.ProjectID,
        COUNT(*)                                                   AS Operaciones,
        ROUND(SUM(f.TotalOreMined), 2)                              AS Total_Mineral,
        ROUND(SUM(f.TotalWasted), 2)                                AS Total_Desperdicio,
        ROUND(SUM(f.TotalOreMined) - SUM(f.TotalWasted), 2)        AS Produccion_Neta,
        ROUND(SUM(f.TotalWasted) / SUM(f.TotalOreMined) * 100, 2)  AS Pct_Desperdicio,
        ROUND(AVG(f.TotalOreMined), 2)                              AS Promedio_Mineral,
        ROUND(STDDEV(f.TotalOreMined), 2)                           AS StdDev_Mineral,
        CASE
          WHEN SUM(f.TotalWasted) / SUM(f.TotalOreMined) < 0.05 THEN 'Alta'
          WHEN SUM(f.TotalWasted) / SUM(f.TotalOreMined) < 0.10 THEN 'Media'
          ELSE 'Baja'
        END                                                         AS Eficiencia
      FROM factmine f
      GROUP BY f.TruckID, f.ProjectID
      ORDER BY Produccion_Neta DESC
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(15, truncate = false)
  }

  // ============================================================
  // MINING: Producción agregada por país
  // ============================================================
  private def buildProduccionPorPais(spark: SparkSession, silverPath: String): Unit = {
    val tableName = "produccion_por_pais"
    if (DataLakeIO.pathExists(s"$silverPath/$tableName")) {
      logger.info(s"⏭ Silver/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        Country,
        COUNT(DISTINCT OperatorID)                        AS Operadores,
        COUNT(DISTINCT TruckID)                           AS Trucks,
        COUNT(DISTINCT ProjectID)                         AS Proyectos,
        COUNT(*)                                          AS Total_Registros,
        ROUND(SUM(TotalOreMined), 2)                      AS Total_Mineral,
        ROUND(SUM(TotalWasted), 2)                        AS Total_Desperdicio,
        ROUND(SUM(TotalOreMined) - SUM(TotalWasted), 2)  AS Produccion_Neta,
        ROUND(AVG(TotalOreMined), 2)                      AS Promedio_Mineral,
        ROUND(AVG(Age), 1)                                AS Edad_Promedio_Operadores
      FROM mine
      GROUP BY Country
      ORDER BY Total_Mineral DESC
    """)

    DataLakeIO.writeParquet(df, silverPath, tableName)
    df.show(10, truncate = false)
  }
}
