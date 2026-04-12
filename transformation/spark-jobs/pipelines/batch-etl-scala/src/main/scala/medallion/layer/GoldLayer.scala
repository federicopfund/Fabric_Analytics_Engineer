package medallion.layer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import medallion.infra.{DataLakeIO, ViewScope}
import medallion.config.TableRegistry

/**
 * GOLD LAYER — Modelos dimensionales Star Schema y KPIs para BI.
 * Produce tablas Delta Lake optimizadas para consumo analítico.
 *
 * Mejoras v4:
 *   - ViewScope loan pattern para gestión automática de vistas Silver
 *   - Eliminado freeMemory()/System.gc() manual (8 instancias)
 *   - TableRegistry como fuente de nombres de tablas
 */
object GoldLayer {

  private val logger = Logger.getLogger(getClass.getName)

  def process(spark: SparkSession, silverPath: String, goldPath: String): Unit = {
    logger.info("═══════════════════════════════════════")
    logger.info("  GOLD LAYER — BI & Analytics Models")
    logger.info("═══════════════════════════════════════")

    // Registrar todas las vistas Silver de una vez con loan pattern
    ViewScope.withSilverViews(spark, silverPath, TableRegistry.silverNames) {
      // --- RETAIL DOMAIN ---
      buildDimProducto(spark, goldPath)
      buildDimCliente(spark, goldPath)
      buildFactVentas(spark, goldPath)
      buildKpiVentasMensuales(spark, goldPath)

      // --- MINING DOMAIN ---
      buildDimOperador(spark, goldPath)
      buildFactProduccionMinera(spark, goldPath)
      buildKpiMineria(spark, goldPath)
    }

    logger.info("✔ GOLD LAYER completada")
  }

  private def buildDimProducto(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "dim_producto"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        cp.Cod_Producto AS producto_key, cp.Producto AS producto_nombre,
        cp.Color AS producto_color, cp.SubCategoria AS subcategoria, cp.Categoria AS categoria,
        COALESCE(rp.Veces_Vendido, 0) AS total_ventas,
        COALESCE(rp.Unidades_Totales, 0) AS unidades_vendidas,
        COALESCE(rp.Revenue_Total, 0) AS revenue_total,
        COALESCE(rp.Margen_Total, 0) AS margen_total,
        COALESCE(rp.Pct_Margen_Promedio, 0) AS pct_margen,
        COALESCE(rp.Precio_Promedio, 0) AS precio_promedio,
        CASE
          WHEN rp.Pct_Margen_Promedio >= 60 THEN 'Estrella'
          WHEN rp.Pct_Margen_Promedio >= 40 THEN 'Rentable'
          WHEN rp.Pct_Margen_Promedio >= 20 THEN 'Standard'
          WHEN rp.Pct_Margen_Promedio > 0   THEN 'Bajo Margen'
          ELSE 'Sin Ventas'
        END AS clasificacion_rentabilidad,
        CASE
          WHEN rp.Unidades_Totales >= 500 THEN 'Alta Rotacion'
          WHEN rp.Unidades_Totales >= 100 THEN 'Media Rotacion'
          WHEN rp.Unidades_Totales > 0    THEN 'Baja Rotacion'
          ELSE 'Sin Movimiento'
        END AS clasificacion_rotacion,
        current_timestamp() AS _gold_updated_at
      FROM silver_catalogo_productos cp
      LEFT JOIN silver_rentabilidad_producto rp ON cp.Cod_Producto = rp.Cod_Producto
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildDimCliente(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "dim_cliente"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        Cod_Cliente AS cliente_key, Segmento AS segmento,
        Frecuencia AS frecuencia_compras, Monetary AS valor_monetario,
        Ticket_Promedio AS ticket_promedio, Unidades_Totales AS unidades_totales,
        Primera_Compra AS fecha_primera_compra, Ultima_Compra AS fecha_ultima_compra,
        Dias_Como_Cliente AS antiguedad_dias,
        CASE WHEN Dias_Como_Cliente > 0 THEN ROUND(Monetary / (Dias_Como_Cliente / 365.0), 2) ELSE Monetary END AS ltv_anualizado,
        CASE
          WHEN Frecuencia >= 10 THEN 5 WHEN Frecuencia >= 5 THEN 4
          WHEN Frecuencia >= 3 THEN 3  WHEN Frecuencia >= 2 THEN 2 ELSE 1
        END AS score_frecuencia,
        CASE
          WHEN Monetary >= 10000 THEN 5 WHEN Monetary >= 5000 THEN 4
          WHEN Monetary >= 1000 THEN 3  WHEN Monetary >= 500 THEN 2 ELSE 1
        END AS score_monetario,
        current_timestamp() AS _gold_updated_at
      FROM silver_segmentacion_clientes
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildFactVentas(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "fact_ventas"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        ve.NumeroOrden AS orden_id, ve.Cod_Producto AS producto_key,
        ve.Cod_Cliente AS cliente_key, ve.Cod_Territorio AS territorio_key,
        YEAR(ve.FechaOrden) AS anio, MONTH(ve.FechaOrden) AS mes,
        QUARTER(ve.FechaOrden) AS trimestre,
        DATE_FORMAT(ve.FechaOrden, 'yyyy-MM') AS periodo,
        ve.FechaOrden AS fecha_orden, ve.Cantidad AS cantidad,
        ve.PrecioUnitario AS precio_unitario, ve.CostoUnitario AS costo_unitario,
        ve.Ingreso_Bruto AS ingreso_bruto, ve.Costo_Total AS costo_total,
        ve.Margen_Bruto AS margen_bruto, ve.Pct_Margen AS pct_margen,
        ve.Ganancia_Neta AS ganancia_neta, ve.Impuesto AS impuesto, ve.Flete AS flete,
        ve.Dias_Envio AS dias_envio, ve.Tipo_Envio AS tipo_envio,
        ve.Tiene_Promocion AS tiene_promocion,
        ve.Categoria AS categoria, ve.SubCategoria AS subcategoria,
        sc.Segmento AS segmento_cliente,
        current_timestamp() AS _gold_updated_at
      FROM silver_ventas_enriquecidas ve
      LEFT JOIN silver_segmentacion_clientes sc ON ve.Cod_Cliente = sc.Cod_Cliente
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildKpiVentasMensuales(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "kpi_ventas_mensuales"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      WITH base AS (
        SELECT Anio, Mes, Categoria, Total_Ordenes, Clientes_Unicos,
          Unidades_Vendidas, Ingreso_Bruto, Costo_Total, Margen_Bruto, Ticket_Promedio
        FROM silver_resumen_ventas_mensuales
      ),
      con_lag AS (
        SELECT b.*,
          LAG(Ingreso_Bruto) OVER (PARTITION BY Categoria ORDER BY Anio, Mes) AS ingreso_mes_anterior,
          LAG(Total_Ordenes) OVER (PARTITION BY Categoria ORDER BY Anio, Mes) AS ordenes_mes_anterior,
          SUM(Ingreso_Bruto) OVER (PARTITION BY Anio, Categoria ORDER BY Mes ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ingreso_ytd,
          SUM(Margen_Bruto) OVER (PARTITION BY Anio, Categoria ORDER BY Mes ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS margen_ytd
        FROM base b
      )
      SELECT
        Anio AS anio, Mes AS mes,
        CONCAT(Anio, '-', LPAD(CAST(Mes AS STRING), 2, '0')) AS periodo,
        Categoria AS categoria, Total_Ordenes AS total_ordenes,
        Clientes_Unicos AS clientes_unicos, Unidades_Vendidas AS unidades_vendidas,
        ROUND(Ingreso_Bruto, 2) AS ingreso_bruto, ROUND(Costo_Total, 2) AS costo_total,
        ROUND(Margen_Bruto, 2) AS margen_bruto, ROUND(Ticket_Promedio, 2) AS ticket_promedio,
        ROUND(CASE WHEN Ingreso_Bruto > 0 THEN (Margen_Bruto / Ingreso_Bruto) * 100 ELSE 0 END, 2) AS pct_margen,
        ROUND(CASE WHEN ingreso_mes_anterior > 0 THEN ((Ingreso_Bruto - ingreso_mes_anterior) / ingreso_mes_anterior) * 100 ELSE NULL END, 2) AS variacion_mom_pct,
        ROUND(CASE WHEN ordenes_mes_anterior > 0 THEN ((Total_Ordenes - ordenes_mes_anterior) * 1.0 / ordenes_mes_anterior) * 100 ELSE NULL END, 2) AS variacion_ordenes_mom_pct,
        ROUND(ingreso_ytd, 2) AS ingreso_ytd, ROUND(margen_ytd, 2) AS margen_ytd,
        current_timestamp() AS _gold_updated_at
      FROM con_lag ORDER BY Anio, Mes, Categoria
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildDimOperador(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "dim_operador"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        OperatorID AS operador_key, FirstName AS nombre, LastName AS apellido,
        CONCAT(FirstName, ' ', LastName) AS nombre_completo, Country AS pais,
        Trucks_Operados AS trucks_operados, Proyectos AS proyectos_asignados,
        Total_Operaciones AS total_operaciones,
        Total_Mineral AS total_mineral_extraido, Total_Desperdicio AS total_desperdicio,
        Promedio_Mineral_Por_Operacion AS promedio_por_operacion,
        Pct_Desperdicio AS pct_desperdicio,
        CASE
          WHEN Pct_Desperdicio < 3  THEN 'Elite'
          WHEN Pct_Desperdicio < 5  THEN 'Eficiente'
          WHEN Pct_Desperdicio < 10 THEN 'Promedio'
          ELSE 'Bajo Rendimiento'
        END AS clasificacion_eficiencia,
        DENSE_RANK() OVER (ORDER BY Total_Mineral DESC) AS ranking_produccion,
        DENSE_RANK() OVER (ORDER BY Pct_Desperdicio ASC) AS ranking_eficiencia,
        current_timestamp() AS _gold_updated_at
      FROM silver_produccion_operador
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildFactProduccionMinera(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "fact_produccion_minera"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        em.TruckID AS truck_key, em.ProjectID AS proyecto_key,
        em.Operaciones AS total_operaciones,
        em.Total_Mineral AS total_mineral, em.Total_Desperdicio AS total_desperdicio,
        em.Produccion_Neta AS produccion_neta, em.Pct_Desperdicio AS pct_desperdicio,
        em.Promedio_Mineral AS promedio_mineral, em.StdDev_Mineral AS stddev_mineral,
        em.Eficiencia AS nivel_eficiencia,
        CASE WHEN em.StdDev_Mineral IS NOT NULL AND em.Promedio_Mineral > 0
          THEN ROUND(em.StdDev_Mineral / em.Promedio_Mineral * 100, 2) ELSE 0
        END AS coef_variacion_pct,
        COALESCE(totals.global_mineral, 0) AS mineral_global_total,
        CASE WHEN totals.global_mineral > 0
          THEN ROUND(em.Total_Mineral / totals.global_mineral * 100, 2) ELSE 0
        END AS pct_contribucion_global,
        current_timestamp() AS _gold_updated_at
      FROM silver_eficiencia_minera em
      CROSS JOIN (SELECT SUM(Total_Mineral) AS global_mineral FROM silver_produccion_por_pais) totals
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }

  private def buildKpiMineria(spark: SparkSession, goldPath: String): Unit = {
    val tableName = "kpi_mineria"
    if (DataLakeIO.pathExists(s"$goldPath/$tableName")) {
      logger.info(s"⏭ Gold/$tableName ya existe — skip"); return
    }
    logger.info(s"▶ Construyendo: $tableName")

    val df = spark.sql("""
      SELECT
        pp.Country AS pais, pp.Operadores AS total_operadores,
        pp.Trucks AS total_trucks, pp.Proyectos AS total_proyectos,
        pp.Total_Registros AS total_registros,
        pp.Total_Mineral AS total_mineral, pp.Total_Desperdicio AS total_desperdicio,
        pp.Produccion_Neta AS produccion_neta, pp.Promedio_Mineral AS promedio_mineral_registro,
        pp.Edad_Promedio_Operadores AS edad_promedio_operadores,
        ROUND(pp.Total_Mineral / pp.Operadores, 2) AS mineral_por_operador,
        ROUND(pp.Total_Mineral / pp.Trucks, 2) AS mineral_por_truck,
        ROUND(pp.Total_Mineral / pp.Proyectos, 2) AS mineral_por_proyecto,
        ROUND(pp.Total_Desperdicio / pp.Total_Mineral * 100, 2) AS tasa_desperdicio_pct,
        ROUND(pp.Produccion_Neta / pp.Total_Mineral * 100, 2) AS tasa_aprovechamiento_pct,
        CASE
          WHEN pp.Total_Desperdicio / pp.Total_Mineral < 0.05 THEN 'Optima'
          WHEN pp.Total_Desperdicio / pp.Total_Mineral < 0.10 THEN 'Aceptable'
          ELSE 'Requiere Mejora'
        END AS evaluacion_operativa,
        current_timestamp() AS _gold_updated_at
      FROM silver_produccion_por_pais pp ORDER BY produccion_neta DESC
    """)
    DataLakeIO.writeDelta(df.coalesce(1), goldPath, tableName)
    println(s"  ✔ gold/$tableName")
  }
}
