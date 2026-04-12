# BI Analytics — Análisis de Datos (Gold Layer)

> **Workflow 2** del Pipeline Orchestrator — Genera gráficos analíticos PNG a partir del Star Schema (Gold Delta Layer).

## Ejecución

```bash
# Como parte del pipeline completo
sbt "runMain main.PipelineOrchestrator"

# Workflow standalone (requiere Gold layer ya procesado)
sbt "runMain scala.workflow.AnalyticsWorkflow"
```

Las imágenes se generan en `src/main/resources/analytics/`.

---

## Dominio Retail — Ventas de Bicicletas

### 01. Ingreso Bruto por Categoría de Producto

![Ingreso por Categoría](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/01_ingresos_por_categoria.png)

**Tipo:** Barras horizontales  
**Tabla Gold:** `kpi_ventas_mensuales`  
**Métrica:** `SUM(ingreso_bruto)` agrupado por categoría

**Análisis:** Identifica qué línea de producto genera el mayor revenue acumulado del negocio. Las categorías con mayor ingreso bruto representan los pilares financieros de la operación retail. Un desbalance excesivo indica riesgo de concentración de ingresos en pocas líneas, lo cual afecta la resiliencia ante cambios de demanda.

---

### 02. Tendencia de Margen Bruto Mensual por Categoría

![Margen Mensual](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/02_margen_mensual_tendencia.png)

**Tipo:** Líneas (serie temporal multi-categoría)  
**Tabla Gold:** `kpi_ventas_mensuales`  
**Métrica:** `margen_bruto` por período y categoría

**Análisis:** Muestra la evolución de la rentabilidad por línea de producto a lo largo del tiempo. Detecta estacionalidad (picos Q4 por temporada alta de ciclismo), erosión de márgenes por presión competitiva, y el impacto de descuentos promocionales. Las categorías cuyo margen decrece consistentemente requieren revisión de pricing o negociación con proveedores.

---

### 03. Segmentación de Clientes (RFM Analysis)

![Segmentación Clientes](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/03_segmentacion_clientes.png)

**Tipo:** Pie chart  
**Tabla Gold:** `dim_cliente`  
**Métrica:** `COUNT(*)` por segmento RFM (Recency, Frequency, Monetary)

**Análisis:** Clasifica la base de clientes en segmentos: **VIP** (alta frecuencia + alto ticket), **Premium** (frecuentes), **Regular** (compras esporádicas), y **Ocasional** (bajo engagement). Una proporción sana muestra ~15-20% VIP que aporta ~60-80% del revenue. Si el segmento Ocasional domina, indica necesidad de estrategias de retención. El análisis RFM permite diseñar campañas diferenciadas de up-selling y cross-selling por segmento.

---

### 04. Top 10 Productos por Revenue Total

![Top 10 Productos](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/04_top10_productos_revenue.png)

**Tipo:** Barras verticales (coloreadas por categoría)  
**Tabla Gold:** `dim_producto`  
**Métrica:** `revenue_total` ordenado descendente, top 10

**Análisis:** Ranking de los 10 productos estrella del portafolio. Si los 10 principales concentran >50% del revenue total, existe el fenómeno del **Long Tail** donde pocos SKUs sostienen la operación. Las decisiones de inventario, promoción, y placement en e-commerce deben priorizar estos productos. Comparar el mix de categorías en el top 10 revela si la diversificación es saludable.

---

### 05. Clasificación de Rentabilidad del Portafolio

![Clasificación Rentabilidad](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/05_clasificacion_rentabilidad.png)

**Tipo:** Pie chart  
**Tabla Gold:** `dim_producto`  
**Métrica:** `COUNT(*)` por `clasificacion_rentabilidad` (Estrella / Rentable / Standard / Bajo Margen / Sin Ventas)

**Análisis:** Evalúa la salud del portafolio de productos. Los productos **Estrella** son los que tienen alto volume y alto margen — el ideal. Los de **Bajo Margen** generan ventas pero destruyen rentabilidad; necesitan reevaluación de costo o discontinuación. Los **Sin Ventas** son inventario muerto que inmoviliza capital de trabajo. Este gráfico es la versión data-driven de la matriz BCG (Boston Consulting Group).

---

### 06. Variación Month-over-Month de Ingresos (MoM %)

![Variación MoM](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/06_ventas_mom_variacion.png)

**Tipo:** Barras verticales agrupadas por categoría  
**Tabla Gold:** `kpi_ventas_mensuales`  
**Métrica:** `variacion_mom_pct` (porcentaje de cambio mensual)

**Análisis:** Detecta los meses de crecimiento vs contracción en cada categoría. Los valores positivos indican expansión, los negativos contracción. Patrones recurrentes revelan estacionalidad; variaciones abruptas sugieren eventos externos (promociones, stock-outs, nuevos competidores). La volatilidad MoM alta indica un negocio sensible a factores externos, mientras que crecimientos sostenidos positivos reflejan un negocio en fase de expansión orgánica.

---

### 10. Evolución del Ticket Promedio Mensual

![Ticket Promedio](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/10_ticket_promedio_mensual.png)

**Tipo:** Líneas (serie temporal multi-categoría)  
**Tabla Gold:** `kpi_ventas_mensuales`  
**Métrica:** `ticket_promedio` por período y categoría

**Análisis:** Monitorea la evolución del valor promedio de compra. Un ticket creciente sugiere éxito en estrategias de up-selling o inflación de precios. Un ticket decreciente puede indicar canibalización por descuentos, mix shift hacia productos más baratos, o pérdida de clientes premium. Es un indicador clave para evaluar la estrategia de pricing y la composición del carrito de compras.

---

## Dominio Minería — Extracción Mineral

### 07. Producción Minera por País: Mineral vs Desperdicio

![Producción por País](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/07_produccion_minera_por_pais.png)

**Tipo:** Barras agrupadas (Mineral / Desperdicio / Neto)  
**Tabla Gold:** `kpi_mineria`  
**Métrica:** `total_mineral`, `total_desperdicio`, `produccion_neta` por país

**Análisis:** Compara la producción bruta, el desperdicio y la producción neta entre países. Un país con alta producción bruta pero alto desperdicio tiene ineficiencias operativas — el ratio neto/bruto es la clave. Países con bajo desperdicio y alta producción neta representan operaciones optimizadas. Las diferencias entre países pueden deberse a calidad del mineral, equipamiento, regulación ambiental, o capacitación del personal.

---

### 08. Distribución de Eficiencia de Operadores Mineros

![Eficiencia Operadores](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/08_eficiencia_operadores.png)

**Tipo:** Barras verticales  
**Tabla Gold:** `dim_operador`  
**Métrica:** `COUNT(*)` por `clasificacion_eficiencia` (Elite / Eficiente / Promedio / Bajo Rendimiento)

**Análisis:** Evalúa la distribución del talento operativo. Una distribución normal (pocos Elite, mayoría Promedio) es esperable. Si predominan los de **Bajo Rendimiento**, indica problemas sistémicos (capacitación, equipos, procesos). Los operadores **Elite** son candidatos para mentoring y replicación de prácticas. Este análisis habilita decisiones de HR basadas en datos: planes de capacitación targeted, bonificaciones por performance, y reasignación de recursos.

---

### 09. Eficiencia Operativa por País: Aprovechamiento vs Desperdicio

![Desperdicio vs Producción](../transformation/spark-jobs/pipelines/batch-etl-scala/src/main/resources/analytics/09_desperdicio_vs_produccion.png)

**Tipo:** Barras apiladas (stacked bar)  
**Tabla Gold:** `kpi_mineria`  
**Métrica:** `tasa_aprovechamiento_pct` y `tasa_desperdicio_pct` por país  
**Contexto:** Incluye `evaluacion_operativa` como label

**Análisis:** Las barras apiladas al 100% muestran qué proporción de la extracción se convierte en producto útil vs desperdicio. Los países con evaluación **"Excelente"** logran >80% de aprovechamiento. Los países con evaluación **"Deficiente"** (<60% aprovechamiento) necesitan inversión en tecnología de procesamiento, mejores prácticas de extracción, o revisión de la calidad del yacimiento. Este gráfico es clave para decisiones de CAPEX: dónde invertir para maximizar el ROI operativo.

---

## Estructura de Archivos

```
src/main/resources/analytics/
├── 01_ingresos_por_categoria.png
├── 02_margen_mensual_tendencia.png
├── 03_segmentacion_clientes.png
├── 04_top10_productos_revenue.png
├── 05_clasificacion_rentabilidad.png
├── 06_ventas_mom_variacion.png
├── 07_produccion_minera_por_pais.png
├── 08_eficiencia_operadores.png
├── 09_desperdicio_vs_produccion.png
└── 10_ticket_promedio_mensual.png
```

## Stack

| Componente | Versión | Uso |
|---|---|---|
| JFreeChart | 1.5.4 | Generación de gráficos PNG headless |
| Spark SQL | 3.3.1 | Lectura de tablas Gold (Delta Lake) |
| Delta Lake | 2.2.0 | Formato de almacenamiento Gold |
