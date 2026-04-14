# Power BI — Modelo de Datos y Medidas DAX

## Resumen

Modelo dimensional en estrella cargado desde la capa GOLD (Delta Lake) con 5 tablas, 57 medidas DAX distribuidas en 6 archivos temáticos, y 6 páginas de dashboard. Soporta análisis de ventas retail (bicicletas y componentes) con segmentación de clientes RFM y time intelligence completa.

---

## Star Schema — Modelo Dimensional

```mermaid
erDiagram
    fact_ventas {
        int OrdenID PK
        int ClienteID FK
        int ProductoID FK
        date FechaOrden FK
        decimal IngresoBruto
        decimal Costo
        decimal Margen
        decimal GananciaNeta
        decimal Impuesto
        decimal Flete
        int Cantidad
        boolean TienePromocion
        int DiasEnTransito
    }

    Calendario {
        date Fecha PK
        int Anio
        int MesNumero
        string NombreMes
        int Trimestre
        string NombreDia
        int SemanaISO
        boolean FinDeSemana
        int OrdenMes
    }

    dim_cliente {
        int ClienteID PK
        string Nombre
        string Segmento
        string SegmentoRFM
        decimal LTV
        decimal FrecuenciaCompra
        string Ciudad
    }

    dim_producto {
        int ProductoID PK
        string Nombre
        string Categoria
        string Subcategoria
        string ClasificacionRentabilidad
        string ClaseRotacion
        decimal MargenPromedio
    }

    kpi_ventas_mensuales {
        date Mes PK
        decimal IngresoTotal
        decimal MargenTotal
        decimal VariacionMoM
        int TotalOrdenes
        int ClientesUnicos
    }

    fact_ventas }o--|| dim_cliente : "ClienteID"
    fact_ventas }o--|| dim_producto : "ProductoID"
    fact_ventas }o--|| Calendario : "FechaOrden = Fecha"
    kpi_ventas_mensuales }o--|| Calendario : "Mes = Fecha"
```

---

## Tablas del Modelo

| Tabla | Tipo | Rows | Origen | Rol |
|-------|------|------|--------|-----|
| `fact_ventas` | Fact | 48,895 | Gold Delta Lake | Transacciones de ventas |
| `dim_cliente` | Dimension | 18,484 | Gold Delta Lake | Atributos de cliente + RFM |
| `dim_producto` | Dimension | 319 | Gold Delta Lake | Jerarquía de producto |
| `kpi_ventas_mensuales` | Aggregate Fact | 37 | Gold Delta Lake | KPIs mensuales pre-calculados |
| `Calendario` | Calculated | 1,184 | DAX ADDCOLUMNS | Dimensión tiempo (2010-2014) |

---

## Medidas DAX — 57 Totales en 6 Archivos

```mermaid
graph TB
    subgraph DAX["57 Medidas DAX"]
        D1["01_Calendario.dax<br/>14 columnas calculadas<br/>Mes, Trimestre, Semana,<br/>Día, Weekend, Sort"]
        D2["02_Medidas_Ventas_Base.dax<br/>16 medidas<br/>Ingreso, Costo, Margen,<br/>Ganancia, Impuestos, Flete"]
        D3["03_Time_Intelligence.dax<br/>12 medidas<br/>MoM, YTD, Prior Year"]
        D4["04_Medidas_Producto.dax<br/>8 medidas<br/>Participación, Ranking,<br/>Clasificación margen"]
        D5["05_Medidas_Cliente.dax<br/>10 medidas<br/>RFM %, LTV, Frecuencia,<br/>Poder adquisitivo"]
        D6["06_Medidas_KPI_Promocion.dax<br/>7 medidas<br/>Promoción %, Envío,<br/>Conversión"]
    end
```

### 01 — Calendario (14 columnas)

| Columna | Fórmula DAX | Ejemplo |
|---------|-------------|---------|
| Año | `YEAR([Fecha])` | 2013 |
| MesNumero | `MONTH([Fecha])` | 6 |
| NombreMes | `FORMAT([Fecha], "MMMM")` | Junio |
| Trimestre | `QUARTER([Fecha])` | Q2 |
| NombreDia | `FORMAT([Fecha], "dddd")` | Lunes |
| SemanaISO | `WEEKNUM([Fecha], 21)` | 24 |
| FinDeSemana | `IF(WEEKDAY(...)>5, TRUE)` | TRUE/FALSE |
| OrdenMes | `MONTH([Fecha])` | 6 (para sort) |

Rango: `2010-12-01` a `2014-01-31` (1,184 días)

### 02 — Medidas Ventas Base (16 medidas)

```mermaid
graph LR
    subgraph Base["Medidas Base"]
        IB["Ingreso Bruto<br/>SUM(IngresoBruto)"]
        COSTO["Costo Total<br/>SUM(Costo)"]
        MARGEN["Margen<br/>SUM(Margen)"]
        GN["Ganancia Neta<br/>Ingreso-Costo-Impuesto-Flete"]
        IMP["Impuestos<br/>SUM(Impuesto)"]
        FLETE["Flete Total<br/>SUM(Flete)"]
        CANT["Cantidad Total<br/>SUM(Cantidad)"]
    end

    subgraph Counts["Conteos"]
        ORD["Total Órdenes<br/>DISTINCTCOUNT(OrdenID)"]
        CLI_U["Clientes Únicos<br/>DISTINCTCOUNT(ClienteID)"]
        PROD_V["Productos Vendidos<br/>DISTINCTCOUNT(ProductoID)"]
    end

    subgraph Pct["Porcentajes"]
        PM["%Margen<br/>Margen/Ingreso"]
        PGN["%Ganancia Neta<br/>GN/Ingreso"]
    end

    subgraph Avg["Promedios"]
        TICKET["Ticket Promedio<br/>Ingreso/Órdenes"]
        PU["Precio Unitario Prom<br/>Ingreso/Cantidad"]
    end
```

### 03 — Time Intelligence (12 medidas)

```mermaid
graph TB
    subgraph MoM["Month-over-Month"]
        ING_ANT["Ingreso Mes Anterior<br/>CALCULATE + PREVIOUSMONTH"]
        VAR_MOM["Variación MoM %<br/>(Actual-Anterior)/Anterior"]
        MAR_ANT["Margen Mes Anterior<br/>CALCULATE + PREVIOUSMONTH"]
        VAR_MAR["Variación Margen MoM %"]
    end

    subgraph YTD["Year-to-Date"]
        ING_YTD["Ingreso YTD<br/>TOTALYTD"]
        MAR_YTD["Margen YTD<br/>TOTALYTD"]
        COST_YTD["Costo YTD<br/>TOTALYTD"]
    end

    subgraph PY["Prior Year"]
        ING_PY["Ingreso Año Anterior<br/>SAMEPERIODLASTYEAR"]
        MAR_PY["Margen Año Anterior"]
        VAR_YOY["Variación YoY %<br/>(Actual-PY)/PY"]
    end
```

### 04 — Medidas Producto (8 medidas)

| Medida | Fórmula | Descripción |
|--------|---------|-------------|
| Participación por Categoría | `DIVIDE(Ingreso_Cat, Ingreso_Total)` | % share por categoría |
| Participación por Subcategoría | `DIVIDE(Ingreso_Sub, Ingreso_Total)` | % share por subcategoría |
| Ranking Producto | `RANKX(ALL, Ingreso, , DESC, Dense)` | Top N productos |
| % Estrella | `DIVIDE(COUNT IF margen>30%)` | Productos alta rentabilidad |
| % Rentable | `DIVIDE(COUNT IF margen 15-30%)` | Productos rentables |
| % Standard | `DIVIDE(COUNT IF margen 5-15%)` | Productos estándar |
| % Bajo Margen | `DIVIDE(COUNT IF margen<5%)` | Productos bajo margen |
| % Sin Ventas | `DIVIDE(COUNT IF ventas=0)` | Productos inactivos |

### 05 — Medidas Cliente (10 medidas)

| Medida | Descripción |
|--------|-------------|
| % Clientes Premium | Proporción segmento Premium |
| Ingreso por Segmento % | Distribución revenue por segmento RFM |
| LTV Anualizado | Lifetime Value proyectado anual |
| Frecuencia Promedio | Compras promedio por cliente |
| Ticket por Segmento | Ticket promedio por segmento |
| Clientes Nuevos | Primer compra en período |
| Clientes Recurrentes | Más de 1 compra |
| Tasa Retención | Recurrentes / Total |
| Power Score | Índice compuesto de poder adquisitivo |
| Concentración Top 20% | % ingreso del top 20% clientes |

### 06 — KPI Promoción (7 medidas)

| Medida | Descripción |
|--------|-------------|
| % Órdenes con Promoción | Proporción con descuento |
| Revenue con Promoción | Ingreso de órdenes promovidas |
| Revenue sin Promoción | Ingreso de órdenes regulares |
| Días en Tránsito Promedio | AVG(DiasEnTransito) |
| % Envío Gratuito | Proporción flete = 0 |
| Tasa de Entrega | Órdenes entregadas / Total |
| Funnel de Conversión | Steps del embudo |

---

## Páginas del Dashboard

```mermaid
graph TB
    subgraph Pages["6 Páginas Power BI"]
        P1["📊 Resumen Ejecutivo<br/>KPIs principales<br/>Tendencias MoM/YoY<br/>Cards + Line charts"]
        P2["💰 Análisis de Ventas<br/>Revenue por período<br/>Desglose categoría<br/>Bar + Treemap"]
        P3["📈 Rentabilidad<br/>Margen por producto<br/>Clasificación tiers<br/>Waterfall + Pie"]
        P4["📦 Análisis de Producto<br/>Top N ranking<br/>Participación %<br/>Bar + Donut"]
        P5["👥 Análisis de Cliente<br/>Segmentación RFM<br/>LTV distribution<br/>Scatter + Pie"]
        P6["🏷️ Promociones<br/>Impacto descuentos<br/>Envío análisis<br/>Combo + Cards"]
    end

    P1 --- P2
    P2 --- P3
    P3 --- P4
    P4 --- P5
    P5 --- P6
```

---

## Relaciones del Modelo

```mermaid
graph LR
    CAL["Calendario<br/>(1 side)"] -->|"1:N"| FV["fact_ventas<br/>(N side)"]
    DC["dim_cliente<br/>(1 side)"] -->|"1:N"| FV
    DP["dim_producto<br/>(1 side)"] -->|"1:N"| FV
    CAL -->|"1:N"| KPI["kpi_ventas_mensuales<br/>(N side)"]
```

Todas las relaciones son **single direction** (de dimensión a hecho) para optimizar performance de DAX filter context.

---

## Flujo de Datos al Dashboard

```mermaid
flowchart LR
    GOLD[("Gold Layer<br/>Delta Lake")] -->|"DirectQuery o<br/>Import"| PBI["Power BI Desktop"]
    PBI --> MODEL["Data Model<br/>5 tablas + relaciones"]
    MODEL --> DAX["57 Medidas DAX<br/>6 archivos"]
    DAX --> PAGES["6 Dashboards<br/>Interactivos"]
    PAGES --> PUBLISH["Power BI Service<br/>Publicación"]
```
