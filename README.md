# Data Engineering Platform — Lakehouse Medallion Architecture

## Idea de Negocio

Este proyecto implementa una plataforma de datos empresarial para dos unidades de negocio complementarias de una corporación multinacional:

**Retail — Venta de Bicicletas y Componentes**: Operación de comercio electrónico con ventas por internet a nivel global. El negocio gestiona un catálogo de productos organizados en categorías y subcategorías (bicicletas de montaña, ruta, touring, componentes, accesorios), con operaciones en múltiples territorios, una red de sucursales y campañas promocionales activas. El desafío principal es optimizar la rentabilidad por producto, identificar patrones de compra del cliente y anticipar la demanda por categoría y temporada.

**Mining — Extracción Mineral Industrial**: Operación minera distribuida en múltiples países con flotas de camiones, operadores especializados y proyectos de extracción concurrentes. El negocio necesita maximizar la producción de mineral, minimizar el desperdicio operativo y evaluar la eficiencia de cada operador y equipo para optimizar la asignación de recursos.

La plataforma de datos unifica ambos dominios en un único Data Lakehouse, permitiendo a la dirección ejecutiva tomar decisiones basadas en datos con una visión consolidada de toda la operación.

---

## Definición de Business Intelligence

La estrategia de BI se estructura en tres niveles de análisis materializados como tablas Delta en la capa Gold, listos para ser consumidos por Power BI:

### Dashboard Ejecutivo — Retail
| KPI | Descripción | Tabla Gold |
|-----|-------------|------------|
| Ingreso Bruto MoM | Variación mensual de ingresos con acumulado YTD | `kpi_ventas_mensuales` |
| Margen por Categoría | Rentabilidad neta por línea de producto | `kpi_ventas_mensuales` |
| Segmentación de Clientes | Distribución VIP / Premium / Regular / Ocasional | `dim_cliente` |
| Ticket Promedio | Valor promedio por transacción y tendencia temporal | `kpi_ventas_mensuales` |
| Top Productos | Ranking por margen total y clasificación de rotación | `dim_producto` |
| LTV Anualizado | Lifetime Value proyectado por segmento de cliente | `dim_cliente` |

### Dashboard Ejecutivo — Mining
| KPI | Descripción | Tabla Gold |
|-----|-------------|------------|
| Producción Neta por País | Total mineral menos desperdicio por geografía | `kpi_mineria` |
| Tasa de Desperdicio | % de mineral perdido sobre el total extraído | `kpi_mineria` |
| Eficiencia por Operador | Ranking y clasificación Elite/Eficiente/Promedio | `dim_operador` |
| Mineral por Truck | Productividad de cada unidad de transporte | `kpi_mineria` |
| Coeficiente de Variación | Estabilidad operativa por proyecto | `fact_produccion_minera` |

### Análisis Operativo (Self-Service)
Las tablas `fact_ventas` y `fact_produccion_minera` están diseñadas como modelos Star Schema que permiten análisis ad-hoc con filtros por período, categoría, territorio, segmento de cliente, país, proyecto y operador.

---

## Arquitectura Lakehouse

```mermaid
graph TB
    subgraph Sources["Fuentes de Datos"]
        SQL[(SQL Server<br/>Transaccional)]
        CSV[CSV Files<br/>Batch Export]
        IOT[IoT Sensors<br/>Kafka Stream]
    end

    subgraph Ingestion["Capa de Ingesta"]
        ADF[Azure Data Factory<br/>Orquestador Cloud]
        KAFKA[Apache Kafka<br/>Event Streaming]
        HDFS_UP[HDFS Upload<br/>hdfs.scala]
    end

    subgraph Storage["HDFS / Local Filesystem"]
        direction TB
        subgraph Lakehouse["Data Lakehouse"]
            RAW["RAW<br/>CSV originales<br/>7 archivos"]
            BRONZE["BRONZE<br/>Parquet + Schema<br/>7 tablas"]
            SILVER["SILVER<br/>Parquet + Business Logic<br/>8 vistas"]
            GOLD["GOLD<br/>Delta Lake + Star Schema<br/>7 modelos"]
        end
    end

    subgraph Processing["Motor de Procesamiento"]
        SPARK[Apache Spark 3.3.1<br/>Scala 2.12]
        DELTA[Delta Lake 2.2.0<br/>ACID / Time Travel]
        K8S[Kubernetes<br/>Spark on K8s]
    end

    subgraph Serving["Capa de Consumo"]
        PBI[Power BI<br/>Dashboards Ejecutivos]
        DBKS[Databricks<br/>Notebooks Interactivos]
        API[SQL Analytics<br/>Queries Ad-hoc]
    end

    SQL -->|Pipelines ADF| ADF
    CSV -->|Batch Upload| HDFS_UP
    IOT -->|Real-time| KAFKA

    ADF --> RAW
    HDFS_UP --> RAW
    KAFKA -->|Stream to Raw| RAW

    RAW -->|BronzeLayer.scala| BRONZE
    BRONZE -->|SilverLayer.scala| SILVER
    SILVER -->|GoldLayer.scala| GOLD

    SPARK --- BRONZE
    SPARK --- SILVER
    DELTA --- GOLD

    GOLD --> PBI
    GOLD --> DBKS
    GOLD --> API

    style RAW fill:#ff9800,color:#000
    style BRONZE fill:#cd7f32,color:#fff
    style SILVER fill:#c0c0c0,color:#000
    style GOLD fill:#ffd700,color:#000
    style DELTA fill:#003366,color:#fff
    style SPARK fill:#e25a1c,color:#fff
```

---

## Pipeline de Procesamiento — Workflow Paralelizable v3.0

El pipeline v3.0 implementa **ejecución paralela de workflows**, **retry con backoff exponencial** y **checkpoint para reanudación**. Los workflows post-ETL (Quality, Lineage, Analytics) se ejecutan concurrentemente usando un thread pool controlado.

```mermaid
flowchart TB
    START([spark-submit<br/>medallion.Pipeline]) --> DETECT{HDFS<br/>disponible?}

    DETECT -->|Sí| HDFS["Modo HDFS + Hive<br/>setupHadoopEnvironment<br/>ingestRawData"]
    DETECT -->|No| LOCAL["Modo LOCAL<br/>initLocalDatalake<br/>copiar CSVs a raw/"]

    HDFS --> INIT["SparkSession local·2<br/>+ Delta Lake + Kryo<br/>Executors.newFixedThreadPool·2"]
    LOCAL --> INIT

    INIT --> CHK_ETL{checkpoint<br/>ETL?}
    CHK_ETL -->|"existe → skip"| FORK
    CHK_ETL -->|no| WF1_R

    subgraph FASE1["FASE 1 — Secuencial"]
        direction TB
        WF1_R["withRetry·3x · backoff 2s·4s·6s<br/>critical = true"] --> WF1_B
        subgraph WF1["WF1: ETL Pipeline — EtlWorkflow.run"]
            direction LR
            WF1_B["BRONZE<br/>7 tablas<br/>Parquet"] --> WF1_S["SILVER<br/>8 tablas<br/>Parquet"] --> WF1_G["GOLD<br/>7 tablas<br/>Delta Lake"]
        end
        WF1 --> CPETL["✔ .checkpoint_ETL"]
    end

    CPETL --> FORK

    FORK{{"FORK — Future.sequence<br/>3 scala.concurrent.Future<br/>ExecutionContext · pool = 2 threads"}}

    FORK -->|"Future·1"| CHK_Q{checkpoint<br/>QUALITY?}
    FORK -->|"Future·2"| CHK_L{checkpoint<br/>LINEAGE?}
    FORK -->|"Future·3"| CHK_A{checkpoint<br/>ANALYTICS?}

    subgraph FASE2["FASE 2 — Paralelo · scala.concurrent.Future"]

        subgraph F1["Future 1 — Thread pool"]
            CHK_Q -->|"skip"| DONE_Q((" "))
            CHK_Q -->|no| RQ["withRetry·3x<br/>critical = false"]
            RQ --> WF4["WF4: DATA QUALITY<br/>DataQualityWorkflow<br/>Bronze + Silver + Gold<br/>Schema · Null · Dedup<br/>Score 0-100 · Grade A+"]
            WF4 --> CPQ["✔ .checkpoint_QUALITY"]
            CPQ --> DONE_Q
        end

        subgraph F2["Future 2 — Thread pool"]
            CHK_L -->|"skip"| DONE_L((" "))
            CHK_L -->|no| RL["withRetry·3x<br/>critical = false"]
            RL --> WF5["WF5: LINEAGE<br/>LineageWorkflow<br/>22 tablas source→target<br/>JSON Manifest export"]
            WF5 --> CPL["✔ .checkpoint_LINEAGE"]
            CPL --> DONE_L
        end

        subgraph F3["Future 3 — Thread pool"]
            CHK_A -->|"skip"| DONE_A((" "))
            CHK_A -->|no| RA["withRetry·3x<br/>critical = false"]
            RA --> WF2["WF2: BI ANALYTICS<br/>AnalyticsWorkflow<br/>10 gráficos PNG<br/>JFreeChart headless"]
            WF2 --> CPA["✔ .checkpoint_ANALYTICS"]
            CPA --> DONE_A
        end

    end

    DONE_Q --> BARRIER
    DONE_L --> BARRIER
    DONE_A --> BARRIER

    BARRIER{{"JOIN — Await.result<br/>Future.sequence · timeout 10 min"}}

    BARRIER --> CHK_HIVE{hiveEnabled<br/>AND no checkpoint<br/>HIVE_AUDIT?}

    CHK_HIVE -->|"Sí"| WF3_R["withRetry·3x · critical = false"]
    WF3_R --> WF3["WF3: HIVE AUDIT<br/>HiveWorkflow<br/>Schema & Data Display<br/>22 tablas"]
    WF3 --> CPHIVE["✔ .checkpoint_HIVE_AUDIT"]
    CPHIVE --> WF6
    CHK_HIVE -->|"No / skip"| WF6

    WF6["WF6: METRICS — MetricsWorkflow<br/>generateReport · exportMetrics<br/>Thread-safe ConcurrentHashMap<br/>Parallel overlap detection"]

    WF6 --> CLEANUP["threadPool.shutdown<br/>spark.stop"]
    CLEANUP --> FINISH([Pipeline v3.0 Completado])

    style START fill:#4caf50,color:#fff
    style FINISH fill:#4caf50,color:#fff
    style FORK fill:#1565c0,color:#fff,stroke:#0d47a1,stroke-width:3px
    style BARRIER fill:#1565c0,color:#fff,stroke:#0d47a1,stroke-width:3px
    style FASE2 fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    style F1 fill:#fce4ec,stroke:#c62828
    style F2 fill:#f3e5f5,stroke:#6a1b9a
    style F3 fill:#e0f2f1,stroke:#00695c
    style FASE1 fill:#fff8e1,stroke:#f57f17,stroke-width:2px
    style WF1_B fill:#cd7f32,color:#fff
    style WF1_S fill:#c0c0c0,color:#000
    style WF1_G fill:#ffd700,color:#000
    style WF4 fill:#e91e63,color:#fff
    style WF5 fill:#9c27b0,color:#fff
    style WF2 fill:#00897b,color:#fff
    style WF3 fill:#5d4037,color:#fff
    style WF6 fill:#ff5722,color:#fff
    style HDFS fill:#003366,color:#fff
    style LOCAL fill:#607d8b,color:#fff
    style INIT fill:#37474f,color:#fff
    style CPETL fill:#81c784,color:#000
    style CPQ fill:#81c784,color:#000
    style CPL fill:#81c784,color:#000
    style CPA fill:#81c784,color:#000
    style CPHIVE fill:#81c784,color:#000
```

### Estrategia de Paralelización

| Etapa | Paralelismo | Descripción |
|-------|-------------|-------------|
| **Bronze** | Por tabla | Las 7 tablas son independientes: cada una lee su CSV, aplica schema y escribe Parquet sin dependencia entre sí |
| **Silver — Retail** | Parcial | `catalogo_productos` debe construirse primero (join Producto+Subcategoría+Categoría). Luego `ventas_enriquecidas` y `rentabilidad_producto` pueden ejecutarse en paralelo. `segmentacion_clientes` es independiente |
| **Silver — Mining** | Total | Las 3 vistas mining leen directamente de `mine` y `factmine`, sin dependencias cruzadas |
| **Gold — Dimensiones** | Total | `dim_producto`, `dim_cliente` y `dim_operador` son independientes entre sí |
| **Gold — Facts** | Parcial | `fact_ventas` necesita `silver_segmentacion_clientes`. `fact_produccion_minera` necesita `silver_eficiencia_minera` + `silver_produccion_por_pais` |
| **Gold — KPIs** | Total | `kpi_ventas_mensuales` y `kpi_mineria` leen de silver sin dependencia cruzada |

### Ejecución Paralela de Workflows (v3.0)

Post-ETL, los workflows **WF4 (Quality)**, **WF5 (Lineage)** y **WF2 (Analytics)** se ejecutan en paralelo usando `scala.concurrent.Future` con un `ExecutionContext` de 2 threads (`Executors.newFixedThreadPool(2)`). Un `Await.result(Future.sequence(...), 10.minutes)` actúa como barrera de sincronización.

| Característica | Implementación |
|---|---|
| **Parallel Execution** | `Future` + `ExecutionContext` con thread pool fijo de 2 hilos |
| **Barrera** | `Await.result(Future.sequence(futures), 10.minutes)` |
| **Retry con backoff** | `withRetry[T](name, critical, maxRetries=3)` — backoff exponencial (2s, 4s, 6s) |
| **Checkpoint/Resume** | Archivos `.checkpoints/.checkpoint_<STAGE>` — si existe, se salta el stage |
| **Thread Safety** | `MetricsWorkflow` usa `ConcurrentHashMap` + `ConcurrentLinkedQueue` + `@volatile` |
| **Spark master** | `local[2]` — 2 cores para paralelismo interno |
| **DAG Engine** | `DagExecutor` con detección de ciclos (DFS), ejecución paralela y reporte visual |
| **Delta MERGE** | `mergeDelta()` para upserts incrementales + `vacuumDelta()` para limpieza |

### Arquitectura HDFS — Datalake Distribuido

Cuando HDFS está disponible, el datalake se extiende sobre un filesystem distribuido con replicación y tolerancia a fallos:

```mermaid
graph LR
    subgraph HDFS_CLUSTER["HDFS Cluster"]
        NN[NameNode<br/>hdfs://namenode:9000]
        subgraph DataNodes["DataNodes"]
            DN1[DataNode 1]
        end
    end

    subgraph HIVE_LAYER["Hive Metastore"]
        HMS[Hive Metastore<br/>thrift://localhost:9083]
        HS2[HiveServer2<br/>jdbc:hive2://localhost:10000]
        PG[(PostgreSQL 15<br/>Metastore DB)]
    end

    subgraph DATALAKE_HDFS["/hive/warehouse/datalake"]
        H_RAW["/raw<br/>CSV originales"]
        H_BRONZE["/bronze<br/>Parquet"]
        H_SILVER["/silver<br/>Parquet"]
        H_GOLD["/gold<br/>Delta Lake<br/>ACID Transactions"]
    end

    NN --> DN1

    DN1 --> H_RAW
    DN1 --> H_BRONZE
    DN1 --> H_SILVER
    DN1 --> H_GOLD

    HMS --> PG
    HS2 --> HMS
    HMS -->|cataloga| H_GOLD
    HMS -->|cataloga| H_SILVER

    subgraph SPARK_EXEC["Spark Executors"]
        EX1["Executor 1<br/>Bronze Retail"]
        EX2["Executor 2<br/>Bronze Mining"]
        EX3["Executor 3<br/>Silver Aggregations"]
    end

    DATALAKE_HDFS <-->|Data Locality| SPARK_EXEC

    style NN fill:#003366,color:#fff
    style H_GOLD fill:#ffd700,color:#000
    style H_SILVER fill:#c0c0c0,color:#000
    style H_BRONZE fill:#cd7f32,color:#fff
    style H_RAW fill:#ff9800,color:#000
```

---

## Capas del Lakehouse — Detalle

### RAW — Zona de Ingesta
Archivos CSV originales tal como llegan desde los sistemas transaccionales. Sin transformación alguna.

| Archivo | Dominio | Registros |
|---------|---------|-----------|
| `Categoria.csv` | Retail | 4 |
| `Subcategoria.csv` | Retail | 37 |
| `Producto.csv` | Retail | 319 |
| `VentasInternet.csv` | Retail | 47,263 |
| `Sucursales.csv` | Retail | 11 |
| `FactMine.csv` | Mining | 49 |
| `Mine.csv` | Mining | 15,205 |

### BRONZE — Data Cleansing (Parquet)
Primera capa de calidad. Aplica schema explícito con tipos estrictos, elimina filas con claves nulas, deduplica por claves naturales y agrega metadatos de auditoría (`_bronze_ingested_at`, `_bronze_source_file`).

| Tabla | Claves de Deduplicación | Registros |
|-------|------------------------|-----------|
| `categoria` | `Cod_Categoria` | 4 |
| `subcategoria` | `Cod_SubCategoria` | 37 |
| `producto` | `Cod_Producto` | 319 |
| `ventasinternet` | `NumeroOrden`, `Cod_Producto` | 47,263 |
| `sucursales` | `Cod_Sucursal` | 11 |
| `factmine` | `TruckID`, `ProjectID`, `Date` | 49 |
| `mine` | `TruckID`, `ProjectID`, `OperatorID`, `Date` | 15,205 |

### SILVER — Business Logic (Parquet)
Lógica de negocio materializada: joins entre entidades, cálculos financieros, métricas de rendimiento y segmentación.

#### Dominio Retail
| Vista | Descripción |
|-------|-------------|
| `catalogo_productos` | Jerarquía completa Producto → Subcategoría → Categoría |
| `ventas_enriquecidas` | Cada venta con ingreso bruto, margen, ganancia neta, tipo de envío y flag de promoción |
| `resumen_ventas_mensuales` | Agregado mensual por categoría: órdenes, clientes únicos, ingreso, margen, ticket promedio |
| `rentabilidad_producto` | Ranking de productos por revenue, margen total y % de margen promedio |
| `segmentacion_clientes` | Análisis RFM: frecuencia, monetary, ticket promedio y segmento (VIP/Premium/Regular/Ocasional) |

#### Dominio Mining
| Vista | Descripción |
|-------|-------------|
| `produccion_operador` | Producción total por operador: mineral extraído, desperdicio y % de desperdicio |
| `eficiencia_minera` | Eficiencia por truck/proyecto: producción neta, desviación estándar, clasificación Alta/Media/Baja |
| `produccion_por_pais` | Agregado por país: operadores, trucks, proyectos, producción neta y edad promedio |

### GOLD — BI & Analytics Models (Delta Lake)
Modelos dimensionales Star Schema optimizados para consumo por Power BI. Escritos en formato **Delta Lake** con soporte para time travel, ACID transactions y schema evolution.

#### Dimensiones
| Tabla | Tipo | Registros | Descripción |
|-------|------|-----------|-------------|
| `dim_producto` | Dimensión | 319 | Producto con clasificación de rentabilidad (Estrella/Rentable/Standard/Bajo Margen) y rotación (Alta/Media/Baja) |
| `dim_cliente` | Dimensión | 17,555 | Cliente con segmento RFM, scores de frecuencia y monetario, LTV anualizado |
| `dim_operador` | Dimensión | 9,132 | Operador minero con clasificación de eficiencia (Elite/Eficiente/Promedio/Bajo) y rankings |

#### Tablas de Hechos
| Tabla | Tipo | Registros | Partición | Descripción |
|-------|------|-----------|-----------|-------------|
| `fact_ventas` | Fact | 47,263 | `anio` | Cada línea de venta con claves a dim_producto, dim_cliente y segmento |
| `fact_produccion_minera` | Fact | 42 | — | Producción por truck/proyecto con coeficiente de variación y % contribución al país |

#### KPIs Pre-calculados
| Tabla | Tipo | Registros | Descripción |
|-------|------|-----------|-------------|
| `kpi_ventas_mensuales` | KPI | 65 | Métricas mensuales con variación MoM (%), ingreso YTD y margen YTD |
| `kpi_mineria` | KPI | 6 | KPIs por país: mineral/operador, mineral/truck, tasa de desperdicio, evaluación operativa |

---

## Dominios de Datos

### Retail — Modelo Relacional

```mermaid
erDiagram
    CATEGORIA ||--o{ SUBCATEGORIA : contiene
    SUBCATEGORIA ||--o{ PRODUCTO : agrupa
    PRODUCTO ||--o{ VENTAS_INTERNET : "se vende en"
    VENTAS_INTERNET }o--|| SUCURSALES : territorio

    CATEGORIA {
        int Cod_Categoria PK
        string Categoria
    }
    SUBCATEGORIA {
        int Cod_SubCategoria PK
        string SubCategoria
        int Cod_Categoria FK
    }
    PRODUCTO {
        int Cod_Producto PK
        string Producto
        int Cod_SubCategoria FK
        string Color
    }
    VENTAS_INTERNET {
        string NumeroOrden PK
        int Cod_Producto FK
        int Cod_Cliente
        int Cod_Territorio FK
        int Cantidad
        double PrecioUnitario
        double CostoUnitario
        double Impuesto
        double Flete
        timestamp FechaOrden
        timestamp FechaEnvio
    }
    SUCURSALES {
        int Cod_Sucursal PK
        string Sucursal
        double Latitud
        double Longitud
    }
```

- **Producto**: 319 artículos en 37 subcategorías y 4 categorías principales
- **VentasInternet**: 47,263 transacciones con métricas de precio, costo, impuesto y flete
- **Sucursales**: 11 puntos con coordenadas geográficas (latitud/longitud)

### Mining — Modelo Relacional

```mermaid
erDiagram
    MINE ||--o{ FACT_MINE : "registra"
    MINE {
        int TruckID PK
        string Truck
        int ProjectID
        string Country
        int OperatorID
        string FirstName
        string LastName
        int Age
        double TotalOreMined
        double TotalWasted
        string Date
    }
    FACT_MINE {
        int TruckID FK
        int ProjectID FK
        int OperatorID
        double TotalOreMined
        double TotalWasted
        string Date
    }
```

- **Mine**: 15,205 registros de operación diaria con detalle de operador
- **FactMine**: 49 registros agregados de producción por truck/proyecto/fecha

---

## Estructura del Proyecto

```
data-engineer/
│
├── database/                        → Objetos SQL Server
│   ├── schemas/                     → Esquemas de clientes
│   │   └── Clientes/
│   │       ├── Cerveceria/          → Modelo cervecería
│   │       └── Ecommerce/           → Modelo e-commerce
│   ├── stored-procedures/           → 10 procedimientos almacenados
│   └── views/                       → 15 vistas analíticas
│
├── orchestration/                   → Azure Data Factory
│   ├── factory/                     → Configuración de factories
│   ├── linked-services/             → 7 conexiones (Blob, ADLS, SQL)
│   ├── pipelines/                   → 7 pipelines de extracción
│   └── images/                      → Capturas de configuración
│
├── staging/                         → Datos intermedios
│   └── transform_csv/               → 9 CSVs de transformación
│
├── transformation/                  → Motor de procesamiento
│   ├── spark-jobs/pipelines/
│   │   ├── batch-etl-scala/         → Pipeline Medallion (Spark + Scala)
│   │   │   └── src/main/scala/medallion/
│   │   │       ├── Pipeline.scala           → Entry point v3.0 (parallel + retry + checkpoint)
│   │   │       ├── config/                  → DatalakeConfig, SparkFactory
│   │   │       ├── infra/                   → DataLakeIO (MERGE/VACUUM), HdfsManager
│   │   │       ├── schema/                  → CsvSchemas (7 StructTypes)
│   │   │       ├── layer/                   → BronzeLayer, SilverLayer, GoldLayer
│   │   │       ├── analytics/               → BIChartGenerator (JFreeChart)
│   │   │       ├── engine/                  → DagTask, DagExecutor (DAG paralelo)
│   │   │       └── workflow/                → 6 workflows (3 paralelos: Quality‖Lineage‖Analytics)
│   │   ├── stream-processing/       → Spark Streaming + Kafka
│   │   └── iot-ingestion/           → Kafka IoT Producer
│   └── notebooks/databricks/
│       ├── retail-client/            → Notebooks retail
│       └── airbnb-analytics/         → Notebooks Airbnb
│
├── infrastructure/                  → IaC y despliegue
│   ├── hadoop/                      → Docker Compose + Hadoop conf
│   ├── kafka/                       → Docker Compose Kafka
│   ├── postgresql/                  → Docker Compose PostgreSQL
│   ├── spark-k8s/                   → Spark on Kubernetes
│   └── databricks/                  → Bicep template
│
├── docs/                            → Documentación e imágenes
│   ├── analytics/                   → Gráficos BI generados (10 PNG)
│   └── ANALYTICS.md                 → Documentación analítica con insights
├── instalacion.md                   → Guía de instalación
└── README.md                        → Este archivo
```

### Navegación Rápida por Directorio

| Directorio | Descripción | Contenido Principal |
|------------|-------------|---------------------|
| [`database/`](database/) | Capa de base de datos relacional | Stored procedures, views, schemas SQL Server |
| [`database/stored-procedures/`](database/stored-procedures/) | Procedimientos almacenados | Agrega_cliente, Nueva_venta, Multi_procedure_ETL |
| [`database/views/`](database/views/) | Vistas analíticas SQL | Calcula_total_ventas, Ganancias_neta, Promedio_pedido |
| [`database/schemas/`](database/schemas/) | Esquemas de cliente | Cervecería, Ecommerce |
| [`orchestration/`](orchestration/) | Orquestación Azure Data Factory | Factories, linked services, pipelines |
| [`orchestration/pipelines/`](orchestration/pipelines/) | Pipelines ADF | ETL, Pipeline_extraccion, Copy_data_sql |
| [`orchestration/linked-services/`](orchestration/linked-services/) | Conexiones ADF | Blob Storage, ADLS, SQL Database |
| [`staging/`](staging/) | Zona de staging | CSVs intermedios de transformación |
| [`staging/transform_csv/`](staging/transform_csv/) | CSVs transformados | 9 archivos de transformación |
| [`transformation/`](transformation/) | Motor de transformación | Spark jobs, notebooks Databricks |
| [`transformation/spark-jobs/pipelines/batch-etl-scala/`](transformation/spark-jobs/pipelines/batch-etl-scala/) | Pipeline Medallion principal | 18 archivos Scala bajo `medallion.*` — Bronze → Silver → Gold + 6 workflows (3 paralelos) + DAG engine |
| [`transformation/spark-jobs/pipelines/stream-processing/`](transformation/spark-jobs/pipelines/stream-processing/) | Procesamiento streaming | Spark Structured Streaming + Kafka |
| [`transformation/spark-jobs/pipelines/iot-ingestion/`](transformation/spark-jobs/pipelines/iot-ingestion/) | Ingesta IoT | Producer Kafka para sensores |
| [`transformation/notebooks/databricks/`](transformation/notebooks/databricks/) | Notebooks interactivos | Retail-client, Airbnb analytics |
| [`infrastructure/`](infrastructure/) | Infraestructura como código | Docker, Kubernetes, Bicep |
| [`infrastructure/hadoop/`](infrastructure/hadoop/) | Cluster Hadoop | Docker Compose + configuración HDFS |
| [`infrastructure/kafka/`](infrastructure/kafka/) | Cluster Kafka | Docker Compose + config |
| [`infrastructure/postgresql/`](infrastructure/postgresql/) | Base PostgreSQL | Docker Compose |
| [`infrastructure/spark-k8s/`](infrastructure/spark-k8s/) | Spark en Kubernetes | Dockerfiles + manifests K8s |
| [`infrastructure/databricks/`](infrastructure/databricks/) | Databricks IaC | Bicep template (main.bicep) |
| [`docs/`](docs/) | Documentación | Imágenes y diagramas |
| [`docs/analytics/`](docs/analytics/) | Gráficos BI Analytics | 10 visualizaciones PNG del Gold layer |
| [`docs/ANALYTICS.md`](docs/ANALYTICS.md) | Documentación analítica | Insights BI por gráfico y dataset |

---

## Stack Tecnológico

```mermaid
graph LR
    subgraph Processing["Procesamiento"]
        SPARK["Apache Spark 3.3.1"]
        SCALA["Scala 2.12.13"]
        DELTA["Delta Lake 2.2.0"]
        PY["PySpark"]
    end

    subgraph Storage["Almacenamiento"]
        HDFS["Apache HDFS"]
        ADLS["Azure Data Lake<br/>Storage Gen2"]
        BLOB["Azure Blob<br/>Storage"]
    end

    subgraph Orchestration["Orquestación"]
        ADF["Azure Data Factory"]
        SBT["SBT 1.8.2"]
        CICD["GitHub Actions"]
    end

    subgraph Infrastructure["Infraestructura"]
        DOCKER["Docker"]
        K8S["Kubernetes"]
        BICEP["Bicep / ARM"]
    end

    subgraph Streaming["Streaming"]
        KAFKA["Apache Kafka"]
        SS["Spark Structured<br/>Streaming"]
    end

    subgraph Serving["BI / Consumo"]
        PBI["Power BI"]
        DBKS["Databricks<br/>Notebooks"]
        SQL["Azure SQL<br/>Database"]
    end

    SPARK --- SCALA
    SPARK --- DELTA
    SPARK --- PY
    HDFS --- SPARK
    ADLS --- ADF
    KAFKA --- SS
    SS --- SPARK
    DOCKER --- K8S
    DELTA --> PBI
    DELTA --> DBKS

    style SPARK fill:#e25a1c,color:#fff
    style DELTA fill:#003366,color:#fff
    style KAFKA fill:#231f20,color:#fff
    style K8S fill:#326ce5,color:#fff
    style PBI fill:#f2c811,color:#000
```

| Componente | Tecnología | Versión |
|------------|-----------|---------|
| Motor de Procesamiento | Apache Spark | 3.3.1 |
| Lenguaje | Scala | 2.12.13 |
| Formato Gold | Delta Lake | 2.2.0 |
| Build Tool | SBT | 1.8.2 |
| Runtime | Java (Microsoft) | 11 |
| Formato Bronze/Silver | Apache Parquet | — |
| **Storage Distribuido** | **Apache HDFS** | **3.3.4** |
| **Metastore** | **Apache Hive** | **3.1.3** |
| **Hive Backend** | **MySQL** | **5.7** |
| Orquestación Cloud | Azure Data Factory | — |
| Streaming | Apache Kafka | — |
| Container Orchestration | Kubernetes | — |
| IaC | Docker Compose / Bicep | — |
| **BI Charts** | **JFreeChart** | **1.5.4** |

---

## Pipeline Medallion — Código Fuente

El pipeline está modularizado bajo el paquete `medallion.*` con 7 sub-paquetes de responsabilidad única. La arquitectura sigue principios de **alta cohesión / bajo acoplamiento**, con ejecución paralela, retry y checkpoint:

```
src/main/scala/medallion/
├── Pipeline.scala                       # Entry point v3.0 — parallel + retry + checkpoint
├── config/
│   ├── DatalakeConfig.scala             # Modelo de configuración inmutable
│   └── SparkFactory.scala               # SparkSession singleton + Delta + Kryo
├── infra/
│   ├── DataLakeIO.scala                 # I/O: readCsv, writeParquet, writeDelta
│   └── HdfsManager.scala               # HDFS: upload, validate, datalake structure
├── schema/
│   └── CsvSchemas.scala                 # StructType explícitos (7 tablas)
├── layer/
│   ├── BronzeLayer.scala                # RAW → Bronze (schema + dedup + audit cols)
│   ├── SilverLayer.scala                # Bronze → Silver (joins + business logic)
│   └── GoldLayer.scala                  # Silver → Gold (Star Schema + Delta Lake)
├── analytics/
│   └── BIChartGenerator.scala           # 10 gráficos PNG (JFreeChart headless)
├── engine/
│   ├── DagTask.scala                    # Modelo declarativo de task + dependencias
│   └── DagExecutor.scala               # Motor DAG: paralelismo, retry, checkpoint, cycle detection
└── workflow/
    ├── EtlWorkflow.scala                # WF1: Pipeline ETL completo
    ├── AnalyticsWorkflow.scala          # WF2: Generación de charts BI
    ├── HiveWorkflow.scala               # WF3: Auditoría Hive + Schema Display
    ├── DataQualityWorkflow.scala        # WF4: Validación de calidad por capa
    ├── LineageWorkflow.scala            # WF5: Trazabilidad source→target
    └── MetricsWorkflow.scala            # WF6: Métricas de ejecución + export JSON
```

| Paquete | Archivo | Responsabilidad |
|---------|---------|-----------------|
| `medallion` | `Pipeline.scala` | Entry point v3.0: parallel workflows, retry con backoff, checkpoint, thread pool |
| `medallion.config` | `DatalakeConfig.scala` | Case class inmutable con paths de todas las capas + lineage + metrics |
| `medallion.config` | `SparkFactory.scala` | SparkSession singleton con Delta Lake Extensions + Kryo + tuning |
| `medallion.infra` | `DataLakeIO.scala` | readCsv con schema, writeParquet coalesce(1), writeDelta, pathExists |
| `medallion.infra` | `HdfsManager.scala` | buildHadoopConfiguration, createDatalakeStructure, uploadToRaw, validateDatalake |
| `medallion.schema` | `CsvSchemas.scala` | StructType explícitos para las 7 tablas CSV fuente |
| `medallion.layer` | `BronzeLayer.scala` | Schema enforcement, deduplicación por claves, filtro de nulos, columnas de auditoría |
| `medallion.layer` | `SilverLayer.scala` | Joins, cálculos financieros, RFM, eficiencia minera, segmentación |
| `medallion.layer` | `GoldLayer.scala` | Star Schema: dim_producto, dim_cliente, dim_operador, fact_ventas, fact_produccion_minera, KPIs |
| `medallion.analytics` | `BIChartGenerator.scala` | Generación headless de 10 gráficos PNG con JFreeChart 1.5.4 |
| `medallion.engine` | `DagTask.scala` | Modelo declarativo: task ID, dependencias, bloque de ejecución, retry count |
| `medallion.engine` | `DagExecutor.scala` | Motor DAG: paralelismo por thread pool, cycle detection, retry con backoff, checkpoint |
| `medallion.workflow` | `EtlWorkflow.scala` | WF1: Setup → Ingest → Bronze(7) → Silver(8) → Gold(7) → Hive Catalog |
| `medallion.workflow` | `AnalyticsWorkflow.scala` | WF2: Lee Gold/Silver → genera 10 visualizaciones PNG |
| `medallion.workflow` | `HiveWorkflow.scala` | WF3: Auditoría completa — schema, preview, conteo por capa |
| `medallion.workflow` | `DataQualityWorkflow.scala` | WF4: Validación de nulls, duplicados, schema conformance, quality score A+/A/B/C/D |
| `medallion.workflow` | `LineageWorkflow.scala` | WF5: Captura source→target por tabla, exporta manifest JSON a `datalake/lineage/` |
| `medallion.workflow` | `MetricsWorkflow.scala` | WF6: Thread-safe (ConcurrentHashMap). Timing, throughput, JVM, parallel detection, JSON export |

---

## Ejecución

### Modo 1 — Local (sin infraestructura)

```bash
cd transformation/spark-jobs/pipelines/batch-etl-scala

# Compilar
sbt compile

# Construir fat JAR
sbt assembly

# Ejecutar pipeline completo (6 workflows — 3 paralelos)
sbt "runMain medallion.Pipeline"
```

### Modo 2 — Lakehouse completo (HDFS + Hive)

```bash
# 1. Levantar infraestructura
cd infrastructure/hadoop
docker-compose up -d

# 2. Ejecutar ETL
cd ../../transformation/spark-jobs/pipelines/batch-etl-scala
export HDFS_URI="hdfs://localhost:9000"
export HIVE_METASTORE_URI="thrift://localhost:9083"
sbt "runMain medallion.Pipeline"

```
```SQL

-- Consultas en hive
-- 1. Verificar que existe la base de datos
SHOW DATABASES;

-- 2. Seleccionar la base de datos del lakehouse
USE lakehouse;

-- 3. Listar todas las tablas registradas
SHOW TABLES;

-- 4. Verificar estructura de tablas Gold (Delta)
DESCRIBE FORMATTED dim_producto;
DESCRIBE FORMATTED dim_cliente;
DESCRIBE FORMATTED fact_ventas;
DESCRIBE FORMATTED kpi_ventas_mensuales;
DESCRIBE FORMATTED dim_operador;
DESCRIBE FORMATTED fact_produccion_minera;
DESCRIBE FORMATTED kpi_mineria;

-- 5. Verificar estructura de tablas Silver (Parquet)
DESCRIBE FORMATTED silver_catalogo_productos;
DESCRIBE FORMATTED silver_ventas_enriquecidas;
DESCRIBE FORMATTED silver_resumen_ventas_mensuales;
DESCRIBE FORMATTED silver_rentabilidad_producto;
DESCRIBE FORMATTED silver_segmentacion_clientes;
DESCRIBE FORMATTED silver_produccion_operador;
DESCRIBE FORMATTED silver_eficiencia_minera;
DESCRIBE FORMATTED silver_produccion_por_pais;

-- 6. Validar ubicaciones en HDFS
SHOW TABLE EXTENDED IN lakehouse LIKE '*';

-- 7. Consulta rápida para confirmar datos en cada capa
SELECT COUNT(*) AS total_filas FROM fact_ventas;
SELECT COUNT(*) AS total_filas FROM dim_producto;
SELECT COUNT(*) AS total_filas FROM kpi_ventas_mensuales;
SELECT COUNT(*) AS total_filas FROM silver_ventas_enriquecidas;

```

```bash
# 3. Consultar con Beeline
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
# > USE lakehouse; SHOW TABLES; SELECT * FROM kpi_ventas_mensuales LIMIT 10;
```

### Modo 3 — Script E2E automatizado

```bash
cd infrastructure/hadoop
bash lakehouse-start.sh
```

### spark-submit (fat JAR)

```bash
spark-submit \
  --class medallion.Pipeline \
  --master "local[*]" \
  --packages io.delta:delta-core_2.12:2.2.0 \
  target/scala-2.12/root-assembly-1.0.0.jar
```

El pipeline detecta automáticamente si HDFS está disponible. Si no, opera en modo local creando el datalake en `./datalake/`.

### Variables de Entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `HDFS_URI` | `hdfs://namenode:9000` | URI del NameNode HDFS |
| `HIVE_METASTORE_URI` | `thrift://localhost:9083` | URI del Hive Metastore |
| `CSV_PATH` | `./src/main/resources/csv` | Ruta a los archivos CSV fuente |

### Interfaces Web

| Servicio | URL | Descripción |
|----------|-----|-------------|
| NameNode | http://localhost:9870 | HDFS health, browsing |
| YARN | http://localhost:8088 | Resource manager, jobs |
| HiveServer2 | http://localhost:10002 | HiveServer2 Web UI |
| DataNode | http://localhost:9864 | DataNode metrics |

---

## Output del Pipeline

```
DATALAKE PIPELINE v3.0 — 6 Workflows (3 paralelos)

  ── SEQUENTIAL PHASE ──────────────────────────────────

  WF1: ETL PIPELINE  [with retry · max 3 attempts · exponential backoff]
    STAGE 0: HIVE — Metastore Registration   (solo modo HDFS)
    STAGE 1: BRONZE — Data Cleansing         → 7 tablas  (Parquet)
    STAGE 2: SILVER — Business Logic         → 8 tablas  (Parquet)
    STAGE 3: GOLD — BI & Analytics Models    → 7 tablas  (Delta Lake)
    STAGE 4: HIVE — Catalog Registration     (solo modo HDFS)
    ✔ Checkpoint: .checkpoints/.checkpoint_ETL

  ── PARALLEL PHASE (Future + ExecutionContext) ────────
  │
  ├─ WF4: DATA QUALITY                              ┐
  │    Bronze Quality: 7/7 tablas | Score: 100.0 ✔   │
  │    Silver Quality: 8/8 tablas | Score: 100.0 ✔   ├─ Ejecutados en
  │    Gold Quality:   7/7 tablas | Score: 100.0 ✔   │  paralelo con
  │    Global Score: 100.0 / 100 — Grade: A+         │  thread pool (2)
  │    ✔ Checkpoint: .checkpoint_QUALITY              │
  │                                                   │
  ├─ WF5: LINEAGE                                    │
  │    Total: 22/22 tablas con lineage                │
  │    Manifest: lineage/lineage_<ts>.json            │
  │    ✔ Checkpoint: .checkpoint_LINEAGE              │
  │                                                   │
  ├─ WF2: BI ANALYTICS                               │
  │    Chart Generation → 10 gráficos PNG             │
  │    ✔ Checkpoint: .checkpoint_ANALYTICS            │
  │                                                   ┘
  └─ ⏳ Await.result(Future.sequence, 10.minutes) ── BARRIER

  ── POST-PARALLEL ─────────────────────────────────────

  WF3: HIVE AUDIT — Schema & Data Display → 22 tablas  (solo modo HDFS)

  WF6: EXECUTION METRICS  [thread-safe · ConcurrentHashMap]
    ETL:       114.36s | 22 tablas | ████████████████████░░  71.9%
    Quality:    30.89s | 22 tablas | ████░░░░░░░░░░░░░░░░░  19.4%
    Lineage:     5.82s | 22 tablas | █░░░░░░░░░░░░░░░░░░░░   3.7%
    Analytics:  28.41s | 10 charts | ████░░░░░░░░░░░░░░░░░  17.9%
    ── Parallel workflows: QUALITY‖LINEAGE, QUALITY‖ANALYTICS
    JVM Memory: 113 MB / 1024 MB (11.0%)
    Metrics exportados: datalake/metrics/metrics_<timestamp>.json
    Total pipeline: 158.94s

DATALAKE SUMMARY
  BRONZE (7 tablas — parquet)
  SILVER (8 tablas — parquet)
  GOLD   (7 tablas — delta)
    ├── dim_producto              319 filas
    ├── dim_cliente            17,555 filas
    ├── dim_operador            9,132 filas
    ├── fact_ventas            47,263 filas  (partitioned by anio)
    ├── fact_produccion_minera     42 filas
    ├── kpi_ventas_mensuales       65 filas
    ├── kpi_mineria                 6 filas

AUDIT REPORT
  🟤 ═══ BRONZE LAYER (parquet) ═══
  ┌── BRONZE/categoria ──────────────────────
  │  Registros: 4 | Columnas: 4
  │  Schema:
  │    ├── Cod_Categoria: integer (nullable=true)
  │    ├── Categoria: string (nullable=true)
  │    ├── _bronze_ingested_at: timestamp (nullable=false)
  │    ├── _bronze_source_file: string (nullable=false)
  │  Preview (5 filas):
  │  +---------------+----------+--------------------+-------------------+
  │  |Cod_Categoria  |Categoria |_bronze_ingested_at |_bronze_source_file|
  │  +---------------+----------+--------------------+-------------------+
  │  |1              |Bicicletas|2026-04-11 19:51:...|Categoria.csv      |
  │  ...
  └────────────────────────────────────────────
  ...
  ─── Resumen BRONZE ───
  Tablas OK: 7 / 7  |  Errores: 0  |  Total filas: 62,888

  ⚪ ═══ SILVER LAYER (parquet) ═══
  ...
  ─── Resumen SILVER ───
  Tablas OK: 8 / 8  |  Errores: 0  |  Total filas: 74,494

  🟡 ═══ GOLD LAYER (delta) ═══
  ...
  ─── Resumen GOLD ───
  Tablas OK: 7 / 7  |  Errores: 0  |  Total filas: 74,382
```

---

## Workflows de Trazabilidad — WF4, WF5, WF6

El pipeline incluye 3 workflows de trazabilidad que se ejecutan automáticamente después del ETL para garantizar calidad, linaje y observabilidad completa.

### WF4: Data Quality — `DataQualityWorkflow`

Valida cada tabla escrita en las 3 capas de la arquitectura medallón. Se ejecuta automáticamente después del ETL.

| Verificación | Descripción |
|---|---|
| **Existencia** | Confirma que cada tabla esperada existe en el path (HDFS o local) |
| **Schema Conformance** | Valida que el schema tiene columnas y tipos esperados |
| **Null Rate** | Muestrea 100 filas y calcula el porcentaje de nulls por columna |
| **Duplicate Rate** | Muestrea filas y detecta duplicados |
| **Quality Score** | Score compuesto 0-100 con grado: A+ (≥95), A (≥85), B (≥70), C (≥50), D (<50) |

Resultado esperado:
```
═══ DATA QUALITY REPORT ═══
  BRONZE (7 tablas): Score 100.0/100 — All tables validated ✔
  SILVER (8 tablas): Score 100.0/100 — All tables validated ✔
  GOLD   (7 tablas): Score 100.0/100 — All tables validated ✔
  Global Quality Score: 100.0 / 100 — Grade: A+
```

### WF5: Lineage — `LineageWorkflow`

Captura el linaje de datos de cada tabla: qué fuentes alimentaron cada destino, cuándo se procesó, y cuántas columnas tiene.

| Campo | Descripción |
|---|---|
| `layer` | Capa donde reside la tabla (Bronze/Silver/Gold) |
| `table` | Nombre de la tabla |
| `sources` | Lista de tablas/archivos fuente |
| `columns` | Cantidad de columnas del schema |
| `timestamp` | Momento de captura |

Exporta un manifest JSON a `datalake/lineage/lineage_<timestamp>.json` con el grafo completo:
```
═══ DATA LINEAGE GRAPH ═══
  CSV files ──→ bronze/categoria (4 cols)
  CSV files ──→ bronze/subcategoria (4 cols)
  ...
  bronze/producto + bronze/subcategoria + bronze/categoria ──→ silver/catalogo_productos (8 cols)
  ...
  silver/catalogo_productos + silver/rentabilidad_producto ──→ gold/dim_producto (12 cols)
  ...
  Total: 22 tablas con lineage capturado
```

### WF6: Metrics — `MetricsWorkflow`

Captura métricas de ejecución en tiempo real: duración por stage, throughput, uso de memoria JVM.

| Métrica | Descripción |
|---|---|
| **Stage Duration** | Tiempo de ejecución de cada workflow (ms) |
| **Tables Processed** | Tablas procesadas por stage |
| **Throughput** | Tablas/segundo por stage |
| **JVM Memory** | Heap usado vs máximo al finalizar |
| **Bottleneck** | Identifica el stage más lento |

Exporta JSON a `datalake/metrics/metrics_<timestamp>.json`:
```json
{
  "pipeline_start": "2025-07-09T...",
  "total_duration_sec": 138.28,
  "stages": [
    {"name": "ETL", "duration_sec": 99.37, "tables_processed": 22},
    {"name": "QUALITY", "duration_sec": 11.05, "tables_processed": 22},
    {"name": "LINEAGE", "duration_sec": 2.32, "tables_processed": 22},
    {"name": "ANALYTICS", "duration_sec": 15.20, "tables_processed": 10}
  ],
  "jvm_memory_mb": 113, "jvm_max_mb": 1024
}
```

### Directorios de Salida de Trazabilidad

| Directorio | Workflow | Contenido |
|------------|----------|-----------|
| `datalake/lineage/` | WF5 | Manifests JSON con grafo de linaje |
| `datalake/metrics/` | WF6 | Reports JSON con métricas de ejecución |
| `docs/analytics/` | WF2 | 10 gráficos PNG generados por JFreeChart |

---

## Auditoría del Pipeline — WF3: Hive Audit

El **WF3: HIVE AUDIT** (`HiveWorkflow`) genera un reporte de auditoría completo recorriendo cada tabla escrita en las tres capas de la arquitectura medallón. Se ejecuta automáticamente al finalizar la escritura y antes de cerrar el SparkContext.

### Qué valida

| Verificación | Descripción |
|---|---|
| **Existencia** | Confirma que cada tabla esperada existe en el path (HDFS o local) con `_SUCCESS` marker |
| **Schema** | Imprime el esquema completo: nombre de columna, tipo de dato (`integer`, `string`, `double`, `timestamp`) y nullable |
| **Conteo de registros** | Total de filas por tabla y acumulado por capa |
| **Preview de datos** | Muestra las primeras 5 filas de cada tabla para validación visual |
| **Integridad por capa** | Resumen de tablas OK vs errores por cada capa (Bronze / Silver / Gold) |

### Función: `HiveWorkflow.run(spark, config)`

```
Pipeline.main(args)
  ├── WF1: EtlWorkflow.run()
  │     ├── STAGE 0: Hive Setup
  │     ├── STAGE 1: Bronze (7 tablas)
  │     ├── STAGE 2: Silver (8 tablas)
  │     ├── STAGE 3: Gold (7 tablas Delta)
  │     └── STAGE 4: Hive Catalog
  ├── WF4: DataQualityWorkflow.run()
  │     ├── validateLayer("BRONZE", 7 tablas, "parquet")
  │     ├── validateLayer("SILVER", 8 tablas, "parquet")
  │     └── validateLayer("GOLD", 7 tablas, "delta")
  ├── WF5: LineageWorkflow.run()
  │     ├── captureLayerLineage("BRONZE", 7 tablas)
  │     ├── captureLayerLineage("SILVER", 8 tablas)
  │     ├── captureLayerLineage("GOLD", 7 tablas)
  │     └── exportManifest() → datalake/lineage/*.json
  ├── WF2: AnalyticsWorkflow.run()
  │     └── BIChartGenerator.generate() → 10 PNG
  ├── WF3: HiveWorkflow.run() ← Auditoría
  │     ├── auditLayer("BRONZE", bronzePath, 7 tablas, "parquet")
  │     ├── auditLayer("SILVER", silverPath, 8 tablas, "parquet")
  │     └── auditLayer("GOLD", goldPath, 7 tablas, "delta")
  └── WF6: MetricsWorkflow.generateReport()
        └── exportMetrics() → datalake/metrics/*.json
```

### Campos auditados por capa

#### Bronze — Columnas de auditoría automáticas
Cada tabla Bronze incluye dos columnas de metadatos inyectadas durante el procesamiento:

| Columna | Tipo | Descripción |
|---|---|---|
| `_bronze_ingested_at` | `timestamp` | Momento exacto de ingesta a Bronze |
| `_bronze_source_file` | `string` | Archivo CSV fuente (ej: `Categoria.csv`) |

#### Silver — Transformaciones verificadas
El audit confirma que los joins y cálculos de negocio produjeron el schema esperado. Ejemplo de campos calculados auditados:

| Tabla | Campos calculados |
|---|---|
| `ventas_enriquecidas` | `Ingreso_Bruto`, `Margen_Bruto`, `Pct_Margen`, `Ganancia_Neta`, `Dias_Envio`, `Tipo_Envio` |
| `segmentacion_clientes` | `Frecuencia`, `Monetary`, `Ticket_Promedio`, `Segmento` (VIP/Premium/Regular/Ocasional) |
| `eficiencia_minera` | `Produccion_Neta`, `Pct_Desperdicio`, `StdDev_Mineral`, `Eficiencia` (Alta/Media/Baja) |

#### Gold — Modelos Delta Lake verificados
El audit lee cada tabla Gold en formato Delta, validando que el `_delta_log` sea consistente:

| Tabla | Tipo | Campos clave auditados |
|---|---|---|
| `dim_producto` | Dimensión | `clasificacion_rentabilidad`, `clasificacion_rotacion`, `_gold_updated_at` |
| `dim_cliente` | Dimensión | `segmento`, `ltv_anualizado`, `score_frecuencia`, `score_monetario` |
| `fact_ventas` | Fact (particionada) | `anio` (partición), `ganancia_neta`, `segmento_cliente` |
| `kpi_ventas_mensuales` | KPI | `variacion_mom_pct`, `ingreso_ytd`, `margen_ytd` |
| `dim_operador` | Dimensión | `clasificacion_eficiencia`, `ranking_produccion`, `ranking_eficiencia` |
| `fact_produccion_minera` | Fact | `coef_variacion_pct`, `pct_contribucion_global` |
| `kpi_mineria` | KPI | `mineral_por_operador`, `tasa_desperdicio_pct`, `evaluacion_operativa` |

### Resultado esperado del audit

El reporte finaliza con un resumen de integridad por capa:

```
─── Resumen BRONZE ───
Tablas OK: 7 / 7  |  Errores: 0  |  Total filas: 62,888

─── Resumen SILVER ───
Tablas OK: 8 / 8  |  Errores: 0  |  Total filas: 74,494

─── Resumen GOLD ───
Tablas OK: 7 / 7  |  Errores: 0  |  Total filas: 74,382
```

Si alguna tabla falla, el reporte indicará:
```
✗ GOLD/dim_producto — ERROR: Unable to infer schema for Delta
```
