# Batch ETL Scala — Lakehouse Medallion Pipeline

Motor de procesamiento de datos empresarial implementado en **Scala 2.12 + Apache Spark 3.3.1 + Delta Lake 2.2.0**. Ejecuta un pipeline ETL completo con arquitectura Medallion (RAW → Bronze → Silver → Gold), orquestado por un DAG declarativo con errores tipados, retry automático y checkpoint con metadatos JSON. Incluye workflows paralelos de calidad/lineage/analytics, Dead Letter Queue, Circuit Breaker para IO, loan pattern para vistas Spark, auditoría Hive y generación de gráficos BI.

---

## Índice

1. [Estructura del Proyecto](#estructura-del-proyecto)
2. [Arquitectura General](#arquitectura-general)
3. [Stack Tecnológico](#stack-tecnológico)
4. [Ejecución](#ejecución)
5. [Componentes](#componentes)
   - [Pipeline — Orquestador Principal](#pipeline--orquestador-principal)
   - [config/ — Configuración](#config--configuración)
   - [schema/ — Esquemas CSV](#schema--esquemas-csv)
   - [infra/ — Infraestructura I/O](#infra--infraestructura-io)
   - [engine/ — Motor DAG](#engine--motor-dag)
   - [layer/ — Capas Medallion](#layer--capas-medallion)
   - [workflow/ — Workflows](#workflow--workflows)
   - [analytics/ — Generación BI](#analytics--generación-bi)
6. [Flujo de Datos por Capa](#flujo-de-datos-por-capa)
7. [Modelos de Datos](#modelos-de-datos)
8. [Changelog v4.0](#changelog-v40)

---

## Estructura del Proyecto

```
batch-etl-scala/
├── build.sbt                          ← Dependencias y configuración SBT
├── datalake/                          ← Output local (bronze/silver/gold/checkpoints)
├── src/main/
│   ├── resources/
│   │   ├── csv/                       ← Archivos CSV fuente (7 archivos)
│   │   ├── analytics/                 ← Output PNG de gráficos BI (10 charts)
│   │   ├── hdfs/                      ← Configuraciones HDFS
│   │   └── log4j.properties           ← Configuración de logging
│   └── scala/medallion/
│       ├── Pipeline.scala             ← Orquestador declarativo v4.0 (DAG)
│       ├── config/
│       │   ├── DatalakeConfig.scala   ← Modelo de configuración inmutable
│       │   ├── SparkFactory.scala     ← Fábrica singleton de SparkSession
│       │   └── TableRegistry.scala    ← Fuente única de verdad para tablas
│       ├── schema/
│       │   └── CsvSchemas.scala       ← Esquemas StructType para ingesta
│       ├── infra/
│       │   ├── DataLakeIO.scala       ← Lectura/escritura Parquet, Delta, CSV
│       │   ├── HdfsManager.scala      ← Gestión HDFS y estructura datalake
│       │   ├── CircuitBreaker.scala   ← Protección contra fallos sistémicos IO
│       │   └── ViewScope.scala        ← Loan pattern para vistas temporales
│       ├── engine/
│       │   ├── DagTask.scala          ← Nodo DAG con errores tipados
│       │   └── DagExecutor.scala      ← Motor paralelo con checkpoint JSON
│       ├── layer/
│       │   ├── BronzeLayer.scala      ← Ingesta + DLQ (RAW → Bronze)
│       │   ├── SilverLayer.scala      ← Lógica de negocio + ViewScope
│       │   └── GoldLayer.scala        ← Star Schema + ViewScope (Delta)
│       ├── workflow/
│       │   ├── EtlWorkflow.scala      ← WF1: Orquestación ETL completa
│       │   ├── AnalyticsWorkflow.scala← WF2: Generación de gráficos BI
│       │   ├── HiveWorkflow.scala     ← WF3: Auditoría Hive catalog
│       │   ├── DataQualityWorkflow.scala ← WF4: Validación de calidad
│       │   ├── LineageWorkflow.scala  ← WF5: Trazabilidad de linaje
│       │   └── MetricsWorkflow.scala  ← WF6: Métricas de ejecución
│       └── analytics/
│           └── BIChartGenerator.scala ← Motor JFreeChart (10 gráficos)
```

---

## Arquitectura General

```mermaid
graph TB
    subgraph Entry["Punto de Entrada"]
        PIPE["Pipeline.scala<br/>Orquestador v4.0 (DAG)"]
    end

    subgraph Config["config/"]
        DC["DatalakeConfig<br/>case class inmutable"]
        SF["SparkFactory<br/>Singleton SparkSession"]
        TR["TableRegistry<br/>Fuente única de tablas"]
    end

    subgraph Infra["infra/"]
        DLIO["DataLakeIO<br/>CSV · Parquet · Delta"]
        HDFS["HdfsManager<br/>HDFS · Estructura · Upload"]
        CB["CircuitBreaker<br/>Closed · Open · HalfOpen"]
        VS["ViewScope<br/>Loan pattern vistas"]
    end

    subgraph Engine["engine/"]
        DT["DagTask<br/>Errores tipados · critical"]
        DE["DagExecutor<br/>NonFatal · JSON checkpoint"]
    end

    subgraph Schema["schema/"]
        CS["CsvSchemas<br/>7 StructType"]
    end

    subgraph Layers["layer/"]
        BL["BronzeLayer<br/>7 tablas Parquet"]
        SL["SilverLayer<br/>8 tablas Parquet"]
        GL["GoldLayer<br/>7 tablas Delta"]
    end

    subgraph Workflows["workflow/"]
        WF1["EtlWorkflow<br/>WF1: Bronze→Silver→Gold"]
        WF2["AnalyticsWorkflow<br/>WF2: Charts PNG"]
        WF3["HiveWorkflow<br/>WF3: Hive Audit"]
        WF4["DataQualityWorkflow<br/>WF4: Quality Score"]
        WF5["LineageWorkflow<br/>WF5: Lineage Graph"]
        WF6["MetricsWorkflow<br/>WF6: Pipeline Metrics"]
    end

    subgraph Analytics["analytics/"]
        BIC["BIChartGenerator<br/>JFreeChart · 10 PNG"]
    end

    PIPE --> DC & SF & TR
    PIPE --> DE
    DE --> DT
    DE --> WF1 & WF2 & WF3 & WF4 & WF5 & WF6

    WF1 --> BL & SL & GL
    WF2 --> BIC

    BL & SL & GL --> DLIO
    BL --> CS & TR
    SL --> VS
    GL --> VS
    DLIO --> HDFS & CB

    style PIPE fill:#e25a1c,color:#fff
    style BL fill:#cd7f32,color:#fff
    style SL fill:#c0c0c0,color:#000
    style GL fill:#ffd700,color:#000
    style BIC fill:#2980b9,color:#fff
```

---

## Stack Tecnológico

| Componente | Versión | Función |
|------------|---------|---------|
| **Scala** | 2.12.13 | Lenguaje de programación |
| **Apache Spark** | 3.3.1 | Motor de procesamiento distribuido |
| **Delta Lake** | 2.2.0 | ACID transactions, time travel (Gold) |
| **Hadoop HDFS** | 3.3.4 | Sistema de archivos distribuido |
| **Apache Hive** | 3.1.3 | Metastore y catálogo de tablas |
| **JFreeChart** | 1.5.4 | Generación headless de gráficos PNG |
| **SBT** | — | Build tool + Assembly plugin |

---

## Ejecución

```bash
# Pipeline completo (auto-detecta HDFS o modo local)
sbt "runMain medallion.Pipeline"

# Solo analytics (requiere Gold layer existente)
sbt "runMain medallion.workflow.AnalyticsWorkflow"

# Solo Hive audit (requiere HDFS + Hive activos)
sbt "runMain medallion.workflow.HiveWorkflow"

# Variables de entorno opcionales
export HDFS_URI="hdfs://namenode:9000"       # Default
export CSV_PATH="./src/main/resources/csv"   # Default
export ANALYTICS_OUT="./src/main/resources/analytics"
```

---

## Componentes

### Pipeline — Orquestador Principal

> [`src/main/scala/medallion/Pipeline.scala`](src/main/scala/medallion/Pipeline.scala)

Punto de entrada del sistema. Define un DAG declarativo de 6 tasks con dependencias que el `DagExecutor` resuelve automáticamente. Retry, checkpoint y paralelismo se delegan al motor DAG.

**Funciones clave:**

| Función | Tipo | Descripción |
|---------|------|-------------|
| `main(args)` | Entry point | Detecta entorno, inicializa Spark, define DAG, ejecuta |

**Tasks del DAG:**

| Task ID | Dependencias | Critical | Descripción |
|---------|-------------|----------|-------------|
| `ETL` | ∅ | ✔ | Bronze → Silver → Gold completo |
| `QUALITY` | `ETL` | ✗ | Validación de calidad por capa |
| `LINEAGE` | `ETL` | ✗ | Captura de linaje de datos |
| `ANALYTICS` | `ETL` | ✗ | Generación de gráficos BI |
| `HIVE_AUDIT` | `QUALITY`, `LINEAGE` | ✗ | Verificación catálogo Hive |
| `METRICS` | `QUALITY`, `LINEAGE`, `ANALYTICS`, `HIVE_AUDIT` | ✔ | Reporte final de métricas |

```mermaid
flowchart TB
    MAIN["main(args)"] --> DETECT{"HdfsManager<br/>.isAvailable?"}

    DETECT -->|Sí| HDFS_MODE["setupHadoopEnvironment<br/>ingestRawData<br/>initHdfsDatalake"]
    DETECT -->|No| LOCAL_MODE["initLocalDatalake<br/>Copiar CSVs a raw/"]

    HDFS_MODE --> SPARK["SparkFactory.getOrCreate"]
    LOCAL_MODE --> SPARK

    SPARK --> DAG_DEF["Definir DAG declarativo<br/>6 tasks con dependencias"]

    DAG_DEF --> EXECUTOR["DagExecutor(tasks, parallelism=3)"]

    EXECUTOR --> DAG_EXEC

    subgraph DAG_EXEC["DAG Execution — Resuelto por DagExecutor"]
        ETL["ETL<br/>EtlWorkflow.run<br/>Bronze → Silver → Gold"]

        ETL --> FORK{{"Paralelo (3 threads)"}}
        FORK --> QUALITY["QUALITY<br/>DataQualityWorkflow<br/>critical=false"]
        FORK --> LINEAGE["LINEAGE<br/>LineageWorkflow<br/>critical=false"]
        FORK --> ANALYTICS["ANALYTICS<br/>AnalyticsWorkflow<br/>critical=false"]

        QUALITY & LINEAGE --> HIVE["HIVE_AUDIT<br/>HiveWorkflow<br/>critical=false"]

        QUALITY & LINEAGE & ANALYTICS & HIVE --> METRICS["METRICS<br/>MetricsWorkflow<br/>Reporte final"]
    end

    EXECUTOR --> SUMMARY["Resumen v4.0"]

    style DAG_EXEC fill:#e3f2fd
    style ETL fill:#fff3e0
    style QUALITY fill:#e8f5e9
    style LINEAGE fill:#e8f5e9
    style ANALYTICS fill:#e8f5e9
```

**v4.0 vs v3.0:**

| Aspecto | v3.0 | v4.0 |
|---------|------|------|
| Orquestación | Imperativa (~190 líneas, Future.sequence) | Declarativa (~20 líneas de definición de tasks) |
| Retry | `withRetry()` manual por workflow | Integrado en DagExecutor con errores tipados |
| Checkpoint | `.checkpoint_STAGE` (archivo vacío) | `.dag_TASK.json` (metadatos: timestamp, duración) |
| Errores | `catch { case e: Throwable }` (captura OOM) | `NonFatal` guards + `TaskError` (Transient/Fatal/Skippable) |
| Tasks no-críticas | `critical = false` en `withRetry` | `critical = false` en DagTask — no bloquea dependientes |
| Listas de tablas | Hardcoded en Pipeline (`BRONZE_TABLES`, etc.) | `TableRegistry.bronzeNames/silverNames/goldNames` |
| Thread pool | `newFixedThreadPool(2)` manual | `DagExecutor(parallelism = 3)` automático |

---

### config/ — Configuración

#### DatalakeConfig

> [`src/main/scala/medallion/config/DatalakeConfig.scala`](src/main/scala/medallion/config/DatalakeConfig.scala)

Case class inmutable con las rutas de cada capa y configuración del entorno.

```mermaid
classDiagram
    class DatalakeConfig {
        +String rawPath
        +String bronzePath
        +String silverPath
        +String goldPath
        +String chartsPath
        +String lineagePath
        +String metricsPath
        +String checkpointPath
        +Option~HadoopConfig~ hadoopConfig
        +Boolean hiveEnabled
    }
```

| Campo | Descripción |
|-------|-------------|
| `rawPath` | Directorio de CSVs originales |
| `bronzePath` | Output de BronzeLayer (Parquet) |
| `silverPath` | Output de SilverLayer (Parquet) |
| `goldPath` | Output de GoldLayer (Delta Lake) |
| `chartsPath` | Directorio de salida para gráficos PNG |
| `lineagePath` | Directorio para manifiestos JSON de linaje |
| `metricsPath` | Directorio para métricas JSON |
| `checkpointPath` | Directorio de archivos `.checkpoint_*` |
| `hadoopConfig` | Configuración Hadoop (None en modo local) |
| `hiveEnabled` | Activa registro en catálogo Hive |

#### SparkFactory

> [`src/main/scala/medallion/config/SparkFactory.scala`](src/main/scala/medallion/config/SparkFactory.scala)

Fábrica singleton thread-safe con double-checked locking.

```mermaid
flowchart LR
    CALL["getOrCreate(useLocal)"] --> CHECK{"instance<br/>== null?"}
    CHECK -->|No| RET["return instance"]
    CHECK -->|Sí| SYNC["synchronized"]
    SYNC --> BUILD{"useLocal?"}
    BUILD -->|Sí| LOCAL["spark.sql.warehouse.dir = ./spark-warehouse<br/>catalogImplementation = in-memory<br/>master = local&lbrack;2&rbrack;"]
    BUILD -->|No| CLUSTER["fs.defaultFS = hdfs://...<br/>hive.metastore.uris = thrift://...<br/>enableHiveSupport()"]
    LOCAL & CLUSTER --> COMMON["Delta extensions<br/>KryoSerializer<br/>shuffle.partitions = 2<br/>driver.memory = 512m"]
    COMMON --> RET
```

**Configuraciones Spark aplicadas:**

| Configuración | Valor | Motivo |
|---------------|-------|--------|
| `spark.serializer` | KryoSerializer | Serialización más eficiente |
| `spark.sql.shuffle.partitions` | 2 | Pipeline low-volume |
| `spark.driver.memory` | 512m | Optimizado para contenedor |
| `spark.sql.extensions` | DeltaSparkSessionExtension | Habilita Delta Lake |
| `spark.sql.catalog.spark_catalog` | DeltaCatalog | Catálogo Delta |

#### TableRegistry

> [`src/main/scala/medallion/config/TableRegistry.scala`](src/main/scala/medallion/config/TableRegistry.scala)

Fuente única de verdad para todas las definiciones de tablas del pipeline. Centraliza nombres, formatos, dependencias, claves de deduplicación y esquemas CSV. Elimina la duplicación de listas de tablas que existía en Pipeline, EtlWorkflow, HiveWorkflow, LineageWorkflow, SilverLayer y GoldLayer.

```mermaid
classDiagram
    class TableRegistry {
        +Seq~TableDef~ bronze
        +Seq~TableDef~ silver
        +Seq~TableDef~ gold
        +Seq~TableDef~ all
        +Seq~String~ bronzeNames
        +Seq~String~ silverNames
        +Seq~String~ goldNames
        +Int totalTables
        +Map sourcesMap(Layer)
        +String formatString(Layer|Format)
    }

    class TableDef {
        +String name
        +Layer layer
        +Format format
        +Seq~String~ sources
        +Seq~String~ deduplicateKeys
        +Option~String~ csvFileName
        +Option~StructType~ csvSchema
    }

    class Layer {
        <<sealed trait>>
    }
    Layer <|-- Raw
    Layer <|-- Bronze
    Layer <|-- Silver
    Layer <|-- Gold

    class Format {
        <<sealed trait>>
    }
    Format <|-- Csv
    Format <|-- Parquet
    Format <|-- Delta

    TableRegistry --> TableDef : contains
    TableDef --> Layer
    TableDef --> Format
```

**Consumidores:**

| Archivo | Antes (v3) | Después (v4) |
|---------|------------|--------------|
| `Pipeline.scala` | `BRONZE_TABLES`, `SILVER_TABLES`, `GOLD_TABLES` hardcoded | `TableRegistry.bronzeNames/silverNames/goldNames` |
| `BronzeLayer.scala` | 7× `processTable()` hardcoded | `TableRegistry.bronze.foreach { td => ... }` |
| `EtlWorkflow.scala` | `goldTables`, `silverTables` hardcoded | `TableRegistry.goldNames`, `TableRegistry.silverNames` |
| `LineageWorkflow.scala` | `BRONZE_SOURCES`, `SILVER_SOURCES`, `GOLD_SOURCES` | `TableRegistry.sourcesMap(layer)` |
| `HiveWorkflow.scala` | `goldTables`, `silverTables` hardcoded | `TableRegistry.goldNames`, `TableRegistry.silverNames` |

---

### schema/ — Esquemas CSV

> [`src/main/scala/medallion/schema/CsvSchemas.scala`](src/main/scala/medallion/schema/CsvSchemas.scala)

Define `StructType` explícitos para los 7 archivos CSV de ingesta, divididos en dos dominios:

```mermaid
graph LR
    subgraph Retail["Dominio Retail"]
        CAT["categoriaSchema<br/>Cod_Categoria · Categoria"]
        SUB["subcategoriaSchema<br/>Cod_SubCategoria · SubCategoria<br/>Cod_Categoria"]
        PROD["productoSchema<br/>Cod_Producto · Producto<br/>Cod_SubCategoria · Color"]
        VENT["ventasInternetSchema<br/>13 campos: orden, cliente,<br/>producto, importes, fechas"]
        SUC["sucursalesSchema<br/>Cod_Sucursal · Sucursal<br/>Latitud · Longitud"]
    end

    subgraph Mining["Dominio Mining"]
        FM["factMineSchema<br/>TruckID · ProjectID · OperatorID<br/>TotalOreMined · TotalWasted · Date"]
        MN["mineSchema<br/>11 campos: truck, project,<br/>operator, country, ore, waste"]
    end

    style Retail fill:#e3f2fd
    style Mining fill:#fff3e0
```

| Schema | Archivo CSV | Columnas | Dominio |
|--------|------------|----------|---------|
| `categoriaSchema` | Categoria.csv | 2 | Retail |
| `subcategoriaSchema` | Subcategoria.csv | 3 | Retail |
| `productoSchema` | Producto.csv | 4 | Retail |
| `ventasInternetSchema` | VentasInternet.csv | 13 | Retail |
| `sucursalesSchema` | Sucursales.csv | 5 | Retail |
| `factMineSchema` | FactMine.csv | 6 | Mining |
| `mineSchema` | Mine.csv | 11 | Mining |

---

### infra/ — Infraestructura I/O

#### DataLakeIO

> [`src/main/scala/medallion/infra/DataLakeIO.scala`](src/main/scala/medallion/infra/DataLakeIO.scala)

Utilidades de lectura/escritura que abstraen el formato de almacenamiento. Soporta CSV, Parquet y Delta Lake, con detección automática de rutas locales vs HDFS.

```mermaid
flowchart TB
    subgraph Read["Lectura"]
        CSV["readCsv(spark, basePath,<br/>fileName, schema)<br/>→ DataFrame"]
    end

    subgraph Write["Escritura"]
        WP["writeParquet(df, basePath,<br/>tableName, partitionCol?)"]
        WD["writeDelta(df, basePath,<br/>tableName, partitionCol?)"]
        WDP["writeDeltaOrParquet<br/>Delta con fallback Parquet"]
    end

    subgraph Delta["Operaciones Delta"]
        MD["mergeDelta(df, basePath,<br/>tableName, mergeKeys)<br/>UPSERT"]
        VD["vacuumDelta(basePath,<br/>tableName, retentionHours)<br/>Limpieza archivos obsoletos"]
    end

    subgraph Check["Verificación"]
        PE["pathExists(filePath)<br/>Local o HDFS<br/>Busca _SUCCESS o _delta_log"]
    end

    CSV --> |"Bronze usa"| WP
    WP --> |"Gold usa"| WD
    WD --> |"Incremental"| MD
    MD --> VD

    style Read fill:#e8f5e9
    style Write fill:#e3f2fd
    style Delta fill:#ffd700,color:#000
    style Check fill:#f5f5f5
```

| Función | Formatos | Descripción |
|---------|----------|-------------|
| `readCsv` | CSV → DataFrame | Lee con schema enforcement y dateFormat ISO |
| `writeParquet` | DataFrame → Parquet | Escritura con partición opcional (Bronze/Silver) |
| `writeDelta` | DataFrame → Delta | Escritura Delta Lake con ACID (Gold) |
| `writeDeltaOrParquet` | Delta / Parquet | Delta con fallback automático a Parquet |
| `mergeDelta` | Delta UPSERT | Merge por claves con `whenMatched.updateAll()` |
| `vacuumDelta` | Delta cleanup | VACUUM con retención configurable (default 7 días) |
| `pathExists` | Local / HDFS | Detecta `_SUCCESS` (Parquet) o `_delta_log` (Delta) |

#### HdfsManager

> [`src/main/scala/medallion/infra/HdfsManager.scala`](src/main/scala/medallion/infra/HdfsManager.scala)

Gestión centralizada de operaciones HDFS: verificación de disponibilidad, configuración Hadoop, creación de estructura y carga de archivos.

```mermaid
flowchart TB
    subgraph Init["Inicialización"]
        AVAIL["isAvailable(hdfsUri)<br/>TCP test con timeout 3s"]
        BUILD["buildHadoopConfiguration<br/>(HadoopConfig) → Configuration"]
        INIT["init(hdfsUri, user)<br/>→ FileSystem"]
    end

    subgraph Structure["Estructura Datalake"]
        CREATE["createDatalakeStructure<br/>raw/ bronze/ silver/ gold/<br/>tmp/ staging/ eventLog/ checkpoint/"]
        VALIDATE["validateDatalake<br/>Verifica 4 capas existen"]
    end

    subgraph DataOps["Operaciones"]
        UPLOAD["uploadToRaw(hdfsUri, localFolder)<br/>Sube CSVs, skip existentes"]
        LIST["list(hdfsFolder)<br/>Listado con tamaños"]
        DELETE["delete(hdfsUri, path)<br/>Borrado recursivo"]
    end

    AVAIL -->|OK| BUILD --> INIT
    INIT --> CREATE --> VALIDATE
    VALIDATE -->|OK| UPLOAD

    style Init fill:#e3f2fd
    style Structure fill:#e8f5e9
    style DataOps fill:#fff3e0
```

**HadoopConfig — case class:**

| Campo | Default | Descripción |
|-------|---------|-------------|
| `hdfsUri` | — | URI del namenode HDFS |
| `user` | `"fede"` | Usuario Hadoop |
| `replication` | 1 | Factor de replicación HDFS |
| `blockSize` | 128MB | Tamaño de bloque HDFS |
| `hiveWarehouse` | `/hive/warehouse` | Directorio warehouse Hive |
| `hiveMetastoreUris` | `thrift://localhost:9083` | URI del Hive Metastore |

#### CircuitBreaker

> [`src/main/scala/medallion/infra/CircuitBreaker.scala`](src/main/scala/medallion/infra/CircuitBreaker.scala)

Protección contra fallos sistémicos en operaciones HDFS/IO. Implementa un state machine con tres estados para evitar cascadas de fallos.

```mermaid
stateDiagram-v2
    [*] --> Closed
    Closed --> Open : failures >= threshold
    Open --> HalfOpen : resetTimeMs elapsed
    HalfOpen --> Closed : operación exitosa
    HalfOpen --> Open : operación falló
```

| Parámetro | Default | Descripción |
|-----------|---------|-------------|
| `name` | `"default"` | Identificador del circuito para logging |
| `threshold` | 3 | Fallos consecutivos para abrir |
| `resetTimeMs` | 60000 | Tiempo (ms) antes de probar half-open |

**API:**

```scala
val cb = new CircuitBreaker("hdfs-io", threshold = 3, resetTimeMs = 60000)
val result = cb.execute {
  // Operación IO protegida — lanza CircuitBreakerOpenException si abierto
  DataLakeIO.writeParquet(df, path, table)
}
```

#### ViewScope

> [`src/main/scala/medallion/infra/ViewScope.scala`](src/main/scala/medallion/infra/ViewScope.scala)

Loan pattern para gestión automática del ciclo de vida de vistas temporales Spark. Garantiza que las vistas se registren al inicio y se liberen al salir, eliminando la necesidad de `System.gc()` manual.

```mermaid
flowchart LR
    REGISTER["Registrar vistas<br/>spark.read.format<br/>.createOrReplaceTempView"] --> BODY["Ejecutar bloque<br/>spark.sql(...)"]
    BODY --> CLEANUP["finally {<br/>dropTempView ×N<br/>clearCache<br/>}"]

    style REGISTER fill:#e8f5e9
    style BODY fill:#e3f2fd
    style CLEANUP fill:#ffebee
```

**Variantes de conveniencia:**

| Método | Prefijo Vista | Formato | Uso |
|--------|--------------|---------|-----|
| `withBronzeViews` | _(ninguno)_ | parquet | SilverLayer |
| `withSilverViews` | `silver_` | parquet | GoldLayer |
| `withGoldViews` | `gold_` | delta | Analytics |

**Antes (v3) vs Después (v4):**

```scala
// v3 — Manual, 8× System.gc()
registerSilverTables(spark, silverPath, Seq("catalogo", "rentabilidad"))
buildDimProducto(spark, goldPath)
freeMemory(spark)  // clearCache + System.gc()
// ...repetir 7 veces más

// v4 — Loan pattern, cleanup automático
ViewScope.withSilverViews(spark, silverPath, TableRegistry.silverNames) {
  buildDimProducto(spark, goldPath)
  buildDimCliente(spark, goldPath)
  // ... todas las tablas Gold
}  // Vistas liberadas automáticamente
```

---

### engine/ — Motor DAG

#### DagTask

> [`src/main/scala/medallion/engine/DagTask.scala`](src/main/scala/medallion/engine/DagTask.scala)

Nodo del grafo dirigido acíclico (DAG) de ejecución. Soporta errores tipados (`TaskError`), flag `critical` para tasks no-bloqueantes, y constructor de compatibilidad `fromUnit`.

```mermaid
classDiagram
    class DagTask {
        +String id
        +Set~String~ dependencies
        +Function0~Either~ execute
        +Int retryCount = 3
        +Boolean critical = true
        +String description = ""
    }

    class DagTask$ {
        +fromUnit(id, deps, body, ...) DagTask
    }

    class TaskError {
        <<sealed trait>>
        +String message
        +Option~Throwable~ cause
    }
    TaskError <|-- Transient : "reintentar"
    TaskError <|-- Fatal : "no reintentar"
    TaskError <|-- Skippable : "completar con warning"

    class TaskStatus {
        <<sealed trait>>
    }
    class Pending
    class Running
    class Completed
    class Failed {
        +Throwable error
    }
    class Skipped
    class CompletedWithWarning {
        +String warning
    }

    TaskStatus <|-- Pending
    TaskStatus <|-- Running
    TaskStatus <|-- Completed
    TaskStatus <|-- Failed
    TaskStatus <|-- Skipped
    TaskStatus <|-- CompletedWithWarning

    DagTask --> TaskStatus : produces
    DagTask --> TaskError : returns Left
    DagTask$ --> DagTask : creates
```

**Canal de error tipado:**

| Tipo | Retry | Propaga | Uso |
|------|-------|---------|-----|
| `Transient` | ✔ Sí | Según `critical` | IO timeout, HDFS temporalmente caído |
| `Fatal` | ✗ No | Según `critical` | Schema mismatch, datos corruptos |
| `Skippable` | ✗ No | ✗ No | Warning no-bloqueante, task se marca `CompletedWithWarning` |

#### DagExecutor

> [`src/main/scala/medallion/engine/DagExecutor.scala`](src/main/scala/medallion/engine/DagExecutor.scala)

Motor de ejecución paralela que resuelve dependencias del DAG, ejecuta tasks concurrentes con un thread pool controlado, y soporta retry con backoff. Usa `NonFatal` para no capturar errores fatales del JVM (OOM, StackOverflow).

```mermaid
flowchart TB
    START["new DagExecutor(tasks, parallelism=3)"]
    START --> EXEC[".execute()"]

    EXEC --> VALIDATE["validateNoCycles()<br/>DFS visitado + inStack"]
    VALIDATE --> INIT_STATUS["Inicializar statusMap<br/>Skipped si checkpointed (.json)<br/>Pending si nuevo"]

    INIT_STATUS --> POOL["FixedThreadPool(parallelism)"]
    POOL --> SCHEDULE

    subgraph Loop["scheduleReady() — Loop recursivo"]
        SCHEDULE["Buscar tasks con:<br/>status == Pending AND<br/>deps ∈ {Completed, Skipped,<br/>CompletedWithWarning,<br/>Failed si dep no es critical}"]
        SCHEDULE --> LAUNCH["Future { executeWithRetry(task) }"]
        LAUNCH --> RESULT{"execute() → Either"}
        RESULT -->|"Right(())"| OK["Completed"]
        RESULT -->|"Left(Transient)"| RETRY["Retry con backoff"]
        RESULT -->|"Left(Fatal)"| FAIL["Failed (no retry)"]
        RESULT -->|"Left(Skippable)"| WARN["CompletedWithWarning"]
        OK & FAIL & WARN --> UPDATE["statusMap.put(id, result)<br/>writeCheckpoint(.json) si OK"]
        UPDATE --> RESCHEDULE["scheduleReady()<br/>Desbloquear dependientes"]
        RESCHEDULE --> SCHEDULE
    end

    Loop --> LATCH["CountDownLatch.await<br/>timeout 30 min"]
    LATCH --> REPORT["printDagReport()<br/>con duraciones por task"]

    style Loop fill:#e3f2fd
```

**Mejoras v4:**

| Aspecto | v3 | v4 |
|---------|----|----|
| Error handling | `catch { case e: Throwable }` | `NonFatal` + `TaskError` tipado |
| Checkpoint | `.dag_TASK` (archivo vacío) | `.dag_TASK.json` con metadatos |
| Tasks no-críticas | No soportado | `critical=false` → fallo no bloquea dependientes |
| Status | 5 estados | 6 estados (+`CompletedWithWarning`) |
| Duraciones | No registradas | `ConcurrentHashMap[taskId, Long]` → reporte |

---

### layer/ — Capas Medallion

#### BronzeLayer

> [`src/main/scala/medallion/layer/BronzeLayer.scala`](src/main/scala/medallion/layer/BronzeLayer.scala)

Ingesta y limpieza: itera declarativamente desde `TableRegistry`, lee CSVs con schema enforcement, deduplica por claves naturales, y escribe filas rechazadas en un Dead Letter Queue (`_rejected/`).

```mermaid
flowchart LR
    REG["TableRegistry.bronze<br/>.foreach { td => ... }"]
    REG --> RAW["RAW<br/>7 CSV files"]
    RAW --> READ["DataLakeIO.readCsv<br/>+ td.csvSchema"]
    READ --> SPLIT{{"keyColumns.isNotNull?"}}
    SPLIT -->|Sí| VALID["Filas válidas"]
    SPLIT -->|No| DLQ["Dead Letter Queue<br/>_rejected/tableName<br/>+ _rejected_at<br/>+ _rejected_reason"]
    VALID --> DEDUP["dropDuplicates<br/>por td.deduplicateKeys"]
    DEDUP --> AUDIT["withColumn<br/>_bronze_ingested_at<br/>_bronze_source_file"]
    AUDIT --> WRITE["DataLakeIO.writeParquet<br/>→ Bronze"]

    style RAW fill:#ff9800,color:#000
    style WRITE fill:#cd7f32,color:#fff
    style DLQ fill:#ffcdd2
```

**Dead Letter Queue (DLQ):**

Las filas con claves naturales nulas ya no se descartan silenciosamente. Se escriben en `bronzePath/_rejected/<tableName>/` con columnas de auditoría:

| Columna DLQ | Contenido |
|-------------|-----------|
| `_rejected_at` | Timestamp de rechazo |
| `_rejected_reason` | `"null key: Cod_Producto,..."` |
| `_source_file` | Nombre del CSV original |

| Tabla Bronze | CSV Fuente | Claves de Deduplicación |
|-------------|------------|------------------------|
| `categoria` | Categoria.csv | `Cod_Categoria` |
| `subcategoria` | Subcategoria.csv | `Cod_SubCategoria` |
| `producto` | Producto.csv | `Cod_Producto` |
| `ventasinternet` | VentasInternet.csv | `NumeroOrden`, `Cod_Producto` |
| `sucursales` | Sucursales.csv | `Cod_Sucursal` |
| `factmine` | FactMine.csv | `TruckID`, `ProjectID`, `Date` |
| `mine` | Mine.csv | `TruckID`, `ProjectID`, `OperatorID`, `Date` |

#### SilverLayer

> [`src/main/scala/medallion/layer/SilverLayer.scala`](src/main/scala/medallion/layer/SilverLayer.scala)

Lógica de negocio: usa `ViewScope.withBronzeViews` loan pattern para registrar automáticamente las 7 vistas Bronze y liberarlas al finalizar. Construye 8 tablas analíticas con Spark SQL.

```mermaid
flowchart TB
    VS["ViewScope.withBronzeViews<br/>(spark, bronzePath, TableRegistry.bronzeNames)"]

    subgraph Block["Bloque protegido — vistas auto-liberadas"]
        subgraph Retail["Dominio Retail — 5 tablas"]
            CP["buildCatalogoProductos<br/>JOIN producto + subcategoria + categoria"]
            VE["buildVentasEnriquecidas<br/>Ingreso, Costo, Margen, Ganancia Neta,<br/>Dias_Envio, Tipo_Envio, Tiene_Promocion"]
            RVM["buildResumenVentasMensuales<br/>GROUP BY año, mes, categoría<br/>Ordenes, Clientes, Unidades, Ticket"]
            RP["buildRentabilidadProducto<br/>Revenue, Costo, Margen por SKU"]
            SC["buildSegmentacionClientes<br/>CTE cliente_metricas<br/>→ Segmento RFM: VIP/Premium/Regular/Ocasional"]
        end

        subgraph Mining["Dominio Mining — 3 tablas"]
            PO["buildProduccionOperador<br/>GROUP BY operador<br/>Mineral, Desperdicio, % Desperdicio"]
            EM["buildEficienciaMinera<br/>GROUP BY truck, proyecto<br/>Producción Neta, StdDev, Eficiencia"]
            PP["buildProduccionPorPais<br/>GROUP BY country<br/>Operadores, Trucks, Mineral, EdadPromedio"]
        end
    end

    VS --> Block
    Block --> CLEANUP["finally: dropTempView ×7<br/>clearCache"]

    style Block fill:#f5f5f5
    style Retail fill:#e3f2fd
    style Mining fill:#fff3e0
    style CLEANUP fill:#ffebee
```

**Detalle de tablas Silver:**

| Tabla Silver | Fuentes Bronze | SQL Principal |
|-------------|---------------|---------------|
| `catalogo_productos` | producto, subcategoria, categoria | 3-way INNER JOIN |
| `ventas_enriquecidas` | ventasinternet, producto, subcategoria, categoria | JOINs + cálculos financieros (Ingreso, Margen, Ganancia Neta) |
| `resumen_ventas_mensuales` | ventasinternet, producto, subcategoria, categoria | GROUP BY Anio, Mes, Categoría + agregaciones |
| `rentabilidad_producto` | ventasinternet, producto, subcategoria, categoria | GROUP BY producto + Revenue, Margen, Precio promedio |
| `segmentacion_clientes` | ventasinternet | CTE con frecuencia, monetary → segmento RFM |
| `produccion_operador` | mine | GROUP BY operador + mineral, desperdicio |
| `eficiencia_minera` | factmine | GROUP BY truck, proyecto + StdDev, eficiencia |
| `produccion_por_pais` | mine | GROUP BY country + producción neta |

#### GoldLayer

> [`src/main/scala/medallion/layer/GoldLayer.scala`](src/main/scala/medallion/layer/GoldLayer.scala)

Modelos dimensionales Star Schema escritos en Delta Lake. Usa `ViewScope.withSilverViews` loan pattern para registrar todas las vistas Silver de una vez y liberarlas automáticamente al salir.

```mermaid
flowchart TB
    VS["ViewScope.withSilverViews<br/>(spark, silverPath, TableRegistry.silverNames)"]

    subgraph Block["Bloque protegido — vistas auto-liberadas"]
        subgraph RetailGold["Retail — 4 tablas Delta"]
            DIM_P["dim_producto<br/>319 productos<br/>clasificacion_rentabilidad<br/>clasificacion_rotacion"]
            DIM_C["dim_cliente<br/>Segmento RFM<br/>LTV Anualizado<br/>score_frecuencia/monetario"]
            FACT_V["fact_ventas<br/>Hechos transaccionales<br/>periodo, segmento_cliente"]
            KPI_V["kpi_ventas_mensuales<br/>MoM%, YTD, pct_margen<br/>Window Functions LAG/SUM"]
        end

        subgraph MiningGold["Mining — 3 tablas Delta"]
            DIM_O["dim_operador<br/>clasificacion_eficiencia<br/>DENSE_RANK ranking"]
            FACT_M["fact_produccion_minera<br/>coef_variacion<br/>pct_contribucion_global"]
            KPI_M["kpi_mineria<br/>mineral_por_operador/truck<br/>tasa_desperdicio<br/>evaluacion_operativa"]
        end
    end

    VS --> Block
    Block --> CLEANUP["finally: dropTempView ×8<br/>clearCache"]

    style Block fill:#f5f5f5
    style RetailGold fill:#fff8e1
    style MiningGold fill:#fff3e0
    style CLEANUP fill:#ffebee
```

**v3 vs v4:**

```
v3: registerSilverTables(2) → build → freeMemory → register(1) → build → freeMemory → ...  (8×)
v4: ViewScope.withSilverViews(all) { build; build; build; ... }  (1× auto-cleanup)
```

---

### workflow/ — Workflows

#### WF1: EtlWorkflow

> [`src/main/scala/medallion/workflow/EtlWorkflow.scala`](src/main/scala/medallion/workflow/EtlWorkflow.scala)

Orquesta el pipeline ETL completo. Soporta dos modos de inicialización y registra tablas en Hive cuando hay HDFS.

```mermaid
flowchart TB
    subgraph Init["Inicialización de Entorno"]
        LOCAL["initLocalDatalake<br/>Crea dirs, copia CSVs<br/>→ DatalakeConfig"]
        HDFS_INIT["setupHadoopEnvironment<br/>+ ingestRawData<br/>+ initHdfsDatalake<br/>→ DatalakeConfig"]
    end

    subgraph Run["run(spark, config)"]
        S0["STAGE 0: HIVE SETUP<br/>CREATE DATABASE lakehouse<br/>ensureHiveCatalog<br/>(solo si hiveEnabled)"]
        S1["STAGE 1: BRONZE<br/>BronzeLayer.process"]
        S2["STAGE 2: SILVER<br/>SilverLayer.process"]
        S3["STAGE 3: GOLD<br/>GoldLayer.process"]
        S4["STAGE 4: HIVE CATALOG<br/>registerHiveTables<br/>(solo si hiveEnabled)"]
        SUMMARY["printDatalakeSummary"]
    end

    LOCAL --> Run
    HDFS_INIT --> Run
    S0 --> S1 --> S2 --> S3 --> S4 --> SUMMARY

    style Init fill:#e3f2fd
    style Run fill:#e8f5e9
```

| Función | Descripción |
|---------|-------------|
| `setupHadoopEnvironment` | Configura Hadoop, crea estructura HDFS, valida datalake |
| `ingestRawData` | Sube CSVs locales a HDFS `/hive/warehouse/datalake/raw` |
| `initLocalDatalake` | Crea directorios locales `./datalake/{raw,bronze,silver,gold}` |
| `initHdfsDatalake` | Construye `DatalakeConfig` con rutas HDFS |
| `run` | Ejecuta STAGE 0→4 secuencialmente |
| `registerHiveTables` | Registra 7 tablas Gold (DELTA) + 8 Silver (PARQUET) en Hive |

#### WF2: AnalyticsWorkflow

> [`src/main/scala/medallion/workflow/AnalyticsWorkflow.scala`](src/main/scala/medallion/workflow/AnalyticsWorkflow.scala)

Wrapper que invoca `BIChartGenerator.generate()` para producir 10 gráficos PNG desde las tablas Gold. Tiene `main()` propio para ejecución standalone.

#### WF3: HiveWorkflow

> [`src/main/scala/medallion/workflow/HiveWorkflow.scala`](src/main/scala/medallion/workflow/HiveWorkflow.scala)

Auditoría cruzada entre catálogo Hive y datos físicos en HDFS. Para cada tabla verifica: registrada en Hive? + datos Delta/Parquet existen en HDFS?

| Estado | Significado |
|--------|-------------|
| `✔ OK` | Registrada en Hive + datos existen en HDFS |
| `⚠ REGISTRADA pero sin datos` | En catálogo pero sin archivos |
| `✗ DATOS EXISTEN pero NO registrada` | Archivos presentes, falta registro Hive |
| `✗ NO EXISTE` | Ni registro ni datos |

#### WF4: DataQualityWorkflow

> [`src/main/scala/medallion/workflow/DataQualityWorkflow.scala`](src/main/scala/medallion/workflow/DataQualityWorkflow.scala)

Validación de calidad por capa con muestreo ligero (100 rows) para evitar impacto en memoria.

```mermaid
flowchart LR
    INPUT["validateLayer<br/>(spark, layer, path,<br/>tables, format)"] --> LOOP["Para cada tabla"]

    LOOP --> EXISTS{"pathExists?"}
    EXISTS -->|No| FAIL["score = 0"]
    EXISTS -->|Sí| READ["spark.read<br/>.format(format)<br/>.load(path)"]

    READ --> SCHEMA["Schema check<br/>colCount ≥ expected"]
    READ --> SAMPLE["df.limit(100)<br/>.collect()"]

    SAMPLE --> NULLS["Tasa de nulos<br/>nullCells / totalCells × 100"]
    SAMPLE --> DUPS["Tasa de duplicados<br/>(total - distinct) / total × 100"]

    SCHEMA & NULLS & DUPS --> SCORE["Quality Score<br/>= 40% schema + 30% nulls + 30% dups"]

    SCORE --> GRADE{"Score?"}
    GRADE -->|"≥ 90"| A["✔"]
    GRADE -->|"≥ 70"| B["⚠"]
    GRADE -->|"< 70"| C["✗"]

    style INPUT fill:#e8f5e9
```

**Score compuesto:**
- **40%** → Existencia + conformidad de schema
- **30%** → Tasa de nulos (penaliza × 2)
- **30%** → Tasa de duplicados (penaliza × 2)

#### WF5: LineageWorkflow

> [`src/main/scala/medallion/workflow/LineageWorkflow.scala`](src/main/scala/medallion/workflow/LineageWorkflow.scala)

Captura y genera un grafo de linaje completo del pipeline. Las dependencias entre tablas se obtienen de `TableRegistry.sourcesMap(layer)` (fuente única de verdad):

```mermaid
flowchart TB
    subgraph RAW["RAW (CSV)"]
        R1["Categoria.csv"]
        R2["Subcategoria.csv"]
        R3["Producto.csv"]
        R4["VentasInternet.csv"]
        R5["Sucursales.csv"]
        R6["FactMine.csv"]
        R7["Mine.csv"]
    end

    subgraph Bronze["BRONZE (Parquet)"]
        B1["categoria"]
        B2["subcategoria"]
        B3["producto"]
        B4["ventasinternet"]
        B5["sucursales"]
        B6["factmine"]
        B7["mine"]
    end

    subgraph Silver["SILVER (Parquet)"]
        S1["catalogo_productos"]
        S2["ventas_enriquecidas"]
        S3["resumen_ventas_mensuales"]
        S4["rentabilidad_producto"]
        S5["segmentacion_clientes"]
        S6["produccion_operador"]
        S7["eficiencia_minera"]
        S8["produccion_por_pais"]
    end

    subgraph Gold["GOLD (Delta Lake)"]
        G1["dim_producto"]
        G2["dim_cliente"]
        G3["fact_ventas"]
        G4["kpi_ventas_mensuales"]
        G5["dim_operador"]
        G6["fact_produccion_minera"]
        G7["kpi_mineria"]
    end

    R1 --> B1
    R2 --> B2
    R3 --> B3
    R4 --> B4
    R5 --> B5
    R6 --> B6
    R7 --> B7

    B3 & B2 & B1 --> S1
    B4 & B3 & B2 & B1 --> S2
    B4 & B3 & B2 & B1 --> S3
    B4 & B3 & B2 & B1 --> S4
    B4 --> S5
    B7 --> S6
    B6 --> S7
    B7 --> S8

    S1 & S4 --> G1
    S5 --> G2
    S2 & S5 --> G3
    S3 --> G4
    S6 --> G5
    S7 & S8 --> G6
    S8 --> G7

    style RAW fill:#ff9800,color:#000
    style Bronze fill:#cd7f32,color:#fff
    style Silver fill:#c0c0c0,color:#000
    style Gold fill:#ffd700,color:#000
```

Exporta un manifiesto JSON con columnas, tipos, fuentes y timestamps: `lineage_YYYYMMDD_HHmmss.json`.

#### WF6: MetricsWorkflow

> [`src/main/scala/medallion/workflow/MetricsWorkflow.scala`](src/main/scala/medallion/workflow/MetricsWorkflow.scala)

Captura thread-safe de métricas de ejecución usando `ConcurrentHashMap` y `ConcurrentLinkedQueue`.

| Función | Descripción |
|---------|-------------|
| `startPipeline()` | Marca t₀ global del pipeline |
| `startStage(name)` | Registra inicio de un stage |
| `endStage(name, tables)` | Calcula duración y registra `StageMetric` |
| `generateReport()` | Timeline visual con barras, throughput, bottleneck, JVM memory |
| `exportMetrics(path)` | Exporta `metrics_YYYYMMDD_HHmmss.json` |

**Métricas calculadas:**
- Duración por stage con timeline visual (`█░`)
- Throughput: tablas/segundo del ETL
- Detección de workflows paralelos (overlap de timestamps)
- Identificación del bottleneck (stage más lento)
- Snapshot de memoria JVM (used/max MB)

---

### analytics/ — Generación BI

#### BIChartGenerator

> [`src/main/scala/medallion/analytics/BIChartGenerator.scala`](src/main/scala/medallion/analytics/BIChartGenerator.scala)

Motor de generación de gráficos PNG headless con JFreeChart. Produce 10 visualizaciones (1200×700px) desde tablas Gold Delta Lake.

```mermaid
flowchart TB
    GEN["generate(spark, goldPath, outputDir)"]

    GEN --> CHARTS

    subgraph CHARTS["10 Gráficos"]
        subgraph RetailCharts["Retail (6)"]
            C01["01 — Ingreso por Categoría<br/>Barras horizontales<br/>kpi_ventas_mensuales"]
            C02["02 — Margen Mensual Tendencia<br/>Líneas temporales<br/>kpi_ventas_mensuales"]
            C03["03 — Segmentación Clientes<br/>Pie chart<br/>dim_cliente"]
            C04["04 — Top 10 Productos Revenue<br/>Barras verticales<br/>dim_producto"]
            C05["05 — Clasificación Rentabilidad<br/>Pie chart<br/>dim_producto"]
            C06["06 — Variación MoM %<br/>Barras agrupadas<br/>kpi_ventas_mensuales"]
        end
        subgraph MiningCharts["Mining (3)"]
            C07["07 — Producción por País<br/>Barras agrupadas<br/>kpi_mineria"]
            C08["08 — Eficiencia Operadores<br/>Barras verticales<br/>dim_operador"]
            C09["09 — Desperdicio vs Producción<br/>Stacked bar<br/>kpi_mineria"]
        end
        subgraph Shared["Retail (1)"]
            C10["10 — Ticket Promedio Mensual<br/>Líneas temporales<br/>kpi_ventas_mensuales"]
        end
    end

    CHARTS --> STYLE["styleBarChart / styleLineChart / stylePieChart<br/>+ saveChart (PNG 1200×700)"]

    style RetailCharts fill:#e3f2fd
    style MiningCharts fill:#fff3e0
```

**Configuración visual:**

| Propiedad | Valor |
|-----------|-------|
| Resolución | 1200 × 700 px |
| Background | `#FAFAFA` |
| Título font | SansSerif Bold 18pt |
| Label font | SansSerif Plain 12pt |
| Paleta | 10 colores Material Design |

---

## Flujo de Datos por Capa

```mermaid
graph LR
    subgraph RAW["RAW — 7 CSVs"]
        direction TB
        R1["Categoria · Subcategoria<br/>Producto · VentasInternet<br/>Sucursales · FactMine · Mine"]
    end

    subgraph BRONZE["BRONZE — 7 tablas Parquet"]
        direction TB
        B1["Schema enforcement<br/>Deduplicación<br/>Filtrado de nulos<br/>Columnas auditoría"]
    end

    subgraph SILVER["SILVER — 8 tablas Parquet"]
        direction TB
        S1["JOINs multi-tabla<br/>Cálculos financieros<br/>Segmentación RFM<br/>Métricas operativas mineras"]
    end

    subgraph GOLD["GOLD — 7 tablas Delta"]
        direction TB
        G1["Star Schema dimensional<br/>Window Functions MoM/YTD<br/>LTV, Rankings, Scores<br/>KPIs ejecutivos"]
    end

    subgraph SERVE["Consumo"]
        direction TB
        PBI["Power BI"]
        PNG["Gráficos PNG"]
        HIVE["Hive Catalog"]
    end

    RAW -->|"BronzeLayer"| BRONZE
    BRONZE -->|"SilverLayer"| SILVER
    SILVER -->|"GoldLayer"| GOLD
    GOLD --> PBI & PNG & HIVE

    style RAW fill:#ff9800,color:#000
    style BRONZE fill:#cd7f32,color:#fff
    style SILVER fill:#c0c0c0,color:#000
    style GOLD fill:#ffd700,color:#000
    style SERVE fill:#2980b9,color:#fff
```

---

## Modelos de Datos

### Gold — Star Schema Retail

```mermaid
erDiagram
    dim_producto {
        int producto_key PK
        string producto_nombre
        string producto_color
        string subcategoria
        string categoria
        int total_ventas
        double revenue_total
        double pct_margen
        string clasificacion_rentabilidad
        string clasificacion_rotacion
    }

    dim_cliente {
        int cliente_key PK
        string segmento
        int frecuencia_compras
        double valor_monetario
        double ticket_promedio
        double ltv_anualizado
        int score_frecuencia
        int score_monetario
    }

    fact_ventas {
        string orden_id PK
        int producto_key FK
        int cliente_key FK
        int territorio_key
        string periodo
        double ingreso_bruto
        double margen_bruto
        double ganancia_neta
        string tipo_envio
        boolean tiene_promocion
    }

    kpi_ventas_mensuales {
        int anio
        int mes
        string categoria
        double ingreso_bruto
        double margen_bruto
        double ticket_promedio
        double variacion_mom_pct
        double ingreso_ytd
    }

    dim_producto ||--o{ fact_ventas : "producto_key"
    dim_cliente ||--o{ fact_ventas : "cliente_key"
```

### Gold — Star Schema Mining

```mermaid
erDiagram
    dim_operador {
        int operador_key PK
        string nombre_completo
        string pais
        int total_operaciones
        double total_mineral_extraido
        double pct_desperdicio
        string clasificacion_eficiencia
        int ranking_produccion
    }

    fact_produccion_minera {
        int truck_key PK
        int proyecto_key PK
        double total_mineral
        double produccion_neta
        double pct_desperdicio
        string nivel_eficiencia
        double coef_variacion_pct
        double pct_contribucion_global
    }

    kpi_mineria {
        string pais PK
        int total_operadores
        int total_trucks
        double total_mineral
        double produccion_neta
        double tasa_desperdicio_pct
        double mineral_por_operador
        double mineral_por_truck
        string evaluacion_operativa
    }
```

---

## Changelog v4.0

### Nuevos componentes

| Archivo | Descripción |
|---------|-------------|
| `config/TableRegistry.scala` | Fuente única de verdad para 22 tablas (7 Bronze + 8 Silver + 7 Gold) con nombres, formatos, dependencias, claves de deduplicación y esquemas CSV |
| `infra/CircuitBreaker.scala` | State machine Closed→Open→HalfOpen para protección contra fallos sistémicos IO con threshold configurable y reset temporal |
| `infra/ViewScope.scala` | Loan pattern para gestión automática del ciclo de vida de vistas temporales Spark con cleanup garantizado en `finally` |

### Archivos refactorizados

| Archivo | Cambios principales |
|---------|-------------------|
| `Pipeline.scala` | v3→v4: reescritura de ~190 líneas imperativas a DAG declarativo (~20 líneas), eliminado `Future.sequence`/`withRetry`/`writeCheckpoint` manuales |
| `engine/DagTask.scala` | Canal de error tipado `Either[TaskError, Unit]`, flag `critical`, `DagTask.fromUnit` para compatibilidad, `CompletedWithWarning` status |
| `engine/DagExecutor.scala` | `NonFatal` guards, checkpoint JSON con metadatos (timestamp, duración), soporte `critical=false` y `CompletedWithWarning` |
| `layer/BronzeLayer.scala` | Iteración declarativa desde TableRegistry, Dead Letter Queue (`_rejected/`) para filas con claves nulas |
| `layer/SilverLayer.scala` | `ViewScope.withBronzeViews` loan pattern, eliminado `registerBronzeTables`/`dropBronzeViews`/`System.gc()` |
| `layer/GoldLayer.scala` | `ViewScope.withSilverViews` loan pattern, eliminado 8× `freeMemory()`/`System.gc()` y gestión manual de vistas |
| `workflow/EtlWorkflow.scala` | `TableRegistry` para todas las listas de tablas, `NonFatal` en todos los catch blocks |
| `workflow/LineageWorkflow.scala` | `TableRegistry.sourcesMap(layer)` reemplaza 3 mapas hardcoded de dependencias |
| `workflow/HiveWorkflow.scala` | `TableRegistry.goldNames`/`silverNames` reemplaza listas hardcoded |

### Mejoras por eje

| Eje | Mejoras |
|-----|---------|
| **Declarativo** | Pipeline como DAG de tasks con dependencias; BronzeLayer itera desde TableRegistry; listas de tablas centralizadas |
| **Robusto** | `NonFatal` en lugar de `catch Throwable`; errores tipados (Transient/Fatal/Skippable); checkpoint JSON con metadatos |
| **Elástico** | DagExecutor con parallelism configurable; tasks no-críticas (`critical=false`) no bloquean dependientes |
| **Resiliente** | CircuitBreaker para IO; Dead Letter Queue en Bronze; ViewScope garantiza cleanup de vistas |
