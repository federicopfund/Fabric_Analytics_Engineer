# ETL Pipeline — Spark Medallion (Scala)

## Resumen

Pipeline ETL batch implementado en Scala sobre Apache Spark 3.5 que sigue el patrón Medallion (RAW → BRONZE → SILVER → GOLD). Incluye un motor de ejecución DAG con typed errors, detección de deadlock, sanitización de dependencias por feature flags, circuit breaker, checkpoints JSON, escritura **adaptativa overwrite/MERGE** en Gold con auditoría Delta, y 6 workflows especializados.

**Versión actual:** v5 — Force-refresh + adaptive Gold writes + audit metrics.

---

## Arquitectura del Pipeline

```mermaid
graph TB
    subgraph Entry["Punto de Entrada"]
        MAIN["Pipeline.scala<br/>main() v4.0 DAG"]
    end

    subgraph Config["Configuración"]
        DC["DatalakeConfig<br/>Paths, formats, names"]
        SF["SparkFactory<br/>Singleton SparkSession"]
        TR["TableRegistry<br/>Metadata de tablas"]
    end

    subgraph Schema["Schemas"]
        CS["CsvSchemas<br/>7 StructType definitions"]
    end

    subgraph Engine["Motor de Ejecución"]
        DT["DagTask<br/>Nodo tipado con errors"]
        DE["DagExecutor<br/>Ejecución paralela<br/>JSON checkpoints"]
    end

    subgraph Infra["Infraestructura"]
        DLIO["DataLakeIO<br/>Parquet/Delta/CSV R/W"]
        HDFS["HdfsManager<br/>Paths + upload"]
        CB["CircuitBreaker<br/>Closed→Open→HalfOpen"]
        VS["ViewScope<br/>Loan pattern temp views"]
    end

    subgraph Layers["Capas Medallion"]
        BZ["BronzeLayer<br/>CSV → Parquet<br/>Dead Letter Queue"]
        SL["SilverLayer<br/>Agregaciones<br/>Business logic"]
        GL["GoldLayer<br/>Star Schema<br/>Delta Lake ACID"]
    end

    subgraph Workflows["6 Workflows"]
        WF1["WF1: EtlWorkflow<br/>RAW→BRONZE→SILVER→GOLD"]
        WF2["WF2: AnalyticsWorkflow<br/>10 Charts PNG"]
        WF3["WF3: HiveWorkflow<br/>Catalog audit"]
        WF4["WF4: DataQualityWorkflow<br/>Schema + nulls + cardinality"]
        WF5["WF5: LineageWorkflow<br/>Tracking source→gold"]
        WF6["WF6: MetricsWorkflow<br/>Timing + counts + stats"]
    end

    MAIN --> Config
    MAIN --> Engine
    Config --> Layers
    Schema --> BZ
    Engine --> Workflows
    Infra --> Layers
    Layers --> Workflows
```

---

## Módulos del Pipeline

### Módulo de Configuración

```mermaid
classDiagram
    class DatalakeConfig {
        +String basePath
        +String rawPath
        +String bronzePath
        +String silverPath
        +String goldPath
        +String checkpointPath
        +String format
    }

    class SparkFactory {
        +getOrCreate() SparkSession
        -configureS3A()
        -configureDelta()
        -configureKryo()
    }

    class TableRegistry {
        +Map tables
        +getSchema(name) StructType
        +getPath(name, layer) String
        +listTables(layer) List
    }

    DatalakeConfig --> SparkFactory
    DatalakeConfig --> TableRegistry
```

### Módulo de Schemas — 7 CSV

| Schema | Campos Clave | Rows Estimados |
|--------|-------------|----------------|
| `Categoria` | CategoriaID, Nombre | ~4 |
| `Subcategoria` | SubcategoriaID, CategoriaID, Nombre | ~37 |
| `Producto` | ProductoID, SubcategoriaID, Nombre, Precio, Costo | ~319 |
| `Cliente` | ClienteID, Nombre, Segmento, Ciudad | ~18,484 |
| `Orden` | OrdenID, ClienteID, FechaOrden, Flete | ~48,895 |
| `OrdenDetalle` | DetalleID, OrdenID, ProductoID, Cantidad, PrecioUnitario | ~48,895 |
| `FactMine` | Operador, Producción, Desperdicio, Turno | Variable |

---

## Motor DAG — Ejecución con Checkpoints

```mermaid
stateDiagram-v2
    [*] --> LoadCheckpoint: Leer task_status.json
    LoadCheckpoint --> BuildDAG: Construir grafo de tareas

    BuildDAG --> Execute: DagExecutor.run()

    state Execute {
        [*] --> Pending
        Pending --> Running: Dependencias resueltas
        Running --> Success: Exit OK
        Running --> Failed: NonFatal exception
        Failed --> Retry: attempts < maxRetries
        Retry --> Running: Backoff wait
        Failed --> Critical: Fatal / max retries
        Success --> [*]
        Critical --> [*]
    }

    Execute --> SaveCheckpoint: Persistir estado JSON
    SaveCheckpoint --> Report: Generar reporte
    Report --> [*]
```

### DagTask — Nodo del Grafo

```mermaid
classDiagram
    class DagTask {
        +String id
        +String name
        +List~String~ dependencies
        +Function action
        +ErrorType errorType
        +Int maxRetries
        +execute() Either~Error, Result~
    }

    class ErrorType {
        <<enumeration>>
        CRITICAL
        FATAL
        NOTIFICATION
    }

    class DagExecutor {
        +List~DagTask~ tasks
        +String checkpointPath
        +run() ExecutionReport
        -resolveOrder() List
        -executeParallel() Results
        -saveCheckpoint()
    }

    DagTask --> ErrorType
    DagExecutor --> DagTask
```

---

## Circuit Breaker

```mermaid
stateDiagram-v2
    [*] --> Closed: Estado inicial

    Closed --> Closed: Éxito → reset counter
    Closed --> Open: Failures ≥ threshold

    Open --> Open: Requests rechazados
    Open --> HalfOpen: Timeout expirado

    HalfOpen --> Closed: Éxito → reset
    HalfOpen --> Open: Fallo → re-open

    note right of Closed: Operaciones normales
    note right of Open: Fallback / error rápido
    note right of HalfOpen: Prueba de recuperación
```

---

## Capas Medallion — Detalle

### Bronze Layer

```mermaid
flowchart TD
    RAW["RAW<br/>7 CSV files"] --> VALIDATE["Schema Validation<br/>CsvSchemas.scala"]
    VALIDATE -->|Valid| PARQUET["Write Parquet<br/>1:1 mapping"]
    VALIDATE -->|Invalid| DLQ["Dead Letter Queue<br/>Registros rechazados"]
    PARQUET --> BRONZE[("BRONZE<br/>7 directorios Parquet")]
    DLQ --> LOG["Log errores<br/>para revisión"]
```

### Silver Layer

```mermaid
flowchart TD
    BRONZE[("BRONZE<br/>Parquet")] --> VIEWS["Crear Temporary Views<br/>ViewScope loan pattern"]
    VIEWS --> AGG["Agregaciones SQL<br/>Business Logic"]

    AGG --> DIM_CAT["dim_categoria<br/>Categoría + Subcategoría"]
    AGG --> DIM_PROD["dim_producto<br/>Producto enriched"]
    AGG --> DIM_CLI["dim_cliente<br/>RFM segmentation"]
    AGG --> FACT_ORD["fact_ordenes<br/>Órdenes joined"]
    AGG --> KPI["kpi_ventas_mensuales<br/>Monthly aggregation"]
    AGG --> DIM_MINE["dim_operador_mineria<br/>Mining operators"]
```

### Gold Layer — Star Schema

```mermaid
erDiagram
    fact_ventas {
        int OrdenID PK
        int ClienteID FK
        int ProductoID FK
        date FechaOrden
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

    dim_cliente {
        int ClienteID PK
        string Nombre
        string Segmento
        string SegmentoRFM
        decimal LTV
        decimal FrecuenciaCompra
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

    Calendario {
        date Fecha PK
        int Anio
        int Mes
        int Trimestre
        string NombreMes
        boolean FinDeSemana
    }

    fact_ventas }o--|| dim_cliente : "ClienteID"
    fact_ventas }o--|| dim_producto : "ProductoID"
    fact_ventas }o--|| Calendario : "FechaOrden"
    kpi_ventas_mensuales }o--|| Calendario : "Mes"
```

**Formato Gold:** Delta Lake con transacciones ACID.

---

## 6 Workflows

```mermaid
graph TB
    subgraph Core["Core Workflows"]
        WF1["WF1: EtlWorkflow<br/>──────────────<br/>RAW → BRONZE → SILVER → GOLD<br/>Pipeline principal"]
    end

    subgraph Analytics["Analytics"]
        WF2["WF2: AnalyticsWorkflow<br/>──────────────<br/>10 Charts PNG<br/>JFreeChart engine"]
    end

    subgraph Quality["Calidad & Governance"]
        WF3["WF3: HiveWorkflow<br/>──────────────<br/>Catálogo de tablas<br/>Particiones, audit"]
        WF4["WF4: DataQualityWorkflow<br/>──────────────<br/>Schema validation<br/>Nulls, cardinality"]
        WF5["WF5: LineageWorkflow<br/>──────────────<br/>Source → Bronze →<br/>Silver → Gold tracking"]
        WF6["WF6: MetricsWorkflow<br/>──────────────<br/>Execution timing<br/>Row counts, stats"]
    end

    WF1 --> WF2
    WF1 --> WF3
    WF1 --> WF4
    WF4 --> WF5
    WF5 --> WF6
```

### WF2: Analytics — 10 Charts BI

| # | Chart | Tipo | Fuente |
|---|-------|------|--------|
| 1 | Ingreso Bruto por Categoría | Horizontal bars | `kpi_ventas_mensuales` |
| 2 | Tendencia Margen Mensual | Line series | `kpi_ventas_mensuales` |
| 3 | Segmentación Clientes RFM | Pie chart | `dim_cliente` |
| 4 | Top 10 Productos por Revenue | Vertical bars | `dim_producto` |
| 5 | Clasificación Rentabilidad | Pie chart | `dim_producto` |
| 6 | Variación MoM de Ingresos | Grouped bars | `kpi_ventas_mensuales` |
| 7-10 | Mining/Operational | Various | `FactMine` |

---

## Build — sbt Configuration

| Dependencia | Versión |
|-------------|---------|
| Scala | 2.12 |
| Spark Core/SQL | 3.5 |
| Delta Lake | 3.x |
| JFreeChart | Latest |
| Db2 JCC | 11.5.5 |

**Artefacto:** `root-assembly-2.0.0.jar` (fat JAR)

---

## Arquitectura Final del ETL (v5)

Esta sección consolida la evolución del pipeline tras los fixes de estabilidad y rendimiento aplicados sobre Bronze/Silver/Gold y el motor DAG.

### Vista end-to-end

```mermaid
flowchart LR
    SIM["sales-simulator<br/>genera ventas en raw"] --> RAW[("RAW CSVs<br/>COS bucket raw")]
    RAW --> BR["BronzeLayer<br/>shouldSkip + force-refresh"]
    BR --> BRONZE[("BRONZE Parquet")]
    BRONZE --> SL["SilverLayer<br/>shouldSkip + force-refresh"]
    SL --> SILVER[("SILVER Parquet")]
    SILVER --> GL["GoldLayer<br/>writeOrMergeDelta + audit"]
    GL --> GOLD[("GOLD Delta Lake")]

    GOLD --> VAC["vacuumDeltaTables<br/>opt-in semanal"]
    GOLD --> AUDIT["AUDIT log<br/>numInserted / numUpdated / numFiles"]
    GOLD --> BI["Power BI / Db2 export"]
```

### Cambios incorporados (v4 → v5)

| Componente | Cambio | Beneficio |
|---|---|---|
| `WorkflowRegistry.buildDag` | Sanitiza dependencias eliminadas por feature flags | Evita deadlocks por dep huérfanas |
| `DagExecutor.scheduleReady` | Detección de deadlock + early exit `Failed(Unreachable)` | Termina la sesión Spark en vez de colgar 15 min |
| `DataLakeIO.shouldSkip` | Reemplaza `pathExists` en 13 sitios; respeta `PIPELINE_FORCE_REFRESH` (default `true`) | Bronze/Silver/Gold reprocesan datos nuevos sin borrado manual |
| `DataLakeIO.writeOrMergeDelta` | Estrategia adaptativa overwrite ↔ MERGE según tamaño actual | Costo bajo hoy, escala cuando crece |
| `DataLakeIO.writeDeltaReplaceWhere` | Overwrite parcial por predicate | KPIs reescriben solo los meses tocados |
| `DataLakeIO.logLastOperationMetrics` | Audita el último commit Delta | Visibilidad: qué se insertó/actualizó/borró |
| `DataLakeIO.vacuumDeltaTables` | VACUUM batch best-effort | Limpia parquets de versiones viejas |
| `submit-to-ae.sh --skip-csv-upload` | Preserva CSVs producidos por el simulator | Permite re-runs sin pisar datos generados |

### Capas: estrategia de escritura

```mermaid
flowchart TB
    subgraph Bronze["BRONZE \u2014 Parquet"]
        B1["mode(overwrite)<br/>shouldSkip respeta PIPELINE_FORCE_REFRESH"]
    end
    subgraph Silver["SILVER \u2014 Parquet"]
        S1["mode(overwrite)<br/>shouldSkip respeta PIPELINE_FORCE_REFRESH"]
    end
    subgraph Gold["GOLD \u2014 Delta Lake"]
        G1["dim_producto / dim_operador<br/>fact_produccion_minera / kpi_mineria<br/>\u2192 writeDelta full overwrite"]
        G2["kpi_ventas_mensuales<br/>\u2192 writeDeltaReplaceWhere<br/>(predicate por a\u00f1os tocados)"]
        G3["fact_ventas / dim_cliente<br/>\u2192 writeOrMergeDelta<br/>(overwrite < umbral, MERGE \u2265 umbral)"]
    end

    Bronze --> Silver --> Gold
```

### Decisión adaptativa overwrite ↔ MERGE

`DataLakeIO.chooseWriteStrategy` resuelve la estrategia con esta precedencia:

```mermaid
flowchart TD
    A["Inicio writeOrMergeDelta(table, threshold)"] --> B{"PIPELINE_GOLD_FORCE_MERGE = true?"}
    B -- s\u00ed --> M["IncrementalMerge"]
    B -- no --> C{"&lt;TABLE&gt;_MERGE_THRESHOLD<br/>en env?"}
    C -- s\u00ed --> D["usar override por tabla"]
    C -- no --> E{"PIPELINE_MERGE_THRESHOLD_ROWS<br/>en env?"}
    E -- s\u00ed --> F["usar override global"]
    E -- no --> G["usar threshold del par\u00e1metro<br/>(5M fact / 1M dim)"]
    D --> H{"countDeltaRows(table) >= threshold?"}
    F --> H
    G --> H
    H -- s\u00ed --> M
    H -- no --> O["FullOverwrite"]
    M --> X["mergeDelta + AUDIT"]
    O --> Y["writeDelta + AUDIT"]
```

### Variables de entorno operativas

| Variable | Default | Efecto |
|---|---|---|
| `PIPELINE_FORCE_REFRESH` | `true` | `false` activa el comportamiento idempotente original (skip si la tabla existe) |
| `PIPELINE_GOLD_FORCE_MERGE` | `false` | Fuerza MERGE en `fact_ventas` y `dim_cliente` sin esperar al umbral |
| `PIPELINE_MERGE_THRESHOLD_ROWS` | — | Cota global que pisa los defaults (5M fact, 1M dim) |
| `FACT_VENTAS_MERGE_THRESHOLD` | `5_000_000` | Override solo para `fact_ventas` |
| `DIM_CLIENTE_MERGE_THRESHOLD` | `1_000_000` | Override solo para `dim_cliente` |
| `PIPELINE_VACUUM` | `false` | `true` corre `VACUUM` 7d sobre las 7 tablas Gold al final del run |

### DAG: deadlock detection

```mermaid
stateDiagram-v2
    [*] --> Build: WorkflowRegistry.buildDag()
    Build --> Sanitize: Filtrar deps removidas por feature flags
    Sanitize --> Schedule: DagExecutor.scheduleReady()
    Schedule --> Running: Hay tareas listas
    Running --> Completed: Todas las deps satisfechas
    Schedule --> Deadlock: No hay listas y quedan pendientes
    Deadlock --> EarlyExit: Marcar Failed(Unreachable) y cerrar sesi\u00f3n
    Completed --> [*]
    EarlyExit --> [*]
```

### Auditoría por tabla Delta

Cada escritura Gold emite una línea por commit con métricas extraídas del Delta log:

```
\ud83d\udcca AUDIT fact_ventas v3 op=MERGE ts=2026-04-17 numOutputRows=42202 \\
  numTargetRowsInserted=42202 numTargetRowsUpdated=0 numFiles=1
```

Métricas reportadas: `numOutputRows`, `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`, `numFiles`, `numRemovedFiles`.

### Mantenimiento programado

```mermaid
flowchart LR
    CRON["Schedule semanal"] --> RUN["submit-to-ae.sh --skip-csv-upload"]
    RUN -- "PIPELINE_VACUUM=true" --> V["vacuumDeltaTables(7d retention)"]
    V --> CLEAN["Bucket gold limpio<br/>parquets v\u2264N-1 eliminados"]
```

Recomendación operativa: VACUUM semanal para Gold; `OPTIMIZE` (compactación) cuando `numFiles > 100` por tabla.

---

## Estructura de Directorios del Pipeline

```mermaid
graph TB
    ROOT["batch-etl-scala/"] --> SRC["src/main/scala/"]
    SRC --> PKG_CONF["config/<br/>DatalakeConfig.scala<br/>SparkFactory.scala<br/>TableRegistry.scala"]
    SRC --> PKG_SCHEMA["schema/<br/>CsvSchemas.scala"]
    SRC --> PKG_INFRA["infrastructure/<br/>DataLakeIO.scala<br/>HdfsManager.scala<br/>CircuitBreaker.scala<br/>ViewScope.scala"]
    SRC --> PKG_ENGINE["engine/<br/>DagTask.scala<br/>DagExecutor.scala"]
    SRC --> PKG_LAYER["layer/<br/>BronzeLayer.scala<br/>SilverLayer.scala<br/>GoldLayer.scala"]
    SRC --> PKG_WF["workflow/<br/>EtlWorkflow.scala<br/>AnalyticsWorkflow.scala<br/>HiveWorkflow.scala<br/>DataQualityWorkflow.scala<br/>LineageWorkflow.scala<br/>MetricsWorkflow.scala"]
    SRC --> PKG_ANALYTICS["analytics/<br/>BIChartGenerator.scala"]
    ROOT --> BUILD["build.sbt"]
    ROOT --> PIPELINE["Pipeline.scala"]
```
