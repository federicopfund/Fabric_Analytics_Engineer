# Azure Data Factory — Pipelines y Orquestación

## Resumen

Capa de orquestación basada en Azure Data Factory (ADF) con 7 pipelines que gestionan la extracción, carga y transformación de datos desde SQL Server hacia Azure Data Lake, pasando por Blob Storage como landing zone y Databricks para transformaciones.

---

## Flujo General de Orquestación

```mermaid
flowchart TD
    subgraph Sources["Fuentes"]
        SQL[("SQL Server<br/>dbRetail<br/>6 tablas")]
        CSV_SRC["CSV Files<br/>Batch upload"]
    end

    subgraph Landing["Landing Zone"]
        BLOB["Azure Blob Storage<br/>storageblobsigoxkyj<br/>CSV delimitado"]
    end

    subgraph Lake["Data Lake"]
        ADLS["Azure Data Lake Gen2<br/>datalakeigoxkyj<br/>Hierarchical NS"]
    end

    subgraph Transform["Transformación"]
        DBX["Databricks Notebooks<br/>Cluster 1123-103841"]
    end

    subgraph Pipelines["ADF Pipelines"]
        P1["Pipeline_llenado_db<br/>Inicializar tablas"]
        P2["Pipeline_extraccion<br/>SQL → CSV"]
        P3["Copy_data_sql<br/>Batch copy"]
        P4["Busqueda de tablas CSV<br/>Descubrimiento"]
        P5["Copy data SQL to CSV1<br/>Replicación"]
        P6["ETL<br/>Orquestación principal"]
    end

    SQL --> P1
    P1 -->|Schema validation| SQL
    SQL --> P2
    P2 --> BLOB
    SQL --> P3
    P3 --> BLOB
    BLOB --> P4
    P4 --> P6
    BLOB --> P5
    P5 --> ADLS
    P6 --> DBX
    DBX --> ADLS
```

---

## Pipeline 1: `Pipeline_llenado_db`

**Propósito:** Inicialización de la base de datos — validación de schema y creación de tablas.

```mermaid
flowchart TD
    START([Trigger]) --> LOOKUP["Lookup<br/>Validar schema existente"]
    LOOKUP --> CONDITION{"¿Tabla<br/>existe?"}
    CONDITION -->|No| SCRIPT["Script Activity<br/>CREATE TABLE"]
    CONDITION -->|Sí| SKIP["Skip — tabla existente"]
    SCRIPT --> END_P([Fin])
    SKIP --> END_P
```

| Actividad | Tipo | Descripción |
|-----------|------|-------------|
| Lookup | Validation | Verifica existencia del schema |
| Script | DDL | `CREATE TABLE` para tablas faltantes |
| IfCondition | Control | Branching condicional |

---

## Pipeline 2: `Pipeline_extraccion`

**Propósito:** Extracción de datos desde SQL Server hacia CSV en Blob Storage.

```mermaid
flowchart TD
    START([Trigger]) --> FOREACH["ForEach<br/>Iterar tablas"]
    FOREACH --> COPY["Copy Activity<br/>SQL Source → CSV Sink"]
    COPY --> NAMING["Parametrización<br/>Nombre archivo dinámico"]
    NAMING --> BLOB[("Blob Storage<br/>Landing Zone")]
```

---

## Pipeline 3: `Copy_data_sql`

**Propósito:** Copia batch de múltiples tablas SQL Server a Azure Storage.

```mermaid
flowchart TD
    START([Trigger]) --> FOREACH["ForEach<br/>Lista de tablas parametrizada"]
    FOREACH --> COPY["Copy Activity<br/>SQL → CSV delimitado"]
    COPY --> BLOB[("Blob Storage<br/>Multi-table export")]
```

---

## Pipeline 4: `Busqueda de tablas CSV`

**Propósito:** Descubrimiento dinámico de archivos CSV en el landing zone.

```mermaid
flowchart TD
    START([Trigger]) --> META["GetMetadata<br/>Enumerar archivos en container"]
    META --> LIST["Generar lista dinámica<br/>de archivos CSV"]
    LIST --> OUTPUT["Output: File list<br/>para pipelines downstream"]
```

---

## Pipeline 5: `Copy data SQL to CSV1`

**Propósito:** Replicación de tablas fuente al landing zone con particionamiento.

```mermaid
flowchart TD
    START([Trigger]) --> PARTITION["SQL Partition<br/>Lectura paralela"]
    PARTITION --> COPY["Copy Activity<br/>SQL → Delimited Text"]
    COPY --> SINK[("Blob Sink<br/>CSV format")]
```

---

## Pipeline 6: `ETL` (Principal)

**Propósito:** Orquestación principal del pipeline Medallion — metadata extraction, variable management, y ejecución de notebooks Databricks.

```mermaid
flowchart TD
    START([Trigger]) --> META["GetMetadata<br/>Extraer metadata de archivos"]
    META --> FOREACH["ForEach<br/>Iterar datasets"]
    FOREACH --> APPEND["AppendVariable<br/>Acumular lista de archivos"]
    APPEND --> SET["SetVariable<br/>Configurar parámetros"]
    SET --> DBX["DatabricksNotebook<br/>Ejecutar transformación<br/>Medallion pipeline"]
    DBX --> END_P([Pipeline completado])
```

| Actividad | Tipo | Detalles |
|-----------|------|----------|
| GetMetadata | Discovery | Enumera archivos en landing zone |
| ForEach | Iterator | Procesa cada dataset encontrado |
| AppendVariable | Array | Construye lista de archivos a procesar |
| SetVariable | Config | Parámetros para notebook |
| DatabricksNotebook | Transform | Ejecuta notebook en cluster `1123-103841` |

---

## Linked Services — Mapa de Conexiones

```mermaid
graph TB
    subgraph ADF["Azure Data Factory"]
        P_ETL["Pipeline ETL"]
        P_EXT["Pipeline Extracción"]
        P_COPY["Pipeline Copy"]
    end

    subgraph SQL_LS["SQL Linked Services"]
        LS1["AzureSqlDatabase1<br/>sqlservergrupo1"]
        LS2["AzureSqlDatabaseLinkedService<br/>server--igoxkyj (SSL)"]
        LS3["AzureLinkServiceSqlDatabas<br/>Duplicado"]
    end

    subgraph Storage_LS["Storage Linked Services"]
        LS4["AzureBlobStorage<br/>storageblobsigoxkyj"]
        LS5["AzureDataLakeStorage<br/>datalakeigoxkyj.dfs"]
        LS6["AzureStorageLinkedService<br/>storageblobsigoxkyj"]
    end

    subgraph Compute_LS["Compute Linked Services"]
        LS7["LinkendServiceTranformaciones<br/>Databricks"]
    end

    P_EXT -->|Source| LS1
    P_EXT -->|Sink| LS4
    P_COPY -->|Source| LS2
    P_COPY -->|Sink| LS6
    P_ETL -->|Transform| LS7
    P_ETL -->|Output| LS5
```

---

## Actividades ADF — Tipos Utilizados

```mermaid
pie title Distribución de Actividades ADF
    "Copy Activity" : 4
    "ForEach" : 3
    "GetMetadata" : 2
    "SetVariable" : 2
    "AppendVariable" : 1
    "Lookup" : 1
    "Script" : 1
    "IfCondition" : 1
    "DatabricksNotebook" : 1
```

---

## Data Factory Configurations

| Archivo | Descripción |
|---------|-------------|
| `adfactory-igoxkyj.json` | Config factory principal |
| `adfactory-xk1rme4.json` | Config factory secundaria |
| `adfactoryqymrutj.json` | Config factory terciaria |
| `publish_config.json` | Configuración de publicación ADF |

---

## Flujo de Datos Completo

```mermaid
graph LR
    SQL[("SQL Server<br/>dbRetail")] -->|"Pipeline_extraccion<br/>Copy_data_sql"| BLOB["Blob Storage<br/>CSV Landing"]
    BLOB -->|"Busqueda CSV<br/>Discovery"| META["File Metadata"]
    META -->|"ETL Pipeline"| DBX["Databricks<br/>Notebook"]
    DBX -->|"Medallion Transform"| ADLS["Data Lake Gen2<br/>Bronze/Silver/Gold"]
    ADLS -->|"Power BI<br/>DirectQuery"| PBI["📊 Dashboards"]
```
