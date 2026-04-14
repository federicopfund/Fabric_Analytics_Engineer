# Azure Infrastructure — Documentación Técnica

## Resumen

Infraestructura en Azure que incluye Azure Data Factory para orquestación ETL, SQL Server como fuente de datos, Blob Storage como landing zone, Data Lake Gen2 para almacenamiento analítico, y Databricks para transformaciones interactivas. Provisionada via PowerShell y Azure Pipelines.

---

## Topología Azure

```mermaid
graph TB
    subgraph RG["Resource Group: Rgfedericopfund"]
        subgraph DataSources["Fuentes de Datos"]
            SQL[("SQL Server<br/>sqlservergrupo1<br/>dbRetail")]
        end

        subgraph Storage_L["Storage"]
            BLOB["Blob Storage<br/>storageblobsigoxkyj<br/>Landing Zone"]
            ADLS["Data Lake Gen2<br/>datalakeigoxkyj<br/>Hierarchical NS"]
        end

        subgraph Compute_L["Compute"]
            ADF["Azure Data Factory<br/>7 Pipelines"]
            DBX["Azure Databricks<br/>adb-4624400229247673.13<br/>Cluster: 1123-103841"]
        end

        subgraph CICD_L["CI/CD"]
            AZP["Azure Pipelines<br/>windows-latest"]
        end
    end

    SQL -->|Extract| ADF
    ADF -->|Stage CSV| BLOB
    ADF -->|Load| ADLS
    ADF -->|Notebooks| DBX
    AZP -->|Deploy| ADF
    DBX -->|R/W| ADLS
```

---

## Provisionamiento — PowerShell (`script.ps1`)

```mermaid
flowchart TD
    START([Azure Pipeline Trigger<br/>Push to main]) --> SUFFIX["1. Generar sufijo aleatorio<br/>para nombres únicos"]
    SUFFIX --> LOGIN["2. Login Azure<br/>Credenciales"]
    LOGIN --> RG["3. Crear/verificar<br/>Resource Group<br/>Rgfedericopfund"]
    RG --> STORAGE["4. Crear Storage Account<br/>storageblobsigoxkyj{suffix}"]
    STORAGE --> CONTAINERS["5. Crear contenedores<br/>Landing zone"]
    CONTAINERS --> SQL_C["6. Crear SQL Server<br/>+ Database dbRetail"]
    SQL_C --> DL["7. Crear Data Lake<br/>datalake{suffix}"]
    DL --> DBX_C["8. Crear Databricks<br/>Workspace"]
    DBX_C --> ADF_C["9. Crear Data Factory"]
    ADF_C --> PUBLISH["10. Publish artifacts"]
```

---

## Azure Pipelines (`azure-pipeline_deploy.yml`)

```mermaid
graph LR
    TRIGGER["Trigger:<br/>main branch"] --> POOL["Pool:<br/>windows-latest"]
    POOL --> PS["Task 1:<br/>AzurePowerShell@5<br/>Execute script.ps1"]
    PS --> PUB["Task 2:<br/>Publish Pipeline<br/>Artifacts"]
```

| Campo | Valor |
|-------|-------|
| Trigger | `main` branch |
| Agent Pool | `windows-latest` |
| PowerShell Version | Az Module (latest) |

---

## Linked Services — Conexiones Configuradas

```mermaid
graph TB
    ADF["Azure Data Factory"] --> LS1["AzureSqlDatabase1<br/>sqlservergrupo1.database.windows.net<br/>dbRetail"]
    ADF --> LS2["AzureSqlDatabaseLinkedService<br/>server--igoxkyj.database.windows.net<br/>dbRetail (SSL)"]
    ADF --> LS3["AzureLinkServiceSqlDatabas<br/>Duplicado de LS2"]
    ADF --> LS4["AzureBlobStorage<br/>storageblobsigoxkyj<br/>HTTPS"]
    ADF --> LS5["AzureDataLakeStorage<br/>datalakeigoxkyj.dfs.core.windows.net<br/>HNS habilitado"]
    ADF --> LS6["AzureStorageLinkedService<br/>storageblobsigoxkyj<br/>Blob endpoint"]
    ADF --> LS7["LinkendServiceTranformaciones<br/>Databricks<br/>adb-4624400229247673.13"]
```

| Linked Service | Tipo | Endpoint |
|---------------|------|----------|
| `AzureSqlDatabase1` | Azure SQL DB | `sqlservergrupo1.database.windows.net/dbRetail` |
| `AzureSqlDatabaseLinkedService` | Azure SQL DB | `server--igoxkyj.database.windows.net/dbRetail` |
| `AzureBlobStorage` | Azure Blob | `storageblobsigoxkyj` |
| `AzureDataLakeStorage` | ADLS Gen2 | `datalakeigoxkyj.dfs.core.windows.net` |
| `AzureStorageLinkedService` | Azure Blob | `storageblobsigoxkyj` |
| `LinkendServiceTranformaciones` | Databricks | `adb-4624400229247673.13.azuredatabricks.net` |

---

## SQL Server — dbRetail

```mermaid
erDiagram
    Categoria {
        int CategoriaID PK
        string Nombre
    }
    Subcategoria {
        int SubcategoriaID PK
        int CategoriaID FK
        string Nombre
    }
    Producto {
        int ProductoID PK
        int SubcategoriaID FK
        string Nombre
        decimal Precio
        decimal Costo
    }
    Cliente {
        int ClienteID PK
        string Nombre
        string Segmento
        string Ciudad
    }
    Orden {
        int OrdenID PK
        int ClienteID FK
        date FechaOrden
        decimal Flete
    }
    OrdenDetalle {
        int DetalleID PK
        int OrdenID FK
        int ProductoID FK
        int Cantidad
        decimal PrecioUnitario
        decimal Descuento
    }

    Categoria ||--o{ Subcategoria : "contiene"
    Subcategoria ||--o{ Producto : "agrupa"
    Cliente ||--o{ Orden : "realiza"
    Orden ||--o{ OrdenDetalle : "contiene"
    Producto ||--o{ OrdenDetalle : "referenciado"
```

---

## Databricks Workspace

| Config | Valor |
|--------|-------|
| URL | `adb-4624400229247673.13.azuredatabricks.net` |
| Cluster ID | `1123-103841-9c1ictdn` |
| Linked Service | `LinkendServiceTranformaciones` |
| Uso | Notebooks de transformación (ETL pipeline + exploratory) |

---

## Archivos de Infraestructura

| Archivo | Contenido |
|---------|-----------|
| `azure-pipeline_deploy.yml` | Pipeline YAML de Azure DevOps |
| `script.ps1` | PowerShell para provisionar recursos Azure |
| `scripts.ps1` | Script secundario (setup adicional) |
| `dbRetail.bacpac` | Backup de la base de datos SQL Server |
| `keys.txt` | Referencias de keys (no credenciales en texto claro) |
