# Arquitectura General — Medallion Data Platform v6.0

## Visión General

Plataforma de datos empresarial que soporta dos unidades de negocio:

| Dominio | Descripción | Fuentes |
|---------|-------------|---------|
| **Retail** | E-commerce de bicicletas y componentes | SQL Server (`dbRetail`), CSV batch |
| **Minería** | Operaciones de extracción mineral | CSV batch, sensores operacionales |

La arquitectura sigue el patrón **Medallion (RAW → BRONZE → SILVER → GOLD)** con soporte multi-cloud (Azure + IBM Cloud) y ejecución local (HDFS/Docker).

---

## Diagrama de Arquitectura de Alto Nivel

```mermaid
graph TB
    subgraph Sources["🔌 Fuentes de Datos"]
        SQL[(SQL Server<br/>dbRetail)]
        BLOB[Azure Blob<br/>Landing Zone]
        CSV[CSV Batch<br/>7 tablas]
    end

    subgraph Orchestration["⚙️ Orquestación"]
        ADF[Azure Data Factory<br/>7 Pipelines]
        TEKTON[Tekton CI/CD<br/>9 Stages]
        CRON[K8s CronJob<br/>Hourly]
    end

    subgraph Ingestion["📥 Ingestion Layer"]
        COS_RAW[COS RAW Bucket<br/>s3a://datalake-raw]
        HDFS_RAW[HDFS RAW<br/>/datalake/raw]
        ADLS[Azure Data Lake<br/>Gen2]
    end

    subgraph Processing["⚡ Spark ETL Pipeline"]
        direction TB
        BRONZE["🥉 BRONZE<br/>Parquet — 1:1 CSV"]
        SILVER["🥈 SILVER<br/>Parquet — Agregaciones"]
        GOLD["🥇 GOLD<br/>Delta Lake — Star Schema"]
    end

    subgraph Compute["🖥️ Motores de Cómputo"]
        LOCAL[Spark Local<br/>Dev/Test]
        HDFS_SPARK[Spark on HDFS<br/>Docker Compose]
        AE[IBM Analytics Engine<br/>Serverless]
        IKS[Spark on K8s<br/>IKS Cluster]
        DBX[Azure Databricks<br/>Notebooks]
    end

    subgraph Consumption["📊 Capa de Consumo"]
        PBI[Power BI<br/>6 Dashboards<br/>57 Medidas DAX]
        CHARTS[BI Charts<br/>10 PNG JFreeChart]
        HIVE[Hive Catalog<br/>Auditoría]
    end

    Sources --> Orchestration
    Orchestration --> Ingestion
    Ingestion --> Compute
    Compute --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> Consumption
```

---

## Flujo Medallion — Detalle por Capa

```mermaid
graph LR
    subgraph RAW["RAW Layer"]
        R1[Categoria.csv]
        R2[Subcategoria.csv]
        R3[Producto.csv]
        R4[Cliente.csv]
        R5[Orden.csv]
        R6[OrdenDetalle.csv]
        R7[FactMine.csv]
    end

    subgraph BRONZE["BRONZE Layer — Parquet"]
        B1[Categoria/]
        B2[Subcategoria/]
        B3[Producto/]
        B4[Cliente/]
        B5[Orden/]
        B6[OrdenDetalle/]
        B7[FactMine/]
    end

    subgraph SILVER["SILVER Layer — Parquet Agregado"]
        S1[dim_categoria]
        S2[dim_producto]
        S3[dim_cliente]
        S4[fact_ordenes]
        S5[kpi_ventas_mensuales]
        S6[dim_operador_mineria]
    end

    subgraph GOLD["GOLD Layer — Delta Lake"]
        G1[fact_ventas<br/>48,895 rows]
        G2[dim_cliente<br/>18,484 rows]
        G3[dim_producto<br/>319 rows]
        G4[kpi_ventas_mensuales<br/>37 rows]
        G5[Calendario<br/>1,184 rows]
    end

    RAW -->|"Schema Validation<br/>Dead Letter Queue"| BRONZE
    BRONZE -->|"Business Logic<br/>Temp Views"| SILVER
    SILVER -->|"Star Schema<br/>ACID Transactions"| GOLD
```

---

## Modos de Ejecución

```mermaid
flowchart TD
    START([Pipeline ETL]) --> MODE{Modo de<br/>Ejecución}

    MODE -->|"1"| LOCAL["🖥️ Local Mode<br/>Spark local[*]<br/>Para desarrollo"]
    MODE -->|"2"| HDFS["🐘 HDFS + Hive<br/>Docker Compose<br/>6 contenedores"]
    MODE -->|"3"| E2E["🔄 E2E Automated<br/>Script bash<br/>CI/CD integration"]
    MODE -->|"4"| AE_MODE["☁️ IBM Analytics Engine<br/>Serverless Spark<br/>COS backend"]
    MODE -->|"5"| K8S["⎈ Spark on K8s<br/>CronJob horario<br/>IKS cluster"]
    MODE -->|"6"| SUBMIT["📦 spark-submit<br/>Fat JAR directo<br/>Cualquier cluster"]

    LOCAL --> OUTPUT[("📊 Output:<br/>Bronze + Silver + Gold<br/>+ 10 Charts PNG")]
    HDFS --> OUTPUT
    E2E --> OUTPUT
    AE_MODE --> OUTPUT
    K8S --> OUTPUT
    SUBMIT --> OUTPUT
```

---

## Topología de Componentes

```mermaid
graph TB
    subgraph Azure["☁️ Azure Cloud"]
        ADF_C[Azure Data Factory]
        SQL_C[(SQL Server<br/>dbRetail)]
        BLOB_C[Blob Storage<br/>storageblobsigoxkyj]
        ADLS_C[Data Lake Gen2<br/>datalakeigoxkyj]
        DBX_C[Databricks<br/>adb-4624400229247673]
        PIPE_AZ[Azure Pipelines<br/>CI/CD]
    end

    subgraph IBM["☁️ IBM Cloud"]
        VPC[VPC<br/>medallion-vpc]
        IKS_C[IKS Cluster<br/>medallion-spark]
        AE_C[Analytics Engine<br/>Serverless]
        COS_C[Cloud Object Storage<br/>4 Buckets]
        DB2[(Db2 Cloud<br/>bludb)]
        KP[Key Protect<br/>Encryption]
        SM[Secrets Manager]
        MON[Sysdig<br/>Monitoring]
    end

    subgraph Local["🏠 Local/Docker"]
        NAMENODE[HDFS NameNode<br/>:9870]
        DATANODE[HDFS DataNode<br/>:9864]
        HIVE_MS[Hive Metastore<br/>:9083]
        MYSQL[(MySQL<br/>Metastore DB)]
        SPARK_L[Spark Local]
    end

    ADF_C -->|Extract| SQL_C
    ADF_C -->|Stage| BLOB_C
    ADF_C -->|Load| ADLS_C
    ADF_C -->|Transform| DBX_C
    PIPE_AZ -->|Deploy| ADF_C

    IKS_C -->|R/W| COS_C
    IKS_C -->|Query| DB2
    AE_C -->|R/W| COS_C
    AE_C -->|Query| DB2
    KP -->|Encrypt| COS_C
    MON -->|Observe| IKS_C
    MON -->|Observe| AE_C

    NAMENODE -->|Metadata| DATANODE
    HIVE_MS -->|Catalog| MYSQL
    SPARK_L -->|R/W| NAMENODE
    SPARK_L -->|Register| HIVE_MS
```

---

## Stack Tecnológico

| Capa | Tecnología | Versión |
|------|-----------|---------|
| **Procesamiento** | Apache Spark | 3.3.1 |
| **Lenguaje** | Scala | 2.12 |
| **Storage Format** | Delta Lake | 2.2.0 |
| **Serialización** | Kryo | 256MB buffer |
| **Container Runtime** | Docker | Multi-stage build |
| **Orquestación K8s** | Kubernetes (IKS) | CronJob batch/v1 |
| **CI/CD** | Tekton Pipelines | 9 stages |
| **Cloud IaC** | Terraform (IBM) | Provider ibm |
| **Orquestación Datos** | Azure Data Factory | 7 pipelines |
| **BI** | Power BI | 57 medidas DAX |
| **Monitoreo** | Prometheus + Sysdig | ServiceMonitor |
| **HDFS** | Apache Hadoop | 3.3.4 |
| **Metastore** | Apache Hive | Thrift 9083 |
| **Base de Datos** | SQL Server / Db2 | Cloud managed |
| **Visualización Code** | JFreeChart | 10 charts PNG |

---

## Índice de Documentación

| Documento | Ubicación | Contenido |
|-----------|-----------|-----------|
| [Arquitectura General](OVERVIEW.md) | `docs/architecture/` | Este documento |
| [Kubernetes & Spark](../infrastructure/KUBERNETES.md) | `docs/infrastructure/` | CronJob, RBAC, networking, monitoring |
| [Hadoop Lakehouse](../infrastructure/HADOOP.md) | `docs/infrastructure/` | HDFS, Hive, Docker Compose |
| [IBM Cloud](../infrastructure/IBM-CLOUD.md) | `docs/infrastructure/` | Terraform, VPC, COS, IKS, AE |
| [Azure](../infrastructure/AZURE.md) | `docs/infrastructure/` | Pipelines, Storage, Databricks |
| [ADF Pipelines](../orchestration/ADF-PIPELINES.md) | `docs/orchestration/` | 7 pipelines, linked services |
| [ETL Pipeline](../transformation/ETL-PIPELINE.md) | `docs/transformation/` | Scala modules, workflows, DAG |
| [Power BI](../analytics/POWERBI.md) | `docs/analytics/` | Data model, 57 medidas DAX |
| [Seguridad](../security/SECURITY.md) | `docs/security/` | Secrets, encryption, network policies |
| [CI/CD](../cicd/TEKTON-CICD.md) | `docs/cicd/` | Tekton pipeline, triggers, tasks |
