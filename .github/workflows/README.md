# GitHub Actions — Automated DataOps Platform

> Plataforma de automatización end-to-end para el pipeline Medallion v6.0 sobre IBM Cloud, orquestada enteramente con GitHub Actions como **scheduler externo serverless**.

---

## Arquitectura General — Event-Driven DataOps

```mermaid
graph TB
    subgraph TRIGGERS["🎯 Trigger Layer"]
        direction LR
        CRON_H["⏰ Cron Horario<br/>Pipeline Scheduler"]
        CRON_6H["⏰ Cron 6h<br/>Drift Detection"]
        CRON_W["⏰ Cron Semanal<br/>Monitor + Catalog"]
        PUSH["🔀 Push to main<br/>Scala code changes"]
        MANUAL["👤 workflow_dispatch<br/>On-demand"]
    end

    subgraph PLATFORM["GitHub Actions — DataOps Platform"]
        direction TB

        subgraph CICD_LAYER["🔧 CI/CD Layer"]
            CI["CI/CD Pipeline<br/>Build → Test → Deploy"]
        end

        subgraph EXEC_LAYER["⚡ Execution Layer"]
            SCHED["Spark Pipeline<br/>Scheduler"]
        end

        subgraph QUALITY_LAYER["🛡️ Quality & Governance Layer"]
            direction LR
            DQ["Data Quality<br/>Gate"]
            DRIFT["Drift<br/>Detection"]
        end

        subgraph OBSERVE_LAYER["📊 Observability Layer"]
            direction LR
            COST["Cost & Resource<br/>Monitor"]
            LINEAGE["Data Lineage<br/>& Catalog"]
        end
    end

    subgraph IBM_CLOUD["☁️ IBM Cloud — Runtime"]
        direction LR
        COS["IBM COS<br/>4 Buckets Medallion"]
        AE["Analytics Engine<br/>Serverless Spark 3.5"]
        DB2["Db2 on Cloud<br/>Gold Persistence"]
    end

    subgraph FEEDBACK["📋 Feedback Loop"]
        SUMMARY["Step Summary<br/>Reports"]
        ISSUES["GitHub Issues<br/>Auto-created"]
        ALERTS["Threshold<br/>Alerts"]
    end

    %% Triggers
    PUSH -->|"code change"| CI
    CRON_H -->|"every hour"| SCHED
    CRON_6H -->|"every 6h"| DRIFT
    CRON_W -->|"weekly"| COST
    CRON_W -->|"weekly"| LINEAGE
    MANUAL -->|"on-demand"| SCHED
    MANUAL -->|"on-demand"| DQ

    %% CI/CD flow
    CI -->|"JAR → COS"| COS
    CI -->|"deploy"| AE

    %% Execution flow
    SCHED -->|"CSV upload"| COS
    SCHED -->|"REST API submit"| AE
    AE -->|"s3a read/write"| COS
    AE -->|"JDBC export"| DB2

    %% Post-pipeline
    SCHED -->|"triggers"| DQ
    DQ -->|"validates"| COS
    DQ -->|"validates"| DB2

    %% Drift
    DRIFT -->|"schema check"| COS
    DRIFT -->|"anomaly check"| COS

    %% Observability
    COST -->|"bucket sizes"| COS
    COST -->|"app history"| AE
    COST -->|"row counts"| DB2
    LINEAGE -->|"parquet schema"| COS

    %% Feedback
    DQ --> SUMMARY
    COST --> SUMMARY
    LINEAGE --> SUMMARY
    DRIFT -->|"on drift"| ISSUES
    COST -->|"on threshold"| ALERTS

    style TRIGGERS fill:#24292e10,stroke:#24292e,stroke-width:2px
    style PLATFORM fill:#0f62fe08,stroke:#0f62fe,stroke-width:3px
    style CICD_LAYER fill:#42be6515,stroke:#42be65,stroke-width:2px
    style EXEC_LAYER fill:#ff832b15,stroke:#ff832b,stroke-width:2px
    style QUALITY_LAYER fill:#da1e2815,stroke:#da1e28,stroke-width:2px
    style OBSERVE_LAYER fill:#0072c315,stroke:#0072c3,stroke-width:2px
    style IBM_CLOUD fill:#16161608,stroke:#0f62fe,stroke-width:3px
    style FEEDBACK fill:#8a3ffc15,stroke:#8a3ffc,stroke-width:2px
    style CI fill:#42be65,color:#fff
    style SCHED fill:#ff832b,color:#fff
    style DQ fill:#da1e28,color:#fff
    style DRIFT fill:#da1e28,color:#fff
    style COST fill:#0072c3,color:#fff
    style LINEAGE fill:#0072c3,color:#fff
    style COS fill:#0072c3,color:#fff
    style AE fill:#ff832b,color:#fff
    style DB2 fill:#009d9a,color:#fff
    style ISSUES fill:#8a3ffc,color:#fff
```

---

## Flujo de Ejecución — Cadena de Workflows

El diseño sigue un patrón **event-driven chain** donde cada workflow puede disparar o ser disparado por otros, formando un grafo de dependencias inteligente:

```mermaid
sequenceDiagram
    participant DEV as Developer
    participant GH as GitHub Actions
    participant COS as IBM COS
    participant AE as Analytics Engine
    participant DB2 as Db2 on Cloud

    Note over GH: ── CI/CD Pipeline (on push) ──
    DEV->>GH: git push (Scala code)
    GH->>GH: sbt compile + test
    GH->>GH: sbt assembly (Fat JAR)
    GH->>COS: Upload JAR → spark-jars/
    GH->>AE: Submit Spark App (REST API)

    Note over GH: ── Spark Pipeline (cron hourly) ──
    GH->>COS: Upload 7 CSVs → raw bucket
    GH->>AE: IAM Token → Submit App
    AE->>COS: Read raw CSVs (s3a://)
    AE->>COS: Write Bronze (Parquet)
    AE->>COS: Write Silver (Parquet)
    AE->>COS: Write Gold (Delta Lake)
    AE->>DB2: JDBC Export (7 tables)
    GH->>GH: Poll status (30s intervals)
    AE-->>GH: State: finished ✔

    Note over GH: ── Data Quality Gate (post-pipeline) ──
    GH->>COS: Check RAW (7 CSVs present?)
    GH->>COS: Check Bronze (14 objects?)
    GH->>COS: Check Silver (16 objects?)
    GH->>COS: Check Gold (7 Delta tables?)
    GH->>COS: Freshness < 2h?
    GH->>DB2: Row counts > 0?
    GH-->>GH: Quality Report → Step Summary

    Note over GH: ── Drift Detection (every 6h) ──
    GH->>COS: Read CSV headers → compare baseline
    GH->>COS: Read Gold Parquet → stats check
    GH->>GH: Schema drift? Data anomaly?
    GH-->>DEV: Auto-create GitHub Issue (if drift)

    Note over GH: ── Weekly: Monitor + Catalog ──
    GH->>COS: Measure bucket sizes (4 buckets)
    GH->>AE: Count apps + avg duration
    GH->>DB2: Row counts per table
    GH->>COS: Read Parquet schemas
    GH-->>GH: Resource Report + Data Catalog
```

---

## Inventario de Workflows

### 1. Spark Pipeline Scheduler

> Orquestador principal del pipeline Medallion. Sube datos, ejecuta Spark en AE Serverless y monitorea hasta completar.

```mermaid
flowchart LR
    subgraph JOB1["Job 1: upload-data"]
        A1["git clone"] --> A2["ibmcloud login"]
        A2 --> A3["Upload 7 CSVs<br/>→ COS raw bucket"]
    end

    subgraph JOB2["Job 2: submit-spark"]
        B1["Get IAM Token<br/>(REST API)"] --> B2["Build JSON Payload<br/>S3A + Delta + Db2"]
        B2 --> B3["POST /spark_applications<br/>→ AE REST API"]
        B3 --> B4["Output: app_id"]
    end

    subgraph JOB3["Job 3: monitor"]
        C1["Get IAM Token"] --> C2["Poll Status<br/>every 30s"]
        C2 --> C3{State?}
        C3 -->|finished| C4["✔ Report<br/>start/end time"]
        C3 -->|failed| C5["✗ Error<br/>exit 1"]
        C3 -->|running| C2
    end

    JOB1 --> JOB2 --> JOB3

    style JOB1 fill:#0072c315,stroke:#0072c3
    style JOB2 fill:#ff832b15,stroke:#ff832b
    style JOB3 fill:#42be6515,stroke:#42be65
    style B3 fill:#ff832b,color:#fff
    style C4 fill:#42be65,color:#fff
    style C5 fill:#da1e28,color:#fff
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `spark-pipeline-scheduler.yml` |
| **Trigger** | `cron: 0 * * * *` (cada hora) + `workflow_dispatch` |
| **Jobs** | 3 (upload-data → submit-spark → monitor) |
| **API** | IBM AE REST API (no requiere CLI en runner) |
| **Timeout** | 30 min (60 polls × 30s) |
| **Parámetros** | `skip_csv_upload`, `spark_version` |

---

### 2. CI/CD Pipeline — Build, Test & Deploy

> Integración continua con deploy automático. Cada push a `main` en código Scala compila, testea y despliega a producción.

```mermaid
flowchart LR
    subgraph BUILD["Job 1: Build & Test"]
        direction TB
        A1["git clone"] --> A2["Setup JDK 11"]
        A2 --> A3["sbt compile"]
        A3 --> A4["sbt test"]
        A4 --> A5["sbt assembly<br/>(Fat JAR ~593MB)"]
        A5 --> A6["SHA256 checksum"]
        A6 --> A7["Upload JAR → COS"]
    end

    subgraph DEPLOY["Job 2: Deploy to AE"]
        direction TB
        B1["IAM Token"] --> B2["Build Spark Payload<br/>conf + env + runtime"]
        B2 --> B3["POST → AE API<br/>Submit Application"]
        B3 --> B4["Output: App ID"]
    end

    BUILD -->|"main only"| DEPLOY

    style BUILD fill:#42be6515,stroke:#42be65
    style DEPLOY fill:#ff832b15,stroke:#ff832b
    style A5 fill:#f1c21b,color:#000
    style B3 fill:#ff832b,color:#fff
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `ci-cd-pipeline.yml` |
| **Trigger** | Push a `main` (paths: `batch-etl-scala/**`) + PR + manual |
| **Scope** | Solo se dispara si cambia código Scala |
| **Deploy** | Solo en push a `main` (no en PRs) |
| **Artifact** | `root-assembly-2.0.0.jar` → SHA256 verificado |

---

### 3. Data Quality Gate

> Validación post-ETL automatizada. Verifica integridad de datos en las 4 capas + Db2 después de cada ejecución del pipeline.

```mermaid
flowchart TB
    TRIGGER["workflow_run:<br/>Spark Pipeline finished"] --> START

    subgraph CHECKS["Quality Validation"]
        direction TB
        START["Install deps"] --> RAW

        subgraph RAW_CHECK["RAW Layer"]
            RAW["7 CSVs present?<br/>Size > 0 bytes?"]
        end

        subgraph BRONZE_CHECK["BRONZE Layer"]
            BRONZE["≥5 objects?<br/>Freshness < 2h?<br/>≥3 tables?"]
        end

        subgraph SILVER_CHECK["SILVER Layer"]
            SILVER["≥5 objects?<br/>Freshness < 2h?<br/>≥3 tables?"]
        end

        subgraph GOLD_CHECK["GOLD Layer"]
            GOLD["7 tables exist?<br/>Delta log intact?<br/>Freshness < 2h?"]
        end

        subgraph DB2_CHECK["DB2"]
            DB2["JDBC connect?<br/>7 tables exist?<br/>Row count > 0?"]
        end

        RAW --> BRONZE --> SILVER --> GOLD --> DB2
    end

    DB2 --> REPORT{Errors?}
    REPORT -->|"0 errors"| PASS["✅ PASS<br/>Step Summary"]
    REPORT -->|"errors > 0"| FAIL["❌ FAIL<br/>exit 1"]
    REPORT -->|"warnings + strict"| FAIL

    style CHECKS fill:#da1e2808,stroke:#da1e28,stroke-width:2px
    style RAW_CHECK fill:#ff980020,stroke:#ff9800
    style BRONZE_CHECK fill:#cd7f3220,stroke:#cd7f32
    style SILVER_CHECK fill:#c0c0c020,stroke:#808080
    style GOLD_CHECK fill:#ffd70020,stroke:#ffd700
    style DB2_CHECK fill:#009d9a20,stroke:#009d9a
    style PASS fill:#42be65,color:#fff
    style FAIL fill:#da1e28,color:#fff
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `data-quality-gate.yml` |
| **Trigger** | `workflow_run` (post-pipeline) + `workflow_dispatch` |
| **Checks** | 30+ validaciones across 5 layers |
| **Modes** | Normal (errors only) / Strict (warnings = fail) |
| **Parámetros** | `layer` (all/raw/bronze/silver/gold/db2), `fail_on_warning` |
| **Output** | Tabla markdown en Step Summary con ✅/❌/⚠️ |

---

### 4. Cost & Resource Monitor

> Reporte semanal de consumo de recursos IBM Cloud. Detecta anomalías de costo y uso antes de que escalen.

```mermaid
flowchart LR
    subgraph MONITOR["Resource Report"]
        direction TB

        subgraph COS_METRIC["📦 COS Storage"]
            M1["Bucket sizes (MB/GB)"]
            M2["Object counts"]
            M3["Last modified dates"]
        end

        subgraph AE_METRIC["⚡ AE Spark"]
            M4["Apps count (7 days)"]
            M5["State distribution"]
            M6["Avg/Max duration"]
        end

        subgraph DB2_METRIC["🗄️ Db2 Tables"]
            M7["Row counts per table"]
            M8["Table existence"]
        end

        COS_METRIC --> THRESHOLD
        AE_METRIC --> THRESHOLD
        DB2_METRIC --> THRESHOLD
    end

    THRESHOLD{Thresholds}
    THRESHOLD -->|"bucket > 10GB"| ALERT_RED["🔴 Critical Alert"]
    THRESHOLD -->|"apps > 200/week"| ALERT_YELLOW["🟡 Warning"]
    THRESHOLD -->|"rows < 10"| ALERT_YELLOW
    THRESHOLD -->|"all OK"| REPORT_OK["✅ Report"]

    style MONITOR fill:#0072c308,stroke:#0072c3,stroke-width:2px
    style COS_METRIC fill:#0072c315,stroke:#0072c3
    style AE_METRIC fill:#ff832b15,stroke:#ff832b
    style DB2_METRIC fill:#009d9a15,stroke:#009d9a
    style ALERT_RED fill:#da1e28,color:#fff
    style ALERT_YELLOW fill:#f1c21b,color:#000
    style REPORT_OK fill:#42be65,color:#fff
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `cost-resource-monitor.yml` |
| **Trigger** | `cron: 0 8 * * 1` (lunes 08:00 UTC) + manual |
| **Métricas** | Storage size, object count, app count, duration, row counts |
| **Thresholds** | Bucket > 10GB (🔴), Apps > 200/week (🟡), Rows < 10 (🟡) |
| **Output** | Reporte markdown con tablas y alertas |

---

### 5. Data Lineage & Catalog

> Genera documentación automática del linaje de datos completo y catálogo con schemas Parquet reales leídos desde COS.

```mermaid
flowchart TB
    subgraph CATALOG_GEN["Catalog Generator"]
        direction TB
        A1["Read lineage definition<br/>(7 entities × 5 layers)"]
        A1 --> A2["Generate Mermaid diagram<br/>RAW → Bronze → Silver → Gold → Db2"]
        A2 --> A3["Read Parquet schemas<br/>from Gold bucket (pyarrow)"]
        A3 --> A4["Build table catalog<br/>columns, types, row counts"]
    end

    subgraph OUTPUT["Generated Artifacts"]
        direction LR
        MD["📄 DATA_CATALOG.md<br/>Full documentation"]
        JSON["📦 lineage.json<br/>Programmatic access"]
        SUMMARY["📊 Step Summary<br/>GitHub UI"]
    end

    CATALOG_GEN --> OUTPUT

    style CATALOG_GEN fill:#0072c308,stroke:#0072c3,stroke-width:2px
    style OUTPUT fill:#8a3ffc15,stroke:#8a3ffc,stroke-width:2px
    style MD fill:#0072c3,color:#fff
    style JSON fill:#42be65,color:#fff
    style SUMMARY fill:#8a3ffc,color:#fff
```

**Mermaid diagram generado automáticamente:**

```mermaid
graph LR
    subgraph RAW["🗂 Raw CSVs"]
        Categoria["Categoria.csv"]
        Subcategoria["Subcategoria.csv"]
        Producto["Producto.csv"]
        VentasInternet["VentasInternet.csv"]
        Sucursales["Sucursales.csv"]
        FactMine["FactMine.csv"]
        Mine["Mine.csv"]
    end

    subgraph BRONZE["🥉 Bronze"]
        b_categoria["categoria"]
        b_subcategoria["subcategoria"]
        b_producto["producto"]
        b_ventas["ventas_internet"]
        b_sucursales["sucursales"]
        b_factmine["fact_mine"]
        b_mine["mine"]
    end

    subgraph SILVER["🥈 Silver"]
        s_catalogo["catalogo_productos"]
        s_clientes["clientes_unicos"]
        s_ventas["ventas_enriched"]
        s_resumen["resumen_ventas_mensuales"]
        s_operadores["operadores_mineros"]
        s_produccion["produccion_minera_enriched"]
    end

    subgraph GOLD["🥇 Gold (Delta Lake)"]
        g_dim_prod["dim_producto"]
        g_dim_cli["dim_cliente"]
        g_dim_op["dim_operador"]
        g_fact_ventas["fact_ventas"]
        g_fact_mine["fact_produccion_minera"]
        g_kpi_ventas["kpi_ventas_mensuales"]
        g_kpi_mine["kpi_mineria"]
    end

    subgraph DB2["🗄️ Db2 on Cloud"]
        d_DIM_PRODUCTO["DIM_PRODUCTO"]
        d_DIM_CLIENTE["DIM_CLIENTE"]
        d_DIM_OPERADOR["DIM_OPERADOR"]
        d_FACT_VENTAS["FACT_VENTAS"]
        d_FACT_PRODUCCION["FACT_PRODUCCION_MINERA"]
        d_KPI_VENTAS["KPI_VENTAS_MENSUALES"]
        d_KPI_MINERIA["KPI_MINERIA"]
    end

    Producto --> b_producto
    Categoria --> b_categoria
    Subcategoria --> b_subcategoria
    VentasInternet --> b_ventas
    Sucursales --> b_sucursales
    FactMine --> b_factmine
    Mine --> b_mine

    b_producto --> s_catalogo
    b_categoria --> s_catalogo
    b_subcategoria --> s_catalogo
    b_ventas --> s_clientes
    b_ventas --> s_ventas
    b_sucursales --> s_ventas
    b_ventas --> s_resumen
    b_mine --> s_operadores
    b_factmine --> s_produccion
    b_mine --> s_produccion

    s_catalogo --> g_dim_prod
    s_clientes --> g_dim_cli
    s_operadores --> g_dim_op
    s_ventas --> g_fact_ventas
    s_produccion --> g_fact_mine
    s_resumen --> g_kpi_ventas
    s_produccion --> g_kpi_mine

    g_dim_prod --> d_DIM_PRODUCTO
    g_dim_cli --> d_DIM_CLIENTE
    g_dim_op --> d_DIM_OPERADOR
    g_fact_ventas --> d_FACT_VENTAS
    g_fact_mine --> d_FACT_PRODUCCION
    g_kpi_ventas --> d_KPI_VENTAS
    g_kpi_mine --> d_KPI_MINERIA

    style RAW fill:#ff980020,stroke:#ff9800,stroke-width:2px
    style BRONZE fill:#cd7f3220,stroke:#cd7f32,stroke-width:2px
    style SILVER fill:#c0c0c020,stroke:#808080,stroke-width:2px
    style GOLD fill:#ffd70020,stroke:#ffd700,stroke-width:2px
    style DB2 fill:#009d9a20,stroke:#009d9a,stroke-width:2px
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `data-lineage-catalog.yml` |
| **Trigger** | `cron: 0 7 * * 1` (lunes 07:00 UTC) + manual |
| **Entities** | 7 tablas Gold × 5 capas de linaje |
| **Schema** | Lectura real de Parquet con pyarrow (columnas + tipos) |
| **Output** | `DATA_CATALOG.md` + `lineage.json` + Step Summary |

---

### 6. Drift Detection — Schema & Data

> Detección proactiva de cambios no esperados en los datos fuente y tablas Gold. Abre issues automáticos en GitHub cuando detecta anomalías.

```mermaid
flowchart TB
    subgraph SCHEMA_DRIFT["🔍 Schema Drift"]
        direction TB
        S1["Read CSV headers<br/>from COS raw bucket"]
        S2["Compare vs baseline<br/>(expected columns)"]
        S3{Changed?}
        S3 -->|"missing cols"| S4["❌ SCHEMA DRIFT"]
        S3 -->|"new cols"| S5["⚠️ NEW COLUMNS"]
        S3 -->|"match"| S6["✅ OK"]
    end

    subgraph DATA_DRIFT["📊 Data Drift"]
        direction TB
        D1["Read Gold Parquet<br/>(pyarrow + pandas)"]
        D2["Compute statistics<br/>rows, nulls%, distributions"]
        D3{Anomaly?}
        D3 -->|"rows < baseline"| D4["❌ ROW DROP"]
        D3 -->|"nulls > threshold"| D5["❌ NULL SPIKE"]
        D3 -->|"normal"| D6["✅ OK"]
    end

    subgraph SNAPSHOT["📸 Snapshot"]
        SAVE["Save stats → COS<br/>_drift_snapshots/YYYYMMDD.json"]
    end

    subgraph ACTION["🎬 Actions"]
        direction LR
        REPORT["Step Summary<br/>Report"]
        ISSUE["GitHub Issue<br/>(auto-created)"]
    end

    SCHEMA_DRIFT --> SNAPSHOT
    DATA_DRIFT --> SNAPSHOT
    S4 --> ISSUE
    S5 --> REPORT
    D4 --> ISSUE
    D5 --> ISSUE
    D6 --> REPORT

    style SCHEMA_DRIFT fill:#da1e2808,stroke:#da1e28,stroke-width:2px
    style DATA_DRIFT fill:#8a3ffc08,stroke:#8a3ffc,stroke-width:2px
    style SNAPSHOT fill:#0072c315,stroke:#0072c3
    style ACTION fill:#42be6515,stroke:#42be65
    style S4 fill:#da1e28,color:#fff
    style D4 fill:#da1e28,color:#fff
    style D5 fill:#da1e28,color:#fff
    style ISSUE fill:#8a3ffc,color:#fff
```

| Propiedad | Valor |
|-----------|-------|
| **Archivo** | `drift-detection.yml` |
| **Trigger** | `cron: 30 */6 * * *` (cada 6 horas) + manual |
| **Schema check** | 7 CSVs × headers vs baseline esperado |
| **Data check** | 7 Gold tables × row count + null% + distributions |
| **Sensibilidad** | `low` / `medium` / `high` (configurable) |
| **Snapshots** | Guardados en COS para comparación histórica |
| **Auto-issue** | Crea GitHub Issue con label `drift-detection` si detecta |

---

## Configuración de Secrets

Los 4 secrets se configuran en **Settings → Secrets and variables → Actions**:

| Secret | Usado por | Descripción |
|--------|-----------|-------------|
| `IBMCLOUD_API_KEY` | Todos | API Key de IBM Cloud (IAM authentication) |
| `COS_ACCESS_KEY` | Todos | HMAC Access Key para IBM COS (S3A) |
| `COS_SECRET_KEY` | Todos | HMAC Secret Key para IBM COS (S3A) |
| `DB2_PASSWORD` | Scheduler, Quality Gate, Cost Monitor | Password de Db2 on Cloud |

---

## Schedule Completo

```mermaid
gantt
    title Cronograma de Ejecución — 24 horas (UTC)
    dateFormat HH:mm
    axisFormat %H:%M

    section Pipeline
    Spark Pipeline (hourly)     :crit, 00:00, 1h
    Spark Pipeline              :crit, 01:00, 1h
    Spark Pipeline              :crit, 02:00, 1h
    Spark Pipeline              :crit, 03:00, 1h
    Spark Pipeline              :crit, 04:00, 1h
    Spark Pipeline              :crit, 05:00, 1h

    section Drift Detection
    Drift Check (every 6h)      :active, 00:30, 30min
    Drift Check                 :active, 06:30, 30min
    Drift Check                 :active, 12:30, 30min
    Drift Check                 :active, 18:30, 30min

    section Weekly (Monday)
    Data Lineage Catalog        :07:00, 30min
    Cost & Resource Monitor     :08:00, 30min
```

| Workflow | Cron | Frecuencia | Día |
|----------|------|-----------|-----|
| Spark Pipeline | `0 * * * *` | Cada hora | Todos |
| Drift Detection | `30 */6 * * *` | Cada 6 horas | Todos |
| Data Lineage | `0 7 * * 1` | Semanal | Lunes 07:00 |
| Cost Monitor | `0 8 * * 1` | Semanal | Lunes 08:00 |
| CI/CD | On push | Event-driven | — |
| Quality Gate | Post-pipeline | Event-driven | — |

---

## Decisiones de Diseño

### ¿Por qué GitHub Actions como scheduler?

| Criterio | GitHub Actions | Airflow | IBM Code Engine |
|----------|---------------|---------|-----------------|
| Costo | **Gratis** (2,000 min/mes) | Self-hosted | Pay-per-run |
| Setup | **0 infraestructura** | Servidor dedicado | Config COS + IAM |
| CI/CD integrado | **Nativo** | Plugin | Externo |
| Secrets management | **Built-in** | Vault/env | Secrets Manager |
| Monitoring | **Step Summary + Issues** | Airflow UI | Log Analysis |
| Complejidad | **Baja** (YAML) | Alta (Python DAGs) | Media |

### ¿Por qué sin external actions?

La organización tiene una política que restringe actions a repositorios del mismo owner. Todos los workflows usan exclusivamente `run:` steps con:
- **`curl`** para REST APIs (IBM IAM + AE)
- **`python3`** para lógica de validación y reportes
- **`ibmcloud` CLI** para operaciones COS (instalado on-the-fly)
- **`git clone`** en lugar de `actions/checkout`

### Patrón de autenticación

```mermaid
sequenceDiagram
    participant GH as GitHub Runner
    participant IAM as IBM IAM
    participant AE as Analytics Engine

    GH->>IAM: POST /identity/token<br/>grant_type=apikey&apikey=***
    IAM-->>GH: access_token (Bearer)
    GH->>AE: POST /spark_applications<br/>Authorization: Bearer ***
    AE-->>GH: {"id": "app-xxx", "state": "accepted"}
```

Todos los workflows usan **IAM token exchange** directo via REST, sin depender del `ibmcloud` CLI para la autenticación con AE.

---

## Ejecución Manual

Todos los workflows soportan `workflow_dispatch` desde la UI de GitHub:

```
GitHub → Actions → [Workflow] → Run workflow → Select branch → Run
```

O via CLI (requiere PAT con scope `repo`):

```bash
# Ejecutar pipeline
gh workflow run spark-pipeline-scheduler.yml

# Ejecutar quality gate en gold solamente
gh workflow run data-quality-gate.yml -f layer=gold -f fail_on_warning=true

# Ejecutar drift detection con alta sensibilidad
gh workflow run drift-detection.yml -f sensitivity=high

# Ver estado de ejecuciones
gh run list --workflow=spark-pipeline-scheduler.yml
gh run watch <run_id>
```
