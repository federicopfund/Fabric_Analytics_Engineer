# IBM Cloud Infrastructure — Documentación Técnica

## Resumen

Infraestructura multi-servicio en IBM Cloud provisionada con Terraform. Incluye VPC, IKS (Kubernetes), Analytics Engine (Serverless Spark), Cloud Object Storage (4 buckets Medallion), Db2 Cloud, Key Protect y monitoreo con Sysdig.

---

## Topología de Servicios

```mermaid
graph TB
    subgraph VPC["VPC: medallion-vpc"]
        SUBNET["Subnet<br/>medallion-compute-subnet<br/>/24 CIDR"]
        GW["Public Gateway<br/>Egress traffic"]
        SG["Security Groups<br/>Default + Custom"]

        subgraph IKS["IKS Cluster: medallion-spark"]
            NS["Namespace<br/>spark-medallion"]
            DRIVER["Spark Driver<br/>CronJob"]
            EXECUTORS["Spark Executors<br/>1-6 dynamic"]
        end
    end

    subgraph Storage["Cloud Object Storage"]
        RAW["🟤 datalake-raw<br/>us-south-dev"]
        BRONZE["🟤 datalake-bronze<br/>us-south-dev"]
        SILVER["⚪ datalake-silver<br/>us-south-dev"]
        GOLD["🟡 datalake-gold<br/>us-south-dev"]
        LOGS["📋 datalake-logs<br/>us-south-dev"]
    end

    subgraph Compute["Analytics Engine"]
        AE["Serverless Spark<br/>Instance"]
    end

    subgraph Data["Db2 Cloud"]
        DB2[("Db2<br/>bludb<br/>Port 30376")]
    end

    subgraph Security["Seguridad"]
        KP["Key Protect<br/>Encryption keys"]
        SM["Secrets Manager"]
        IAM["IAM<br/>Service IDs"]
    end

    subgraph Observability["Observabilidad"]
        SYSDIG["Sysdig<br/>Dashboard + Alerts"]
        AT["Activity Tracker<br/>Audit logs"]
    end

    IKS -->|S3A protocol| Storage
    IKS -->|JDBC SSL| DB2
    AE -->|S3A protocol| Storage
    AE -->|JDBC SSL| DB2
    KP -->|Encrypt| Storage
    KP -->|Encrypt| DB2
    SYSDIG -->|Monitor| IKS
    SYSDIG -->|Monitor| AE
    AT -->|Audit| Storage
    SUBNET --> GW
    IKS --> SUBNET
```

---

## Terraform — Recursos Provisionados

```mermaid
graph LR
    subgraph Networking["Networking"]
        VPC_R[ibm_is_vpc<br/>medallion-vpc]
        SUB[ibm_is_subnet<br/>medallion-compute-subnet]
        PGW[ibm_is_public_gateway]
        SG_R[ibm_is_security_group]
    end

    subgraph Compute["Compute"]
        IKS_R[ibm_container_vpc_cluster<br/>medallion-spark]
        AE_R[ibm_resource_instance<br/>analytics-engine]
    end

    subgraph Storage_R["Storage"]
        COS_R[ibm_resource_instance<br/>cos-instance]
        B1[ibm_cos_bucket × 4<br/>raw, bronze, silver, gold]
    end

    subgraph Database["Database"]
        DB2_R[ibm_resource_instance<br/>db2-cloud]
    end

    subgraph Security_R["Security"]
        KP_R[ibm_resource_instance<br/>key-protect]
        AT_R[ibm_resource_instance<br/>activity-tracker]
    end

    VPC_R --> SUB
    SUB --> PGW
    VPC_R --> SG_R
    SUB --> IKS_R
    COS_R --> B1
```

### Variables de Entrada

| Variable | Tipo | Default | Descripción |
|----------|------|---------|-------------|
| `ibmcloud_api_key` | string (sensitive) | — | API key de IBM Cloud |
| `project_name` | string | `medallion` | Prefijo para nombres |
| `environment` | string | `dev` | dev / staging / prod |
| `cost_center` | string | `data-engineering` | Centro de costos |
| `region` | string | `us-south` | Región IBM Cloud |
| `resource_group` | string | `Default` | Resource Group |
| `cluster_zone` | string | — | Zona de disponibilidad |

### Convención de Nombres (locals.tf)

```
Patrón: ${project_name}-${environment}
Ejemplo: medallion-dev

Buckets: datalake-{layer}-${region}-${environment}
Ejemplo: datalake-raw-us-south-dev
```

### Outputs

| Output | Contenido |
|--------|-----------|
| `vpc_id`, `vpc_crn` | Identificadores de VPC |
| `subnet_id` | Subnet de cómputo |
| `cluster_id`, `cluster_name` | IKS cluster |
| `cos_writer_access_key` | HMAC key (Writer) |
| `cos_writer_secret_key` | HMAC secret (Writer) |
| `cos_reader_access_key` | HMAC key (Reader) |

---

## Scripts de Despliegue

```mermaid
flowchart TD
    START([Inicio]) --> SETUP["setup.sh<br/>1. Instalar IBM CLI<br/>2. Plugins: container-service,<br/>cos, vpc, analytics-engine,<br/>cd, key-protect, schematics<br/>3. Verificar autenticación<br/>4. Validar prerrequisitos"]

    SETUP --> TF["terraform apply<br/>Provisionar infraestructura"]

    TF --> DEPLOY["deploy-spark.sh<br/>1. Target account/RG<br/>2. Fetch kubeconfig<br/>3. Verify connectivity<br/>4. Create namespace<br/>5. Create COS secret<br/>6. Deploy Spark pods<br/>7. Wait for readiness"]

    DEPLOY --> SUBMIT["submit-to-ae.sh<br/>1. Build sbt fat JAR<br/>2. Upload JAR → COS<br/>3. Submit Spark app<br/>4. Monitor estado"]

    SUBMIT --> HEALTH["health-check.sh<br/>✓ CLI login status<br/>✓ COS 4 buckets<br/>✓ Db2 JDBC connectivity<br/>✓ Analytics Engine state<br/>✓ IKS node readiness"]

    HEALTH --> MONITOR{Pipeline<br/>Operativo}
    MONITOR -->|Rotación| ROTATE["rotate-credentials.sh<br/>Update COS keys<br/>Db2 passwords"]
    MONITOR -->|Teardown| DESTROY["destroy.sh<br/>Terraform destroy<br/>Cleanup completo"]
```

### Detalle de Scripts

| Script | Propósito | Opciones |
|--------|-----------|----------|
| `setup.sh` | Setup inicial (CLI + plugins + auth) | — |
| `deploy-spark.sh` | Deploy manifiestos K8s a IKS | — |
| `submit-to-ae.sh` | Submit JAR a Analytics Engine | `--skip-build`, `--status`, `--logs`, `--list` |
| `health-check.sh` | Validación de infraestructura | Output: JSON o human-readable |
| `setup-cicd.sh` | Deploy Tekton pipelines + triggers | — |
| `rotate-credentials.sh` | Rotar secrets COS/Db2 | — |
| `cos-lifecycle.sh` | Políticas de retención COS | — |
| `destroy.sh` | Destruir toda la infraestructura | Confirma antes de ejecutar |

---

## Monitoreo — Sysdig Dashboard

```mermaid
graph TB
    subgraph Dashboard["Sysdig Dashboard: Medallion Pipeline"]
        P1["📊 Panel 1<br/>Pipeline Status<br/>Running / Failed / Success"]
        P2["📊 Panel 2<br/>Failed Apps Count<br/>ibm_analytics_engine<br/>.spark_app.failed"]
        P3["📊 Panel 3<br/>Avg Duration (min)<br/>ibm_analytics_engine<br/>.spark_app.duration"]
        P4["📊 Panel 4<br/>Total COS Storage (GB)<br/>ibm_cos.bucket<br/>.bytes_used"]
    end

    subgraph Alerts["Alert Policies"]
        A1["🔴 CRITICAL<br/>Spark App Failed<br/>metric = 'failed'"]
        A2["🟠 HIGH<br/>COS Access Denied<br/>5 events / 5 min<br/>Activity Tracker"]
        A3["🟡 WARNING<br/>Gold Bucket > 10GB<br/>ibm_cos threshold"]
    end

    subgraph Channels["Notification Channels"]
        SLACK[Slack]
        EMAIL[Email]
    end

    Dashboard --> Alerts
    A1 --> SLACK
    A1 --> EMAIL
    A2 --> SLACK
    A3 --> EMAIL
```

---

## Analytics Engine — Serverless Spark

```mermaid
sequenceDiagram
    participant DEV as Developer
    participant CLI as IBM CLI
    participant AE as Analytics Engine
    participant COS as Cloud Object Storage
    participant DB2 as Db2 Cloud

    DEV->>CLI: submit-to-ae.sh
    CLI->>CLI: sbt assembly (fat JAR)
    CLI->>COS: Upload JAR → spark-jars/
    CLI->>AE: POST /v3/analytics_engines/{id}/spark_applications
    Note over AE: Spark App Config:<br/>driver: 1 core, 2GB<br/>executor: 2 cores, 4GB × 3

    AE->>COS: Read RAW bucket
    AE->>COS: Write BRONZE/SILVER/GOLD
    AE->>DB2: Query source tables

    AE-->>CLI: Application ID
    CLI->>AE: GET /status (polling)
    AE-->>CLI: RUNNING → FINISHED
    CLI->>AE: GET /logs
    AE-->>DEV: Logs output
```

---

## Makefile — Comandos Principales

| Comando | Acción |
|---------|--------|
| `make setup` | Ejecutar setup.sh (CLI + plugins) |
| `make plan` | Terraform plan |
| `make apply` | Terraform apply |
| `make deploy` | Deploy K8s manifests |
| `make submit` | Submit job a Analytics Engine |
| `make status` | Health check completo |
| `make destroy` | Teardown infraestructura |
| `make rotate` | Rotar credenciales |
