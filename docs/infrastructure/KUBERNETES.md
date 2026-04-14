# Kubernetes & Spark on K8s — Documentación Técnica

## Resumen

Despliegue de Spark en Kubernetes (IKS) usando un CronJob que ejecuta el pipeline Medallion ETL v6.0 de forma horaria. El driver pod descarga el fat JAR desde COS y los executor pods se crean dinámicamente via Spark K8s backend.

---

## Arquitectura del CronJob

```mermaid
sequenceDiagram
    participant CRON as K8s CronJob<br/>(0 * * * *)
    participant INIT as Init Container<br/>preflight.sh
    participant DRIVER as Spark Driver<br/>spark-submit
    participant K8S as K8s API Server
    participant EXEC as Executor Pods<br/>(1-6 dinámico)
    participant COS as IBM COS<br/>4 Buckets
    participant DB2 as Db2 Cloud

    CRON->>INIT: Crea Job → Pod con Init Container
    INIT->>COS: Descarga root-assembly-2.0.0.jar
    INIT->>DB2: Verifica conectividad JDBC
    INIT->>INIT: Valida credenciales COS
    INIT-->>DRIVER: Init OK → Main container arranca

    DRIVER->>K8S: spark-submit --master k8s://
    K8S->>EXEC: Crea executor pods (3 iniciales)
    DRIVER->>COS: Lee RAW → Escribe BRONZE
    EXEC->>COS: Shuffle + escritura paralela
    DRIVER->>COS: BRONZE → SILVER (agregaciones)
    DRIVER->>COS: SILVER → GOLD (Delta Lake)
    DRIVER->>DB2: Queries fuente (opcional)

    Note over DRIVER,EXEC: Dynamic Allocation: 1-6 executors

    DRIVER-->>K8S: Job completado (exit 0)
    K8S->>K8S: TTL cleanup (3600s)
```

---

## Manifiestos Kubernetes — 8 Componentes

```mermaid
graph TB
    subgraph NS["Namespace: spark-medallion"]
        SA[ServiceAccount<br/>spark-driver]
        ROLE[Role + RoleBinding<br/>pods, logs, configmaps]
        QUOTA[ResourceQuota<br/>8 CPU / 16GB / 20 pods]
        LIMITS[LimitRange<br/>1 CPU / 2GB default]

        CM_CONFIG[ConfigMap<br/>spark-config]
        CM_SCRIPTS[ConfigMap<br/>spark-scripts]
        SECRET[Secrets<br/>cos / db2 / ibmcloud]

        CRONJOB[CronJob<br/>spark-medallion-etl]
        SVC[Service ClusterIP<br/>spark-driver-ui]
        NETPOL[NetworkPolicy<br/>Zero-trust]
        SMON[ServiceMonitor<br/>Prometheus]
        PMON[PodMonitor<br/>Executor metrics]
        RULES[PrometheusRule<br/>Alertas]
    end

    SA --> ROLE
    QUOTA --> NS
    LIMITS --> NS
    CM_CONFIG --> CRONJOB
    CM_SCRIPTS --> CRONJOB
    SECRET --> CRONJOB
    CRONJOB --> SVC
    SVC --> NETPOL
    SMON --> SVC
    PMON --> CRONJOB
    RULES --> SMON
```

---

## Detalle de Manifiestos

### 1. Namespace (`namespace.yaml`)

| Campo | Valor |
|-------|-------|
| Nombre | `spark-medallion` |
| Labels | `app.kubernetes.io/part-of: medallion-pipeline` |
| Ambiente | `environment: production` |

### 2. ConfigMaps (`configmaps.yaml`)

**spark-config:**

| Parámetro | Valor |
|-----------|-------|
| `spark.driver.memory` | 2g |
| `spark.executor.memory` | 4g |
| `spark.executor.cores` | 2 |
| `spark.executor.instances` | 3 |
| `spark.dynamicAllocation.minExecutors` | 1 |
| `spark.dynamicAllocation.maxExecutors` | 6 |
| `spark.sql.shuffle.partitions` | 200 |
| `spark.serializer` | KryoSerializer |
| `spark.sql.extensions` | DeltaSparkSessionExtension |
| `spark.sql.catalog.spark_catalog` | DeltaCatalog |

**COS Buckets:**

| Bucket | Uso |
|--------|-----|
| `datalake-raw-us-south-dev` | CSV fuente |
| `datalake-bronze-us-south-dev` | Parquet validado |
| `datalake-silver-us-south-dev` | Parquet agregado |
| `datalake-gold-us-south-dev` | Delta Lake Star Schema |

### 3. RBAC (`rbac.yaml`)

```mermaid
graph LR
    SA[ServiceAccount<br/>spark-driver] --> RB[RoleBinding]
    RB --> ROLE[Role:<br/>spark-driver-role]

    ROLE --> P1[pods: get, list,<br/>create, delete, watch]
    ROLE --> P2[pods/log: get, list]
    ROLE --> P3[configmaps: get, list,<br/>create, update]
    ROLE --> P4[services: get, list,<br/>create, delete]
    ROLE --> P5[persistentvolumeclaims:<br/>get, list, create, delete]

    subgraph Quotas
        Q1[ResourceQuota<br/>requests.cpu: 8<br/>requests.memory: 16Gi<br/>limits.cpu: 16<br/>limits.memory: 24Gi<br/>pods: 20<br/>PVCs: 5]
        Q2[LimitRange<br/>default cpu: 1<br/>default memory: 2Gi<br/>max cpu: 4<br/>max memory: 8Gi]
    end
```

### 4. Secrets (`secrets.yaml`)

| Secret Name | Keys | Descripción |
|-------------|------|-------------|
| `cos-credentials` | `access-key`, `secret-key`, `endpoint` | IBM COS (S3-compatible) |
| `db2-credentials` | `hostname`, `port`, `database`, `username`, `password`, `jdbc-url` | Db2 Cloud (SSL, puerto 30376) |
| `ibmcloud-api` | `api-key` | IBM Cloud IAM API key |

> ⚠️ Los secrets usan template `envsubst` — requieren variables de entorno al aplicar.

### 5. CronJob (`cronjob.yaml`)

```mermaid
stateDiagram-v2
    [*] --> Scheduled: schedule 0 * * * *
    Scheduled --> Deadline: startingDeadlineSeconds 300
    Deadline --> Skipped: > 5 min tarde
    Deadline --> Running: Dentro de ventana

    Running --> InitContainer: preflight.sh
    InitContainer --> Driver: JAR descargado + checks OK
    InitContainer --> Failed: Preflight falla

    Driver --> Executors: spark-submit → K8s backend
    Executors --> Processing: Dynamic 1-6 pods
    Processing --> Completed: Exit 0

    Driver --> Timeout: > 2700s (45 min)
    Timeout --> Failed

    Completed --> Cleanup: TTL 3600s
    Failed --> Retry: backoffLimit 2
    Retry --> InitContainer: Reintento
    Retry --> Failed: > 2 reintentos

    Failed --> Cleanup
    Cleanup --> [*]
```

**Configuración del CronJob:**

| Parámetro | Valor | Descripción |
|-----------|-------|-------------|
| `schedule` | `0 * * * *` | Cada hora en punto |
| `concurrencyPolicy` | `Forbid` | Sin overlap entre ejecuciones |
| `startingDeadlineSeconds` | `300` | No iniciar si pasaron > 5 min |
| `backoffLimit` | `2` | Máximo 2 reintentos |
| `activeDeadlineSeconds` | `2700` | Timeout 45 min |
| `ttlSecondsAfterFinished` | `3600` | Limpieza 1h post-ejecución |
| `successfulJobsHistoryLimit` | `5` | Mantener últimos 5 exitosos |
| `failedJobsHistoryLimit` | `3` | Mantener últimos 3 fallidos |

**Scheduling del Pod:**

| Concepto | Configuración |
|----------|---------------|
| **Tolerations** | `spark-workload=etl` (NoSchedule) |
| **Node Affinity** | Preferir `workload-type: spark\|compute` (weight 80) |
| **Pod Anti-Affinity** | No 2 drivers en mismo nodo (`kubernetes.io/hostname`) |
| **Grace Period** | 60s para cleanup de Spark |

### 6. Service (`service.yaml`)

| Puerto | Nombre | Uso |
|--------|--------|-----|
| 4040 | `spark-ui` | Spark Web UI |
| 7078 | `driver-rpc` | Driver RPC Port |
| 7079 | `block-manager` | Block Manager |

### 7. Network Policy (`network-policy.yaml`)

```mermaid
graph TB
    subgraph Ingress["Ingress Rules"]
        EX_IN[Executor Pods<br/>→ 7078/7079 TCP]
        MON_IN[Monitoring NS<br/>→ 4040 TCP]
    end

    subgraph DRIVER["Driver Pod"]
        D[spark-driver]
    end

    subgraph Egress["Egress Rules"]
        DNS[kube-system DNS<br/>53 UDP/TCP]
        API[K8s API Server<br/>443 TCP]
        COS_E[IBM COS<br/>443 TCP]
        DB2_E[Db2 Cloud<br/>30376 TCP]
        EXEC_E[Executor RPC<br/>Namespace pods]
    end

    EX_IN --> DRIVER
    MON_IN --> DRIVER
    DRIVER --> DNS
    DRIVER --> API
    DRIVER --> COS_E
    DRIVER --> DB2_E
    DRIVER --> EXEC_E
```

### 8. Monitoring (`monitoring.yaml`)

```mermaid
graph LR
    subgraph Metrics["Recolección"]
        SM[ServiceMonitor<br/>30s interval<br/>/metrics/prometheus/]
        PM[PodMonitor<br/>Executor pods]
    end

    subgraph Alerts["Alertas PrometheusRule"]
        A1["🔴 SparkJobFailed<br/>severity: critical<br/>Spark failures > 0"]
        A2["🟡 SparkJobStuck<br/>severity: warning<br/>Duration > 2h"]
    end

    subgraph Notify["Notificaciones"]
        SLACK[Slack Channel]
        EMAIL[Email Alert]
    end

    SM --> A1
    SM --> A2
    PM --> A1
    A1 --> SLACK
    A1 --> EMAIL
    A2 --> SLACK
```

---

## Docker Image — Multi-stage Build

```mermaid
graph TB
    subgraph Stage1["Stage 1: Builder (JDK 11)"]
        DL_SPARK[Download Spark 3.3.1<br/>+ Hadoop 3]
        ADD_JARS["Add JARs:<br/>• Delta Core 2.2.0<br/>• Delta Storage<br/>• AWS S3A SDK 1.12.262<br/>• Hadoop AWS 3.3.1<br/>• Db2 JCC 11.5.5"]
        RM_JARS["Remove JARs:<br/>• Mesos, YARN<br/>• MLlib, Breeze<br/>• RocksDB<br/>• Scala compiler<br/>• Hive metastore"]
    end

    subgraph Stage2["Stage 2: Runtime (JRE 11)"]
        COPY[COPY --from=builder<br/>/opt/spark]
        ENTRY[entrypoint.sh<br/>JVM --add-opens<br/>spark-submit]
        USER[USER 185:185<br/>Non-root]
    end

    Stage1 --> Stage2
    Stage2 --> IMAGE["us.icr.io/medallion/<br/>spark-medallion:latest<br/>~850MB"]
```

---

## Recursos por Pod

| Componente | CPU Request | CPU Limit | Mem Request | Mem Limit |
|------------|-------------|-----------|-------------|-----------|
| **Init (preflight)** | 100m | 500m | 256Mi | 512Mi |
| **Driver** | 1 | 2 | 2Gi | 4Gi |
| **Executor** (×3-6) | 2 cores | — | 4g | — |

**Security Context del Driver:**

| Campo | Valor |
|-------|-------|
| `runAsNonRoot` | `true` |
| `runAsUser` | `185` |
| `runAsGroup` | `185` |
| `allowPrivilegeEscalation` | `false` |
| `capabilities.drop` | `ALL` |

---

## Volúmenes

| Nombre | Tipo | Mount | Tamaño |
|--------|------|-------|--------|
| `spark-work` | emptyDir | `/opt/spark/work` | 2Gi |
| `spark-conf` | ConfigMap | `/opt/spark/conf/spark-defaults.conf` | — |
| `scripts` | ConfigMap | `/opt/spark/scripts` | mode 0755 |
| `tmp` | emptyDir | `/tmp` | 1Gi |
