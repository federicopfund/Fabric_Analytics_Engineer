# CI/CD — Tekton Pipelines

## Resumen

Pipeline CI/CD de 9 etapas implementado en Tekton para IBM Cloud. Integra testing (Python + Scala), security scanning, build de artefactos (JAR + Docker image), deploy a Analytics Engine e IKS, y notificaciones automáticas. Triggers configurados para push a main (producción) y pull requests (validación).

---

## Pipeline de 9 Etapas

```mermaid
flowchart TD
    TRIGGER([GitHub Push/PR]) --> T1

    T1["1. Clone Repository<br/>git-clone workspace"]
    T1 --> T2["2. Run Tests<br/>pytest + sbt test"]
    T2 --> T3["3. Security Scan<br/>pip-audit, safety,<br/>detect-secrets, Trivy"]
    T3 --> T4["4. Build JAR<br/>sbt assembly<br/>root-assembly-2.0.0.jar"]
    T4 --> T5["5. Build Image<br/>Docker multi-stage<br/>us.icr.io/medallion/"]
    T5 --> T6["6. Upload to COS<br/>JAR → spark-jars/<br/>bucket"]
    T6 --> T7["7. Submit to AE<br/>Analytics Engine<br/>Serverless Spark"]
    T7 --> T8["8. Deploy to IKS<br/>kubectl apply<br/>K8s manifests"]
    T8 --> T9["9. Notify<br/>Slack + Email<br/>Status report"]

    style T1 fill:#e3f2fd
    style T2 fill:#e8f5e9
    style T3 fill:#fff3e0
    style T4 fill:#e3f2fd
    style T5 fill:#e3f2fd
    style T6 fill:#f3e5f5
    style T7 fill:#f3e5f5
    style T8 fill:#e8f5e9
    style T9 fill:#fce4ec
```

---

## Parámetros del Pipeline

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| `git-url` | string | URL del repositorio |
| `git-branch` | string | Branch a construir |
| `cos-endpoint` | string | Endpoint COS para upload |
| `ae-instance-id` | string | ID Analytics Engine |
| `deploy-target` | string | Target de deploy (iks/ae) |
| `notify-webhook` | string | Webhook Slack/Teams |

**Workspaces:**

| Workspace | Tamaño | Uso |
|-----------|--------|-----|
| `source` | 2Gi PVC | Código fuente clonado |
| `credentials` | Secret | Credenciales de acceso |

---

## Task 1: Clone Repository

```mermaid
flowchart LR
    GH["GitHub<br/>Repository"] -->|git clone| WS["Workspace<br/>source (2Gi)"]
```

## Task 2: Run Tests

```mermaid
flowchart TD
    SRC["Source Code"] --> PY["Python Tests<br/>pytest --cov<br/>transformation/notebooks/"]
    SRC --> SCALA["Scala Tests<br/>sbt test<br/>spark-jobs/"]

    PY --> REPORT_PY["Coverage Report"]
    SCALA --> REPORT_SC["Test Report"]

    REPORT_PY --> GATE{¿Tests OK?}
    REPORT_SC --> GATE
    GATE -->|✅| NEXT["→ Security Scan"]
    GATE -->|❌| FAIL["Pipeline FAIL"]
```

**Recursos:**

| Componente | CPU | Memory |
|------------|-----|--------|
| Test runner | 250m - 500m | 512Mi - 1Gi |

## Task 3: Security Scan

```mermaid
flowchart TD
    CODE["Code + Dependencies"] --> SCAN["Security Scan"]

    SCAN --> S1["pip-audit<br/>Python package<br/>vulnerabilities"]
    SCAN --> S2["safety check<br/>Known CVEs"]
    SCAN --> S3["detect-secrets<br/>Leaked credentials<br/>in source code"]
    SCAN --> S4["Trivy<br/>Container image<br/>vulnerabilities"]

    S1 --> RESULT{¿Critical?}
    S2 --> RESULT
    S3 --> RESULT
    S4 --> RESULT

    RESULT -->|No criticals| PASS["✅ PASS"]
    RESULT -->|Criticals found| BLOCK["❌ BLOCK"]
```

## Task 4: Build JAR

```mermaid
flowchart LR
    SRC["Scala Source"] -->|sbt assembly| JAR["root-assembly-2.0.0.jar<br/>Fat JAR con dependencias"]
```

## Task 5: Build Docker Image

```mermaid
flowchart TD
    DF["Dockerfile.k8s<br/>Multi-stage"] --> BUILD["Docker Build"]
    BUILD --> STAGE1["Stage 1: Builder<br/>JDK 11 + Spark 3.3.1<br/>+ Delta + S3A + Db2 JARs"]
    STAGE1 --> STAGE2["Stage 2: Runtime<br/>JRE 11 + Spark minimal"]
    STAGE2 --> PUSH["Push to Registry<br/>us.icr.io/medallion/<br/>spark-medallion:latest"]
```

## Task 6: Upload JAR to COS

```mermaid
flowchart LR
    JAR["root-assembly-<br/>2.0.0.jar"] -->|"S3 PUT<br/>aws s3 cp"| COS["COS Bucket<br/>spark-jars/"]
```

## Task 7: Submit to Analytics Engine

```mermaid
sequenceDiagram
    participant TEK as Tekton Task
    participant AE as Analytics Engine
    participant COS as Cloud Object Storage

    TEK->>AE: POST /v3/analytics_engines/{id}/spark_applications
    Note over TEK,AE: Config: driver 1 core/2GB<br/>executor 2 cores/4GB × 3
    AE->>COS: Read JAR from spark-jars/
    AE->>AE: Execute Pipeline
    TEK->>AE: GET /status (polling)
    AE-->>TEK: RUNNING → FINISHED
```

## Task 8: Deploy to IKS

```mermaid
flowchart TD
    MANIFESTS["K8s Manifests<br/>8 YAML files"] --> APPLY["kubectl apply -f"]
    APPLY --> NS["namespace.yaml"]
    APPLY --> RBAC_D["rbac.yaml"]
    APPLY --> CM["configmaps.yaml"]
    APPLY --> SEC_D["secrets.yaml"]
    APPLY --> SVC_D["service.yaml"]
    APPLY --> NP["network-policy.yaml"]
    APPLY --> MON_D["monitoring.yaml"]
    APPLY --> CJ["cronjob.yaml"]
    CJ --> VERIFY["kubectl rollout status"]
```

## Task 9: Notify

```mermaid
flowchart LR
    RESULT["Pipeline Result<br/>Success / Failure"] --> SLACK["Slack Webhook<br/>Channel notification"]
    RESULT --> EMAIL["Email Alert<br/>Team distribution"]
```

---

## Triggers — GitHub Webhooks

```mermaid
flowchart TD
    GH["GitHub Repository"] -->|Webhook| EL["EventListener<br/>Public endpoint"]

    EL --> TB1["TriggerBinding<br/>push event"]
    EL --> TB2["TriggerBinding<br/>pull_request event"]

    TB1 --> TT1["TriggerTemplate<br/>Production"]
    TB2 --> TT2["TriggerTemplate<br/>PR Validation"]

    TT1 --> FULL["Full Pipeline<br/>9 stages<br/>Timeout: 30 min"]
    TT2 --> PARTIAL["Tests + Scan Only<br/>Stages 1-3<br/>Timeout: 15 min"]
```

### Trigger de Producción (Push to main)

| Config | Valor |
|--------|-------|
| Evento | `push` to `main` |
| Pipeline | Completo (9 stages) |
| Timeout | 30 min |
| Deploy target | IKS + Analytics Engine |

### Trigger de PR (Pull Request)

| Config | Valor |
|--------|-------|
| Evento | `pull_request` (open/sync) |
| Pipeline | Parcial (stages 1-3) |
| Timeout | 15 min |
| Deploy target | Ninguno (solo validación) |

---

## Flujo Completo CI/CD

```mermaid
graph TB
    subgraph Dev["Desarrollo"]
        CODE["Developer Push<br/>Feature branch"]
    end

    subgraph PR_Phase["Fase PR"]
        PR["Pull Request"] --> TESTS["Tests<br/>pytest + sbt"]
        TESTS --> SECURITY["Security Scan<br/>4 herramientas"]
        SECURITY --> REVIEW["Code Review"]
    end

    subgraph CI["Integration (main)"]
        MERGE["Merge to main"] --> CLONE["Clone + Test + Scan"]
        CLONE --> BUILD["Build JAR + Image"]
        BUILD --> UPLOAD["Upload COS"]
    end

    subgraph CD["Delivery"]
        UPLOAD --> AE_DEPLOY["Submit to<br/>Analytics Engine"]
        UPLOAD --> K8S_DEPLOY["Deploy to IKS<br/>CronJob update"]
        AE_DEPLOY --> NOTIFY["Slack + Email<br/>Notification"]
        K8S_DEPLOY --> NOTIFY
    end

    Dev --> PR_Phase
    PR_Phase --> CI
    CI --> CD
```

---

## Recursos del Pipeline

| Task | CPU Request | CPU Limit | Mem Request | Mem Limit |
|------|-------------|-----------|-------------|-----------|
| Tests | 250m | 500m | 512Mi | 1Gi |
| Security Scan | 250m | 500m | 512Mi | 1Gi |
| Build JAR | 500m | 1 | 1Gi | 2Gi |
| Build Image | 500m | 1 | 1Gi | 2Gi |
| Deploy | 100m | 250m | 256Mi | 512Mi |
