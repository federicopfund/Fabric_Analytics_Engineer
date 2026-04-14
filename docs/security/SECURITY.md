# Seguridad — Documentación Técnica

## Resumen

Estrategia de seguridad multi-capa que abarca gestión de secretos, encriptación en tránsito, network policies zero-trust en Kubernetes, RBAC granular, y controles de acceso en ambos clouds (Azure + IBM Cloud).

---

## Modelo de Seguridad — Capas

```mermaid
graph TB
    subgraph L1["Capa 1 — Identidad & Acceso"]
        IAM["IBM Cloud IAM<br/>Service IDs + API Keys"]
        AAD["Azure AD<br/>Service Principals"]
        SA["K8s ServiceAccount<br/>spark-driver"]
    end

    subgraph L2["Capa 2 — Secretos"]
        KP["IBM Key Protect<br/>Encryption Keys"]
        SM["Secrets Manager"]
        K8S_SEC["K8s Secrets<br/>cos / db2 / ibmcloud"]
        AKV["Azure Key Vault<br/>ADF Credentials"]
    end

    subgraph L3["Capa 3 — Red"]
        NP["K8s NetworkPolicy<br/>Zero-trust ingress/egress"]
        SG["IBM VPC Security Groups"]
        NSG["Azure NSG<br/>Storage firewall"]
    end

    subgraph L4["Capa 4 — Encriptación"]
        TLS["TLS/SSL en tránsito<br/>SQL, Db2, COS, HTTPS"]
        ENC["Encriptación at-rest<br/>Key Protect managed"]
    end

    subgraph L5["Capa 5 — Contenedor"]
        SEC_CTX["SecurityContext<br/>Non-root, drop ALL caps"]
        IMG["Image from private registry<br/>us.icr.io/medallion/"]
        SCAN["Trivy + pip-audit<br/>Vulnerability scan"]
    end

    subgraph L6["Capa 6 — Auditoría"]
        AT["Activity Tracker<br/>IBM Cloud events"]
        PROM["PrometheusRule<br/>Failure alerts"]
        SYSDIG["Sysdig Alerts<br/>Access denied events"]
    end

    L1 --> L2 --> L3 --> L4 --> L5 --> L6
```

---

## Gestión de Secretos

```mermaid
flowchart TD
    subgraph Sources["Origen de Secretos"]
        ENV[".env files<br/>(local dev, no versionados)"]
        TF_VARS["terraform.tfvars<br/>(sensitive vars)"]
        ENVSUBST["envsubst templates<br/>K8s secrets.yaml"]
    end

    subgraph Stores["Almacenes de Secretos"]
        KP_S["IBM Key Protect"]
        SM_S["IBM Secrets Manager"]
        K8S_S["Kubernetes Secrets"]
        AKV_S["Azure Key Vault"]
    end

    subgraph Consumers["Consumidores"]
        SPARK["Spark Driver/Executors"]
        ADF_C["Azure Data Factory"]
        TEKTON_C["Tekton Pipeline"]
        TF_C["Terraform"]
    end

    ENV -->|Local dev| SPARK
    TF_VARS -->|terraform apply| KP_S
    ENVSUBST -->|kubectl apply| K8S_S
    KP_S -->|Encrypt| K8S_S
    SM_S -->|Inject| TEKTON_C
    K8S_S -->|volumeMount / env| SPARK
    AKV_S -->|Linked Service| ADF_C
```

### Kubernetes Secrets — 3 Objetos

| Secret | Keys | Uso |
|--------|------|-----|
| `cos-credentials` | `access-key`, `secret-key`, `endpoint` | IBM COS S3A protocol |
| `db2-credentials` | `hostname`, `port`, `database`, `username`, `password`, `jdbc-url` | Db2 Cloud JDBC SSL |
| `ibmcloud-api` | `api-key` | IBM Cloud API (IAM) |

### Rotación de Credenciales

```mermaid
sequenceDiagram
    participant OPS as Operador
    participant SCRIPT as rotate-credentials.sh
    participant KP as Key Protect
    participant K8S as K8s Secrets
    participant COS as COS HMAC
    participant DB2 as Db2 Cloud

    OPS->>SCRIPT: Ejecutar rotación
    SCRIPT->>COS: Generar nuevas HMAC keys
    SCRIPT->>DB2: Rotar password
    SCRIPT->>KP: Actualizar encryption keys
    SCRIPT->>K8S: kubectl patch secrets
    K8S-->>SCRIPT: Secrets actualizados
    Note over K8S: Pods se reinician<br/>para tomar nuevas credenciales
```

---

## Network Policies — Zero Trust

```mermaid
graph TB
    subgraph Allowed_Ingress["✅ Ingress Permitido"]
        EX_P["Executor Pods<br/>→ Driver 7078/7079 TCP"]
        MON_P["Namespace monitoring<br/>→ Driver 4040 TCP"]
    end

    subgraph Driver_Pod["Driver Pod (spark-medallion)"]
        D["spark-driver<br/>Ports: 4040, 7078, 7079"]
    end

    subgraph Allowed_Egress["✅ Egress Permitido"]
        DNS_E["kube-system (DNS)<br/>53 UDP/TCP"]
        API_E["K8s API Server<br/>443 TCP"]
        COS_E["IBM COS<br/>443 TCP (HTTPS)"]
        DB2_E["Db2 Cloud<br/>30376 TCP (SSL)"]
        EXEC_RPC["Executor RPC<br/>Namespace pods"]
    end

    subgraph Blocked["❌ Bloqueado"]
        INT["Internet general"]
        OTHER["Otros namespaces"]
        UNKNOWN["Puertos no declarados"]
    end

    Allowed_Ingress --> Driver_Pod
    Driver_Pod --> Allowed_Egress
    Blocked -.->|DENY| Driver_Pod
```

---

## RBAC — Kubernetes

```mermaid
graph TB
    SA["ServiceAccount<br/>spark-driver"] --> RB["RoleBinding<br/>spark-driver-binding"]
    RB --> ROLE["Role: spark-driver-role"]

    ROLE --> R1["pods<br/>get, list, create,<br/>delete, watch"]
    ROLE --> R2["pods/log<br/>get, list"]
    ROLE --> R3["configmaps<br/>get, list, create, update"]
    ROLE --> R4["services<br/>get, list, create, delete"]
    ROLE --> R5["PVCs<br/>get, list, create, delete"]

    subgraph Limits["Restricciones"]
        QUOTA["ResourceQuota<br/>max 8 CPU / 16Gi mem<br/>max 20 pods"]
        LR["LimitRange<br/>default 1 CPU / 2Gi<br/>max 4 CPU / 8Gi"]
    end

    ROLE --> Limits
```

---

## Security Context — Container

| Campo | Valor | Propósito |
|-------|-------|-----------|
| `runAsNonRoot` | `true` | Prohibir ejecución como root |
| `runAsUser` | `185` | UID de spark user |
| `runAsGroup` | `185` | GID de spark group |
| `allowPrivilegeEscalation` | `false` | Sin escalamiento de privilegios |
| `readOnlyRootFilesystem` | `false` | Spark necesita escribir en work dir |
| `capabilities.drop` | `ALL` | Eliminar todas las capacidades Linux |

---

## Encriptación en Tránsito

```mermaid
graph LR
    subgraph Connections["Conexiones Encriptadas"]
        SQL_TLS["SQL Server<br/>TLS (Encrypt=True)"]
        DB2_SSL["Db2 Cloud<br/>SSL puerto 30376<br/>sslConnection=true"]
        COS_HTTPS["IBM COS<br/>HTTPS (S3A)<br/>ssl.enabled=true"]
        ADLS_HTTPS["ADLS Gen2<br/>HTTPS (.dfs endpoint)"]
        K8S_TLS["K8s API<br/>TLS 443<br/>ServiceAccount token"]
    end

    subgraph Protocols["Protocolos"]
        TLS12["TLS 1.2+"]
        CERT["Certificate validation"]
    end

    Connections --> Protocols
```

---

## Seguridad CI/CD — Tekton

```mermaid
flowchart TD
    CODE["Code Push"] --> SCAN["Security Scan Task"]

    SCAN --> PIP["pip-audit<br/>Python dependencies"]
    SCAN --> SAFETY["safety check<br/>Known vulnerabilities"]
    SCAN --> DETECT["detect-secrets<br/>Leaked credentials"]
    SCAN --> TRIVY["Trivy<br/>Container image scan"]

    PIP --> GATE{¿Vulnerabilidades<br/>críticas?}
    SAFETY --> GATE
    DETECT --> GATE
    TRIVY --> GATE

    GATE -->|Sí| BLOCK["❌ Pipeline bloqueado<br/>Notificar equipo"]
    GATE -->|No| CONTINUE["✅ Continuar deploy"]
```

---

## Alertas de Seguridad — Sysdig

| Alerta | Trigger | Severidad | Acción |
|--------|---------|-----------|--------|
| COS Access Denied | 5 eventos en 5 min (Activity Tracker) | HIGH | Slack + Email |
| Spark App Failed | metric = "failed" | CRITICAL | Slack + Email |
| Gold Bucket > 10GB | `ibm_cos.bucket.bytes_used` threshold | WARNING | Email |

---

## Checklist de Seguridad

| Control | Estado | Ubicación |
|---------|--------|-----------|
| Non-root containers | ✅ | `cronjob.yaml` securityContext |
| Drop ALL capabilities | ✅ | `cronjob.yaml` securityContext |
| Network Policies | ✅ | `network-policy.yaml` |
| Secret rotation script | ✅ | `rotate-credentials.sh` |
| Container image scan | ✅ | Tekton security-scan task |
| Dependency audit | ✅ | pip-audit + safety |
| Credential detection | ✅ | detect-secrets |
| ResourceQuota | ✅ | `rbac.yaml` |
| LimitRange | ✅ | `rbac.yaml` |
| SSL/TLS connections | ✅ | Todos los linked services |
| Key management | ✅ | IBM Key Protect |
| Activity tracking | ✅ | IBM Activity Tracker |
| Pod Anti-Affinity | ✅ | `cronjob.yaml` |
| Private container registry | ✅ | `us.icr.io` con `icr-secret` |
