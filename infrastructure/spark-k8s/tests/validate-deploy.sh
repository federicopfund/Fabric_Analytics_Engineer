#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# validate-deploy.sh — Pre-deploy + Live Cluster Validation
# Spark Medallion Pipeline on IBM IKS
#
# Uso:
#   ./tests/validate-deploy.sh              # dry-run solo (sin cluster)
#   ./tests/validate-deploy.sh --live       # validación contra cluster real
#   ./tests/validate-deploy.sh --live --cleanup  # + limpia después
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../kubernetes"
DOCKER_DIR="${SCRIPT_DIR}/../docker"
NAMESPACE="spark-medallion"
CRONJOB_NAME="spark-medallion-etl"

# ── Colores ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

pass() { PASS=$((PASS + 1)); echo -e "  ${GREEN}✓${NC} $1"; }
fail() { FAIL=$((FAIL + 1)); echo -e "  ${RED}✗${NC} $1"; }
warn() { WARN=$((WARN + 1)); echo -e "  ${YELLOW}⚠${NC} $1"; }
section() { echo -e "\n${BLUE}══ $1 ══${NC}"; }

# ═══════════════════════════════════════════════════════════════
# PHASE 1: Static Validation (no cluster needed)
# ═══════════════════════════════════════════════════════════════

phase1_static_validation() {
    section "Phase 1: Static Manifest Validation"

    # 1.1 — Archivos requeridos
    echo -e "\n${BLUE}── 1.1 Required Files ──${NC}"
    local REQUIRED_FILES=(
        "${K8S_DIR}/namespace.yaml"
        "${K8S_DIR}/rbac.yaml"
        "${K8S_DIR}/secrets.yaml"
        "${K8S_DIR}/configmaps.yaml"
        "${K8S_DIR}/cronjob.yaml"
        "${K8S_DIR}/network-policy.yaml"
        "${K8S_DIR}/service.yaml"
        "${K8S_DIR}/monitoring.yaml"
        "${DOCKER_DIR}/Dockerfile.k8s"
        "${DOCKER_DIR}/entrypoint.sh"
    )

    for f in "${REQUIRED_FILES[@]}"; do
        if [[ -f "$f" ]]; then
            pass "$(basename "$f") exists"
        else
            fail "$(basename "$f") MISSING"
        fi
    done

    # 1.2 — YAML syntax para cada manifest
    echo -e "\n${BLUE}── 1.2 YAML Syntax ──${NC}"
    for f in "${K8S_DIR}"/*.yaml; do
        if python3 -c "import yaml; yaml.safe_load_all(open('${f}'))" 2>/dev/null; then
            pass "$(basename "$f") — valid YAML"
        else
            fail "$(basename "$f") — invalid YAML"
        fi
    done

    # 1.3 — kubectl dry-run (client-side)
    echo -e "\n${BLUE}── 1.3 kubectl dry-run (client) ──${NC}"
    if kubectl cluster-info > /dev/null 2>&1; then
        for f in "${K8S_DIR}/namespace.yaml" "${K8S_DIR}/rbac.yaml" \
                 "${K8S_DIR}/configmaps.yaml" "${K8S_DIR}/service.yaml" \
                 "${K8S_DIR}/cronjob.yaml" "${K8S_DIR}/network-policy.yaml"; do
            local base
            base=$(basename "$f")
            if kubectl apply -f "$f" --dry-run=client -o yaml > /dev/null 2>&1; then
                pass "${base} — dry-run OK"
            else
                fail "${base} — dry-run FAILED"
            fi
        done
    else
        # Sin cluster: validar con python yaml parser como fallback
        for f in "${K8S_DIR}/namespace.yaml" "${K8S_DIR}/rbac.yaml" \
                 "${K8S_DIR}/configmaps.yaml" "${K8S_DIR}/service.yaml" \
                 "${K8S_DIR}/cronjob.yaml" "${K8S_DIR}/network-policy.yaml"; do
            local base
            base=$(basename "$f")
            if python3 -c "
import yaml, sys
with open('${f}') as fh:
    docs = list(yaml.safe_load_all(fh))
for d in docs:
    if d is None: continue
    assert 'apiVersion' in d, 'missing apiVersion'
    assert 'kind' in d, 'missing kind'
    assert 'metadata' in d, 'missing metadata'
" 2>/dev/null; then
                pass "${base} — structure OK (no cluster for dry-run)"
            else
                fail "${base} — invalid structure"
            fi
        done
    fi

    # 1.4 — Secrets template validation (no expandir)
    echo -e "\n${BLUE}── 1.4 Secrets Template ──${NC}"
    if grep -q '${COS_ACCESS_KEY}' "${K8S_DIR}/secrets.yaml"; then
        pass "secrets.yaml uses template variables (envsubst)"
    else
        fail "secrets.yaml may contain hardcoded credentials"
    fi

    if grep -q 'sslConnection=true' "${K8S_DIR}/secrets.yaml"; then
        pass "Db2 JDBC URL includes SSL"
    else
        warn "Db2 JDBC URL missing SSL"
    fi

    # 1.5 — Security checks estáticos
    echo -e "\n${BLUE}── 1.5 Security Checks ──${NC}"

    if grep -q 'runAsNonRoot: true' "${K8S_DIR}/cronjob.yaml"; then
        pass "CronJob enforces runAsNonRoot"
    else
        fail "CronJob missing runAsNonRoot"
    fi

    if grep -q 'allowPrivilegeEscalation: false' "${K8S_DIR}/cronjob.yaml"; then
        pass "No privilege escalation"
    else
        fail "Missing allowPrivilegeEscalation: false"
    fi

    if grep -q 'drop:' "${K8S_DIR}/cronjob.yaml" && grep -q '"ALL"' "${K8S_DIR}/cronjob.yaml"; then
        pass "Capabilities: drop ALL"
    else
        warn "Capabilities not fully dropped"
    fi

    if grep -q 'concurrencyPolicy: Forbid' "${K8S_DIR}/cronjob.yaml"; then
        pass "ConcurrencyPolicy: Forbid"
    else
        fail "ConcurrencyPolicy should be Forbid"
    fi

    if grep -q 'secretKeyRef' "${K8S_DIR}/cronjob.yaml"; then
        pass "Credentials use secretKeyRef"
    else
        fail "Credentials not using secretKeyRef"
    fi

    # 1.6 — Dockerfile checks
    echo -e "\n${BLUE}── 1.6 Dockerfile Checks ──${NC}"
    local DF="${DOCKER_DIR}/Dockerfile.k8s"

    local FROM_COUNT
    FROM_COUNT=$(grep -c '^FROM ' "$DF" || true)
    if [[ $FROM_COUNT -ge 2 ]]; then
        pass "Multi-stage build (${FROM_COUNT} stages)"
    else
        fail "Not a multi-stage build"
    fi

    if grep -q 'USER' "$DF"; then
        pass "Non-root USER set"
    else
        fail "No USER instruction (runs as root)"
    fi

    if grep -q 'tini' "$DF"; then
        pass "Uses tini as PID 1"
    else
        warn "No tini init system"
    fi

    if grep -q 'EXPOSE.*4040' "$DF"; then
        pass "Exposes Spark UI port 4040"
    else
        warn "Port 4040 not explicitly exposed"
    fi

    # 1.7 — NetworkPolicy
    echo -e "\n${BLUE}── 1.7 NetworkPolicy ──${NC}"

    local NP_COUNT
    NP_COUNT=$(grep -c 'kind: NetworkPolicy' "${K8S_DIR}/network-policy.yaml" || true)
    if [[ $NP_COUNT -ge 2 ]]; then
        pass "${NP_COUNT} NetworkPolicies defined (driver + executor)"
    else
        warn "Expected 2 NetworkPolicies, found ${NP_COUNT}"
    fi

    if grep -q 'port: 30376' "${K8S_DIR}/network-policy.yaml"; then
        pass "Db2 port 30376 in driver egress"
    else
        warn "Db2 port not in NetworkPolicy"
    fi

    # 1.8 — Monitoring
    echo -e "\n${BLUE}── 1.8 Monitoring ──${NC}"

    if grep -q 'kind: ServiceMonitor' "${K8S_DIR}/monitoring.yaml"; then
        pass "ServiceMonitor defined"
    else
        warn "No ServiceMonitor"
    fi

    if grep -q 'kind: PrometheusRule' "${K8S_DIR}/monitoring.yaml"; then
        pass "PrometheusRule defined"
    else
        warn "No alerting rules"
    fi

    local ALERT_COUNT
    ALERT_COUNT=$(grep -c 'alert:' "${K8S_DIR}/monitoring.yaml" || true)
    if [[ $ALERT_COUNT -ge 5 ]]; then
        pass "${ALERT_COUNT} alert rules defined"
    else
        warn "Only ${ALERT_COUNT} alert rules (recommended: 5+)"
    fi
}

# ═══════════════════════════════════════════════════════════════
# PHASE 2: Live Cluster Validation
# ═══════════════════════════════════════════════════════════════

phase2_live_validation() {
    section "Phase 2: Live Cluster Validation"

    # 2.0 — Cluster connectivity
    echo -e "\n${BLUE}── 2.0 Cluster Connectivity ──${NC}"
    if ! kubectl cluster-info > /dev/null 2>&1; then
        fail "Cannot connect to Kubernetes cluster"
        echo -e "${RED}Abortando Phase 2 — no hay conexión al cluster${NC}"
        return 1
    fi
    pass "Connected to cluster: $(kubectl config current-context)"

    local K8S_VERSION
    K8S_VERSION=$(kubectl version --short 2>/dev/null | grep Server | awk '{print $3}' || kubectl version -o json 2>/dev/null | python3 -c "import sys,json; v=json.load(sys.stdin)['serverVersion']; print(f\"v{v['major']}.{v['minor']}\")" 2>/dev/null || echo "unknown")
    pass "Server version: ${K8S_VERSION}"

    # 2.1 — Deploy namespace
    echo -e "\n${BLUE}── 2.1 Namespace Deploy ──${NC}"
    if kubectl apply -f "${K8S_DIR}/namespace.yaml" --dry-run=server -o yaml > /dev/null 2>&1; then
        pass "namespace.yaml — server dry-run OK"
    else
        fail "namespace.yaml — server dry-run FAILED"
    fi

    kubectl apply -f "${K8S_DIR}/namespace.yaml" 2>/dev/null && \
        pass "Namespace ${NAMESPACE} created/exists" || \
        fail "Cannot create namespace"

    # 2.2 — RBAC
    echo -e "\n${BLUE}── 2.2 RBAC ──${NC}"
    kubectl apply -f "${K8S_DIR}/rbac.yaml" 2>/dev/null && \
        pass "RBAC resources applied" || \
        fail "RBAC apply failed"

    if kubectl get serviceaccount spark-driver -n "${NAMESPACE}" > /dev/null 2>&1; then
        pass "ServiceAccount spark-driver exists"
    else
        fail "ServiceAccount spark-driver not found"
    fi

    if kubectl get resourcequota -n "${NAMESPACE}" > /dev/null 2>&1; then
        pass "ResourceQuota active"
    else
        warn "ResourceQuota not found"
    fi

    # 2.3 — Secrets (template — requiere env vars)
    echo -e "\n${BLUE}── 2.3 Secrets ──${NC}"
    # Usar valores dummy para test
    export COS_ACCESS_KEY="${COS_ACCESS_KEY:-test-access-key}"
    export COS_SECRET_KEY="${COS_SECRET_KEY:-test-secret-key}"
    export DB2_HOSTNAME="${DB2_HOSTNAME:-test-db2-host}"
    export DB2_PORT="${DB2_PORT:-30376}"
    export DB2_DATABASE="${DB2_DATABASE:-bludb}"
    export DB2_USERNAME="${DB2_USERNAME:-testuser}"
    export DB2_PASSWORD="${DB2_PASSWORD:-testpass}"
    export IBMCLOUD_API_KEY="${IBMCLOUD_API_KEY:-test-api-key}"

    if envsubst < "${K8S_DIR}/secrets.yaml" | kubectl apply -f - --dry-run=server > /dev/null 2>&1; then
        pass "secrets.yaml — server dry-run with envsubst OK"
    else
        fail "secrets.yaml — server dry-run failed"
    fi

    envsubst < "${K8S_DIR}/secrets.yaml" | kubectl apply -f - 2>/dev/null && \
        pass "Secrets applied (test values)" || \
        fail "Secrets apply failed"

    # 2.4 — ConfigMaps
    echo -e "\n${BLUE}── 2.4 ConfigMaps ──${NC}"
    kubectl apply -f "${K8S_DIR}/configmaps.yaml" 2>/dev/null && \
        pass "ConfigMaps applied" || \
        fail "ConfigMaps apply failed"

    if kubectl get configmap spark-config -n "${NAMESPACE}" > /dev/null 2>&1; then
        pass "spark-config ConfigMap exists"
    fi

    if kubectl get configmap spark-scripts -n "${NAMESPACE}" > /dev/null 2>&1; then
        pass "spark-scripts ConfigMap exists"
    fi

    # 2.5 — NetworkPolicy
    echo -e "\n${BLUE}── 2.5 NetworkPolicy ──${NC}"
    kubectl apply -f "${K8S_DIR}/network-policy.yaml" 2>/dev/null && \
        pass "NetworkPolicies applied" || \
        fail "NetworkPolicies apply failed"

    local NP_COUNT
    NP_COUNT=$(kubectl get networkpolicy -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l)
    if [[ $NP_COUNT -ge 2 ]]; then
        pass "${NP_COUNT} NetworkPolicies active"
    else
        warn "Expected 2 NetworkPolicies, found ${NP_COUNT}"
    fi

    # 2.6 — Service
    echo -e "\n${BLUE}── 2.6 Service ──${NC}"
    kubectl apply -f "${K8S_DIR}/service.yaml" 2>/dev/null && \
        pass "Service applied" || \
        fail "Service apply failed"

    local SVC_TYPE
    SVC_TYPE=$(kubectl get svc spark-driver-ui -n "${NAMESPACE}" -o jsonpath='{.spec.type}' 2>/dev/null || echo "")
    if [[ "$SVC_TYPE" == "ClusterIP" ]]; then
        pass "Service type: ClusterIP (correct)"
    elif [[ -n "$SVC_TYPE" ]]; then
        warn "Service type: ${SVC_TYPE} (expected ClusterIP)"
    fi

    # 2.7 — CronJob
    echo -e "\n${BLUE}── 2.7 CronJob ──${NC}"
    kubectl apply -f "${K8S_DIR}/cronjob.yaml" --dry-run=server -o yaml > /dev/null 2>&1 && \
        pass "cronjob.yaml — server dry-run OK" || \
        fail "cronjob.yaml — server dry-run FAILED"

    kubectl apply -f "${K8S_DIR}/cronjob.yaml" 2>/dev/null && \
        pass "CronJob applied" || \
        fail "CronJob apply failed"

    if kubectl get cronjob "${CRONJOB_NAME}" -n "${NAMESPACE}" > /dev/null 2>&1; then
        pass "CronJob ${CRONJOB_NAME} exists"

        local SCHEDULE
        SCHEDULE=$(kubectl get cronjob "${CRONJOB_NAME}" -n "${NAMESPACE}" \
            -o jsonpath='{.spec.schedule}' 2>/dev/null)
        pass "Schedule: ${SCHEDULE}"

        local SUSPEND
        SUSPEND=$(kubectl get cronjob "${CRONJOB_NAME}" -n "${NAMESPACE}" \
            -o jsonpath='{.spec.suspend}' 2>/dev/null)
        if [[ "$SUSPEND" == "false" ]]; then
            pass "CronJob is active (not suspended)"
        else
            warn "CronJob is suspended"
        fi
    else
        fail "CronJob not found"
    fi

    # 2.8 — Monitoring (puede fallar si no hay prometheus-operator)
    echo -e "\n${BLUE}── 2.8 Monitoring ──${NC}"
    if kubectl apply -f "${K8S_DIR}/monitoring.yaml" 2>/dev/null; then
        pass "Monitoring resources applied"
    else
        warn "Monitoring apply failed (prometheus-operator may not be installed)"
    fi

    # 2.9 — Trigger manual job para validar ejecución
    echo -e "\n${BLUE}── 2.9 Manual Job Trigger ──${NC}"
    local JOB_NAME="${CRONJOB_NAME}-test-$(date +%s)"

    if kubectl create job "${JOB_NAME}" -n "${NAMESPACE}" \
        --from="cronjob/${CRONJOB_NAME}" --dry-run=server -o yaml > /dev/null 2>&1; then
        pass "Manual job creation dry-run OK"
    else
        fail "Cannot create manual job from CronJob"
    fi

    # 2.10 — Resource overview
    echo -e "\n${BLUE}── 2.10 Resource Summary ──${NC}"
    echo ""
    kubectl get all -n "${NAMESPACE}" 2>/dev/null || true
    echo ""
    kubectl get networkpolicy -n "${NAMESPACE}" 2>/dev/null || true
    echo ""
    kubectl get resourcequota -n "${NAMESPACE}" 2>/dev/null || true
}

# ═══════════════════════════════════════════════════════════════
# PHASE 3: Cleanup (optional)
# ═══════════════════════════════════════════════════════════════

phase3_cleanup() {
    section "Phase 3: Cleanup"
    echo -e "${YELLOW}Removing test resources from namespace ${NAMESPACE}...${NC}"
    kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=false 2>/dev/null
    pass "Cleanup initiated (namespace deletion is async)"
}

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

main() {
    echo "═══════════════════════════════════════════════════"
    echo "  Spark Medallion — K8s Deployment Validation"
    echo "  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "═══════════════════════════════════════════════════"

    local LIVE=false
    local CLEANUP=false

    for arg in "$@"; do
        case $arg in
            --live) LIVE=true ;;
            --cleanup) CLEANUP=true ;;
        esac
    done

    # Phase 1 siempre se ejecuta
    phase1_static_validation

    # Phase 2 solo con --live
    if [[ "$LIVE" == "true" ]]; then
        phase2_live_validation
    else
        echo -e "\n${YELLOW}Skipping Phase 2 (live validation). Use --live to enable.${NC}"
    fi

    # Phase 3 solo con --cleanup
    if [[ "$CLEANUP" == "true" && "$LIVE" == "true" ]]; then
        phase3_cleanup
    fi

    # ── Summary ──
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo -e "  Results: ${GREEN}${PASS} passed${NC}  ${RED}${FAIL} failed${NC}  ${YELLOW}${WARN} warnings${NC}"
    echo "═══════════════════════════════════════════════════"

    if [[ $FAIL -gt 0 ]]; then
        echo -e "${RED}DEPLOYMENT VALIDATION FAILED${NC}"
        exit 1
    else
        echo -e "${GREEN}DEPLOYMENT VALIDATION PASSED${NC}"
        exit 0
    fi
}

main "$@"
