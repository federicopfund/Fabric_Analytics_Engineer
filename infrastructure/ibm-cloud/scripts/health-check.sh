#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# health-check.sh — Infrastructure Health Check
#
# Validates connectivity and status of all IBM Cloud services:
#   - IBM Cloud CLI login
#   - Cloud Object Storage (4 medallion buckets)
#   - Db2 on Cloud (JDBC connectivity)
#   - Analytics Engine Serverless (instance state)
#   - IKS Cluster (node status)
#
# Usage:
#   ./health-check.sh          # Full check
#   ./health-check.sh --ae     # Analytics Engine only
#   ./health-check.sh --cos    # COS only
#   ./health-check.sh --db2    # Db2 only
#   ./health-check.sh --iks    # IKS only
#   ./health-check.sh --json   # Output as JSON
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration ──
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${SCRIPT_DIR}/../.env" ]; then
    set -a && source "${SCRIPT_DIR}/../.env" && set +a
fi

AE_INSTANCE_ID="${AE_INSTANCE_ID:-}"
REGION="${IBMCLOUD_REGION:-us-south}"

# ── Colors ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── Counters ──
PASS=0; FAIL=0; WARN=0; TOTAL=0
RESULTS=()
JSON_MODE=false

log_pass() { ((++PASS)); ((++TOTAL)); RESULTS+=("PASS|$1|$2"); echo -e "  ${GREEN}✔ PASS${NC}  $1 — $2"; }
log_fail() { ((++FAIL)); ((++TOTAL)); RESULTS+=("FAIL|$1|$2"); echo -e "  ${RED}✘ FAIL${NC}  $1 — $2"; }
log_warn() { ((++WARN)); ((++TOTAL)); RESULTS+=("WARN|$1|$2"); echo -e "  ${YELLOW}⚠ WARN${NC}  $1 — $2"; }
log_skip() { RESULTS+=("SKIP|$1|$2"); echo -e "  ${CYAN}→ SKIP${NC}  $1 — $2"; }

# ── Check: IBM Cloud CLI ──
check_cli() {
    echo ""
    echo -e "${BOLD}IBM Cloud CLI${NC}"
    echo "─────────────────────────────────────"

    if command -v ibmcloud &>/dev/null; then
        local version
        version=$(ibmcloud version 2>/dev/null | head -1)
        log_pass "CLI installed" "$version"
    else
        log_fail "CLI installed" "ibmcloud not found"
        return
    fi

    if ibmcloud target &>/dev/null; then
        local account
        account=$(ibmcloud target 2>/dev/null | grep "Account:" | sed 's/Account: *//')
        log_pass "Authenticated" "$account"
    else
        log_fail "Authenticated" "Not logged in (ibmcloud login --sso)"
    fi

    local plugins
    plugins=$(ibmcloud plugin list 2>/dev/null | grep -c "^[a-z]" || echo "0")
    if (( plugins >= 3 )); then
        log_pass "Plugins" "$plugins plugins installed"
    else
        log_warn "Plugins" "Only $plugins plugins (need: cos, ae-v3, ks)"
    fi
}

# ── Check: COS Buckets ──
check_cos() {
    echo ""
    echo -e "${BOLD}Cloud Object Storage${NC}"
    echo "─────────────────────────────────────"

    local buckets=("datalake-raw-${REGION}" "datalake-bronze-${REGION}" "datalake-silver-${REGION}" "datalake-gold-${REGION}")

    for bucket in "${buckets[@]}"; do
        local count
        count=$(ibmcloud cos list-objects --bucket "$bucket" --region "$REGION" --output json 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('Contents',[])))" 2>/dev/null || echo "-1")

        if [[ "$count" == "-1" ]]; then
            log_fail "$bucket" "Bucket inaccessible"
        elif [[ "$count" == "0" ]]; then
            log_warn "$bucket" "Bucket empty"
        else
            log_pass "$bucket" "$count objects"
        fi
    done
}

# ── Check: Analytics Engine ──
check_ae() {
    echo ""
    echo -e "${BOLD}Analytics Engine Serverless${NC}"
    echo "─────────────────────────────────────"

    if [[ -z "$AE_INSTANCE_ID" ]]; then
        log_skip "AE Status" "AE_INSTANCE_ID not configured"
        return
    fi

    if ! ibmcloud plugin show analytics-engine-v3 &>/dev/null; then
        log_fail "AE Plugin" "analytics-engine-v3 not installed"
        return
    fi

    local state
    state=$(ibmcloud ae-v3 instance show --id "${AE_INSTANCE_ID}" --output json 2>/dev/null \
        | grep -v '^Performing' \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null || echo "error")

    if [[ "$state" == "active" ]]; then
        log_pass "AE Instance" "State: active (Spark 3.5)"
    elif [[ "$state" == "error" ]]; then
        log_fail "AE Instance" "Cannot reach instance $AE_INSTANCE_ID"
    else
        log_warn "AE Instance" "State: $state"
    fi

    # Check recent applications
    local app_count
    app_count=$(ibmcloud ae-v3 spark-app list --instance-id "${AE_INSTANCE_ID}" --output json 2>/dev/null \
        | grep -v '^Performing' \
        | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('applications',[])))" 2>/dev/null || echo "0")
    log_pass "Spark Apps" "$app_count applications registered"

    # Check last app status
    local last_state
    last_state=$(ibmcloud ae-v3 spark-app list --instance-id "${AE_INSTANCE_ID}" --output json 2>/dev/null \
        | grep -v '^Performing' \
        | python3 -c "
import sys,json
apps = json.load(sys.stdin).get('applications',[])
if apps: print(apps[0].get('state','unknown'))
else: print('none')
" 2>/dev/null || echo "unknown")

    if [[ "$last_state" == "finished" ]]; then
        log_pass "Last App" "State: finished"
    elif [[ "$last_state" == "none" ]]; then
        log_skip "Last App" "No applications submitted"
    elif [[ "$last_state" == "failed" ]]; then
        log_fail "Last App" "State: failed"
    else
        log_warn "Last App" "State: $last_state"
    fi
}

# ── Check: Db2 ──
check_db2() {
    echo ""
    echo -e "${BOLD}Db2 on Cloud${NC}"
    echo "─────────────────────────────────────"

    local host="${DB2_HOSTNAME:-}"
    local port="${DB2_PORT:-30376}"

    if [[ -z "$host" ]]; then
        log_skip "Db2 Connectivity" "DB2_HOSTNAME not configured"
        return
    fi

    # TCP connectivity check
    if timeout 5 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
        log_pass "Db2 TCP" "$host:$port reachable"
    else
        log_fail "Db2 TCP" "$host:$port unreachable"
        return
    fi

    # Python JDBC check
    if python3 -c "
import ibm_db
import os
conn_str = f\"DATABASE={os.environ.get('DB2_DATABASE','bludb')};HOSTNAME={os.environ.get('DB2_HOSTNAME')};PORT={os.environ.get('DB2_PORT','30376')};PROTOCOL=TCPIP;UID={os.environ.get('DB2_USERNAME','')};PWD={os.environ.get('DB2_PASSWORD','')};SECURITY=SSL\"
conn = ibm_db.connect(conn_str, '', '')
info = ibm_db.server_info(conn)
print(f'{info.DBMS_NAME} {info.DBMS_VER}')
ibm_db.close(conn)
" 2>/dev/null; then
        log_pass "Db2 Auth" "JDBC connection successful"
    else
        log_warn "Db2 Auth" "TCP OK but JDBC failed (missing ibm_db or wrong credentials)"
    fi
}

# ── Check: IKS Cluster ──
check_iks() {
    echo ""
    echo -e "${BOLD}IKS Cluster${NC}"
    echo "─────────────────────────────────────"

    local cluster="${CLUSTER_NAME:-}"

    if [[ -z "$cluster" ]]; then
        log_skip "IKS Cluster" "CLUSTER_NAME not configured"
        return
    fi

    if ! ibmcloud ks cluster get --cluster "$cluster" &>/dev/null; then
        log_warn "IKS Cluster" "$cluster not found or not accessible"
        return
    fi

    local state
    state=$(ibmcloud ks cluster get --cluster "$cluster" --output json 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null || echo "unknown")

    if [[ "$state" == "normal" ]]; then
        log_pass "IKS Cluster" "$cluster — State: normal"
    else
        log_warn "IKS Cluster" "$cluster — State: $state"
    fi

    if kubectl get nodes &>/dev/null; then
        local ready
        ready=$(kubectl get nodes --no-headers 2>/dev/null | grep -c "Ready" || echo "0")
        log_pass "K8s Nodes" "$ready nodes Ready"
    else
        log_warn "K8s Nodes" "kubectl not configured for this cluster"
    fi
}

# ── Summary ──
print_summary() {
    echo ""
    echo "═══════════════════════════════════════════════════════"
    echo -e "${BOLD}Health Check Summary${NC}"
    echo "═══════════════════════════════════════════════════════"
    echo -e "  ${GREEN}PASS:${NC} $PASS  ${RED}FAIL:${NC} $FAIL  ${YELLOW}WARN:${NC} $WARN  Total: $TOTAL"

    if (( FAIL > 0 )); then
        echo ""
        echo -e "  ${RED}FAILED CHECKS:${NC}"
        for r in "${RESULTS[@]}"; do
            if [[ "$r" == FAIL* ]]; then
                echo -e "    ${RED}✘${NC} $(echo "$r" | cut -d'|' -f2): $(echo "$r" | cut -d'|' -f3)"
            fi
        done
    fi

    echo "═══════════════════════════════════════════════════════"

    if (( FAIL > 0 )); then
        echo -e "  ${RED}Status: UNHEALTHY${NC}"
        return 1
    elif (( WARN > 0 )); then
        echo -e "  ${YELLOW}Status: DEGRADED${NC}"
        return 0
    else
        echo -e "  ${GREEN}Status: HEALTHY ✔${NC}"
        return 0
    fi
}

# ── JSON Output ──
print_json() {
    echo "{"
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
    echo "  \"status\": \"$(if (( FAIL > 0 )); then echo "unhealthy"; elif (( WARN > 0 )); then echo "degraded"; else echo "healthy"; fi)\","
    echo "  \"pass\": $PASS, \"fail\": $FAIL, \"warn\": $WARN, \"total\": $TOTAL,"
    echo "  \"checks\": ["
    local first=true
    for r in "${RESULTS[@]}"; do
        local status=$(echo "$r" | cut -d'|' -f1)
        local name=$(echo "$r" | cut -d'|' -f2)
        local detail=$(echo "$r" | cut -d'|' -f3)
        if $first; then first=false; else echo ","; fi
        printf '    {"status": "%s", "name": "%s", "detail": "%s"}' "$status" "$name" "$detail"
    done
    echo ""
    echo "  ]"
    echo "}"
}

# ── Main ──
main() {
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  IBM Cloud — Infrastructure Health Check                    ║"
    echo "║  $(date -u +%Y-%m-%dT%H:%M:%SZ)                                       ║"
    echo "╚══════════════════════════════════════════════════════════════╝"

    case "${1:-}" in
        --ae)    check_cli; check_ae ;;
        --cos)   check_cli; check_cos ;;
        --db2)   check_cli; check_db2 ;;
        --iks)   check_cli; check_iks ;;
        --json)  JSON_MODE=true; check_cli; check_cos; check_ae; check_db2; check_iks ;;
        *)       check_cli; check_cos; check_ae; check_db2; check_iks ;;
    esac

    if $JSON_MODE; then
        print_json
    else
        print_summary
    fi
}

main "$@"
