#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# submit-simulator.sh — Build, upload y submit del Sales Simulator
#                        a IBM Analytics Engine Serverless
#
# Uso:
#   ./submit-simulator.sh                              # Build + upload + submit (1000 órdenes)
#   ./submit-simulator.sh --skip-build                 # Submit pre-built JAR
#   ./submit-simulator.sh --orders 5000                # Generar 5000 órdenes
#   ./submit-simulator.sh --orders 2000 --start 2026-01-01 --end 2026-03-31
#   ./submit-simulator.sh --status <app_id>            # Ver estado
#   ./submit-simulator.sh --logs <app_id>              # Ver logs
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration (hereda del .env o usa defaults del pipeline) ──
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../../../infrastructure/ibm-cloud/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a; source "$ENV_FILE"; set +a
fi

AE_INSTANCE_ID="${AE_INSTANCE_ID:-a688f3c4-efa9-4f12-842c-6a8d73a7ed2e}"
AE_REGION="${AE_REGION:-us-south}"

COS_ACCESS_KEY="${COS_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-}}"
COS_SECRET_KEY="${COS_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-}}"
COS_ENDPOINT="${COS_ENDPOINT:-s3.us-south.cloud-object-storage.appdomain.cloud}"
COS_BUCKET_RAW="${COS_BUCKET_RAW:-datalake-raw-us-south-dev}"

SCALA_PROJECT_DIR="${SCRIPT_DIR}"
JAR_NAME="sales-simulator-assembly-1.0.0.jar"
JAR_LOCAL="${SCALA_PROJECT_DIR}/target/scala-2.12/${JAR_NAME}"
JAR_COS_KEY="spark-jars/${JAR_NAME}"

# Default simulation params
SIM_ORDERS="${SIM_ORDERS:-1000}"
SIM_START="${SIM_START:-}"
SIM_END="${SIM_END:-}"
SIM_SEED="${SIM_SEED:-}"

# ── Colors ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Parse args ──
SKIP_BUILD=false
ACTION="submit"
APP_ID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        --orders)     SIM_ORDERS="$2"; shift 2 ;;
        --start)      SIM_START="$2"; shift 2 ;;
        --end)        SIM_END="$2"; shift 2 ;;
        --seed)       SIM_SEED="$2"; shift 2 ;;
        --status)     ACTION="status"; APP_ID="$2"; shift 2 ;;
        --logs)       ACTION="logs"; APP_ID="$2"; shift 2 ;;
        --list)       ACTION="list"; shift ;;
        *) log_error "Argumento desconocido: $1"; exit 1 ;;
    esac
done

# ── Preflight ──
preflight() {
    if ! command -v ibmcloud &>/dev/null; then
        log_error "ibmcloud CLI no encontrado"
        exit 1
    fi
    if ! ibmcloud plugin show analytics-engine-v3 &>/dev/null 2>&1; then
        log_info "Instalando plugin analytics-engine-v3..."
        ibmcloud plugin install analytics-engine-v3 -f
    fi
}

# ── Build FAT JAR ──
build_jar() {
    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Saltando build (--skip-build)"
        if [[ ! -f "$JAR_LOCAL" ]]; then
            log_error "JAR no encontrado: $JAR_LOCAL"
            exit 1
        fi
        return 0
    fi

    log_info "Compilando sales-simulator..."
    cd "$SCALA_PROJECT_DIR"

    sbt clean assembly 2>&1 | tail -20

    if [[ ! -f "$JAR_LOCAL" ]]; then
        log_error "Build falló — JAR no generado"
        exit 1
    fi

    local size
    size=$(du -h "$JAR_LOCAL" | awk '{print $1}')
    log_info "JAR generado: ${JAR_LOCAL} (${size})"
}

# ── Upload JAR a COS ──
upload_to_cos() {
    log_info "Subiendo JAR a COS: s3a://${COS_BUCKET_RAW}/${JAR_COS_KEY}"

    local date_value content_type resource signature
    date_value=$(date -u +"%a, %d %b %Y %H:%M:%S GMT")
    content_type="application/java-archive"
    resource="/${COS_BUCKET_RAW}/${JAR_COS_KEY}"
    signature=$(echo -en "PUT\n\n${content_type}\n${date_value}\n${resource}" \
        | openssl dgst -sha1 -hmac "${COS_SECRET_KEY}" -binary | base64)

    local http_code
    http_code=$(curl -sS -w "%{http_code}" -o /dev/null \
        -X PUT \
        -H "Date: ${date_value}" \
        -H "Content-Type: ${content_type}" \
        -H "Authorization: AWS ${COS_ACCESS_KEY}:${signature}" \
        --data-binary "@${JAR_LOCAL}" \
        "https://${COS_ENDPOINT}/${COS_BUCKET_RAW}/${JAR_COS_KEY}")

    if [[ "$http_code" == "200" ]]; then
        log_info "Upload exitoso (HTTP ${http_code})"
    else
        log_error "Upload falló (HTTP ${http_code})"
        exit 1
    fi
}

# ── Submit a Analytics Engine ──
submit_application() {
    log_info "Enviando Sales Simulator a Analytics Engine..."

    # Build spark-submit args
    local sim_args="--orders ${SIM_ORDERS}"
    [[ -n "$SIM_START" ]] && sim_args+=" --start ${SIM_START}"
    [[ -n "$SIM_END" ]]   && sim_args+=" --end ${SIM_END}"
    [[ -n "$SIM_SEED" ]]  && sim_args+=" --seed ${SIM_SEED}"

    local conf_json
    conf_json=$(cat <<EOF
{
    "spark.app.name": "SalesSimulator-Medallion",
    "spark.hadoop.fs.s3a.access.key": "${COS_ACCESS_KEY}",
    "spark.hadoop.fs.s3a.secret.key": "${COS_SECRET_KEY}",
    "spark.hadoop.fs.s3a.endpoint": "https://${COS_ENDPOINT}",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
EOF
)

    local env_json
    env_json=$(cat <<EOF
{
    "COS_ACCESS_KEY": "${COS_ACCESS_KEY}",
    "COS_SECRET_KEY": "${COS_SECRET_KEY}",
    "COS_ENDPOINT": "${COS_ENDPOINT}",
    "COS_BUCKET_RAW": "${COS_BUCKET_RAW}",
    "EXECUTION_MODE": "IBM_AE",
    "SIM_ORDERS": "${SIM_ORDERS}",
    "SIM_START": "${SIM_START}",
    "SIM_END": "${SIM_END}",
    "SIM_SEED": "${SIM_SEED:-}"
}
EOF
)

    local app_name="SalesSimulator-$(date +%Y%m%d-%H%M%S)"
    log_info "App name: ${app_name}"
    log_info "Params: ${sim_args}"

    local response
    response=$(ibmcloud ae-v3 spark-app submit \
        --instance-id "${AE_INSTANCE_ID}" \
        --app "s3a://${COS_BUCKET_RAW}/${JAR_COS_KEY}" \
        --class "simulator.SimulatorApp" \
        --name "${app_name}" \
        --conf "${conf_json}" \
        --env "${env_json}" \
        --runtime '{"spark_version": "3.5"}' \
        --output json 2>&1 | grep -v '^Performing')

    local app_id
    app_id=$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")

    if [[ -n "$app_id" ]]; then
        log_info "Aplicación enviada: ${CYAN}${app_id}${NC}"
        echo ""
        echo "  Monitorear:"
        echo "    $0 --status ${app_id}"
        echo "    $0 --logs ${app_id}"
    else
        log_error "Error al enviar aplicación:"
        echo "$response"
        exit 1
    fi
}

# ── Status / Logs / List ──
check_status() {
    ibmcloud ae-v3 spark-app show --instance-id "${AE_INSTANCE_ID}" --app-id "$1" --output json 2>&1 \
        | grep -v '^Performing' | python3 -m json.tool
}

get_logs() {
    ibmcloud ae-v3 spark-app status --instance-id "${AE_INSTANCE_ID}" --app-id "$1" --output json 2>&1 \
        | grep -v '^Performing' | python3 -m json.tool
}

list_apps() {
    ibmcloud ae-v3 spark-app list --instance-id "${AE_INSTANCE_ID}" --output json 2>&1 \
        | grep -v '^Performing' | python3 -c "
import sys, json
data = json.load(sys.stdin)
apps = data.get('applications', [])
print(f'Total aplicaciones: {len(apps)}')
print(f'{\"ID\":<40} {\"Estado\":<12} {\"Nombre\":<35} {\"Creado\"}')
print('─' * 110)
for a in sorted(apps, key=lambda x: x.get('start_time',''), reverse=True)[:20]:
    print(f'{a.get(\"id\",\"?\"):<40} {a.get(\"state\",\"?\"):<12} {a.get(\"spark_application\",{}).get(\"conf\",{}).get(\"spark.app.name\",\"?\"):<35} {a.get(\"start_time\",\"?\")}')
" 2>/dev/null
}

# ── Main ──
main() {
    preflight

    case "$ACTION" in
        status) check_status "$APP_ID" ;;
        logs)   get_logs "$APP_ID" ;;
        list)   list_apps ;;
        submit)
            build_jar
            upload_to_cos
            submit_application
            ;;
    esac
}

main
