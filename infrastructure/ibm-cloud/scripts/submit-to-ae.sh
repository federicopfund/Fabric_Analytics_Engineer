#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# submit-to-ae.sh — Submit Spark Application to IBM Analytics Engine
#
# Requires: ibmcloud CLI + analytics-engine-v3 plugin
#
# Usage:
#   ./submit-to-ae.sh                   # Build + upload + submit
#   ./submit-to-ae.sh --skip-build      # Submit pre-built JAR
#   ./submit-to-ae.sh --status <app_id> # Check application status
#   ./submit-to-ae.sh --logs <app_id>   # Get application logs
#   ./submit-to-ae.sh --list            # List all applications
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration ──
AE_INSTANCE_ID="${AE_INSTANCE_ID:-a688f3c4-efa9-4f12-842c-6a8d73a7ed2e}"
AE_REGION="${AE_REGION:-us-south}"

COS_ACCESS_KEY="${COS_ACCESS_KEY:-786065478ff34d3b84016125490a4d12}"
COS_SECRET_KEY="${COS_SECRET_KEY:-838da9c9a9cd2521d51e856da0dd876884f8108226f5bd7c}"
COS_ENDPOINT="${COS_ENDPOINT:-s3.us-south.cloud-object-storage.appdomain.cloud}"
COS_BUCKET_JARS="${COS_BUCKET_JARS:-datalake-raw-us-south-dev}"

DB2_HOSTNAME="${DB2_HOSTNAME:-6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud}"
DB2_PORT="${DB2_PORT:-30376}"
DB2_DATABASE="${DB2_DATABASE:-bludb}"
DB2_USERNAME="${DB2_USERNAME:-qtn87286}"
DB2_PASSWORD="${DB2_PASSWORD:-wiHDV4ror4E71lMw}"
DB2_SSL="${DB2_SSL:-true}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCALA_PROJECT_DIR="$(cd "${SCRIPT_DIR}/../../transformation/spark-jobs/pipelines/batch-etl-scala" 2>/dev/null && pwd || echo "${SCRIPT_DIR}/../../../transformation/spark-jobs/pipelines/batch-etl-scala")"
JAR_NAME="root-assembly-2.0.0.jar"
JAR_LOCAL="${SCALA_PROJECT_DIR}/target/scala-2.12/${JAR_NAME}"
JAR_COS_KEY="spark-jars/${JAR_NAME}"

# ── Colors ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Pre-flight checks ──
preflight() {
    if ! command -v ibmcloud &>/dev/null; then
        log_error "ibmcloud CLI no encontrado. Instalar: curl -fsSL https://clis.cloud.ibm.com/install/linux | sh"
        exit 1
    fi
    if ! ibmcloud plugin show analytics-engine-v3 &>/dev/null; then
        log_info "Instalando plugin analytics-engine-v3..."
        ibmcloud plugin install analytics-engine-v3 -f
    fi
    # Verify logged in
    if ! ibmcloud target &>/dev/null; then
        log_error "No logueado en IBM Cloud. Ejecutar: ibmcloud login --sso"
        exit 1
    fi
}

# ── Check AE Instance Status ──
check_ae_status() {
    log_info "Verificando estado de Analytics Engine..."
    local state
    state=$(ibmcloud ae-v3 instance show --id "${AE_INSTANCE_ID}" --output json 2>/dev/null \
        | grep -v '^Performing' \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null || echo "unknown")

    if [[ "$state" == "active" ]]; then
        log_info "Analytics Engine: ${GREEN}ACTIVE${NC} (Spark 3.5) ✔"
        return 0
    else
        log_error "Analytics Engine estado: $state (se requiere 'active')"
        return 1
    fi
}

# ── Build Fat JAR ──
build_jar() {
    log_info "Compilando fat JAR con sbt assembly..."
    cd "$SCALA_PROJECT_DIR"
    sbt clean assembly
    if [[ ! -f "$JAR_LOCAL" ]]; then
        log_error "JAR no encontrado: $JAR_LOCAL"
        exit 1
    fi
    local size
    size=$(du -h "$JAR_LOCAL" | awk '{print $1}')
    log_info "JAR compilado: ${JAR_LOCAL} ($size) ✔"
}

# ── Upload JAR to COS ──
upload_jar_to_cos() {
    log_info "Subiendo JAR a IBM COS: ${COS_BUCKET_JARS}/${JAR_COS_KEY}..."
    ibmcloud cos put-object \
        --bucket "${COS_BUCKET_JARS}" \
        --key "${JAR_COS_KEY}" \
        --body "${JAR_LOCAL}" \
        --region "${AE_REGION}" 2>/dev/null
    log_info "JAR subido a COS ✔"
}

# ── Submit Spark Application via CLI ──
submit_application() {
    log_info "Enviando aplicación Spark a Analytics Engine..."

    local conf_json env_json runtime_json

    conf_json=$(cat <<'EOF'
{
    "spark.app.name": "BatchETL-Medallion-v6",
    "spark.hadoop.fs.s3a.access.key": "COS_AK_PLACEHOLDER",
    "spark.hadoop.fs.s3a.secret.key": "COS_SK_PLACEHOLDER",
    "spark.hadoop.fs.s3a.endpoint": "COS_EP_PLACEHOLDER",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.connection.establish.timeout": "30000",
    "spark.hadoop.fs.s3a.connection.timeout": "60000",
    "spark.hadoop.fs.s3a.attempts.maximum": "3",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "io.delta.storage.S3SingleDriverLogStore",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.databricks.delta.snapshotPartitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.shuffle.partitions": "8",
    "spark.dynamicAllocation.enabled": "false",
    "spark.dynamicAllocation.executorIdleTimeout": "30s",
    "spark.dynamicAllocation.shutdownTimeout": "10s",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://datalake-raw-us-south-dev/spark-events"
}
EOF
)
    # Inject actual values (avoids issues with special chars in heredoc)
    conf_json="${conf_json//COS_AK_PLACEHOLDER/$COS_ACCESS_KEY}"
    conf_json="${conf_json//COS_SK_PLACEHOLDER/$COS_SECRET_KEY}"
    conf_json="${conf_json//COS_EP_PLACEHOLDER/https://$COS_ENDPOINT}"

    env_json=$(cat <<EOF
{
    "EXECUTION_MODE": "IBM_AE",
    "PIPELINE_CLEAR_CHECKPOINTS": "true",
    "PIPELINE_DAG_TIMEOUT_MIN": "10",
    "PIPELINE_CHARTS_ENABLED": "false",
    "DB2_EXPORT_PARALLELISM": "7",
    "AE_INSTANCE_ID": "${AE_INSTANCE_ID}",
    "COS_ACCESS_KEY": "${COS_ACCESS_KEY}",
    "COS_SECRET_KEY": "${COS_SECRET_KEY}",
    "COS_ENDPOINT": "${COS_ENDPOINT}",
    "DB2_HOSTNAME": "${DB2_HOSTNAME}",
    "DB2_PORT": "${DB2_PORT}",
    "DB2_DATABASE": "${DB2_DATABASE}",
    "DB2_USERNAME": "${DB2_USERNAME}",
    "DB2_PASSWORD": "${DB2_PASSWORD}",
    "DB2_SSL": "${DB2_SSL}"
}
EOF
)

    runtime_json='{"spark_version": "3.5"}'

    local app_name="Medallion-v6-$(date +%H%M%S)"
    log_info "App name: ${app_name}"

    local response
    response=$(ibmcloud ae-v3 spark-app submit \
        --instance-id "${AE_INSTANCE_ID}" \
        --app "s3a://${COS_BUCKET_JARS}/${JAR_COS_KEY}" \
        --class "medallion.Pipeline" \
        --name "$app_name" \
        --conf "$conf_json" \
        --env "$env_json" \
        --runtime "$runtime_json" \
        --output json 2>&1) || true

    local app_id
    app_id=$(echo "$response" | grep -v '^Performing' | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")

    if [[ -n "$app_id" && "$app_id" != "" ]]; then
        log_info "Aplicación enviada ✔"
        log_info "Application ID: ${CYAN}${app_id}${NC}"
        echo ""
        echo "═══════════════════════════════════════════════════════"
        echo "  Monitorear: $0 --status ${app_id}"
        echo "  Logs:       $0 --logs ${app_id}"
        echo "  Lista:      $0 --list"
        echo "  History UI: https://spark-console.${AE_REGION}.ae.cloud.ibm.com/v3/analytics_engines/${AE_INSTANCE_ID}/spark_history_ui"
        echo "═══════════════════════════════════════════════════════"
    else
        log_warn "Respuesta del submit:"
        echo "$response"
    fi
}

# ── Check Application Status ──
check_app_status() {
    local app_id="$1"
    ibmcloud ae-v3 spark-app show \
        --instance-id "${AE_INSTANCE_ID}" \
        --app-id "$app_id" \
        --output json 2>/dev/null \
    | grep -v '^Performing' \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"Application:  {d.get('id', 'N/A')}\")
print(f\"State:        {d.get('state', 'N/A')}\")
rt = d.get('runtime', {})
if rt:
    print(f'Spark:        {rt.get(\"spark_version\",\"\")}')
start = d.get('start_time', '')
end = d.get('end_time', '')
if start: print(f'Start time:   {start}')
if end:   print(f'End time:     {end}')
" 2>/dev/null || ibmcloud ae-v3 spark-app show --instance-id "${AE_INSTANCE_ID}" --app-id "$app_id"
}

# ── Get Application Logs ──
get_app_logs() {
    local app_id="$1"
    ibmcloud ae-v3 spark-app show \
        --instance-id "${AE_INSTANCE_ID}" \
        --app-id "$app_id" 2>&1 | cat
}

# ── List all applications ──
list_apps() {
    ibmcloud ae-v3 spark-app list \
        --instance-id "${AE_INSTANCE_ID}" \
        --output json 2>/dev/null \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
apps = d.get('applications', [])
if not apps:
    print('No hay aplicaciones registradas.')
else:
    print(f'{'ID':<40} {'State':<12} {'Submitted'}')
    print('─' * 80)
    for a in apps:
        print(f\"{a.get('id',''):<40} {a.get('state',''):<12} {a.get('submission_time','')}\")
" 2>/dev/null || ibmcloud ae-v3 spark-app list --instance-id "${AE_INSTANCE_ID}"
}

# ── Upload CSV data files to COS raw bucket ──
upload_csv_data() {
    local csv_dir="${SCALA_PROJECT_DIR}/datalake/raw"
    if [[ ! -d "$csv_dir" ]]; then
        csv_dir="${SCALA_PROJECT_DIR}/src/main/resources/csv"
    fi
    if [[ ! -d "$csv_dir" ]]; then
        log_warn "No se encontró directorio con CSVs. Saltando upload de datos."
        return 0
    fi

    local csv_count
    csv_count=$(find "$csv_dir" -name "*.csv" -type f 2>/dev/null | wc -l)
    if [[ "$csv_count" -eq 0 ]]; then
        log_warn "No hay archivos CSV en $csv_dir"
        return 0
    fi

    log_info "Subiendo $csv_count archivos CSV a COS bucket ${COS_BUCKET_JARS}..."
    for csv_file in "$csv_dir"/*.csv; do
        local filename
        filename=$(basename "$csv_file")
        ibmcloud cos put-object \
            --bucket "${COS_BUCKET_JARS}" \
            --key "${filename}" \
            --body "${csv_file}" \
            --region "${AE_REGION}" 2>/dev/null
        log_info "  ✔ ${filename}"
    done
    log_info "Datos CSV subidos a COS ✔"
}

# ── Main ──
main() {
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  IBM Analytics Engine — Spark Application Submitter         ║"
    echo "║  Pipeline Medallion v6.0 (Serverless Spark 3.5)             ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""

    preflight

    case "${1:-}" in
        --status)
            [[ -z "${2:-}" ]] && { log_error "Uso: $0 --status <app_id>"; exit 1; }
            check_app_status "$2"
            ;;
        --logs)
            [[ -z "${2:-}" ]] && { log_error "Uso: $0 --logs <app_id>"; exit 1; }
            get_app_logs "$2"
            ;;
        --list)
            list_apps
            ;;
        *)
            check_ae_status

            if [[ "${1:-}" != "--skip-build" ]]; then
                build_jar
            else
                log_info "Saltando compilación (--skip-build)"
                if [[ ! -f "$JAR_LOCAL" ]]; then
                    log_error "JAR no encontrado. Ejecute sin --skip-build primero."
                    exit 1
                fi
            fi

            # --skip-csv-upload conserva los CSVs ya en COS (útil después del simulator)
            local skip_csv=false
            for arg in "$@"; do
                [[ "$arg" == "--skip-csv-upload" ]] && skip_csv=true
            done
            if [[ "$skip_csv" == "true" ]]; then
                log_info "Saltando upload de CSVs (--skip-csv-upload)"
            else
                upload_csv_data
            fi
            upload_jar_to_cos
            submit_application
            ;;
    esac
}

main "$@"
