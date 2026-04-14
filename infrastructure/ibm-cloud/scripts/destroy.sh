#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# destroy.sh — Clean Infrastructure Teardown
#
# Safely destroys IBM Cloud resources with confirmation gates.
# Removes IKS pods first, then Terraform-managed resources,
# then optionally cleans COS bucket data.
#
# Usage:
#   ./destroy.sh             # Interactive destroy
#   ./destroy.sh --data-only # Delete COS bucket contents only
#   ./destroy.sh --force     # Skip confirmation (CI/CD only)
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}/../terraform"
ENV_FILE="${SCRIPT_DIR}/../.env"

if [ -f "$ENV_FILE" ]; then
    set -a && source "$ENV_FILE" && set +a
fi

REGION="${IBMCLOUD_REGION:-us-south}"
FORCE=false

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Confirmation ──
confirm() {
    local msg="$1"
    if $FORCE; then return 0; fi
    echo ""
    echo -e "${RED}╔══════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  WARNING: DESTRUCTIVE OPERATION                  ║${NC}"
    echo -e "${RED}╚══════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  $msg"
    echo ""
    read -p "  Type 'DESTROY' to confirm: " input
    [[ "$input" == "DESTROY" ]] || { echo "Aborted."; exit 0; }
}

# ── Clean IKS workloads ──
clean_iks() {
    log_info "Cleaning Kubernetes workloads..."
    if kubectl get namespace spark &>/dev/null; then
        kubectl delete namespace spark --timeout=60s 2>/dev/null || true
        log_info "Namespace 'spark' deleted ✔"
    else
        log_info "No 'spark' namespace found"
    fi
}

# ── Clean COS data ──
clean_cos_data() {
    local buckets=("datalake-raw-${REGION}" "datalake-bronze-${REGION}" "datalake-silver-${REGION}" "datalake-gold-${REGION}")

    for bucket in "${buckets[@]}"; do
        log_info "Cleaning bucket: $bucket"
        local objects
        objects=$(ibmcloud cos list-objects --bucket "$bucket" --region "$REGION" --output json 2>/dev/null \
            | python3 -c "
import sys, json
contents = json.load(sys.stdin).get('Contents', [])
for obj in contents:
    print(obj['Key'])
" 2>/dev/null || echo "")

        if [[ -n "$objects" ]]; then
            while IFS= read -r key; do
                ibmcloud cos delete-object --bucket "$bucket" --key "$key" --region "$REGION" --force 2>/dev/null || true
            done <<< "$objects"
            log_info "  Cleaned $bucket ✔"
        else
            log_info "  $bucket already empty"
        fi
    done
}

# ── Clean Spark applications ──
clean_ae_apps() {
    local ae_id="${AE_INSTANCE_ID:-}"
    if [[ -z "$ae_id" ]]; then return; fi

    log_info "No running AE applications to clean (serverless auto-cleans)"
}

# ── Terraform destroy ──
terraform_destroy() {
    log_info "Running Terraform destroy..."
    cd "$TERRAFORM_DIR"

    if [ ! -f terraform.tfstate ] && [ ! -d .terraform ]; then
        log_warn "No Terraform state found. Nothing to destroy."
        return
    fi

    local env="${ENV:-dev}"
    local var_file="environments/${env}.tfvars"

    if [ -f "$var_file" ]; then
        terraform destroy -var-file="$var_file" -auto-approve
    else
        terraform destroy -auto-approve
    fi

    log_info "Terraform destroy complete ✔"
}

# ── Main ──
main() {
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  IBM Cloud — Infrastructure Teardown                        ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""

    case "${1:-}" in
        --data-only)
            confirm "This will DELETE all data in COS buckets for region $REGION"
            clean_cos_data
            ;;
        --force)
            FORCE=true
            clean_iks
            clean_cos_data
            clean_ae_apps
            terraform_destroy
            ;;
        *)
            confirm "This will DESTROY all IBM Cloud infrastructure and data"
            clean_iks
            clean_cos_data
            clean_ae_apps
            terraform_destroy
            ;;
    esac

    echo ""
    log_info "Teardown complete"
}

main "$@"
