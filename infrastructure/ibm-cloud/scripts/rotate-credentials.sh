#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# rotate-credentials.sh — Automated Credential Rotation
#
# Rotates HMAC credentials for COS and regenerates service keys
# for Db2 and Analytics Engine. Updates .env file automatically.
#
# Usage:
#   ./rotate-credentials.sh              # Rotate all
#   ./rotate-credentials.sh --cos-only   # COS HMAC only
#   ./rotate-credentials.sh --db2-only   # Db2 key only
#   ./rotate-credentials.sh --ae-only    # AE key only
#   ./rotate-credentials.sh --dry-run    # Show what would happen
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"
BACKUP_DIR="${SCRIPT_DIR}/../.credentials-backup"
DRY_RUN=false

# ── Colors ──
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Backup current credentials ──
backup_env() {
    if [ -f "$ENV_FILE" ]; then
        mkdir -p "$BACKUP_DIR"
        local backup="${BACKUP_DIR}/env-$(date +%Y%m%d_%H%M%S).bak"
        cp "$ENV_FILE" "$backup"
        chmod 600 "$backup"
        log_info "Backed up .env to $backup"
    fi
}

# ── Update .env variable ──
update_env_var() {
    local key="$1" value="$2"
    if [ -f "$ENV_FILE" ]; then
        if grep -q "^${key}=" "$ENV_FILE"; then
            sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        else
            echo "${key}=${value}" >> "$ENV_FILE"
        fi
    fi
}

# ── Rotate COS HMAC credentials ──
rotate_cos() {
    log_info "Rotating COS HMAC credentials..."

    if $DRY_RUN; then
        log_warn "[DRY-RUN] Would delete old key and create new HMAC key"
        return
    fi

    local cos_instance="${COS_INSTANCE_NAME:-CloudObjectStorage}"

    # List existing HMAC keys (find the one matching our naming pattern)
    local old_key_id
    old_key_id=$(ibmcloud resource service-keys --instance-name "$cos_instance" --output json 2>/dev/null \
        | python3 -c "
import sys, json
keys = json.load(sys.stdin)
for k in keys:
    if 'hmac' in k.get('name','').lower():
        print(k['id'])
        break
" 2>/dev/null || echo "")

    # Create new key with timestamp
    local new_key_name="cos-hmac-$(date +%Y%m%d)"
    log_info "Creating new HMAC key: $new_key_name"

    local creds
    creds=$(ibmcloud resource service-key-create "$new_key_name" Writer \
        --instance-name "$cos_instance" \
        --parameters '{"HMAC": true}' \
        --output json 2>/dev/null | grep -v '^Creating\|^OK')

    local new_access new_secret
    new_access=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['cos_hmac_keys']['access_key_id'])" 2>/dev/null)
    new_secret=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['cos_hmac_keys']['secret_access_key'])" 2>/dev/null)

    if [[ -n "$new_access" && -n "$new_secret" ]]; then
        update_env_var "COS_ACCESS_KEY" "$new_access"
        update_env_var "COS_SECRET_KEY" "$new_secret"
        update_env_var "AWS_ACCESS_KEY_ID" "$new_access"
        update_env_var "AWS_SECRET_ACCESS_KEY" "$new_secret"
        log_info "COS credentials updated in .env ✔"

        # Delete old key
        if [[ -n "$old_key_id" ]]; then
            ibmcloud resource service-key-delete "$old_key_id" -f 2>/dev/null || true
            log_info "Old COS key deleted ✔"
        fi
    else
        log_error "Failed to create new COS HMAC key"
        return 1
    fi
}

# ── Rotate Db2 credentials ──
rotate_db2() {
    log_info "Rotating Db2 service key..."

    if $DRY_RUN; then
        log_warn "[DRY-RUN] Would regenerate Db2 service key"
        return
    fi

    local db2_instance="${DB2_INSTANCE_NAME:-retail-db2}"
    local new_key_name="db2-key-$(date +%Y%m%d)"

    local creds
    creds=$(ibmcloud resource service-key-create "$new_key_name" Manager \
        --instance-name "$db2_instance" \
        --output json 2>/dev/null | grep -v '^Creating\|^OK')

    local new_host new_port new_user new_pass
    new_host=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['connection']['db2']['hosts'][0]['hostname'])" 2>/dev/null || echo "")
    new_port=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['connection']['db2']['hosts'][0]['port'])" 2>/dev/null || echo "")
    new_user=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['connection']['db2']['authentication']['username'])" 2>/dev/null || echo "")
    new_pass=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['connection']['db2']['authentication']['password'])" 2>/dev/null || echo "")

    if [[ -n "$new_host" ]]; then
        update_env_var "DB2_HOSTNAME" "$new_host"
        update_env_var "DB2_PORT" "$new_port"
        update_env_var "DB2_USERNAME" "$new_user"
        update_env_var "DB2_PASSWORD" "$new_pass"
        log_info "Db2 credentials updated in .env ✔"
    else
        log_error "Failed to create new Db2 key"
        return 1
    fi
}

# ── Rotate AE credentials ──
rotate_ae() {
    log_info "Rotating Analytics Engine API key..."

    if $DRY_RUN; then
        log_warn "[DRY-RUN] Would regenerate AE service key"
        return
    fi

    local ae_instance_id="${AE_INSTANCE_ID:-}"
    if [[ -z "$ae_instance_id" ]]; then
        log_warn "AE_INSTANCE_ID not set, skipping"
        return
    fi

    local new_key_name="ae-key-$(date +%Y%m%d)"

    local creds
    creds=$(ibmcloud resource service-key-create "$new_key_name" Manager \
        --instance-id "$ae_instance_id" \
        --output json 2>/dev/null | grep -v '^Creating\|^OK')

    local new_apikey
    new_apikey=$(echo "$creds" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['credentials']['apikey'])" 2>/dev/null || echo "")

    if [[ -n "$new_apikey" ]]; then
        update_env_var "AE_API_KEY" "$new_apikey"
        log_info "AE credentials updated in .env ✔"
    else
        log_error "Failed to create new AE key"
        return 1
    fi
}

# ── Main ──
main() {
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║  IBM Cloud — Credential Rotation                            ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""

    # Load existing env
    if [ -f "$ENV_FILE" ]; then
        set -a && source "$ENV_FILE" && set +a
    fi

    case "${1:-}" in
        --cos-only)
            backup_env
            rotate_cos
            ;;
        --db2-only)
            backup_env
            rotate_db2
            ;;
        --ae-only)
            backup_env
            rotate_ae
            ;;
        --dry-run)
            DRY_RUN=true
            rotate_cos
            rotate_db2
            rotate_ae
            ;;
        *)
            backup_env
            rotate_cos
            rotate_db2
            rotate_ae
            ;;
    esac

    echo ""
    log_info "Credential rotation complete"
    log_warn "Remember to update any running applications with new credentials"
}

main "$@"
