#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════
# cos-lifecycle.sh — Configure COS Bucket Lifecycle Policies
#
# Sets archival, expiration, and abort rules for COS buckets
# using the IBM Cloud COS API.
#
# Usage:
#   ./cos-lifecycle.sh                # Apply all policies
#   ./cos-lifecycle.sh --dry-run      # Show what would be applied
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${SCRIPT_DIR}/../.env" ]; then
    set -a && source "${SCRIPT_DIR}/../.env" && set +a
fi

REGION="${IBMCLOUD_REGION:-us-south}"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  COS Lifecycle Policy Configuration                         ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── RAW: Archive after 90d, expire after 365d ──
apply_policy() {
    local bucket="$1" policy="$2" desc="$3"
    log_info "Bucket: $bucket — $desc"
    if $DRY_RUN; then
        log_warn "  [DRY-RUN] Would apply: $policy"
    else
        ibmcloud cos put-bucket-lifecycle-configuration \
            --bucket "$bucket" \
            --lifecycle-configuration "$policy" \
            --region "$REGION" 2>/dev/null || log_warn "  Failed (bucket may not support lifecycle)"
    fi
}

apply_policy "datalake-raw-${REGION}" '{
  "Rules": [
    {"ID": "archive-90d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}]},
    {"ID": "expire-365d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "Expiration": {"Days": 365}},
    {"ID": "abort-multipart-7d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}
  ]
}' "Archive 90d → Expire 365d"

apply_policy "datalake-bronze-${REGION}" '{
  "Rules": [
    {"ID": "abort-multipart-7d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}
  ]
}' "Abort stale multipart (7d)"

apply_policy "datalake-silver-${REGION}" '{
  "Rules": [
    {"ID": "abort-multipart-7d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}
  ]
}' "Abort stale multipart (7d)"

apply_policy "datalake-gold-${REGION}" '{
  "Rules": [
    {"ID": "abort-multipart-7d", "Status": "Enabled", "Filter": {"Prefix": ""},
     "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}}
  ]
}' "Abort stale multipart (7d)"

echo ""
log_info "Lifecycle policies configured ✔"
