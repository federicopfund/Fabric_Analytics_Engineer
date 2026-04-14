#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════
# IBM Cloud — Initial Setup & Validation
# Installs CLI, plugins, verifies prerequisites, initializes env
# ═══════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  IBM Cloud — Initial Setup                                  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Install IBM Cloud CLI ──
log_info "Step 1/5: IBM Cloud CLI"
if command -v ibmcloud &> /dev/null; then
    version=$(ibmcloud version 2>/dev/null | head -1)
    log_info "  Already installed: $version"
else
    log_info "  Installing IBM Cloud CLI..."
    curl -fsSL https://clis.cloud.ibm.com/install/linux | sh
fi

# ── 2. Required plugins ──
log_info "Step 2/5: CLI Plugins"
PLUGINS=(
    "container-service"       # IKS cluster management
    "container-registry"      # Container image registry
    "cloud-object-storage"    # COS bucket operations
    "vpc-infrastructure"      # VPC networking
    "analytics-engine-v3"     # Analytics Engine Serverless
    "continuous-delivery"     # CI/CD Toolchains
    "key-protect"             # Key management
    "schematics"              # Terraform automation
)

for plugin in "${PLUGINS[@]}"; do
    if ibmcloud plugin show "$plugin" &> /dev/null; then
        echo -e "  ${GREEN}✓${NC} $plugin"
    else
        echo -e "  ${CYAN}→${NC} Installing $plugin..."
        ibmcloud plugin install "$plugin" -f -q
    fi
done

# ── 3. Verify login ──
log_info "Step 3/5: Authentication"
if ibmcloud target &>/dev/null; then
    account=$(ibmcloud target 2>/dev/null | grep "Account:" | sed 's/Account: *//')
    region=$(ibmcloud target 2>/dev/null | grep "Region:" | sed 's/Region: *//')
    log_info "  Logged in: $account (Region: $region)"
else
    log_warn "  Not logged in. Run: ibmcloud login --sso"
fi

# ── 4. Verify external tools ──
log_info "Step 4/5: External Tools"
TOOLS=("terraform" "kubectl" "sbt" "python3" "jq")
for tool in "${TOOLS[@]}"; do
    if command -v "$tool" &>/dev/null; then
        ver=$("$tool" --version 2>/dev/null | head -1 || echo "installed")
        echo -e "  ${GREEN}✓${NC} $tool"
    else
        echo -e "  ${YELLOW}⚠${NC} $tool (not found — optional)"
    fi
done

# ── 5. Initialize .env ──
log_info "Step 5/5: Environment File"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [ -f "$ENV_FILE" ]; then
    log_info "  .env already exists"
else
    if [ -f "${SCRIPT_DIR}/../.env.example" ]; then
        cp "${SCRIPT_DIR}/../.env.example" "$ENV_FILE"
        chmod 600 "$ENV_FILE"
        log_info "  Created .env from .env.example (chmod 600)"
        log_warn "  Edit .env and fill in your credentials!"
    fi
fi

# ── Summary ──
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo -e "${GREEN}Setup complete ✔${NC}"
echo ""
echo "Next steps:"
echo "  1.  Edit .env with your credentials"
echo "  2.  Login:              ibmcloud login --sso"
echo "  3.  Target:             ibmcloud target -r us-south -g Default"
echo "  4.  Init Terraform:     make infra-init"
echo "  5.  Plan:               make infra-plan ENV=dev"
echo "  6.  Apply:              make infra-apply ENV=dev"
echo "  7.  Health check:       make health"
echo "═══════════════════════════════════════════════════════════════"
