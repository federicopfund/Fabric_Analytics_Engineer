#!/bin/bash
set -euo pipefail

# -------------------------------------------------------------------
# IBM Cloud - Configure CI/CD Toolchain with GitHub
# Links the repo and creates a Tekton pipeline
# -------------------------------------------------------------------

TOOLCHAIN_NAME="spark-data-pipeline"
REGION="us-south"
RESOURCE_GROUP="Default"
GITHUB_REPO="https://github.com/federicopfund/Fabric_Analytics_Engineer"

echo "=== IBM Cloud CI/CD Toolchain Setup ==="

# 1. Target
ibmcloud target -r "$REGION" -g "$RESOURCE_GROUP"

# 2. Install CD plugin if not present
if ! ibmcloud plugin show continuous-delivery &> /dev/null; then
    echo "Installing continuous-delivery plugin..."
    ibmcloud plugin install continuous-delivery -f
fi

# 3. Create Toolchain
echo "[1/3] Creating Toolchain '$TOOLCHAIN_NAME'..."
TOOLCHAIN_ID=$(ibmcloud dev toolchain-create "$TOOLCHAIN_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --region "$REGION" \
    --output json 2>/dev/null | grep -o '"toolchain_id": "[^"]*"' | cut -d'"' -f4) || true

if [ -z "$TOOLCHAIN_ID" ]; then
    echo "  Toolchain may already exist. Checking..."
    TOOLCHAIN_ID=$(ibmcloud dev toolchains --output json 2>/dev/null \
        | grep -B5 "$TOOLCHAIN_NAME" | grep -o '"toolchain_id": "[^"]*"' | cut -d'"' -f4) || true
fi

echo "  Toolchain ID: ${TOOLCHAIN_ID:-'Create manually at https://cloud.ibm.com/devops/toolchains'}"

# 4. Print manual steps (Toolchain GUI required for full integration)
echo ""
echo "=== Next Steps ==="
echo ""
echo "Complete the setup in the IBM Cloud console:"
echo "  https://cloud.ibm.com/devops/toolchains"
echo ""
echo "1. Open the toolchain '$TOOLCHAIN_NAME'"
echo "2. Add tool: GitHub → Link existing repo → $GITHUB_REPO"
echo "3. Add tool: Delivery Pipeline (Tekton)"
echo "4. Configure triggers:"
echo "   - Push to 'main' branch → Run tests + deploy"
echo "   - Pull Request → Run tests only"
echo ""
echo "The Tekton pipeline definition is at:"
echo "  infrastructure/ibm-cloud/tekton/"
echo ""
echo "=== Done ==="
