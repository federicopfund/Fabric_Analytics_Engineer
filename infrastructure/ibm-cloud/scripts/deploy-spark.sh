#!/bin/bash
set -euo pipefail

# -------------------------------------------------------------------
# IBM Cloud - Deploy Spark on IKS
# Prerequisites: ibmcloud CLI, kubectl, plugins (ks, cos, is)
# -------------------------------------------------------------------

CLUSTER_NAME="${CLUSTER_NAME:-spark-cluster}"
REGION="${REGION:-us-south}"
RESOURCE_GROUP="${RESOURCE_GROUP:-Default}"
NAMESPACE="spark"

echo "=== IBM Cloud Spark Deployment ==="
echo "Cluster: $CLUSTER_NAME | Region: $REGION | RG: $RESOURCE_GROUP"
echo ""

# 1. Target account
echo "[1/6] Configuring IBM Cloud target..."
ibmcloud target -r "$REGION" -g "$RESOURCE_GROUP"

# 2. Get cluster config for kubectl
echo "[2/6] Fetching cluster kubeconfig..."
ibmcloud ks cluster config --cluster "$CLUSTER_NAME"

# 3. Verify connection
echo "[3/6] Verifying cluster connectivity..."
kubectl cluster-info
kubectl get nodes

# 4. Create namespace
echo "[4/6] Creating namespace '$NAMESPACE'..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# 5. Create COS secret for Spark S3A access
echo "[5/6] Creating COS credentials secret..."
if [ -z "${COS_ACCESS_KEY:-}" ] || [ -z "${COS_SECRET_KEY:-}" ]; then
    echo "  WARNING: COS_ACCESS_KEY and COS_SECRET_KEY not set."
    echo "  Generate HMAC keys: ibmcloud resource service-key-create cos-hmac-key Writer --instance-name CloudObjectStorage --parameters '{\"HMAC\": true}'"
    echo "  Then export COS_ACCESS_KEY and COS_SECRET_KEY"
else
    kubectl create secret generic cos-credentials \
        --namespace "$NAMESPACE" \
        --from-literal=access-key="$COS_ACCESS_KEY" \
        --from-literal=secret-key="$COS_SECRET_KEY" \
        --dry-run=client -o yaml | kubectl apply -f -
    echo "  COS credentials secret created."
fi

# 6. Deploy Spark
echo "[6/6] Deploying Spark master and workers..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$SCRIPT_DIR/../spark-k8s/kubernetes"

kubectl apply -f "$K8S_DIR/spark-master-deployment.yaml" -n "$NAMESPACE"
kubectl apply -f "$K8S_DIR/spark-master-service.yaml" -n "$NAMESPACE"

echo "  Waiting for Spark master to be ready..."
kubectl rollout status deployment/spark-master -n "$NAMESPACE" --timeout=120s

kubectl apply -f "$K8S_DIR/spark-worker-deployment.yaml" -n "$NAMESPACE"
kubectl rollout status deployment/spark-worker -n "$NAMESPACE" --timeout=120s

echo ""
echo "=== Deployment Complete ==="
kubectl get pods -n "$NAMESPACE"
kubectl get svc -n "$NAMESPACE"
echo ""
echo "Access Spark UI: kubectl port-forward svc/spark-master-service 8080:80 -n $NAMESPACE"
