#!/bin/bash

# Nombres de los archivos
certificado="tu_certificado.crt"
clave_privada="tu_clave_privada.key"
port="8081"
# Verifica la existencia del certificado en el directorio actual
if [ -f "./$certificado" ]; then
    echo "El certificado $certificado existe en el directorio actual."
else
    echo "Error: El certificado $certificado no se encontró en el directorio actual. Creando certificado..."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout "$clave_privada" -out "$certificado"
    echo "Certificado creado exitosamente en $certificado."
fi





# Deploy Spark Master
kubectl create -f ./kubernetes/spark-master-deployment.yaml
kubectl create -f ./kubernetes/spark-master-service.yaml

# Sleep for 10 seconds (adjust as needed)
sleep 10

# Deploy Spark Worker
kubectl create -f ./kubernetes/spark-worker-deployment.yaml

# Create TLS secret (uncomment and adjust paths if needed)
# kubectl create secret tls fede-tls-secret --cert=./my-certificate.crt --key=./my-private.key

# Deploy Ingress
kubectl apply -f ./kubernetes/minikube-ingress.yaml

# Deploy Kubernetes Dashboard
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.3.1/aio/deploy/recommended.yaml

# Wait for Dashboard to be ready (adjust sleep time as needed)
sleep 20

# Create ServiceAccount and ClusterRoleBinding for Dashboard access
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard

apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: admin-user
  namespace: kubernetes-dashboard

apiVersion: v1
kind: Service
metadata:
  name: kubernetes-dashboard
  namespace: kubernetes-dashboard
spec:
  type: NodePort
  ports:
  - port: 443
    targetPort: 8443
  selector:
    k8s-app: kubernetes-dashboard

apiVersion: apps/v1
kind: Deployment
metadata:
  name: kubernetes-dashboard
  namespace: kubernetes-dashboard
spec:
  selector:
    matchLabels:
      k8s-app: kubernetes-dashboard
  replicas: 1
  template:
    metadata:
      labels:
        k8s-app: kubernetes-dashboard
    spec:
      serviceAccountName: admin-user
      containers:
      - name: kubernetes-dashboard
        image: kubernetesui/dashboard:v2.3.1
        ports:
        - containerPort: 8443

EOF

kubectl create secret tls spark-tls-secret --cert="${PWD}/tu_certificado.crt" --key="${PWD}/tu_clave_privada.key"
# Create token for admin-user
kubectl -n kubernetes-dashboard create serviceaccount admin-user
kubectl -n kubernetes-dashboard create clusterrolebinding admin-user --clusterrole=cluster-admin --serviceaccount=kubernetes-dashboard:admin-user
kubectl -n kubernetes-dashboard create token admin-user


kubectl proxy

xdg-open "http://localhost:8081/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"