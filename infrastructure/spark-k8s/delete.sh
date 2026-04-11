#!/bin/bash

kubectl delete -f ./kubernetes/spark-master-deployment.yaml
kubectl delete -f ./kubernetes/spark-master-service.yaml
kubectl delete -f ./kubernetes/spark-worker-deployment.yaml
kubectl delete -f ./kubernetes/minikube-ingress.yaml
kubectl delete -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.3.1/aio/deploy/recommended.yaml
kubectl delete serviceaccount -n kubernetes-dashboard admin-user
kubectl delete deployment spark-master
