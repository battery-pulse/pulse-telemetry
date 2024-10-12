#!/bin/bash

# Step 1: Install Stackable operators (Hive and MinIO)
helm repo add stackable https://repo.stackable.tech/repository/helm-stable/
helm repo update
helm install hive-operator stackable/hive-operator --namespace stackable --create-namespace --wait
helm install minio-operator stackable/minio-operator --namespace stackable --wait

# Step 2: Apply the MinIO and Hive Metastore Kinds and NodePort services
kubectl apply -f hive-metastore.yaml
kubectl apply -f minio.yaml

# Step 3: Wait for services to be ready
echo "Waiting for MinIO and Hive Metastore services to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/minio
kubectl wait --for=condition=available --timeout=120s deployment/hive-metastore
