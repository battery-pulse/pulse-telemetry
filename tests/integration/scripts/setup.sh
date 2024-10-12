#!/bin/bash

# Step 1: Install operators (Hive and MinIO)
helm repo add stackable https://repo.stackable.tech/repository/helm-stable/
helm repo add minio https://operator.min.io/
helm repo update
helm install hive-operator stackable/hive-operator --wait --timeout=120s
helm install minio-operator minio/operator --wait --timeout=120s

# Step 2: Apply the MinIO and Hive Metastore Kinds and NodePort services
kubectl apply -f hive-metastore.yaml
kubectl apply -f minio.yaml

# Step 3: Wait for services to be ready
echo "Waiting for MinIO and Hive Metastore services to be ready..."
sleep 120
