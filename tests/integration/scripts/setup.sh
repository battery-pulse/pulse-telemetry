#!/bin/bash

# Step 1: Install stackable operators
helm repo add stackable https://repo.stackable.tech/repository/helm-stable/
helm repo add minio https://operator.min.io/
helm repo update
helm install secret-operator stackable/secret-operator
helm install commons-operator stackable/commons-operator
helm install hive-operator stackable/hive-operator

# Step 2: Install minio using helm
helm install minio \
--version 4.0.2 \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set buckets[0].name=hive,buckets[0].policy=none \
--set buckets[1].name=my-warehouse,buckets[1].policy=public \
--set rootUser=admin \
--set rootPassword=adminadmin \
--set users[0].accessKey=hive,users[0].secretKey=hivehive,users[0].policy=readwrite \
--set resources.requests.memory=1Gi \
--set service.type=NodePort,service.nodePort=null \
--set consoleService.type=NodePort,consoleService.nodePort=null \
--repo https://charts.min.io/ minio

# Step 3: Install hive resources
kubectl apply -f hive-metastore.yaml

# Step 4: Wait for services to be ready
echo "Waiting for MinIO and Hive Metastore services to be ready..."
sleep 60
kubectl wait --for=condition=ready pod/hive-metastore-metastore-default-0 --timeout=300s
# kubectl wait --for=condition=ready pod/minio-tenant-pool-0-0 --timeout=300s
