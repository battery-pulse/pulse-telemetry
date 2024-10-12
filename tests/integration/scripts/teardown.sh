#!/bin/bash

# Uninstall Stackable Hive Operator
helm uninstall hive-operator --namespace stackable

# Uninstall Stackable MinIO Operator
helm uninstall minio-operator --namespace stackable

# Optionally delete the namespace to clean up resources
kubectl delete namespace stackable

# Optionally delete the specific MinIO and Hive CRDs
kubectl delete minio minio
kubectl delete hivemetastore hive-metastore

echo "Stackable operators, services, and custom resources have been cleaned up."
