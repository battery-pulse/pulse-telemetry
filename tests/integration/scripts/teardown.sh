#!/bin/bash

# Uninstall manifests
kubectl delete -f hive-metastore.yaml
kubectl delete -f minio.yaml

# Uninstall Stackable Operators
helm uninstall hive-operator
helm uninstall minio-operator
