#!/bin/bash


wait_for_pod_ready() {
    local app_label=$1
    local timeout=90  # 90 seconds timeout
    local interval=10  # Check every 10 seconds
    local start_time=$(date +%s)

    # Get the pod name based on the app label
    pod_name=$(kubectl get pods -l $app_label -o jsonpath='{.items[0].metadata.name}')
    if [ -z "$pod_name" ]; then
        echo "No pod found with label: $app_label"
        return 1
    fi

    echo "Waiting for pod '$pod_name' to be ready..."

    # Loop to check pod readiness
    while true; do
        # Check if the pod is ready
        ready_status=$(kubectl get pod "$pod_name" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
        
        if [ "$ready_status" == "True" ]; then
            echo "Pod '$pod_name' is ready."
            return 0
        fi

        # Check if the timeout has been reached
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))

        if [ "$elapsed_time" -ge "$timeout" ]; then
            echo "Timeout reached. Pod '$pod_name' is not ready."
            return 1
        fi

        echo "Pod '$pod_name' is not ready yet. Checking again in $interval seconds..."
        sleep $interval
    done
}

# Deploy stackable operators
helm repo add stackable https://repo.stackable.tech/repository/helm-stable/
helm repo add minio https://operator.min.io/
helm repo update
echo "Deploying stackable operators..."
helm install secret-operator stackable/secret-operator --version 24.7.0
helm install commons-operator stackable/commons-operator --version 24.7.0
helm install hive-operator stackable/hive-operator --version 24.7.0
helm install spark-k8s-operator stackable/spark-k8s-operator --version 24.7.0


# Deploy minio service
echo "Deploying minio..."
helm install minio \
--version 4.0.2 \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set buckets[0].name=lakehouse \
--set buckets[0].policy=public \
--set rootUser=admin \
--set rootPassword=adminadmin \
--set resources.requests.memory=1Gi \
--set service.type=NodePort,service.nodePort=null \
--set consoleService.type=NodePort,consoleService.nodePort=null \
--repo https://charts.min.io/ minio
echo "Waiting for minio pod..."
sleep 2
wait_for_pod_ready "app=minio"

# Deploy s3 connection
echo "Deploying s3 connection..."
kubectl apply -f s3-connection.yaml

# Deploy hive service
echo "Deploying hive..."
kubectl apply -f hive-metastore.yaml
echo "Waiting for hive pod..."
sleep 30
wait_for_pod_ready "app.kubernetes.io/name=hive"
