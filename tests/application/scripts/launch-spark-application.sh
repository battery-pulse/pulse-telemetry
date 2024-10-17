#!/bin/bash

# Function to display usage instructions
usage() {
  echo "Usage: $0 -a <spark-app-name> -f <manifest-file> -t <timeout-seconds>"
  exit 1
}

# Parse command-line arguments
while getopts ":a:f:t:" opt; do
  case ${opt} in
    a ) SPARK_APP_NAME=$OPTARG ;;   # SparkApplication name (job-name)
    f ) MANIFEST_FILE=$OPTARG ;;    # Path to SparkApplication manifest file
    t ) TIMEOUT=$OPTARG ;;          # Timeout in seconds
    \? ) usage ;;                   # Invalid option
  esac
done

# Ensure all required parameters are provided
if [ -z "$SPARK_APP_NAME" ] || [ -z "$MANIFEST_FILE" ] || [ -z "$TIMEOUT" ]; then
  usage
fi

# Apply the SparkApplication manifest to start the job
echo "Launching SparkApplication $SPARK_APP_NAME using manifest $MANIFEST_FILE..."
kubectl apply -f "$MANIFEST_FILE"

# Monitor the driver pod status using kubectl wait
echo "Monitoring SparkApplication status..."

# Wait for the driver pod to succeed or fail with the given timeout
sleep 3
if kubectl wait pods -l "job-name=$SPARK_APP_NAME" \
  --for=jsonpath='{.status.phase}'=Succeeded --timeout="${TIMEOUT}s"; then
  echo "SparkApplication completed successfully."
  echo "Removing spark custom resource..."
  kubectl delete -f "$MANIFEST_FILE"
  exit 0
else
  echo "SparkApplication failed or timed out."
  echo "Removing spark custom resource..."
  kubectl delete -f "$MANIFEST_FILE"
  exit 1
fi
